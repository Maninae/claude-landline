"""Telegram HTTP transport — API calls, send + retry, markdown chunking.

This module owns the request/response path: building the JSON payload, the
429/5xx retry policy (with `Retry-After` honoring), chunking markdown into
size-safe pieces, and the `send_response` entry point that converts each
chunk to HTML and falls back to plain text on a 400. The tag-aware HTML
chunker lives separately in `landline.html_chunker`.

Cluster 5:
- ``_send_with_retry`` persists every chunk to the outbound spool at entry
  and marks it success/failed on the terminal branch; a background thread
  and a startup pass replay pending files (see ``landline.outbound_spool``).
- ``send_typing`` uses a per-thread pooled HTTPS connection for
  ``sendChatAction`` so the typing indicator doesn't pay a TLS handshake
  every 4 seconds. The pool is deliberately NOT used for ``sendMessage`` —
  Telegram has no idempotency keys, and a stale-keep-alive dupe on a real
  message is worse than the saved handshake. Duplicate ``typing`` is
  invisible to the user, so the pool is safe there.
"""

import http.client
import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from landline.telegram import spool as outbound_spool
from landline.config import (
    POLL_TIMEOUT,
    SEND_MAX_ATTEMPTS,
    SEND_RETRY_AFTER_CAP,
    SEND_RETRY_AFTER_FALLBACK,
    SEND_RETRY_BACKOFF_SECONDS,
)
from landline.telegram.chunker import _utf16_len
from landline.runtime.logging import log
from landline.telegram.fmt import md_to_telegram_html


# -----------------------------------------------------------------------------
# Telegram API
# -----------------------------------------------------------------------------

def telegram_api(token: str, method: str, payload: Optional[Dict] = None,
                 timeout: Optional[int] = None) -> Dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = json.dumps(payload).encode() if payload else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers)
    t = timeout if timeout is not None else (POLL_TIMEOUT + 10)
    with urllib.request.urlopen(req, timeout=t) as resp:
        return json.loads(resp.read())


# -----------------------------------------------------------------------------
# Markdown chunking
# -----------------------------------------------------------------------------

def chunk_text(text: str, limit: int = 4096) -> List[str]:
    if _utf16_len(text) <= limit:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        if _utf16_len(remaining) <= limit:
            chunks.append(remaining)
            break
        # Find the largest code-point prefix whose UTF-16 length fits `limit`.
        # Binary-search the code-point index so we never start the search past
        # the real budget (emoji-heavy strings have UTF-16 len > code-point len).
        lo, hi = 0, len(remaining)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if _utf16_len(remaining[:mid]) <= limit:
                lo = mid
            else:
                hi = mid - 1
        window_end = lo  # largest code-point count that fits
        window = remaining[:window_end]
        # Quarter threshold expressed in UTF-16 units (matching the budget).
        quarter = limit // 4
        cut = window.rfind("\n\n")
        sep_len = 2
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window.rfind("\n")
            sep_len = 1
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window.rfind(" ")
            sep_len = 1
        if cut < 0 or _utf16_len(window[:cut]) <= quarter:
            cut = window_end
            sep_len = 0
        chunks.append(remaining[:cut])
        remaining = remaining[cut + sep_len:]
    return [c for c in chunks if c]


# -----------------------------------------------------------------------------
# Send
# -----------------------------------------------------------------------------

# HTTP statuses we treat as transient — retried with a short backoff. 429
# (rate limit) is handled separately because we honor the server-advertised
# Retry-After delay instead of our own backoff schedule.
_TRANSIENT_HTTP_STATUSES = (500, 502, 503, 504)


def _parse_retry_after(http_error: urllib.error.HTTPError) -> int:
    """Best-effort extraction of the Retry-After delay from a 429 (or any)
    HTTPError. Mirrors `poller._poll_loop`'s precedence:

      1. `Retry-After` HTTP response header (Telegram and the HTTP spec both
         allow this; the existing code only read the JSON body).
      2. `parameters.retry_after` in the JSON error body (Telegram's primary
         signal for rate limits — `{"parameters": {"retry_after": 35}}`).
      3. `SEND_RETRY_AFTER_FALLBACK` (3s) as a sensible default when neither
         is present or parseable.

    Always returns a positive int — callers downstream still clamp by
    `SEND_RETRY_AFTER_CAP` before sleeping.
    """
    # 1. HTTP header (RFC 9110 — seconds form; date form is uncommon for
    #    Telegram and we deliberately don't parse it here, matching
    #    `poller._poll_loop`).
    try:
        headers = getattr(http_error, "headers", None)
        if headers is not None:
            header_value = headers.get("Retry-After")
            if header_value is not None:
                parsed = float(header_value)
                if parsed > 0:
                    return int(parsed) if parsed >= 1 else 1
    except (AttributeError, TypeError, ValueError):
        pass

    # 2. JSON body (`parameters.retry_after`).
    try:
        body_bytes = http_error.read()
        if body_bytes:
            body = json.loads(body_bytes.decode("utf-8", errors="replace"))
            ra = body.get("parameters", {}).get("retry_after")
            if isinstance(ra, (int, float)) and ra > 0:
                return int(ra) if ra >= 1 else 1
    except (AttributeError, ValueError, TypeError, OSError,
            json.JSONDecodeError):
        pass

    # 3. Fallback.
    return SEND_RETRY_AFTER_FALLBACK


def _send_chunk(token: str, chat_id: str, chunk: str,
                html_mode: bool) -> Tuple[bool, Optional[int], int]:
    """Send a single chunk. Returns (success, http_code, retry_after_seconds).

    On 429, the third value is the parsed Retry-After delay (header → body →
    fallback). On other failures it's 0 and the caller decides whether to
    retry based on the http_code and exception type.

    Connection/timeout errors return (False, None, 0).
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": chunk,
        "disable_web_page_preview": True,
    }
    if html_mode:
        payload["parse_mode"] = "HTML"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"},
    )
    try:
        body = urllib.request.urlopen(req, timeout=10).read()
        # Ledger line: the server-assigned message_id proves the message
        # exists server-side, so a future "didn't render" report can be
        # split into daemon-side loss vs client-side staleness from the log
        # alone (see inbox/telegram-desync-rootcause-brief.md, H3).
        try:
            message_id = json.loads(body).get("result", {}).get("message_id")
        except Exception:
            message_id = None
        log(f"Telegram sent chat_id={chat_id} message_id={message_id}")
        return (True, None, 0)
    except urllib.error.HTTPError as e:
        retry_after = 0
        if e.code == 429:
            retry_after = _parse_retry_after(e)
        return (False, e.code, retry_after)
    except Exception as e:
        log(
            f"Telegram send error: chat_id={chat_id} "
            f"exc={type(e).__name__}: {e}"
        )
        return (False, None, 0)


def _send_with_retry(token: str, chat_id: str, chunk: str,
                     html_mode: bool, label: str) -> Tuple[bool, Optional[int]]:
    """Public 2-tuple wrapper for backwards compatibility.

    See ``_send_with_retry_tracked`` for the docstring; this variant drops
    the trailing spool_id so external callers (``html_chunker.send_html``,
    tests) keep their 2-tuple unpack contract.
    """
    ok, code, _spool_id = _send_with_retry_tracked(
        token, chat_id, chunk, html_mode=html_mode, label=label,
    )
    return ok, code


def _send_with_retry_tracked(
    token: str, chat_id: str, chunk: str,
    html_mode: bool, label: str,
    defer_failure_finalization: bool = False,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """Send a chunk with bounded retries for transient failures.

    Retries on:
      - HTTP 429 (rate limited) — honor `Retry-After` (header → body →
        SEND_RETRY_AFTER_FALLBACK), clamped to [1, SEND_RETRY_AFTER_CAP].
      - HTTP 5xx (502/503/504/500) — short fixed backoff per retry index.
      - Connection/timeout errors (`URLError`, socket timeout) surfaced
        as `code == None` — same fixed backoff.

    Does NOT retry on:
      - HTTP 400 — caller falls back to plain text.
      - Other 4xx — caller decides; usually plain-text fallback.

    Up to `SEND_MAX_ATTEMPTS` total attempts (initial + retries). Returns
    ``(success, http_code_from_last_attempt, spool_id)`` — spool_id is the
    identifier assigned by ``outbound_spool.persist`` at entry, or ``None``
    if the initial persist raised (disk-full → degraded to best-effort).
    Callers that need to cancel the persisted variant (e.g. ``send_response``
    switching to the plain-text fallback) use the spool_id with
    ``outbound_spool.discard``.

    Cluster 5: every entry persists the chunk to the outbound spool at
    ``inflight-<pid>`` state before the first attempt; a successful send
    unlinks it; any terminal failure branch (retry-exhaustion, unfixable
    4xx) renames it back to ``pending`` so the periodic replay pass — and
    the sync replay at next daemon startup — picks it up. Disk-full or
    other I/O errors on persist are swallowed and the send proceeds
    without persistence (best-effort > fail-closed).

    ``defer_failure_finalization`` (default False): when True, ANY terminal
    failure branch leaves the spool file in its ``inflight-<pid>`` state
    instead of renaming it to ``pending``. Caller assumes ownership of the
    spool_id and MUST subsequently call either ``outbound_spool.discard``
    (drop the chunk — used by ``send_response`` before persisting a
    plain-text fallback) or ``outbound_spool.mark_failed`` (release to
    ``pending`` for the replayer). Purpose: eliminate the race between
    ``mark_failed`` (inflight→pending) and a caller's subsequent
    ``discard`` — a background ``OutboundSpoolReplayer`` tick between
    those two operations otherwise reads the pending payload and can
    deliver the same chunk while ``discard`` unlinks it, causing
    double-delivery (HTML variant AND plain-text fallback both reach the
    user). Keeping the file in ``inflight-<pid>`` state until the caller
    decides makes the file invisible to the replayer during the window.
    """
    spool_id: Optional[str] = None
    try:
        spool_id = outbound_spool.persist(chat_id, chunk, html_mode, label)
    except OSError as spool_err:
        # Disk full, mode failure, dir gone — degrade to un-persisted send.
        # A duplicate write of this same chunk after a crash is impossible
        # anyway if we couldn't persist it, so at-least-once → best-effort.
        log(
            "outbound_spool: persist failed for chat_id=%s label=%s: %r "
            "— proceeding without persistence"
            % (chat_id, label, spool_err)
        )
        spool_id = None
    except Exception as spool_err:  # pragma: no cover — defensive
        log(
            "outbound_spool: unexpected persist error for chat_id=%s: %r"
            % (chat_id, spool_err)
        )
        spool_id = None

    last_code: Optional[int] = None
    for attempt in range(SEND_MAX_ATTEMPTS):
        ok, code, retry_after = _send_chunk(
            token, chat_id, chunk, html_mode=html_mode,
        )
        last_code = code
        if ok:
            if spool_id is not None:
                outbound_spool.mark_success(spool_id)
            return True, code, spool_id

        # Decide whether this failure is retryable.
        is_last_attempt = attempt >= SEND_MAX_ATTEMPTS - 1
        if is_last_attempt:
            if spool_id is not None and not defer_failure_finalization:
                outbound_spool.mark_failed(spool_id)
            return False, code, spool_id

        if code == 429:
            delay = min(max(retry_after, 1), SEND_RETRY_AFTER_CAP)
            log(
                f"Telegram 429 on {label} for chat_id={chat_id} "
                f"(attempt {attempt + 1}/{SEND_MAX_ATTEMPTS}), "
                f"sleeping {delay}s and retrying"
            )
            time.sleep(delay)
            continue

        if code in _TRANSIENT_HTTP_STATUSES or code is None:
            # `code is None` covers connection refused, DNS failure, socket
            # timeout, and any non-HTTPError exception bubbled to `_send_chunk`.
            # Use the positional backoff entry; fall back to the last value if
            # SEND_MAX_ATTEMPTS exceeds the tuple length.
            backoff_idx = min(attempt, len(SEND_RETRY_BACKOFF_SECONDS) - 1)
            delay = SEND_RETRY_BACKOFF_SECONDS[backoff_idx]
            code_label = "HTTP %d" % code if code is not None else "network"
            log(
                f"Telegram {code_label} on {label} for chat_id={chat_id} "
                f"(attempt {attempt + 1}/{SEND_MAX_ATTEMPTS}), "
                f"sleeping {delay}s and retrying"
            )
            time.sleep(delay)
            continue

        # Non-retryable status (400, 401, 403, etc.) — return immediately so
        # caller can fall back to plain text. Leave the spool file in
        # ``pending`` state; the replay pass will attempt it once more and
        # discard on a repeated 400 (see outbound_spool.replay_all).
        if spool_id is not None and not defer_failure_finalization:
            outbound_spool.mark_failed(spool_id)
        return False, code, spool_id

    if spool_id is not None and not defer_failure_finalization:
        outbound_spool.mark_failed(spool_id)
    return False, last_code, spool_id


def _send_chunk_raw(chat_id: str, chunk: str, html_mode: bool,
                    label: str) -> Tuple[bool, Optional[int]]:
    """Bare single-attempt send for spool replay.

    Does NOT re-persist to the spool (that would loop) and does NOT retry
    (the replay loop is the retry vehicle). Fetches the bot token from
    Keychain on each call — cheap given the 60s+ replay interval, and it
    keeps the spool module credential-free.

    Returns (False, None) if the token can't be fetched, so the caller
    (``outbound_spool.replay_all``) rolls the file back to ``pending``.
    """
    from landline.runtime.security import keychain_get  # lazy import — avoid cycle
    token = keychain_get("telegram-bot-token") or ""
    if not token:
        log("outbound_spool replay: no bot token available — skipping")
        return (False, None)
    ok, code, _retry_after = _send_chunk(token, chat_id, chunk, html_mode)
    return ok, code


def send_response(token: str, chat_id: str, text: str) -> None:
    """Send text to Telegram with HTML formatting. Handles 429 and 400 fallback.

    Chunking happens on the raw markdown BEFORE HTML conversion. Each chunk is
    then converted to HTML independently, guaranteeing well-formed standalone
    HTML per chunk — `chunk_text` operating on already-rendered HTML would
    happily split `<pre>...</pre>` blocks mid-tag and trigger Telegram 400s.

    Multi-chunk observability: when a chunk fails after all retries AND the
    plain-text fallback also fails, the remaining chunks would silently never
    send — the user sees a half-message with no signal in the logs. We log a
    clear truncation notice with `(index, total)` so this is greppable.
    """
    if not text or not text.strip():
        return

    # Use a slightly smaller markdown budget so HTML expansion (entity escapes,
    # added tags) is very unlikely to push a chunk past Telegram's 4096 cap.
    plain_chunks = chunk_text(text, 4000)
    total = len(plain_chunks)

    for index, plain_chunk in enumerate(plain_chunks, start=1):
        html_chunk = md_to_telegram_html(plain_chunk)
        # Pass ``defer_failure_finalization=True`` so a retry-exhausted or
        # non-retryable HTML failure leaves the spool file in its
        # ``inflight-<pid>`` state (invisible to the background replayer)
        # instead of renaming it to ``pending`` right away. This closes
        # the race window where a replayer tick between ``mark_failed``
        # and ``discard`` below would read the pending payload and
        # double-deliver the HTML variant alongside the plain-text
        # fallback. ``send_response`` owns finalization for this file —
        # every non-success branch below MUST call ``discard`` before
        # falling through.
        ok, code, html_spool_id = _send_with_retry_tracked(
            token, chat_id, html_chunk, html_mode=True, label="HTML chunk",
            defer_failure_finalization=True,
        )

        if ok:
            continue

        if code == 400:
            log(
                f"Telegram 400 on HTML chunk for chat_id={chat_id}, "
                f"falling back to plain text"
            )
        elif code is not None:
            log(
                f"Telegram HTTP {code} on HTML send for chat_id={chat_id}, "
                f"falling back to plain text"
            )
        else:
            log(
                f"Telegram network error on HTML send for chat_id={chat_id}, "
                f"falling back to plain text"
            )

        # Discard the HTML variant's spool file BEFORE persisting the
        # plain-text fallback. The file is still in ``inflight-<pid>``
        # state (see ``defer_failure_finalization=True`` above), so the
        # background replayer can never see it — discard atomically
        # removes it without any race. Same logical chunk, two variants:
        # without this, the HTML variant would be retried by the replayer
        # AND the plain-text fallback below would also deliver — the operator
        # gets the same reply twice.
        if html_spool_id is not None:
            outbound_spool.discard(html_spool_id)

        # Plain-text fallback uses the SAME markdown chunk — boundaries align
        # exactly, so no duplication or loss.
        fallback_ok, fallback_code = _send_with_retry(
            token, chat_id, plain_chunk, html_mode=False, label="plain fallback",
        )
        if fallback_ok:
            continue

        # Both HTML and plain fallback failed. If this is part of a multi-chunk
        # message, the user is about to see a silently truncated reply — log
        # loudly so the partial delivery is observable in the daemon log.
        if total > 1:
            log(
                f"Telegram send aborted mid-stream for chat_id={chat_id}: "
                f"chunk {index}/{total} failed after retries "
                f"(last code={fallback_code}); "
                f"chunks {index}..{total} will NOT be delivered"
            )
        else:
            log(
                f"Telegram send failed for chat_id={chat_id}: "
                f"single chunk failed after retries (last code={fallback_code})"
            )
        return


# -----------------------------------------------------------------------------
# Scoped HTTPS keep-alive for the typing indicator ONLY.
#
# Rationale: Telegram has no idempotency keys. On a stale-keep-alive
# connection, we can't distinguish "request never reached the server" from
# "request was processed but response was lost". Retrying a lost response on
# a fresh connection would then double-send the message. That risk is
# unacceptable for real sends but harmless for ``sendChatAction`` (typing) —
# a duplicate typing indicator is invisible.
#
# The connection is per-thread via ``threading.local`` — the poller thread,
# each per-chat StreamSender worker, and the streaming module's
# ``typing_loop`` each maintain their own connection. Never shared.
# -----------------------------------------------------------------------------

_TELEGRAM_API_HOST = "api.telegram.org"
_TYPING_POOL_TIMEOUT = 10

_typing_conn_local = threading.local()


def _get_typing_conn() -> http.client.HTTPSConnection:
    conn = getattr(_typing_conn_local, "conn", None)
    if conn is None:
        conn = http.client.HTTPSConnection(
            _TELEGRAM_API_HOST, timeout=_TYPING_POOL_TIMEOUT,
        )
        _typing_conn_local.conn = conn
    return conn


def _reset_typing_conn() -> None:
    """Close and drop the current thread's pooled typing connection.

    Called after any exception on the pooled path so the next call starts
    on a fresh connection.
    """
    conn = getattr(_typing_conn_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
    _typing_conn_local.conn = None


def send_typing(token: str, chat_id: str) -> None:
    """Send a ``sendChatAction: typing`` request.

    Fast path: per-thread pooled HTTPS connection (no fresh TLS handshake
    every 4 seconds). If the pool call raises OR the server returns a 5xx,
    we drop the pooled connection and fall back to the one-shot
    ``telegram_api`` path so semantics match the pre-cluster behaviour
    exactly (best-effort, silently swallowed on failure).
    """
    body = json.dumps({"chat_id": chat_id, "action": "typing"}).encode()
    headers = {"Content-Type": "application/json"}
    try:
        conn = _get_typing_conn()
        conn.request(
            "POST", f"/bot{token}/sendChatAction",
            body=body, headers=headers,
        )
        resp = conn.getresponse()
        # Drain per HTTP/1.1 keep-alive rules — leaving unread body bytes
        # on the socket poisons the next request on the same connection.
        resp.read()
        if resp.status >= 500:
            raise RuntimeError("typing 5xx %d" % resp.status)
        return
    except Exception:
        _reset_typing_conn()

    # Fallback: one-shot, exactly matching the pre-cluster path (silent on
    # failure). Duplicate typing indicators are invisible, so retrying on a
    # fresh connection is safe here.
    try:
        telegram_api(token, "sendChatAction", {
            "chat_id": chat_id,
            "action": "typing",
        }, timeout=10)
    except Exception:
        pass
