"""Telegram HTTP transport — API calls, send + retry, markdown chunking.

Owns the request/response path: JSON payload build, 429/5xx retry with
``Retry-After`` honoring, markdown chunking, and ``send_response`` (HTML with
plain-text fallback on 400). Tag-aware HTML chunker lives in
``landline.telegram.chunker``.

- ``_send_with_retry`` persist-first at entry, unlink on success, rename to
  ``pending`` on terminal failure; a background thread + startup pass replay
  pending files (see ``landline.telegram.spool``).
- ``sendMessage`` opens a fresh connection each call — deliberately no
  keep-alive pool. Telegram has no idempotency keys, so a stale-keep-alive
  dupe on a real message would double-send. (Typing indicators are pooled
  in ``landline.telegram.typing`` because a duplicate typing is invisible.)
"""

import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

from landline.telegram import spool as outbound_spool
from landline.config import (
    POLL_TIMEOUT,
    SEND_MAX_ATTEMPTS,
    SEND_RETRY_AFTER_CAP,
    SEND_RETRY_AFTER_FALLBACK,
    SEND_RETRY_BACKOFF_SECONDS,
)
from landline.telegram.chunker import chunk_text
from landline.runtime.logging import log
from landline.telegram.fmt import md_to_telegram_html


def telegram_api(token: str, method: str, payload: Optional[Dict] = None,
                 timeout: Optional[int] = None) -> Dict:
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = json.dumps(payload).encode() if payload else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers)
    t = timeout if timeout is not None else (POLL_TIMEOUT + 10)
    with urllib.request.urlopen(req, timeout=t) as resp:
        return json.loads(resp.read())


# Transient: short-backoff retry. 429 is separate — honors Retry-After.
_TRANSIENT_HTTP_STATUSES = (500, 502, 503, 504)


def _parse_retry_after(http_error: urllib.error.HTTPError) -> int:
    """Extract Retry-After delay from an HTTPError; always a positive int.

    Precedence (mirrors ``poller._poll_loop``):
      1. ``Retry-After`` HTTP response header (seconds form only).
      2. ``parameters.retry_after`` in the JSON error body.
      3. ``SEND_RETRY_AFTER_FALLBACK`` (3s) default.
    Caller clamps by ``SEND_RETRY_AFTER_CAP`` before sleeping.
    """
    # 1. HTTP header (RFC 9110 seconds form; date form intentionally unparsed).
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
        # Ledger: server-assigned message_id in the log separates daemon-side
        # loss from client-side staleness on any future "didn't render" report.
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
    """Send a chunk with bounded retries. Returns (ok, code, spool_id).

    Args:
        defer_failure_finalization: when True, terminal failure branches
            leave the spool file in ``inflight-<pid>``; caller MUST later
            call ``discard`` or ``mark_failed``. Used by ``send_response``
            to close the race with the background replayer when it swaps
            to a plain-text fallback.

    Returns:
        ``(success, last_http_code, spool_id)``. ``spool_id`` is None only
        if the initial persist raised (disk-full → best-effort send).

    - Retries HTTP 429 (honor ``Retry-After`` header→body→fallback, clamped
      to ``[1, SEND_RETRY_AFTER_CAP]``), HTTP 5xx, and connection/timeout
      errors (surfaced as ``code is None``) up to ``SEND_MAX_ATTEMPTS``.
    - Does NOT retry HTTP 400/other 4xx — caller decides (plain-text fallback).
    - Persist-first: chunk written to spool at ``inflight-<pid>`` before the
      first attempt; success unlinks, terminal failure renames to ``pending``
      for periodic + startup replay. Disk-full swallowed → un-persisted send.
    """
    spool_id: Optional[str] = None
    try:
        spool_id = outbound_spool.persist(chat_id, chunk, html_mode, label)
    except OSError as spool_err:
        # Disk full / dir gone — degrade to un-persisted send.
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
            # ``code is None`` = connection refused / DNS / timeout / other
            # non-HTTPError. Clamp backoff index to the tuple's tail.
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

        # Non-retryable (400/401/403/...) → return so caller can fall back to
        # plain text. Left as ``pending`` for one more replay attempt.
        if spool_id is not None and not defer_failure_finalization:
            outbound_spool.mark_failed(spool_id)
        return False, code, spool_id

    if spool_id is not None and not defer_failure_finalization:
        outbound_spool.mark_failed(spool_id)
    return False, last_code, spool_id


def _send_chunk_raw(chat_id: str, chunk: str, html_mode: bool,
                    label: str) -> Tuple[bool, Optional[int]]:
    """Bare single-attempt send for spool replay.

    - No re-persist (would loop) and no retry (replay loop IS the retry vehicle).
    - Token fetched per-call from Keychain so the spool module stays credential-free.
    - Returns ``(False, None)`` if the token is unavailable → caller rolls
      the file back to ``pending``.
    """
    from landline.runtime.security import keychain_get  # lazy import — avoid cycle
    token = keychain_get("telegram-bot-token") or ""
    if not token:
        log("outbound_spool replay: no bot token available — skipping")
        return (False, None)
    ok, code, _retry_after = _send_chunk(token, chat_id, chunk, html_mode)
    return ok, code


def send_response(token: str, chat_id: str, text: str) -> None:
    """Send markdown text to Telegram as HTML; fall back to plain text on 400.

    - Chunk on RAW markdown, then convert each chunk to HTML independently —
      chunking already-rendered HTML would split ``<pre>...</pre>`` mid-tag.
    - If a chunk and its plain-text fallback BOTH fail, remaining chunks are
      abandoned; log ``(index, total)`` so the truncation is greppable.
    """
    if not text or not text.strip():
        return

    # Sub-4096 markdown budget so HTML expansion stays under Telegram's cap.
    plain_chunks = chunk_text(text, 4000)
    total = len(plain_chunks)

    for index, plain_chunk in enumerate(plain_chunks, start=1):
        html_chunk = md_to_telegram_html(plain_chunk)
        # ``defer_failure_finalization=True``: keep the spool file inflight
        # so the background replayer can't see it before ``discard`` below.
        # Every non-success branch MUST reach ``discard`` before falling through.
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

        # Discard the HTML variant's inflight spool BEFORE persisting the
        # plain-text fallback — otherwise the replayer double-delivers.
        if html_spool_id is not None:
            outbound_spool.discard(html_spool_id)

        # Plain-text fallback reuses the SAME markdown chunk (aligned boundaries).
        fallback_ok, fallback_code = _send_with_retry(
            token, chat_id, plain_chunk, html_mode=False, label="plain fallback",
        )
        if fallback_ok:
            continue

        # Both HTML and plain failed — log truncation so partial delivery is observable.
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
