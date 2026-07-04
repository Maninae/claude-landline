"""Cluster 3 — Ordered Telegram ``setMessageReaction`` client.

Adds visual receipt (👀) and completion (👌) feedback per message. This
module is a strictly cosmetic UX polish path — it must NEVER delay or
fail message processing:

  * Every entrypoint enqueues to a single background worker and returns
    immediately.
  * The worker swallows all exceptions after logging metadata.
  * A single kill switch (``config.REACTION_ACKS_ENABLED``) at the top
    prevents both enqueue and worker startup.

**Ordering discipline (load-bearing)**: reactions on the same
``(chat_id, message_id)`` MUST be delivered to Telegram in enqueue
order. The earlier per-call-thread model spawned independent HTTP
threads whose ordering at the Telegram edge depended on DNS/TLS/RTT
jitter — a CLEAR could reach the API BEFORE its matching SET, leaving
👀 stuck forever with no matching 👌 (the exact "lie by docstring" the
design tries to prevent — batch-level lock-gate rejection, silent
unlock, photo-all-fail all triggered this in the wild). A single FIFO
worker queue removes the race: dispatch is single-threaded, so every
SET is enqueued strictly before its matching CLEAR, and the worker
processes them in that order — the two POSTs never race on independent
sockets.

The wire surface is Telegram Bot API 7.0's ``setMessageReaction``
(https://core.telegram.org/bots/api#setmessagereaction):

    POST /bot<token>/setMessageReaction
    {
      "chat_id": <int|str>,
      "message_id": <int>,
      "reaction": [{"type": "emoji", "emoji": "<one-of-allowed>"}]
    }

An empty ``reaction`` array clears the previous reaction. Passing a new
array replaces it (non-premium bots can only carry one reaction per
message; that's fine for our two-emoji flow).

Chosen emoji live in ``config.REACTION_ACK_EMOJI`` (👀) and
``config.REACTION_DONE_EMOJI`` (👌) — both members of the Bot API
allowed set as of 7.x. If Telegram ever removes one, flip
``REACTION_ACKS_ENABLED`` to False and pick another from the allow-list;
no code change to callers.

Security & PII discipline:

  * ``chat_id`` and ``message_id`` are metadata (semi-public per the
    workspace's PII policy) — safe to log. NEVER log the emoji if it
    could ever be user-controlled (it can't be here; ``config`` owns it)
    but keep the discipline in case a future caller passes a variable.
  * No user-controlled content flows through this module. No spool
    integration — a lost reaction is invisible.
"""

import json
import queue
import threading
import urllib.error
import urllib.request
from typing import Iterable, Optional, Tuple

from landline import config
from landline.logging import log


_REACTION_ENDPOINT = "https://api.telegram.org/bot{token}/setMessageReaction"

# One FIFO queue processed by one long-lived worker thread. Callers
# enqueue synchronously (queue.put is well under a microsecond) and
# return immediately — the fire-and-forget contract is preserved. The
# single-worker model is load-bearing: it's what serializes SET-then-
# CLEAR on the same ``(chat_id, message_id)`` at the Telegram edge.
#
# Unbounded queue: reactions are strictly cosmetic UX, and dispatch is
# single-threaded, so worst-case backlog is bounded by ``MAX_QUEUED_
# UPDATES`` (=30) messages * 2 reactions/message = ~60 items. Dropping
# under back-pressure would produce the very race the docstring warns
# about, so we take the unbounded queue instead.
_reaction_queue = queue.Queue()  # type: queue.Queue
_worker_lock = threading.Lock()
_worker_thread = None  # type: Optional[threading.Thread]


def _ensure_worker_started() -> None:
    """Lazy-start the single reactions worker. Idempotent; self-heals if
    the worker died (mirrors the StreamSender registry pattern in
    landline.sender_registry — a permanently-dead worker would be a silent
    black hole for UX polish, so recreate on next enqueue)."""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return
        _worker_thread = threading.Thread(
            target=_worker_loop,
            daemon=True,
            name="landline-react-worker",
        )
        _worker_thread.start()


def _worker_loop() -> None:
    """Drain the reactions queue forever. Each item is one HTTP POST.
    All exceptions are swallowed after ``_do_react`` logs metadata —
    a broken reaction path must NEVER kill the worker (which would
    silently drop all future reactions, including CLEARs that leave 👀
    stuck)."""
    while True:
        try:
            item = _reaction_queue.get()
        except Exception:
            # Queue.get() can't normally raise, but defense-in-depth
            # against a future stdlib change / a subclass that does.
            continue
        try:
            token, chat_id, message_id, emoji = item
            _do_react(token, chat_id, message_id, emoji)
        except Exception:
            # _do_react already swallows and logs metadata; this outer
            # catch is defense-in-depth against a future refactor that
            # lets it raise (e.g., an attribute error on a malformed
            # tuple).
            pass
        finally:
            try:
                _reaction_queue.task_done()
            except Exception:
                pass


def set_reaction_async(
    token: str,
    chat_id: str,
    message_id: int,
    emoji: Optional[str],
) -> None:
    """Fire-and-forget ``setMessageReaction``. Enqueues to the single
    reactions worker and returns immediately (the caller MUST NEVER
    block on the reaction).

    Passing ``emoji=None`` clears the reaction (sends an empty array).

    Respects ``config.REACTION_ACKS_ENABLED``: when False, no enqueue
    happens and no HTTP request is made.

    **Ordering guarantee**: two calls in program order arrive at
    Telegram in the same program order — the FIFO queue + single worker
    prevents the SET-vs-CLEAR race that plagued the earlier per-call-
    thread model.
    """
    if not config.REACTION_ACKS_ENABLED:
        return
    _ensure_worker_started()
    _reaction_queue.put((token, chat_id, message_id, emoji))


def set_reactions_batch_async(
    token: str,
    chat_id: str,
    message_ids: Iterable[int],
    emoji: Optional[str],
) -> None:
    """Batched variant — enqueues one item per message_id, preserving
    order across the iterable. A batch cap of ``MAX_QUEUED_UPDATES``
    (=30) messages caps the enqueue count per finalize.
    """
    if not config.REACTION_ACKS_ENABLED:
        return
    ids = list(message_ids)
    if not ids:
        return
    _ensure_worker_started()
    for mid in ids:
        _reaction_queue.put((token, chat_id, mid, emoji))


def _do_react(
    token: str,
    chat_id: str,
    message_id: int,
    emoji: Optional[str],
) -> None:
    """Perform the POST with a single retry. NEVER raises.

    Runs on the reactions worker thread. Any failure logs a metadata-
    only line (chat_id + message_id + exception TYPE, no emoji or
    payload) and swallows the exception.
    """
    if emoji is None:
        reaction_array = []
    else:
        reaction_array = [{"type": "emoji", "emoji": emoji}]
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "reaction": reaction_array,
    }
    body = json.dumps(payload).encode("utf-8")
    url = _REACTION_ENDPOINT.format(token=token)

    last_error_type = ""
    for attempt in range(config.REACTION_MAX_ATTEMPTS):
        try:
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(
                req, timeout=config.REACTION_HTTP_TIMEOUT_SECONDS,
            ) as resp:
                # HTTPResponse.status was added in 3.9; fall back to
                # getcode() for older shapes / mocks that don't expose it.
                status = getattr(resp, "status", None)
                if status is None:
                    try:
                        status = resp.getcode()
                    except Exception:
                        status = None
                if status is None or 200 <= int(status) < 300:
                    return
                last_error_type = "HTTP%s" % status
        except Exception as exc:
            last_error_type = type(exc).__name__
            # Fall through to retry loop.
    # All attempts failed. Log metadata only — never the emoji or
    # payload (defense-in-depth even though config owns the emoji).
    log(
        "reaction failed chat_id=%s message_id=%s exc=%s"
        % (chat_id, message_id, last_error_type or "unknown")
    )


def _wait_for_queue_idle(timeout: float = 3.0) -> bool:
    """Test-only helper: wait until the worker has drained the queue.

    Returns True if the queue idled within ``timeout``, False otherwise.
    Kept in the module (not the test file) so tests in multiple files
    can reuse it without cross-file imports.
    """
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if _reaction_queue.unfinished_tasks == 0:
                return True
        except AttributeError:
            return True
        time.sleep(0.01)
    return False
