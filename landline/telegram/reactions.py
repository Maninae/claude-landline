"""Ordered Telegram ``setMessageReaction`` client — 👀 receipt / 👌 done.

Strictly cosmetic UX polish — MUST NEVER delay or fail message processing.

- Every entrypoint enqueues to one background worker and returns immediately.
- The worker swallows exceptions after logging metadata.
- Kill switch: ``config.REACTION_ACKS_ENABLED`` gates enqueue AND startup.

Load-bearing ordering: reactions on the same ``(chat_id, message_id)`` MUST
arrive at Telegram in enqueue order. Per-call HTTP threads (the old design)
raced on DNS/TLS/RTT — a CLEAR could beat its own SET, leaving 👀 stuck
forever. One FIFO worker + single-threaded dispatch guarantees SET-before-CLEAR.

Wire (Bot API 7.0 ``setMessageReaction``):

    POST /bot<token>/setMessageReaction
    {"chat_id": …, "message_id": …,
     "reaction": [{"type": "emoji", "emoji": "…"}]}   # [] clears.

Emoji live in ``config.REACTION_ACK_EMOJI`` / ``REACTION_DONE_EMOJI``.
If Telegram removes one from the allow-list, flip ``REACTION_ACKS_ENABLED``
to False and pick another; no caller changes.

PII: ``chat_id`` / ``message_id`` are safe to log. No user-controlled
content flows through; no spool integration (a lost reaction is invisible).
"""

import json
import queue
import threading
import urllib.error
import urllib.request
from typing import Iterable, Optional, Tuple

from landline import config
from landline.runtime.logging import log


_REACTION_ENDPOINT = "https://api.telegram.org/bot{token}/setMessageReaction"

# One FIFO queue + one long-lived worker: load-bearing SET-before-CLEAR
# serialization. Enqueue is well under a microsecond so fire-and-forget
# still holds. Queue is unbounded (worst-case backlog ~60 items on a
# 30-update batch; dropping would reintroduce the race).
_reaction_queue = queue.Queue()  # type: queue.Queue
_worker_lock = threading.Lock()
_worker_thread = None  # type: Optional[threading.Thread]


def _ensure_worker_started() -> None:
    """Lazy-start the reactions worker; idempotent, self-heals on death.

    Mirrors the StreamSender registry pattern in ``landline.claude.registry`` —
    a permanently-dead worker would silently swallow all future reactions.
    """
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
    """Drain the reactions queue forever. One HTTP POST per item.

    Exceptions are swallowed — a broken reaction path must NEVER kill the
    worker (which would silently drop CLEARs and strand 👀).
    """
    while True:
        try:
            item = _reaction_queue.get()
        except Exception:
            # Defense-in-depth (Queue.get normally can't raise).
            continue
        try:
            token, chat_id, message_id, emoji = item
            _do_react(token, chat_id, message_id, emoji)
        except Exception:
            # Defense-in-depth; _do_react already swallows internally.
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
    """Fire-and-forget ``setMessageReaction``. Enqueues + returns immediately.

    Args:
        emoji: an allowed emoji, or ``None`` to clear (empty reaction array).

    - Caller MUST NEVER block on the reaction.
    - No-op when ``config.REACTION_ACKS_ENABLED`` is False.
    - Ordering guarantee: two calls in program order arrive at Telegram
      in the same order (FIFO queue + single worker).
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
    """Batched variant — one enqueue per message_id, order preserved.

    Batch cap of ``MAX_QUEUED_UPDATES`` (=30) bounds enqueue count per finalize.
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
    """Perform the POST with a bounded retry loop. NEVER raises.

    Runs on the reactions worker. Failure logs metadata only
    (chat_id + message_id + exception TYPE) and swallows.
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
                # ``status`` is 3.9+; fall back to ``getcode()`` for mocks
                # / older response shapes that don't expose it.
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
    # Metadata-only log — never the emoji or payload.
    log(
        "reaction failed chat_id=%s message_id=%s exc=%s"
        % (chat_id, message_id, last_error_type or "unknown")
    )


def _wait_for_queue_idle(timeout: float = 3.0) -> bool:
    """Test-only: wait until the worker has drained the queue.

    Kept here so tests across files can reuse it without cross-imports.
    Returns True if idle within ``timeout``, False otherwise.
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
