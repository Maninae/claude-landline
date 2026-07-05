"""Per-chat long-lived StreamSender registry.

See ARCHITECTURE.md "StreamSender unifies text + status on one ordered queue"
for the full narrative. Key invariants:

- One `StreamSender` (+ worker thread) per chat, lives for the daemon's life —
  never created/torn down per turn.
- End-of-turn calls `flush()` (a cheap boundary), NEVER `close()` — dispatch
  never blocks on a drain, and ordering across turns is preserved.
- `close()` runs only at shutdown, so an in-flight send can't leak onto a
  fresh sender (there is no fresh sender mid-life).
"""

import threading
from typing import Callable, Dict, Optional

from landline.claude.sender import StreamSender, _SHUTDOWN_DRAIN_TIMEOUT


_senders: Dict[str, StreamSender] = {}
_senders_lock = threading.Lock()


def _log(*args, **kwargs) -> None:
    """Resolve `log` through the `landline.claude` facade so tests patching
    `landline.claude.log` still intercept. Late binding avoids a load-order
    coupling to the facade re-export."""
    from landline import claude as _claude_mod
    return _claude_mod.log(*args, **kwargs)


def _get_or_create_sender(
    chat_id: str,
    token: str,
    text_send_fn: Callable[[str, str, str], None],
    status_send_fn: Callable[[str, str, str], None],
) -> StreamSender:
    """Return the live long-lived sender for `chat_id`, creating on miss.

    Args:
        chat_id: chat this sender serves.
        token: Telegram bot token.
        text_send_fn: HTML text transport.
        status_send_fn: HTML status-line transport.
    Returns:
        The live `StreamSender` for `chat_id`.

    - Creates on: no entry, previous sender closed (post-shutdown), or worker
      thread died. A dead worker would silently swallow every future bubble.
    - Logs drop count when replacing a dead sender so the defect isn't masked.
    """
    with _senders_lock:
        sender = _senders.get(chat_id)
        if sender is None or sender.is_closed or not sender.worker_alive:
            if sender is not None and not sender.is_closed:
                # Worker died — log before replace so the defect isn't masked.
                dropped = sender.q.qsize()
                if dropped > 0:
                    _log(
                        "StreamSender worker for chat %s died — recreating "
                        "(dropping %d queued entries from the dead worker)"
                        % (chat_id, dropped)
                    )
                else:
                    _log("StreamSender worker for chat %s died — recreating" % chat_id)
            sender = StreamSender(token, chat_id, text_send_fn, status_send_fn)
            _senders[chat_id] = sender
        return sender


def _close_all_senders(timeout: float = _SHUTDOWN_DRAIN_TIMEOUT) -> None:
    """Drain and stop every registered sender. Called once at daemon shutdown.
    Clears the registry so a late (post-SIGTERM) dispatch creates a fresh
    sender rather than reusing a closed one."""
    with _senders_lock:
        senders = list(_senders.values())
        _senders.clear()
    for sender in senders:
        try:
            sender.close(timeout=timeout)
        except Exception as e:
            _log(f"_close_all_senders: error closing sender: {e}")


def try_enqueue_chat_notice(
    chat_id: str,
    *,
    html: Optional[str] = None,
    text: Optional[str] = None,
) -> bool:
    """Enqueue a daemon-generated notice onto the chat's ordered sender queue.

    Args:
        chat_id: destination chat.
        html: pre-built HTML body (exclusive with `text`).
        text: plain text body (exclusive with `html`).
    Returns:
        True if enqueued; False if there is no live sender yet (before the
        first turn) — the caller must then fall back to its own direct send.

    - Lands AFTER any bubbles still draining from the turn (preserves ordering).
    - Fallback at the call site preserves each caller's DI transport.
    """
    if (html is None) == (text is None):
        raise ValueError("try_enqueue_chat_notice requires exactly one of html/text")
    with _senders_lock:
        sender = _senders.get(chat_id)
    if sender is None or sender.is_closed or not sender.worker_alive:
        return False
    if html is not None:
        sender.status(html)
    else:
        sender.text(text)
    return True


def try_enqueue_or_send(
    chat_id: str,
    *,
    html: Optional[str] = None,
    text: Optional[str] = None,
    direct_fn: Callable[[str], None],
) -> None:
    """Route a notice through the chat's queue if a live sender exists,
    else fall back to `direct_fn(body)`.

    - `direct_fn` is a 1-arg body callable; callers bind their own transport
      (e.g. `functools.partial(send_html, token, chat_id)`) so this module
      stays independent of `landline.telegram`.
    - Exactly one of `html`/`text` (delegated validation).
    """
    if try_enqueue_chat_notice(chat_id, html=html, text=text):
        return
    body = html if html is not None else text
    # body non-None here: try_enqueue_chat_notice raises on both/neither.
    direct_fn(body)
