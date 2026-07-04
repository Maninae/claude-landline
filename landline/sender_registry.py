"""Per-chat long-lived StreamSender registry.

One StreamSender (and its single worker thread) lives for the daemon's life
per chat, instead of being created and torn down every turn. This is what
guarantees ordering and kills the drain-stall/desync class:

  * One FIFO queue + one worker per chat => bubbles deliver in the exact
    order they were enqueued, ACROSS turns. Dispatch is single-threaded, so
    turn N fully enqueues (text/status + a FLUSH boundary) before turn N+1
    begins — no cross-turn handoff, no interleaving.
  * The dispatch thread NEVER blocks on a drain: end-of-turn calls flush()
    (a cheap boundary marker), not a blocking close(). The worker drains in
    the background at Telegram's rate; a slow turn just delays the next
    turn's bubbles, in order — it never freezes the daemon or drops them.
  * close() runs ONLY at shutdown, so an in-flight send can never leak past
    a turn boundary onto a fresh sender — there is no fresh sender mid-life.
"""

import threading
from typing import Callable, Dict, Optional

from landline.stream_sender import StreamSender, _SHUTDOWN_DRAIN_TIMEOUT


_senders: Dict[str, StreamSender] = {}
_senders_lock = threading.Lock()


def _log(*args, **kwargs) -> None:
    """Resolve `log` through `landline.claude` so tests patching
    `landline.claude.log` still intercept calls made from this module. The
    facade re-exports `log` from `landline.logging`; this late binding keeps
    runtime patches working without coupling load order."""
    from landline import claude as _claude_mod
    return _claude_mod.log(*args, **kwargs)


def _get_or_create_sender(
    chat_id: str,
    token: str,
    text_send_fn: Callable[[str, str, str], None],
    status_send_fn: Callable[[str, str, str], None],
) -> StreamSender:
    """Return the live long-lived sender for ``chat_id``, creating one if there
    is none, if the previous one was closed (post-shutdown), or if its worker
    thread has died (self-heal — a dead worker would silently swallow every
    future bubble)."""
    with _senders_lock:
        sender = _senders.get(chat_id)
        if sender is None or sender.is_closed or not sender.worker_alive:
            if sender is not None and not sender.is_closed:
                # Worker died on us — log before replacing so the defect that
                # killed it isn't masked by a silent swap. Capture queue depth
                # so the volume of dropped bubbles is observable, not silent.
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

    The registry is cleared so a late dispatch (shouldn't happen post-SIGTERM,
    but defensively) creates a fresh sender rather than reusing a closed one.
    """
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
    """Enqueue a daemon-generated notice (e.g. "(Paused.)", context warnings)
    onto the chat's ordered sender queue so it lands AFTER any bubbles still
    draining from the turn — preserving ordering.

    Returns True if it was enqueued, False if there is no live sender for the
    chat (e.g. before the first turn) — in which case the caller should fall
    back to its own direct send. Keeping the fallback at the call site
    preserves each caller's dependency-injected transport. Exactly one of
    ``html``/``text`` must be provided.
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
    """Route a daemon-generated notice through the chat's ordered sender queue
    when a live sender exists; otherwise fall back to ``direct_fn(body)``.

    ``direct_fn`` is a 1-arg callable taking the body string — callers bind
    their own transport (e.g. ``functools.partial(send_html, token, chat_id)``).
    Keeping the transport caller-supplied preserves dependency injection and
    keeps ``sender_registry`` independent of ``landline.client``.

    Exactly one of ``html``/``text`` must be provided (delegated validation).
    """
    if try_enqueue_chat_notice(chat_id, html=html, text=text):
        return
    body = html if html is not None else text
    # body is non-None here: try_enqueue_chat_notice would have raised
    # ValueError if both/neither were provided.
    direct_fn(body)
