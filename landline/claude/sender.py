"""Unified streaming sender for Claude's response pipeline.

- One worker thread per chat pulls (type, content) entries off a single
  ordered queue and fans them to text (markdown→HTML) or status (pre-built
  HTML) transport.
- Senders are long-lived (one per chat, daemon's life) — created and managed
  by ``sender_registry``. See docs/ARCHITECTURE.md § StreamSender.
"""

import queue
import threading
import time
from typing import Callable, List, Optional, Tuple

from landline.config import AGENT_NAME, STATUS_BUFFER_WINDOW, STREAM_BUFFER_WINDOW
from landline.runtime.logging import log
from landline.claude.tool_status import _format_repeated_status


# One ordered (tag, payload) queue preserves text-vs-status ordering as
# observed by the reader thread.
_ENTRY_TEXT = "text"
_ENTRY_STATUS = "status"
_ENTRY_FLUSH = "flush"   # message boundary (new assistant turn)
_ENTRY_STOP = "stop"     # shutdown signal

_StreamEntry = Tuple[str, Optional[str]]

# Worker idle-poll cadence — cap on queue-get timeout when no deadlines pend.
_IDLE_POLL_SECONDS = 0.5

# Shutdown-only drain bound (close() runs only at shutdown; the daemon-thread
# worker dies with the process — leftover queued entries are lost, fine here).
_SHUTDOWN_DRAIN_TIMEOUT = 2.0

# Unbounded queue on purpose (dropping is the bug); log once past this depth.
_QUEUE_HIGH_WATER = 1000


class StreamSender:
    """Unified streaming sender for Claude's response pipeline.

    One worker + one ordered queue per chat guarantees Telegram receives
    messages in the exact order the reader enqueued them — no cross-thread
    synchronization needed.

    - TEXT deltas coalesce for up to ``text_window`` seconds, emit as one
      markdown message via ``text_send_fn``.
    - STATUS lines batch for up to ``status_window`` seconds; identical
      consecutive lines collapse with "(N times)"; emit as HTML via
      ``status_send_fn``.
    - TYPE TRANSITIONS (TEXT after STATUS, or vice versa) flush the previous
      bucket first — that guarantees ordering at the Telegram level.
    - FLUSH marks an assistant-turn boundary (flush both buckets; restart
      coalescing). STOP drains everything and exits.
    """

    # Consecutive emit failures (text+status combined) before a one-time
    # fallback notice via iMessage; ``_fallback_sent`` prevents looping.
    _EMIT_FAILURE_THRESHOLD = 3

    def __init__(self, token: str, chat_id: str,
                 text_send_fn: Callable[[str, str, str], None],
                 status_send_fn: Callable[[str, str, str], None],
                 text_window: float = STREAM_BUFFER_WINDOW,
                 status_window: float = STATUS_BUFFER_WINDOW) -> None:
        self.token = token
        self.chat_id = chat_id
        self._text_send_fn = text_send_fn
        self._status_send_fn = status_send_fn
        self._text_window = text_window
        self._status_window = status_window
        self.q: "queue.Queue[_StreamEntry]" = queue.Queue()
        self._closed = False
        # Serialises producer-side ``_closed``-check + ``q.put`` against a
        # concurrent ``close()`` (TOCTOU). Held only for the cheap enqueue.
        self._close_lock = threading.RLock()
        # Combined consecutive-failure counter (text+status). Worker-only
        # writes; the synchronous fallback paths run only post-worker-exit.
        self._consecutive_emit_failures = 0
        self._fallback_sent = False
        # One-shot latch for the queue-depth warning (see _note_queue_depth).
        self._queue_high_water_logged = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    # Lifecycle introspection — used by the per-chat sender registry.

    @property
    def is_closed(self) -> bool:
        """True once close() has run — sender is spent; registry creates a fresh one."""
        return self._closed

    @property
    def worker_alive(self) -> bool:
        """True while the worker thread is running. A long-lived sender whose
        worker died would silently swallow every future bubble, so the
        registry treats a dead worker as a signal to replace the sender."""
        return self._thread.is_alive()

    # Producer interface.

    def text(self, delta: str) -> None:
        """Queue a text delta for coalescing + markdown send."""
        if not delta:
            return
        with self._close_lock:
            if not self._closed:
                self.q.put((_ENTRY_TEXT, delta))
                self._note_queue_depth()
                return
        # Closed sender — drop. close() runs only at shutdown; a synchronous
        # emit here would race the daemon-thread final drain.

    def status(self, line: str) -> None:
        """Queue a tool-status line (pre-formatted HTML) for batched HTML send."""
        if not line:
            return
        with self._close_lock:
            if not self._closed:
                self.q.put((_ENTRY_STATUS, line))
                self._note_queue_depth()
                return
        # Closed sender — drop. See .text() for rationale.

    def _note_queue_depth(self) -> None:
        """Log once past the high-water mark; latch resets below half (hysteresis).

        Makes a Telegram-delivery backlog or a runaway emit loop observable
        instead of silently growing toward OOM. qsize is O(1).
        """
        depth = self.q.qsize()
        if depth > _QUEUE_HIGH_WATER:
            if not self._queue_high_water_logged:
                self._queue_high_water_logged = True
                log(
                    "StreamSender queue for chat %s is backing up (%d queued) "
                    "— Telegram delivery is falling behind" % (self.chat_id, depth)
                )
        elif depth < _QUEUE_HIGH_WATER // 2:
            self._queue_high_water_logged = False

    def flush(self) -> None:
        """Mark a message boundary (e.g. new assistant turn) — flushes both buckets."""
        with self._close_lock:
            if self._closed:
                return
            self.q.put((_ENTRY_FLUSH, None))

    def close(self, timeout: float = _SHUTDOWN_DRAIN_TIMEOUT) -> None:
        """Drain remaining entries and stop the worker (shutdown-only).

        Enqueues STOP and joins up to ``timeout``. Timed-out worker is
        acceptable — the daemon thread dies with the process; unsent bubbles
        are lost at exit.
        """
        with self._close_lock:
            if self._closed:
                # Idempotent — re-draining would re-send anything still queued.
                return
            self._closed = True
            self.q.put((_ENTRY_STOP, None))
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            log(
                "StreamSender.close: worker still draining at shutdown after "
                "%.1fs, exiting anyway" % timeout
            )
            return
        # Clean exit — drain any leftover producer-side entries on this
        # thread so nothing is lost during the in-time shutdown path.
        self._drain_remaining()

    # Transport helpers (worker-only, except post-close fallbacks).

    def _emit_text(self, buf: List[str]) -> None:
        # Defense-in-depth — producers + _flush_text already reject empty.
        if not buf:
            return
        try:
            self._text_send_fn(self.token, self.chat_id, "".join(buf))
            self._record_emit_success()
        except Exception as e:
            log(f"StreamSender text send error: {e}")
            self._record_emit_failure(e)

    def _emit_status(self, lines: List[str]) -> None:
        # Defense-in-depth — see _emit_text.
        if not lines:
            return
        try:
            self._status_send_fn(self.token, self.chat_id, "\n".join(lines))
            self._record_emit_success()
        except Exception as e:
            log(f"StreamSender status send error: {e}")
            self._record_emit_failure(e)

    def _record_emit_success(self) -> None:
        """Reset the consecutive-failure counter on any successful send."""
        if self._consecutive_emit_failures:
            self._consecutive_emit_failures = 0

    def _record_emit_failure(self, exc: BaseException) -> None:
        """Track consecutive emit failures; after threshold send a one-time
        fallback notice via iMessage.

        Guarded by ``_fallback_sent`` so we never loop; if the fallback itself
        fails, log and give up. Route via iMessage (not ``_text_send_fn`` —
        which just failed n times); see docs/ARCHITECTURE.md.
        """
        self._consecutive_emit_failures += 1
        if (
            self._consecutive_emit_failures < self._EMIT_FAILURE_THRESHOLD
            or self._fallback_sent
        ):
            return
        self._fallback_sent = True
        n = self._consecutive_emit_failures
        log(
            f"StreamSender: {n} consecutive emit failures — sending fallback "
            "notice via independent transport (iMessage)"
        )
        try:
            from landline.runtime import notifications  # local import breaks the cycle
            notifications.send_health_alert(
                subject="telegram-send-failing",
                body=(
                    f"[{AGENT_NAME}] StreamSender for chat {self.chat_id} "
                    f"failed {n} consecutive sends — check logs."
                ),
            )
        except Exception as fallback_err:
            log(
                "StreamSender fallback notice also failed: "
                f"{fallback_err}"
            )

    # Worker loop + helpers.

    def _run(self) -> None:
        state = _StreamSenderState()

        while True:
            timeout = _IDLE_POLL_SECONDS
            if state.text_deadline is not None:
                timeout = min(timeout, max(0.01, state.text_deadline - time.time()))
            if state.status_deadline is not None:
                timeout = min(timeout, max(0.01, state.status_deadline - time.time()))

            try:
                entry_type, content = self.q.get(timeout=timeout)
            except queue.Empty:
                # Deadline-flush bug must not kill a long-lived worker.
                try:
                    self._handle_timeout(state)
                except Exception as e:
                    log(f"StreamSender worker timeout-handler error: {e}")
                continue

            # Catch-log-continue so one malformed entry can't turn a long-lived
            # sender into a permanent black hole. STOP still returns.
            try:
                if entry_type == _ENTRY_TEXT:
                    self._handle_text(state, content or "")
                elif entry_type == _ENTRY_STATUS:
                    self._handle_status(state, content or "")
                elif entry_type == _ENTRY_FLUSH:
                    self._flush_status(state)
                    self._flush_text(state)
                elif entry_type == _ENTRY_STOP:
                    try:
                        self._flush_status(state)
                        self._flush_text(state)
                    except Exception as e:
                        log(f"StreamSender worker stop-flush error: {e}")
                    return
            except Exception as e:
                log(f"StreamSender worker error on '{entry_type}': {e}")

    def _handle_timeout(self, state: "_StreamSenderState") -> None:
        now = time.time()
        if state.text_deadline is not None and now >= state.text_deadline:
            self._flush_text(state)
        if state.status_deadline is not None and now >= state.status_deadline:
            self._flush_status(state)

    def _handle_text(self, state: "_StreamSenderState", delta: str) -> None:
        # Text-after-status → flush status FIRST so status lines hit Telegram
        # before the text that references them. Single worker + single HTTP
        # stream: order of _emit_* calls is what Telegram observes.
        self._flush_status(state)
        state.text_buf.append(delta)
        if state.text_deadline is None:
            state.text_deadline = time.time() + self._text_window

    def _handle_status(self, state: "_StreamSenderState", line: str) -> None:
        # Symmetric: status-after-text flushes text first.
        self._flush_text(state)
        # Status collapsing ("(N times)") does NOT span text boundaries —
        # _handle_text flushes held_status, so identical status lines
        # bookending a text reply show twice (correct: the text in between
        # makes them feel like distinct events).
        if line == state.held_status:
            state.held_count += 1
        else:
            if state.held_status is not None:
                state.status_batch.append(
                    _format_repeated_status(state.held_status, state.held_count)
                )
            state.held_status = line
            state.held_count = 1
        if state.status_deadline is None:
            state.status_deadline = time.time() + self._status_window

    def _flush_text(self, state: "_StreamSenderState") -> None:
        if not state.text_buf:
            state.text_deadline = None
            return
        buf = state.text_buf
        state.text_buf = []
        state.text_deadline = None
        self._emit_text(buf)

    def _flush_status(self, state: "_StreamSenderState") -> None:
        if state.held_status is not None:
            state.status_batch.append(
                _format_repeated_status(state.held_status, state.held_count)
            )
            state.held_status = None
            state.held_count = 0
        if not state.status_batch:
            state.status_deadline = None
            return
        batch = state.status_batch
        state.status_batch = []
        state.status_deadline = None
        self._emit_status(batch)

    # Post-close safety net — only runs if the worker didn't drain in time.

    def _drain_remaining(self) -> None:
        """Drain anything still queued after close() raced the worker.

        Honours FLUSH boundaries (split messages stay split) and type
        transitions (STATUS/TEXT don't mix across the post-close boundary).
        """
        state = _StreamSenderState()
        try:
            while True:
                entry_type, content = self.q.get_nowait()
                if entry_type == _ENTRY_TEXT:
                    self._flush_status(state)
                    state.text_buf.append(content or "")
                elif entry_type == _ENTRY_STATUS:
                    self._flush_text(state)
                    line = content or ""
                    if line == state.held_status:
                        state.held_count += 1
                    else:
                        if state.held_status is not None:
                            state.status_batch.append(
                                _format_repeated_status(state.held_status, state.held_count)
                            )
                        state.held_status = line
                        state.held_count = 1
                elif entry_type == _ENTRY_FLUSH:
                    self._flush_status(state)
                    self._flush_text(state)
                elif entry_type == _ENTRY_STOP:
                    # Expected: close() enqueues one STOP before join; the
                    # worker may have exited without consuming it. Producers
                    # can't enqueue more (idempotent close under _close_lock),
                    # so any STOP here is one the worker missed — skip.
                    continue
        except queue.Empty:
            pass
        self._flush_status(state)
        self._flush_text(state)


class _StreamSenderState:
    """Worker-thread-only state for the StreamSender loop.

    Kept off the StreamSender instance so producer-side methods can't
    accidentally mutate worker state.
    """

    __slots__ = (
        "text_buf",
        "text_deadline",
        "status_batch",
        "held_status",
        "held_count",
        "status_deadline",
    )

    def __init__(self) -> None:
        self.text_buf: List[str] = []
        self.text_deadline: Optional[float] = None
        self.status_batch: List[str] = []
        self.held_status: Optional[str] = None
        self.held_count: int = 0
        self.status_deadline: Optional[float] = None
