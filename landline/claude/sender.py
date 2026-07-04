"""Unified streaming sender for Claude's response pipeline.

A single worker thread reads `(type, content)` entries from one queue and
fans them out to either the text-send (markdown→HTML) or status-send
(pre-built HTML) transport. Senders are long-lived (one per chat, for the
daemon's life) — created and managed by `sender_registry`.
"""

import queue
import threading
import time
from typing import Callable, List, Optional, Tuple

from landline.config import AGENT_NAME, STATUS_BUFFER_WINDOW, STREAM_BUFFER_WINDOW
from landline.runtime.logging import log
from landline.claude.tool_status import _format_repeated_status


# Entry-type tags for the StreamSender queue. Tuples of (tag, payload)
# flow through one ordered pipeline so the worker can preserve the order
# in which the reader thread observed text vs status events.
_ENTRY_TEXT = "text"
_ENTRY_STATUS = "status"
_ENTRY_FLUSH = "flush"   # message boundary (new assistant turn)
_ENTRY_STOP = "stop"     # shutdown signal

_StreamEntry = Tuple[str, Optional[str]]

# Worker idle-poll cadence: maximum time between queue-get timeouts when there
# are no pending deadlines. Keeps the worker responsive enough to notice a
# `_text_window` / `_status_window` deadline that was set just before an empty
# stretch on the queue, without burning CPU.
_IDLE_POLL_SECONDS = 0.5

# Drain ceiling for close(): senders are long-lived in production and close()
# runs ONLY at shutdown (drain_for_shutdown / _close_all_senders), so this is a
# tight fast-path bound to stay within launchd's grace window. The worker is a
# daemon thread; whatever is still queued dies with the process, which is fine
# at shutdown.
_SHUTDOWN_DRAIN_TIMEOUT = 2.0

# Queue-depth high-water mark. The long-lived queue is intentionally unbounded
# (dropping is exactly the bug we're avoiding), but a sustained Telegram outage
# or a runaway emit loop could grow it without bound and be invisible until OOM.
# Log once when the queue crosses this depth so a backlog is observable; reset
# the one-shot latch once it drains back below half (hysteresis, no log spam).
_QUEUE_HIGH_WATER = 1000


class StreamSender:
    """Unified streaming sender for Claude's response pipeline.

    A single worker thread reads `(type, content)` entries from one queue and
    fans them out to either the text-send (markdown→HTML) or status-send
    (pre-built HTML) transport. Because both kinds flow through the same
    ordered queue and the same thread, Telegram receives messages in the
    exact order the reader enqueued them — no cross-thread synchronization
    needed.

    Behaviour:
    - TEXT deltas are coalesced for up to `text_window` seconds, then emitted
      as one markdown message via `text_send_fn`.
    - STATUS lines are batched for up to `status_window` seconds and identical
      consecutive lines are collapsed with a "(N times)" suffix, then emitted
      as one HTML message via `status_send_fn`.
    - When the worker sees a TYPE TRANSITION (TEXT after STATUS, or STATUS
      after TEXT) it flushes the previous type first — that's what guarantees
      ordering at the Telegram level.
    - FLUSH marks an assistant-turn boundary: flush both buckets, restart
      coalescing for the next turn.
    - STOP drains everything and exits.
    """

    # After this many consecutive emit failures (text or status, combined),
    # send a one-time plain-text fallback notice so persistent 5xx / network
    # errors aren't invisible to the operator. Guarded by `_fallback_sent` so we
    # never loop.
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
        # Serialises producer-side `_closed`-check + `q.put` so a concurrent
        # `close()` can't slip in between (TOCTOU). The lock is only held for
        # the cheap check + enqueue.
        self._close_lock = threading.RLock()
        # Combined consecutive-failure counter across both transports —
        # incremented on any emit error, reset on any successful emit.
        # Read/written only by the worker thread (and the synchronous fallback
        # paths run only after the worker has exited), so no lock needed.
        self._consecutive_emit_failures = 0
        self._fallback_sent = False
        # One-shot latch for the queue-depth warning (see _note_queue_depth).
        self._queue_high_water_logged = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------
    # Lifecycle introspection (used by the per-chat sender registry)
    # ------------------------------------------------------------------

    @property
    def is_closed(self) -> bool:
        """True once close() has run — the sender is spent and must not be
        reused (the registry creates a fresh one in its place)."""
        return self._closed

    @property
    def worker_alive(self) -> bool:
        """True while the worker thread is running. A long-lived sender whose
        worker has died would silently swallow every future bubble, so the
        registry treats a dead worker as a signal to replace the sender."""
        return self._thread.is_alive()

    # ------------------------------------------------------------------
    # Public producer interface
    # ------------------------------------------------------------------

    def text(self, delta: str) -> None:
        """Queue a text delta to be coalesced and sent as a markdown message."""
        if not delta:
            return
        with self._close_lock:
            if not self._closed:
                self.q.put((_ENTRY_TEXT, delta))
                self._note_queue_depth()
                return
        # Closed sender — drop. In production close() runs only at shutdown,
        # so a producer racing close() means the process is exiting; emitting
        # synchronously here would just race the daemon thread on the way out
        # and risk reordering against the worker's final drain.

    def status(self, line: str) -> None:
        """Queue a tool-status line (pre-formatted HTML) for batched HTML send."""
        if not line:
            return
        with self._close_lock:
            if not self._closed:
                self.q.put((_ENTRY_STATUS, line))
                self._note_queue_depth()
                return
        # Closed sender — drop. See `.text()` for rationale.

    def _note_queue_depth(self) -> None:
        """Log once if the queue backs up past the high-water mark; reset the
        latch when it drains below half. Cheap (qsize is O(1)); the warning
        makes a Telegram-delivery backlog or a runaway emit loop observable
        instead of silently growing toward OOM. Never drops — see
        _QUEUE_HIGH_WATER."""
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
        """Mark a message boundary (e.g. new assistant turn).

        Forces a flush of both text and status buckets so the next batch
        starts cleanly in a new Telegram message.
        """
        with self._close_lock:
            if self._closed:
                return
            self.q.put((_ENTRY_FLUSH, None))

    def close(self, timeout: float = _SHUTDOWN_DRAIN_TIMEOUT) -> None:
        """Drain remaining entries and stop the worker.

        Called only at shutdown (via `_close_all_senders`). Enqueues STOP and
        waits up to ``timeout`` for the worker to finish flushing. If the
        worker is still draining when the timeout elapses, we log and return
        — the daemon-thread worker dies with the process, so any unsent
        bubbles are lost at exit, which is acceptable at shutdown.
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
        # Worker exited cleanly within the timeout — drain any leftover
        # producer-side entries from this thread so nothing is lost during
        # the in-time shutdown path.
        self._drain_remaining()

    # ------------------------------------------------------------------
    # Transport helpers (no state, no locking — called only by the worker
    # except in the post-close fallbacks above, where the worker is gone).
    # ------------------------------------------------------------------

    def _emit_text(self, buf: List[str]) -> None:
        # Defense-in-depth: producers (`.text()`) already drop empty deltas,
        # and `_flush_text` early-returns on an empty buffer, so this guard
        # should never fire in practice. Kept as a cheap belt-and-braces
        # check because the alternative is sending an empty Telegram message.
        if not buf:
            return
        try:
            self._text_send_fn(self.token, self.chat_id, "".join(buf))
            self._record_emit_success()
        except Exception as e:
            log(f"StreamSender text send error: {e}")
            self._record_emit_failure(e)

    def _emit_status(self, lines: List[str]) -> None:
        # Defense-in-depth, same rationale as `_emit_text`.
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
        """Track consecutive emit failures and, after a threshold, send a
        one-time plain-text fallback notice so persistent transport errors
        aren't invisible. Guarded by ``_fallback_sent`` so we never loop —
        if the fallback itself fails, we log and give up."""
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
        # Cluster 1 M7: the fallback used to re-use ``self._text_send_fn`` —
        # the very callable that just failed ``n`` times in a row. That
        # notice reliably never landed. Route through the async iMessage
        # path in landline.runtime.notifications so the alert reaches the operator
        # even when Telegram is the thing that's broken. Best-effort import
        # + call: any exception here is caught and logged; _fallback_sent
        # is already set, so we can never loop.
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

    # ------------------------------------------------------------------
    # Worker loop and its small helpers
    # ------------------------------------------------------------------

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
                # A deadline-flush bug must not kill a long-lived worker.
                try:
                    self._handle_timeout(state)
                except Exception as e:
                    log(f"StreamSender worker timeout-handler error: {e}")
                continue

            # The worker is long-lived (one per chat, for the daemon's life).
            # `_emit_*` already swallow transport errors, but a defect anywhere
            # else in entry handling would otherwise terminate the thread and
            # turn the sender into a permanent black hole. Catch-log-continue so
            # one malformed entry can never silence the chat. STOP still returns.
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
        # Ordering guarantee: when text follows status, flush status FIRST so
        # the status lines hit Telegram before the text reply that references
        # them. With a single worker thread + single HTTP stream, the order
        # of `_emit_*` calls is the order Telegram observes.
        self._flush_status(state)
        state.text_buf.append(delta)
        if state.text_deadline is None:
            state.text_deadline = time.time() + self._text_window

    def _handle_status(self, state: "_StreamSenderState", line: str) -> None:
        # Symmetric ordering guarantee: status after text flushes text first.
        self._flush_text(state)
        # By design, status collapsing (the "(N times)" suffix) does NOT span
        # text boundaries: `_flush_text` above doesn't touch held_status, but
        # `_flush_status` (called whenever a text delta arrives in
        # `_handle_text`) emits and clears held_status. So an identical status
        # line that bookends a text reply is shown twice, not collapsed — which
        # is what we want, because the text in between makes them feel like
        # distinct events to the reader.
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

    # ------------------------------------------------------------------
    # Post-close safety net (only runs if the worker didn't drain in time)
    # ------------------------------------------------------------------

    def _drain_remaining(self) -> None:
        """Drain anything still on the queue after close() raced the worker.

        Honours FLUSH boundaries (so split messages stay split) and type
        transitions (so STATUS and TEXT don't get mixed across the
        post-close boundary).
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
                    # STOP entries are expected here: `close()` puts one STOP
                    # on the queue before joining, and the worker may have
                    # exited (timed out, raised, or returned) without
                    # consuming it. Producers can't enqueue more STOPs since
                    # `close()` is gated by `_close_lock` + idempotency, so
                    # the only STOPs we ever see are ones the worker missed.
                    # Skip and keep draining real entries.
                    continue
        except queue.Empty:
            pass
        self._flush_status(state)
        self._flush_text(state)


class _StreamSenderState:
    """Mutable per-call state for the StreamSender worker loop.

    Kept off the StreamSender instance itself so the producer-side methods
    (`.text()`, `.status()`, `.flush()`, `.close()`) can't accidentally
    mutate worker state — only the worker thread reads/writes here.
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
