"""Persistent stdout pump for the long-lived Claude subprocess.

THE INVARIANT THIS MODULE ENFORCES: the persistent Claude process's stdout is
ONE continuous stream shared by every agent turn — both turns the daemon
dispatches (the operator's messages) and turns the Claude Code harness starts on its
own (background task completions: subagents, `run_in_background` Bash). That
stream must have exactly ONE reader for the LIFE of the process.

Why (the 2026-06/07 "desync" root cause): the old design attached a fresh
reader per dispatched turn and read "until the first `result` event". When a
background task completed while no turn was in flight, the harness ran an
UNSOLICITED turn whose events — including a `result` — piled up unread in the
pipe. The next dispatched turn then consumed that stale turn's output, stopped
at the stale `result`, and left its OWN response unread. Every turn after that
delivered the PREVIOUS turn's answer (the operator: A -> nothing -> B -> A' -> C -> B'),
until a daemon restart or /new killed the process. Verified empirically:
after `result`, a finished background task emits
`system/task_notification` -> `system/init` -> assistant... -> `result`
on stdout with no stdin write at all.

The pump reads every event as it arrives, for the life of the process:

  * Turn blocks are delimited by `system/init` ... `result` (every turn,
    dispatched or unsolicited, opens with `system/init` — verified).
  * A dispatched turn registers a `TurnHandle` BEFORE its stdin write; the
    next `system/init` is attributed to it. Its events are routed to the
    per-chat StreamSender AND accumulated on the handle; its `result`
    completes the handle.
  * Blocks with no registered handle are unsolicited (background-task turns):
    their text is routed to the chat sender IMMEDIATELY — the operator receives
    background results when they finish, not one message later.

Attribution note: if a background turn begins in the sub-second window between
`register_turn` and the dispatched turn's `system/init`, the handle may be
attributed to the background block (stream order is authoritative and the two
are indistinguishable in-band). ALL text is routed to the sender either way,
so nothing is lost and nothing hangs; in the common idle case the dispatched
turn's real output simply arrives as an unsolicited block moments later. If a
follow-up dispatch is already queued when that happens (rapid-fire messages
overlapping a background completion), attribution can skew by one turn until
the burst ends — a rare, compound-race, self-healing echo of the old bug, not
a persistent state. One sharper edge inside the same race: if the
misattributed background block happens to be the empty-clean-exit shape, the
dispatched turn's result can look stale (`looks_like_stale_session`) and
trigger a fresh-session retry — background turns essentially always carry
content, so this is a compound-compound rarity, but don't let the word
"cosmetic" lull you. Do not "fix" any of this with task-notification
counting: a miscount can orphan a dispatched turn (a hang), which is strictly
worse than cosmetic skew.

Watchdog note: `_touch()` bumps the pending turn's activity clock on EVERY
event, including another block's — deliberate OLD-code parity (the old reader
also reset its silence clock on any line of the shared pipe). Scoping it to
the owned block would let a >CLAUDE_TIMEOUT background turn get a healthy
process killed while a dispatch waits behind it.
"""

import json
import threading
import time
import weakref
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from landline.logging import log
from landline.tool_status import _extract_text_blocks, _format_tool_status


class TurnHandle:
    """Bookkeeping for one dispatched turn.

    Registered with the pump BEFORE the user message is written to stdin.
    The pump completes it (``done.set()``) when the block attributed to it
    closes with a `result` event, or on EOF / read error — a registered
    handle is ALWAYS completed eventually, so the dispatch thread can never
    wait on a turn the pump has abandoned.
    """

    def __init__(self) -> None:
        self.done = threading.Event()
        # Session id observed on this turn's `system/init` (init_session_id)
        # and on its `result` event (result_session_id). The result-event id
        # wins, mirroring the old reader's precedence.
        self.init_session_id: Optional[str] = None
        self.result_session_id: Optional[str] = None
        self.final_result: str = ""
        self.saw_result: bool = False
        # Text deltas the pump routed to the sender for this turn's block —
        # joined by the streaming layer into ClaudeStreamResult.streamed_text.
        self.streamed_parts: List[str] = []
        # Set by the read-error/EOF paths so the streaming layer can surface
        # the same "Stream read error" result.error the old reader produced.
        self.error: Optional[str] = None
        # Set by the interrupt watchdog: the pump stops routing/accumulating
        # this turn's assistant events (parity with the old `interrupt_sent`
        # skip). Unsolicited blocks are unaffected.
        self.interrupt_suppress = threading.Event()
        # Single-cell activity clock (same shape the old reader used) —
        # bumped by the pump on EVERY event while this handle is pending, so
        # the caller's CLAUDE_TIMEOUT watchdog keeps working unchanged.
        self.last_active: List[float] = [time.time()]
        # Cluster 2 (stale-resume auto-recovery) — the pump records the
        # dispatched turn's terminal `result` event's error flag / subtype
        # (in _close_block) and whether a `system/init` opened this turn's
        # block (in _open_block). Together they distinguish the
        # pruned/nonexistent --resume shape (is_error + no init on this
        # turn) from a mid-session API error (is_error + saw_init True) —
        # see landline.claude_dispatch.looks_like_pruned_resume. Pump-thread
        # writes are read by the streaming layer AFTER handle.done.wait()
        # completes, same happens-before rule as session_id/final_result.
        self.result_is_error: bool = False
        self.result_subtype: Optional[str] = None
        self.saw_init: bool = False
        # Cluster 4 (usage/cost stats): pump captures the optional
        # accounting fields from the terminal `result` event so the
        # dispatcher can persist a daily aggregate on successful turns.
        # All safe defaults — existing tests that build TurnHandle by
        # hand keep working unchanged.
        self.result_usage: Optional[Dict[str, Any]] = None
        self.result_model_usage: Optional[Dict[str, Any]] = None
        self.result_total_cost_usd: Optional[float] = None
        self.result_num_turns: Optional[int] = None
        self.result_duration_ms: Optional[int] = None


class _Block:
    """Pump-thread-only state for one open turn block (init..result)."""

    __slots__ = ("handle", "sender", "text_sent_per_msg", "seen_tool_ids",
                 "routed_any_text")

    def __init__(self, handle: Optional[TurnHandle],
                 sender: Optional[Any]) -> None:
        self.handle = handle
        self.sender = sender
        self.text_sent_per_msg: Dict[str, str] = {}
        self.seen_tool_ids: Set[str] = set()
        self.routed_any_text = False


class StreamPump:
    """Single long-lived reader of one Claude subprocess's stdout.

    Created once per process by ``get_or_create_pump`` and never replaced:
    if the pump thread dies while the process lives, the pipe's read position
    is no longer trustworthy and the caller must kill/respawn the process
    (see ``landline.streaming``). All producer-side state (``_pending``,
    ``_idle_route``) is lock-guarded; block state is pump-thread-only.
    """

    def __init__(self, proc: Any) -> None:
        self.proc = proc
        self._lock = threading.Lock()
        self._pending: Optional[TurnHandle] = None
        self._pending_sender: Optional[Any] = None
        # (chat_id, token, text_send_fn, status_send_fn) for routing
        # unsolicited blocks. Refreshed on every dispatched turn.
        self._idle_route: Optional[Tuple[str, str, Callable, Callable]] = None
        self._read_error: Optional[str] = None
        self._init_anomaly_logged = False
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="claude-stream-pump",
        )
        self._thread.start()

    # ------------------------------------------------------------------
    # Producer interface (dispatch thread)
    # ------------------------------------------------------------------

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def set_idle_route(self, chat_id: str, token: str,
                       text_send_fn: Callable, status_send_fn: Callable) -> None:
        """Remember how to reach the chat so unsolicited blocks (background
        task completions) can be delivered while no turn is in flight."""
        with self._lock:
            self._idle_route = (chat_id, token, text_send_fn, status_send_fn)

    def register_turn(self, handle: TurnHandle, sender: Any) -> None:
        """Register the upcoming dispatched turn. Call BEFORE the stdin write
        so the turn's `system/init` can never race past an empty slot."""
        with self._lock:
            if self._pending is not None:
                # Dispatch is single-threaded; a second concurrent turn is a
                # programmer error. Complete the orphan so nothing hangs.
                log("StreamPump: register_turn found a pending handle — "
                    "completing the orphan (dispatch overlap?)")
                self._pending.error = "orphaned by overlapping dispatch"
                self._pending.done.set()
            self._pending = handle
            self._pending_sender = sender
            handle.last_active[0] = time.time()
        if not self.alive:
            # Pump died between the caller's liveness check and now — never
            # leave a registered handle waiting on a dead reader.
            self._complete_pending("stream pump is not running")

    def cancel_turn(self, handle: TurnHandle) -> None:
        """Unregister a turn whose stdin write failed (nothing was sent, so
        no block will ever arrive for it)."""
        with self._lock:
            if self._pending is handle:
                self._pending = None
                self._pending_sender = None
        handle.done.set()

    # ------------------------------------------------------------------
    # Pump thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        block: Optional[_Block] = None
        try:
            for raw in self.proc.stdout:
                line = raw.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._touch()
                try:
                    block = self._handle_event(block, event)
                except Exception as event_error:
                    # One malformed event must not kill the process's only
                    # reader — log and keep pumping. Behavioral drift vs the
                    # old per-turn reader, accepted knowingly: a sender that
                    # RAISES here is logged but no longer surfaces as
                    # result.error on the turn (StreamSender's producer
                    # methods are q.put behind a lock and don't raise in
                    # practice; grep "StreamPump event error" if a turn ever
                    # looks successful while nothing rendered).
                    log("StreamPump event error on %r: %s"
                        % (event.get("type"), event_error))
        except Exception as read_error:
            # Includes ValueError from the watchdog closing stdout mid-read
            # (interrupt / timeout / process-death paths) — same class of
            # exit the old per-turn reader surfaced as "Stream read error".
            self._read_error = str(read_error)
            log(f"Stream read error: {read_error}")
        finally:
            # EOF or error: flush a dangling unsolicited block's tail and
            # ALWAYS complete a pending handle so the dispatcher never hangs.
            if block is not None and block.handle is None:
                self._flush_block_sender(block)
            self._complete_pending(self._read_error)
            # Break the `_pumps[proc] -> pump -> pump.proc -> proc` reference
            # cycle: with a strong self.proc, the WeakKeyDictionary entry
            # (and the dead proc) could never be collected, leaking one
            # Popen + pump per respawn (/new, interrupts, watchdog kills)
            # for the daemon's lifetime. The pump is spent once _run exits,
            # so nothing reads self.proc after this.
            self.proc = None

    def _complete_pending(self, error: Optional[str]) -> None:
        with self._lock:
            handle = self._pending
            sender = self._pending_sender
            self._pending = None
            self._pending_sender = None
        if handle is not None and not handle.done.is_set():
            if error is not None and not handle.saw_result:
                handle.error = error
            # Bubble boundary for any partial text this turn routed (the old
            # reader's finally-flush did this for the EOF/error/interrupt
            # exits). Must happen before done.set() — same sole-producer
            # rule as _close_block.
            if sender is not None:
                try:
                    sender.flush()
                except Exception as flush_error:
                    log(f"StreamPump: flush failed: {flush_error}")
            handle.done.set()

    def _touch(self) -> None:
        with self._lock:
            handle = self._pending
        if handle is not None:
            handle.last_active[0] = time.time()

    # ------------------------------------------------------------------
    # Event handling (pump thread only)
    # ------------------------------------------------------------------

    def _handle_event(self, block: Optional[_Block],
                      event: Dict) -> Optional[_Block]:
        etype = event.get("type")

        if etype == "system" and event.get("subtype") == "init":
            return self._open_block(block, event)

        if etype == "assistant":
            if block is None:
                # Assistant event with no init — not observed in practice;
                # treat as the start of an unsolicited block so the text is
                # still delivered rather than dropped.
                block = self._attribute_block()
            self._route_assistant(block, event)
            return block

        if etype == "result":
            return self._close_block(block, event)

        # Everything else (rate_limit_event, system/thinking_tokens,
        # system/task_started|updated|notification, user tool_results, ...)
        # carries no routable content — _touch() above already counted it
        # as activity.
        return block

    def _open_block(self, block: Optional[_Block],
                    event: Dict) -> _Block:
        if block is not None:
            # An init while a block is open means the previous turn never
            # emitted a result (not observed in practice). Keep the SAME
            # handle attached to the fresh block so a dispatched turn can
            # never be orphaned by the anomaly; flush an unsolicited tail.
            if not self._init_anomaly_logged:
                self._init_anomaly_logged = True
                log("StreamPump: system/init arrived while a turn block was "
                    "still open — re-anchoring on the new block")
            if block.handle is None:
                self._flush_block_sender(block)
                block = self._attribute_block()
            else:
                block = _Block(block.handle, block.sender)
        else:
            block = self._attribute_block()

        if block.handle is not None:
            # Cluster 2: mark saw_init BEFORE recording session_id so the
            # ordering matches the load-bearing distinguisher — a mid-session
            # error necessarily had an init on this turn (saw_init True); a
            # pruned/nonexistent --resume never opens with init on the
            # failing turn (saw_init stays False).
            block.handle.saw_init = True
            sid = event.get("session_id")
            if sid:
                block.handle.init_session_id = sid
        return block

    def _attribute_block(self) -> _Block:
        """A new block goes to the pending dispatched turn if there is one,
        else it is unsolicited (harness-initiated) and routes to the chat via
        the idle route."""
        with self._lock:
            handle = self._pending
            sender = self._pending_sender
            idle_route = self._idle_route
        if handle is not None:
            return _Block(handle, sender)
        return _Block(None, self._idle_sender(idle_route))

    def _idle_sender(self, idle_route: Optional[Tuple]) -> Optional[Any]:
        if idle_route is None:
            # No dispatched turn has ever run on this process — nothing to
            # route to. Not reachable in practice (a fresh process emits no
            # events before its first message); log so a drop is observable.
            log("StreamPump: unsolicited turn before any dispatched turn — "
                "no chat route yet, text will be dropped")
            return None
        chat_id, token, text_send_fn, status_send_fn = idle_route
        # Late import through the facade — the canonical patch surface.
        from landline import claude as _claude_facade
        try:
            return _claude_facade._get_or_create_sender(
                chat_id, token, text_send_fn, status_send_fn,
            )
        except Exception as sender_error:
            log(f"StreamPump: could not create idle sender: {sender_error}")
            return None

    def _route_assistant(self, block: _Block, event: Dict) -> None:
        if event.get("parent_tool_use_id"):
            return
        handle = block.handle
        if handle is not None and handle.interrupt_suppress.is_set():
            return
        sender = block.sender
        if sender is None:
            return
        msg_obj = event.get("message", {}) or {}
        msg_id = msg_obj.get("id") or ""

        # Flush on a new assistant message id so each agent-loop turn becomes
        # a separate Telegram bubble (same rule as the old reader).
        if msg_id and block.text_sent_per_msg and msg_id not in block.text_sent_per_msg:
            sender.flush()

        for content_block in (msg_obj.get("content") or []):
            if isinstance(content_block, dict) and content_block.get("type") == "tool_use":
                tool_id = content_block.get("id", "")
                if tool_id and tool_id not in block.seen_tool_ids:
                    block.seen_tool_ids.add(tool_id)
                    status = _format_tool_status(content_block)
                    if status:
                        sender.status(status)

        event_text = _extract_text_blocks(msg_obj.get("content", []))
        if not event_text:
            return
        prev = block.text_sent_per_msg.get(msg_id, "")
        if event_text.startswith(prev):
            delta = event_text[len(prev):]
        else:
            delta = event_text
        block.text_sent_per_msg[msg_id] = event_text
        if delta.strip():
            sender.text(delta)
            block.routed_any_text = True
            if handle is not None:
                handle.streamed_parts.append(delta)

    def _close_block(self, block: Optional[_Block],
                     event: Dict) -> Optional[_Block]:
        if block is None:
            # A result with no open block — anomalous in the common case, but
            # ALSO the empirically-verified pruned/nonexistent --resume shape
            # (Cluster 2): the CC process emits a bare `result` event with
            # is_error=true / subtype=error_during_execution and no preceding
            # system/init. If a dispatch is pending we must (a) record the
            # is_error signal on its handle so the dispatcher's
            # looks_like_pruned_resume can observe it, AND (b) complete the
            # handle so the caller never hangs.
            log("StreamPump: result event with no open turn block")
            with self._lock:
                pending = self._pending
            if pending is not None:
                if event.get("is_error") is True:
                    pending.result_is_error = True
                subtype = event.get("subtype")
                if subtype is not None:
                    pending.result_subtype = subtype
            # No saw_result on this path — the pruned shape did not emit a
            # dispatched-turn result block, and _complete_pending's error
            # branch is gated on ``not saw_result``.
            self._complete_pending("result event with no open turn block")
            return None

        handle = block.handle
        if handle is not None:
            handle.final_result = event.get("result", "") or ""
            sid = event.get("session_id")
            if sid:
                handle.result_session_id = sid
            handle.saw_result = True
            # Cluster 2: capture the result event's error signal for the
            # dispatcher's pruned-resume detector. An unsolicited block that
            # emits is_error=True is discarded silently (block.handle is
            # None in the fall-through path below) — parity with today; no
            # dispatched turn is affected by a background is_error.
            if event.get("is_error") is True:
                handle.result_is_error = True
            handle.result_subtype = event.get("subtype")
            # Cluster 4: capture usage/cost fields from the result event.
            # Defensive isinstance checks — CC event shapes drift and a
            # missing/renamed key must never crash the pump thread. All
            # fields are optional; downstream code treats None as "no data".
            self._capture_usage_fields(event, handle)
            # Final-result tail + the turn-boundary flush happen HERE, on the
            # pump thread, BEFORE the handle completes. The pump must be the
            # SOLE producer of turn content on the per-chat sender: if the
            # dispatch thread appended the tail after done.wait(), a
            # back-to-back unsolicited block (already sitting in the pipe)
            # would race its text in between this turn's deltas and the
            # tail/flush, welding background text into this turn's bubble
            # and orphaning the tail into the next one (2026-07-02 audit,
            # finding H1 — reproduced 100% with back-to-back events).
            try:
                # A sender defect here must not leave the handle pending —
                # the dispatch thread would hang until the CLAUDE_TIMEOUT
                # watchdog. done.set() below is unconditional.
                self._append_final_tail(block, handle)
            except Exception as tail_error:
                log(f"StreamPump: final-tail append failed: {tail_error}")
            self._flush_block_sender(block)
            with self._lock:
                if self._pending is handle:
                    self._pending = None
                    self._pending_sender = None
            handle.done.set()
            return None

        # Unsolicited block (background-task turn): if the model's text came
        # only through the result payload, deliver that; then mark the bubble
        # boundary so the next turn starts a fresh message.
        final_text = (event.get("result", "") or "").strip()
        if final_text and not block.routed_any_text and block.sender is not None:
            block.sender.text(final_text)
        self._flush_block_sender(block)
        # Cluster 4: unsolicited-turn usage attribution. Background subagents
        # / run_in_background completions consume tokens on the operator's Max plan,
        # so they belong in the daily aggregate — but tagged separately from
        # dispatched turns so the operator can distinguish "my messages" cost from
        # "background stuff" cost. Lazy import so a broken usage_stats module
        # never breaks the pump; swallow everything so this is truly
        # fire-and-forget from the pump's perspective.
        self._record_unsolicited_usage(event)
        return None

    @staticmethod
    def _capture_usage_fields(event: Dict, handle: TurnHandle) -> None:
        """Copy the optional accounting fields from a ``result`` event onto
        the handle. Defensive isinstance checks — CC event shapes drift and
        a missing/renamed key must never crash the pump thread.

        Cluster 4. Fields mirror the persistent-stream docstring:
        ``usage`` (input/output/cache tokens), ``modelUsage`` (per-model
        breakdown), ``total_cost_usd``, ``num_turns``, ``duration_ms``.
        """
        usage = event.get("usage")
        if isinstance(usage, dict):
            handle.result_usage = usage
        model_usage = event.get("modelUsage")
        if isinstance(model_usage, dict):
            handle.result_model_usage = model_usage
        total_cost = event.get("total_cost_usd")
        if isinstance(total_cost, (int, float)):
            handle.result_total_cost_usd = float(total_cost)
        num_turns = event.get("num_turns")
        if isinstance(num_turns, int):
            handle.result_num_turns = num_turns
        duration_ms = event.get("duration_ms")
        if isinstance(duration_ms, int):
            handle.result_duration_ms = duration_ms

    @staticmethod
    def _record_unsolicited_usage(event: Dict) -> None:
        """Persist an unsolicited (background) turn's usage into the daily
        aggregate. Fully wrapped in try/except — a broken usage_stats module
        must never affect the pump's routing responsibilities. Lazy import
        keeps stream_pump import-time free of the state-file side effect.

        Cluster 4 hardening: ``usage_stats.record_turn`` performs synchronous
        ``os.fsync`` inside its module-level ``_lock``. Calling it from the
        pump thread would let a competing dispatch-thread ``record_turn``
        (holding ``_lock`` during an SSD stall) stall the pump — Claude's
        stdout pipe would fill and back-pressure the subprocess, breaking
        the "pump reads continuously for the process's life" invariant
        (see ``daemon/CLAUDE.md`` -> "StreamPump" -> "NOTHING else may
        read that pipe"). Dispatch it to a short-lived daemon thread so the
        pump returns immediately; the daemon thread can afford to block on
        the lock and fsync.
        """
        try:
            usage_snapshot = (
                event.get("usage")
                if isinstance(event.get("usage"), dict) else None
            )
            model_usage_snapshot = (
                event.get("modelUsage")
                if isinstance(event.get("modelUsage"), dict) else None
            )
            total_cost_snapshot = (
                float(event["total_cost_usd"])
                if isinstance(event.get("total_cost_usd"), (int, float))
                else None
            )
            duration_snapshot = (
                event.get("duration_ms")
                if isinstance(event.get("duration_ms"), int) else None
            )
        except Exception as capture_error:
            log(
                f"usage_stats.record_turn (unsolicited) capture failed: "
                f"{capture_error}"
            )
            return

        def _record_in_background() -> None:
            try:
                from landline import usage_stats
                usage_stats.record_turn(
                    result_usage=usage_snapshot,
                    result_model_usage=model_usage_snapshot,
                    total_cost_usd=total_cost_snapshot,
                    duration_ms=duration_snapshot,
                    dispatched=False,
                )
            except Exception as stats_error:
                log(
                    f"usage_stats.record_turn (unsolicited) failed: "
                    f"{stats_error}"
                )

        try:
            threading.Thread(
                target=_record_in_background,
                daemon=True,
                name="landline-usage-stats",
            ).start()
        except Exception as spawn_error:
            # Thread creation failed (process resource exhaustion). Log
            # metadata; the pump keeps going — losing one unsolicited
            # bucket update is strictly better than stalling the pipe.
            log(
                f"usage_stats.record_turn (unsolicited) thread spawn "
                f"failed: {spawn_error}"
            )

    @staticmethod
    def _append_final_tail(block: _Block, handle: TurnHandle) -> None:
        """Mirror of the old reader's end-of-turn logic: if the result
        payload extends (or entirely replaces) the streamed deltas, send the
        missing tail so Telegram shows the full reply. Runs on the pump
        thread so no other producer can interleave (see _close_block).

        ``handle.streamed_parts`` is updated so that
        ``"".join(handle.streamed_parts)`` equals what the old code exposed
        as ``result.streamed_text`` in every branch.
        """
        final = handle.final_result or ""
        if not final.strip():
            return
        if handle.interrupt_suppress.is_set():
            return
        sender = block.sender
        if sender is None:
            return
        streamed = "".join(handle.streamed_parts)
        if not streamed.strip():
            sender.text(final)
            handle.streamed_parts = [final]
        elif final.startswith(streamed):
            tail = final[len(streamed):]
            if tail.strip():
                sender.text(tail)
                handle.streamed_parts.append(tail)

    @staticmethod
    def _flush_block_sender(block: _Block) -> None:
        if block.sender is not None:
            try:
                block.sender.flush()
            except Exception as flush_error:
                log(f"StreamPump: flush failed: {flush_error}")


# ---------------------------------------------------------------------------
# Per-process pump registry
# ---------------------------------------------------------------------------

# One pump per subprocess, for the subprocess's whole life. Weak keys so
# test fakes and dead Popen objects don't accumulate. A dead pump is never
# replaced for a live process — the stream position is unknowable — the
# caller must respawn the process instead (landline.streaming enforces this).
_pumps: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_pumps_lock = threading.Lock()


def get_or_create_pump(proc: Any) -> StreamPump:
    with _pumps_lock:
        pump = _pumps.get(proc)
        if pump is None:
            pump = StreamPump(proc)
            _pumps[proc] = pump
        return pump
