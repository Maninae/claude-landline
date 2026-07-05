"""Persistent stdout pump for the long-lived Claude subprocess.

Invariant: the persistent Claude process's stdout has exactly ONE reader for
the LIFE of the process. It is a single stream shared by dispatched turns
(operator messages) AND unsolicited turns (harness-initiated background-task
completions: subagents, `run_in_background` Bash).

- Turn blocks are delimited by `system/init` ... `result`. Dispatched turns
  register a `TurnHandle` BEFORE their stdin write; the next `system/init` is
  attributed to it. Events route to the per-chat StreamSender AND accumulate
  on the handle; `result` completes the handle.
- Blocks with no registered handle are unsolicited: text routes to the chat
  sender IMMEDIATELY (background results reach the operator when they finish,
  not one message later).
- A registered handle is ALWAYS completed (result / EOF / read error), so
  dispatch can never wait on a turn the pump has abandoned.
- Attribution race: a background turn in the sub-second window between
  `register_turn` and the dispatched turn's `system/init` can swap handles.
  All text is delivered either way; the burst self-heals. Do NOT counter with
  task-notification counting — a miscount orphans a dispatched turn (hang),
  strictly worse than cosmetic skew.
- Watchdog note: `_touch()` bumps the pending turn's activity clock on EVERY
  event (any block) — deliberate OLD-code parity. Scoping it to the owned
  block would let a >CLAUDE_TIMEOUT background turn kill a healthy process
  while a dispatch waits behind it.

Symptom of violating the invariant (one-turn lag): sends A and gets nothing;
sends B and receives A's answer; every reply lags one turn until restart or
/new. Full desync narrative + attribution-race sharper edges:
docs/ARCHITECTURE.md § StreamPump.
"""

import json
import threading
import time
import weakref
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from landline.runtime.logging import log
from landline.claude.tool_status import _extract_text_blocks, _format_tool_status


class TurnHandle:
    """Bookkeeping for one dispatched turn.

    Registered with the pump BEFORE the stdin write; the pump completes it
    (``done.set()``) on the attributed block's `result`, or on EOF / read
    error — completion is unconditional so dispatch can't hang on an
    abandoned turn.
    """

    def __init__(self) -> None:
        self.done = threading.Event()
        # Session ids observed on init/result events; result-event id wins
        # (mirrors the old reader's precedence).
        self.init_session_id: Optional[str] = None
        self.result_session_id: Optional[str] = None
        self.final_result: str = ""
        self.saw_result: bool = False
        # Text deltas routed to the sender for this block — joined into
        # ClaudeStreamResult.streamed_text by the streaming layer.
        self.streamed_parts: List[str] = []
        # Set on read-error/EOF paths so the streaming layer surfaces the
        # same "Stream read error" result.error the old reader produced.
        self.error: Optional[str] = None
        # Interrupt watchdog: pump stops routing/accumulating this turn's
        # assistant events (parity with old ``interrupt_sent`` skip).
        # Unsolicited blocks are unaffected.
        self.interrupt_suppress = threading.Event()
        # Activity clock (single-cell list, same shape as the old reader) —
        # bumped on EVERY event while this handle is pending so the caller's
        # CLAUDE_TIMEOUT watchdog keeps working unchanged.
        self.last_active: List[float] = [time.time()]
        # Result-shape fields for the pruned-resume vs mid-session-error
        # discriminator (see landline.claude.predicates.looks_like_pruned_resume).
        # Pump-thread writes read by the streaming layer AFTER handle.done.wait().
        self.result_is_error: bool = False
        self.result_subtype: Optional[str] = None
        self.saw_init: bool = False
        # Optional accounting fields from the terminal `result` event;
        # safe defaults so hand-built TurnHandles in tests still work.
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
    if the pump thread dies while the process lives, the pipe read position
    is unknowable and the caller must kill/respawn (see landline.streaming).
    Producer-side state (``_pending``, ``_idle_route``) is lock-guarded;
    block state is pump-thread-only.
    """

    def __init__(self, proc: Any) -> None:
        self.proc = proc
        self._lock = threading.Lock()
        self._pending: Optional[TurnHandle] = None
        self._pending_sender: Optional[Any] = None
        # (chat_id, token, text_send_fn, status_send_fn) for unsolicited
        # blocks; refreshed on every dispatched turn.
        self._idle_route: Optional[Tuple[str, str, Callable, Callable]] = None
        self._read_error: Optional[str] = None
        self._init_anomaly_logged = False
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="claude-stream-pump",
        )
        self._thread.start()

    # Producer interface (dispatch thread).

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
        """Register the upcoming dispatched turn.

        Call BEFORE the stdin write so the turn's `system/init` can never
        race past an empty slot.
        """
        with self._lock:
            if self._pending is not None:
                # Dispatch is single-threaded; complete the orphan so nothing hangs.
                log("StreamPump: register_turn found a pending handle — "
                    "completing the orphan (dispatch overlap?)")
                self._pending.error = "orphaned by overlapping dispatch"
                self._pending.done.set()
            self._pending = handle
            self._pending_sender = sender
            handle.last_active[0] = time.time()
        if not self.alive:
            # Pump died between the caller's liveness check and now.
            self._complete_pending("stream pump is not running")

    def cancel_turn(self, handle: TurnHandle) -> None:
        """Unregister a turn whose stdin write failed (no block will arrive)."""
        with self._lock:
            if self._pending is handle:
                self._pending = None
                self._pending_sender = None
        handle.done.set()

    # Pump thread.

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
                    # reader — log and keep pumping. Drift from the old
                    # per-turn reader accepted: a sender that RAISES here
                    # is logged but no longer surfaces as result.error on
                    # the turn (grep "StreamPump event error" if a turn
                    # ever looks successful while nothing rendered).
                    log("StreamPump event error on %r: %s"
                        % (event.get("type"), event_error))
        except Exception as read_error:
            # Includes ValueError from the watchdog closing stdout mid-read
            # (interrupt / timeout / process-death) — same class of exit the
            # old per-turn reader surfaced as "Stream read error".
            self._read_error = str(read_error)
            log(f"Stream read error: {read_error}")
        finally:
            # EOF/error: flush a dangling unsolicited block's tail, ALWAYS
            # complete a pending handle (so dispatcher never hangs).
            if block is not None and block.handle is None:
                self._flush_block_sender(block)
            self._complete_pending(self._read_error)
            # Break the _pumps[proc] → pump → pump.proc → proc ref cycle
            # (WeakKeyDictionary entry would otherwise never collect →
            # one leaked Popen+pump per respawn for the daemon's life).
            # Pump is spent once _run exits; nothing reads self.proc after.
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
            # Flush partial text BEFORE done.set() — same sole-producer rule
            # as _close_block (see docs/ARCHITECTURE.md § H1).
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

    # Event handling (pump thread only).

    def _handle_event(self, block: Optional[_Block],
                      event: Dict) -> Optional[_Block]:
        etype = event.get("type")

        if etype == "system" and event.get("subtype") == "init":
            return self._open_block(block, event)

        if etype == "assistant":
            if block is None:
                # Assistant with no init — not observed; treat as start of an
                # unsolicited block so text is delivered rather than dropped.
                block = self._attribute_block()
            self._route_assistant(block, event)
            return block

        if etype == "result":
            return self._close_block(block, event)

        # rate_limit_event / system/thinking_tokens / task_started|updated|
        # notification / user tool_results — no routable content; _touch()
        # already counted it as activity.
        return block

    def _open_block(self, block: Optional[_Block],
                    event: Dict) -> _Block:
        if block is not None:
            # init while a block is open — previous turn never emitted result
            # (not observed). Keep the SAME handle so a dispatched turn is
            # never orphaned; flush an unsolicited tail.
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
            # Mark saw_init BEFORE recording session_id — ordering matches
            # the pruned-resume-vs-mid-session-error discriminator (mid-error
            # had init on this turn; pruned-resume never did).
            block.handle.saw_init = True
            sid = event.get("session_id")
            if sid:
                block.handle.init_session_id = sid
        return block

    def _attribute_block(self) -> _Block:
        """A new block goes to the pending dispatched turn if one is registered,
        else it is unsolicited and routes via the idle route."""
        with self._lock:
            handle = self._pending
            sender = self._pending_sender
            idle_route = self._idle_route
        if handle is not None:
            return _Block(handle, sender)
        return _Block(None, self._idle_sender(idle_route))

    def _idle_sender(self, idle_route: Optional[Tuple]) -> Optional[Any]:
        if idle_route is None:
            # No dispatched turn has ever run on this process; not reachable
            # in practice (fresh process emits no events pre-first-message).
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
            # Bare result with no open block: anomalous OR the verified
            # pruned/nonexistent --resume shape (bare result with is_error=true
            # + subtype=error_during_execution, no preceding system/init). If a
            # dispatch is pending, record the is_error signal so
            # looks_like_pruned_resume can observe it, then complete the handle.
            log("StreamPump: result event with no open turn block")
            with self._lock:
                pending = self._pending
            if pending is not None:
                if event.get("is_error") is True:
                    pending.result_is_error = True
                subtype = event.get("subtype")
                if subtype is not None:
                    pending.result_subtype = subtype
            # No saw_result on this path — _complete_pending's error branch
            # is gated on ``not saw_result``.
            self._complete_pending("result event with no open turn block")
            return None

        handle = block.handle
        if handle is not None:
            handle.final_result = event.get("result", "") or ""
            sid = event.get("session_id")
            if sid:
                handle.result_session_id = sid
            handle.saw_result = True
            # Capture is_error for the pruned-resume discriminator. An
            # unsolicited is_error is discarded (falls through below).
            if event.get("is_error") is True:
                handle.result_is_error = True
            handle.result_subtype = event.get("subtype")
            self._capture_usage_fields(event, handle)
            # SOLE-PRODUCER rule: final-tail + turn-boundary flush run HERE
            # on the pump thread, BEFORE done.set(). If the dispatch thread
            # appended the tail after done.wait(), a back-to-back unsolicited
            # block already in the pipe would race text between this turn's
            # deltas and the tail — welding background text into this bubble
            # and orphaning the tail into the next. See docs/ARCHITECTURE.md
            # § H1 (100% reproducible with back-to-back events).
            try:
                # done.set() below is unconditional — a sender defect must
                # not leave the handle pending (would hang dispatch until
                # the CLAUDE_TIMEOUT watchdog).
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

        # Unsolicited block: if model's text only arrived in the result
        # payload, deliver it; then mark the bubble boundary.
        final_text = (event.get("result", "") or "").strip()
        if final_text and not block.routed_any_text and block.sender is not None:
            block.sender.text(final_text)
        self._flush_block_sender(block)
        # Attribute background token cost to a separate bucket in the daily
        # aggregate so "my messages" cost is distinguishable from background.
        # Lazy import + fully swallowed — fire-and-forget from the pump.
        self._record_unsolicited_usage(event)
        return None

    @staticmethod
    def _capture_usage_fields(event: Dict, handle: TurnHandle) -> None:
        """Copy optional accounting fields from a ``result`` event onto the handle.

        Fields: ``usage`` (in/out/cache tokens), ``modelUsage`` (per-model),
        ``total_cost_usd``, ``num_turns``, ``duration_ms``. Defensive
        isinstance checks — CC event shapes drift and a missing/renamed key
        must never crash the pump thread.
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
        """Persist an unsolicited (background) turn's usage into the daily aggregate.

        - Fire-and-forget: fully wrapped so a broken usage_stats never affects
          the pump's routing. Lazy import keeps pump import-time side-effect-free.
        - Dispatched to a daemon thread — ``usage_stats.record_turn`` does a
          synchronous ``os.fsync`` inside its module-level ``_lock``; calling
          it on the pump thread would let a competing dispatch-thread
          ``record_turn`` (SSD stall) back-pressure the stdout pipe and break
          the "one reader for the process's life" invariant. See
          docs/ARCHITECTURE.md § StreamPump.
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
                from landline.runtime import usage_stats
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
            # Thread creation failed (resource exhaustion). Pump keeps going
            # — losing one unsolicited bucket update beats stalling the pipe.
            log(
                f"usage_stats.record_turn (unsolicited) thread spawn "
                f"failed: {spawn_error}"
            )

    @staticmethod
    def _append_final_tail(block: _Block, handle: TurnHandle) -> None:
        """Emit any tail-only portion of the result payload as a final delta.

        If the result payload extends (or replaces) the streamed deltas, send
        the missing tail so Telegram shows the full reply. Runs on the pump
        thread — sole-producer rule (see ``_close_block``). Updates
        ``handle.streamed_parts`` so ``"".join(...)`` equals the old
        ``result.streamed_text`` in every branch.
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


# One pump per subprocess for its whole life. Weak keys so test fakes and
# dead Popen objects don't accumulate. A dead pump is never replaced for a
# live process — stream position is unknowable → caller must respawn (see
# landline.claude.streaming).
_pumps: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_pumps_lock = threading.Lock()


def get_or_create_pump(proc: Any) -> StreamPump:
    with _pumps_lock:
        pump = _pumps.get(proc)
        if pump is None:
            pump = StreamPump(proc)
            _pumps[proc] = pump
        return pump
