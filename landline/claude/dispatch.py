"""Claude call lifecycle — rate limiting, backoff, invocation, and finalization.

- Rate-limit + drain backoff queue → gate on failure backoff → invoke with
  stale/pruned-resume retry → finalize (outcome / paused notice / session-id
  reconcile / usage stats / completion 👌 / context threshold warn).
- Session-id ordering: pc updated BEFORE state, state mirrored BEFORE save;
  see docs/ARCHITECTURE.md "Session id — single source of truth".
"""

import collections
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from landline import config
from landline.config import AGENT_NAME, CONTEXT_WARN_THRESHOLDS, RATE_LIMIT_SECONDS
from landline.runtime.inject import commit_inject_queue
from landline.runtime.logging import log
from landline.runtime.state import (
    get_context_percent,
    log_conversation,
    read_recent_conversation_history,
    save_state,
)
from landline.telegram import send_html
from landline.claude.types import ClaudeStreamResult
from landline.telegram.fmt import italic

# Re-imported so ``patch("landline.claude.dispatch.<pred>")`` and existing
# ``from landline.claude.dispatch import <pred>`` sites keep resolving after
# predicates moved to ``landline.claude.predicates``.
from landline.claude.predicates import (  # noqa: F401
    is_result_successful,
    looks_like_stale_session,
    _stderr_looks_like_auth_failure,
    looks_like_pruned_resume,
)


class ClaudeDispatcher:
    """Manages the Claude call lifecycle: rate limiting, backoff, invocation
    with stale/pruned-resume retry, and response finalization.

    The orchestrator owns a single instance and calls ``send_to_claude`` per
    coalesced message batch.
    """

    def __init__(
        self,
        token: str,
        state: Dict[str, Any],
        failure_tracker: Any,
        shutdown_hook: Any,
        run_claude_fn: Callable[..., ClaudeStreamResult],
        send_response_fn: Callable[[str, str, str], None],
        send_typing_fn: Callable[[str, str], None],
        pause_flag: Any = None,
        clear_pause_fn: Optional[Callable[[], None]] = None,
    ) -> None:
        self._token = token
        self._state = state
        self._failure_tracker = failure_tracker
        self._shutdown_hook = shutdown_hook
        self._run_claude = run_claude_fn
        self._send_response = send_response_fn
        self._send_typing = send_typing_fn
        # Sole interrupt mechanism. Orchestrator always wires one; direct-
        # constructing tests must pass one too.
        self._pause_flag = pause_flag
        # Orchestrator's _pause_requested.clear. None default for tests that
        # don't exercise the interrupted-clear path.
        self._clear_pause_fn: Optional[Callable[[], None]] = clear_pause_fn
        self._backoff_queue: collections.deque = collections.deque(maxlen=20)
        self.last_process_time: float = 0.0
        self.running: bool = True
        # Flips True after first send_to_claude seeds pc from the state dict.
        # Idempotent — never clobbers a sid pc already learned from a stream event.
        self._pc_seeded: bool = False
        # One-shot latch for the "Claude auth expired" iMessage; cleared on
        # the next successful stream. Single-writer (_record_outcome) — no lock.
        self._auth_alert_sent: bool = False
        self._last_auth_alert_at: float = 0.0
        # Per-call scratch for the message_ids that earned 👀 at classify time;
        # _finalize_response fires 👌 on the same ids on a successful turn.
        self._pending_ack_message_ids: List[int] = []
        self._pending_ack_chat_id: str = ""

    def send_to_claude(
        self,
        text: str,
        chat_id: str,
        consumed_paths: Optional[List[Path]] = None,
        ack_message_ids: Optional[List[int]] = None,
    ) -> bool:
        """Top-level dispatch — rate limit, backoff gate, invoke, finalize.

        Args:
            consumed_paths: inject-queue files that produced the prepended
                context. Committed (unlinked) only AFTER a real Claude call —
                never on the gated/queued path, so a daemon death mid-backoff
                doesn't drop reports on the floor.
            ack_message_ids: Telegram message_ids 👀'd at classify time.
                Stashed for ``_finalize_response`` to fire 👌 on a successful
                turn. Reset at entry; on backoff-queue path they ride the
                queue tuple so a later drain still fires 👌.

        Returns:
            True if the call reached ``_invoke_claude_call`` (watchdog +
            _finalize_response had a chance to consume the pause flag).
            False if backoff-gated (text stashed on queue, no Claude call).
            Caller uses this to decide whether a deferred /pause is stranded
            — see ``orchestrator._inject_and_dispatch``.
        """
        self._pending_ack_message_ids = list(ack_message_ids or [])
        self._pending_ack_chat_id = chat_id
        self._seed_pc_session_from_state_once()
        accumulated_paths: List[Path] = list(consumed_paths or [])
        text, drained_paths, drained_ack_ids = (
            self._apply_rate_limit_and_drain_backoff(text, chat_id)
        )
        accumulated_paths.extend(drained_paths)
        # Drained-queue entries were 👀'd when originally queued; their 👌
        # fires on THIS finalize. Prepend so ordering matches [queued 1..N, current].
        if drained_ack_ids:
            self._pending_ack_message_ids = (
                list(drained_ack_ids) + self._pending_ack_message_ids
            )
        if self._gate_if_in_backoff(
            text, chat_id, accumulated_paths,
            list(self._pending_ack_message_ids),
        ):
            return False
        result = self._invoke_with_stale_retry(text, chat_id)
        self._finalize_response(result, chat_id)
        # Commit AFTER finalize: the stdin write happened even on error/
        # interrupt, so we don't want to re-inject next turn. Don't re-raise
        # commit failures — a transient failure would re-prepend reports
        # on every subsequent turn.
        if accumulated_paths:
            try:
                commit_inject_queue(accumulated_paths)
            except Exception as commit_error:
                log(
                    f"Failed to commit inject queue (paths will remain, "
                    f"may re-inject next turn): {commit_error}"
                )
        return True

    def _apply_rate_limit_and_drain_backoff(
        self, text: str, chat_id: str,
    ) -> Tuple[str, List[Path], List[int]]:
        """Enforce min spacing between calls; drain queued messages from a prior
        backoff period into the current text.

        Only entries matching ``chat_id`` are drained — cross-chat entries
        stay queued (only one chat is allowlisted today, but the guard is
        cheap insurance).

        Returns:
            (merged_text, inherited_inject_paths, inherited_ack_ids).
            Caller extends ``_pending_ack_message_ids`` with the ack ids so
            👌 fires on all drained messages at the successful finalize.
        """
        elapsed_since_last_process = time.time() - self.last_process_time
        if elapsed_since_last_process < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed_since_last_process)

        drained_paths: List[Path] = []
        drained_ack_ids: List[int] = []
        if self._backoff_queue and not self._failure_tracker.is_in_backoff():
            kept: collections.deque = collections.deque(
                maxlen=self._backoff_queue.maxlen,
            )
            queued_texts: List[str] = []
            for entry in self._backoff_queue:
                queued_text, queued_chat, queued_paths, queued_ack_ids = (
                    self._unpack_entry(entry)
                )
                if queued_chat != chat_id:
                    kept.append(entry)
                    continue
                queued_texts.append(queued_text)
                drained_paths.extend(queued_paths)
                drained_ack_ids.extend(queued_ack_ids)
            self._backoff_queue = kept
            if kept:
                log(
                    f"Backoff drain skipped {len(kept)} cross-chat entry(ies) "
                    "— leaving queued"
                )
            if queued_texts:
                queued_texts.append(text)
                if len(queued_texts) > 1:
                    log(
                        f"Draining {len(queued_texts) - 1} queued message(s) "
                        "from backoff period"
                    )
                    text = "\n---\n".join(
                        f"[queued message {i+1}]\n{t}"
                        for i, t in enumerate(queued_texts)
                    )
        return text, drained_paths, drained_ack_ids

    def _gate_if_in_backoff(
        self,
        text: str,
        chat_id: str,
        consumed_paths: List[Path],
        ack_message_ids: Optional[List[int]] = None,
    ) -> bool:
        """Queue + notify if Claude is in failure backoff; return True if gated.

        - ``consumed_paths`` ride the queue tuple → committed only when the
          message is finally dispatched.
        - ``ack_message_ids`` also ride the queue so a later drain fires 👌
          on every message that earned 👀 during the outage (preserves the
          "every 👀 gets a matching 👌 on success" invariant).
        """
        if not self._failure_tracker.is_in_backoff():
            return False

        remaining_backoff_seconds = int(
            self._failure_tracker.seconds_until_next_attempt()
        )
        log(
            f"Claude backoff active "
            f"({self._failure_tracker.consecutive_failure_count} "
            f"consecutive failures, ~{remaining_backoff_seconds}s "
            f"remaining) — queueing message for retry"
        )
        try:
            self._send_response(
                self._token, chat_id,
                f"(Got your message. Claude is temporarily unavailable "
                f"— will auto-retry in ~{remaining_backoff_seconds}s.)",
            )
        except Exception as backoff_notify_error:
            log(f"Failed to send backoff notice: {backoff_notify_error}")

        self._backoff_queue.append((
            text, chat_id,
            list(consumed_paths),
            list(ack_message_ids or []),
        ))
        self.last_process_time = time.time()
        return True

    @staticmethod
    def _unpack_entry(
        entry: Tuple,
    ) -> Tuple[str, str, List[Path], List[int]]:
        """Normalise a backoff-queue entry to (text, chat_id, paths, ack_ids).

        Canonical shape is the 4-tuple; older 2/3-tuple shapes may appear in
        tests / hand-populated state. Normalising here keeps callers branch-free.
        """
        if len(entry) == 2:
            text, chat_id = entry
            return text, chat_id, [], []
        if len(entry) == 3:
            text, chat_id, paths = entry
            return text, chat_id, list(paths or []), []
        text, chat_id, paths, ack_ids = entry
        return text, chat_id, list(paths or []), list(ack_ids or [])

    def _invoke_with_stale_retry(
        self, text: str, chat_id: str,
    ) -> ClaudeStreamResult:
        """Invoke Claude with history injection; retry on stale/pruned-resume.

        Falls back to a fresh session with history context if --resume
        produces no output or the pruned-resume shape.
        """
        self._send_typing(self._token, chat_id)

        from landline.claude import _get_persistent_claude
        pc = _get_persistent_claude()
        current_session_id = pc.get_session_id()
        is_new_session = current_session_id is None
        watchdog = self._start_response_watchdog(chat_id)

        # try/finally: watchdog Timer is ALWAYS cancelled — else a raised
        # exception leaks the Timer → spurious "(Still working...)" message
        # after the real response.
        try:
            effective_text = self._inject_history(text, is_new_session)
            result = self._invoke_claude_call(
                effective_text, chat_id, current_session_id,
                is_new=is_new_session,
                suppress_empty=(not is_new_session),
            )

            if (
                not is_new_session
                and self.running
                and (
                    looks_like_stale_session(result)
                    or looks_like_pruned_resume(result)
                )
            ):
                result = self._retry_with_fresh_session(text, chat_id)

            return result
        finally:
            watchdog.cancel()

    def _retry_with_fresh_session(
        self, text: str, chat_id: str,
    ) -> ClaudeStreamResult:
        """Reset session state and retry with a fresh session.

        Ordering: pc (source of truth) FIRST, then mirror into the state dict.
        """
        from landline.claude import _get_persistent_claude
        pc = _get_persistent_claude()
        stale_display = (pc.get_session_id() or "")[:12]
        log(f"--resume failed for session {stale_display}... — falling back to fresh")
        self._send_response(
            self._token, chat_id, "(Previous session expired, starting fresh.)",
        )
        pc.set_session_id(None)
        self._state["session_id"] = None
        self._state["turn_count"] = 0
        self._state.pop("_context_warned_at", None)
        save_state(self._state)

        effective_text = self._inject_history(text, is_new=True)
        return self._invoke_claude_call(
            effective_text, chat_id, None, is_new=True, suppress_empty=False,
        )

    def _inject_history(self, text: str, is_new: bool) -> str:
        """Prepend conversation history to text for new/fresh sessions."""
        if not is_new:
            return text
        history = read_recent_conversation_history()
        if history:
            log("Injected conversation history into new session")
            return history + "\n\n---\n\n" + text
        return text

    def _finalize_response(
        self, result: ClaudeStreamResult, chat_id: str,
    ) -> None:
        """Record outcome, persist session, log conversation, notify context.

        - Call order below IS the finalize contract — do NOT reorder.
        - Session-id ordering (pc → state → save) lives in ``_reconcile_session_id``.
        """
        self._record_outcome(result, chat_id)
        self._route_paused_notice(result, chat_id)
        self._reconcile_session_id(result)
        self._record_usage_stats_from(result)
        self._fire_completion_reactions(result)
        self._warn_context_threshold(chat_id)
        self.last_process_time = time.time()

    def _route_paused_notice(
        self, result: ClaudeStreamResult, chat_id: str,
    ) -> None:
        """Enqueue "(Paused.)" behind the interrupted turn's draining bubbles."""
        if result.interrupted:
            # Route via the chat's ordered queue so "(Paused.)" lands AFTER
            # draining bubbles. Direct-send fallback if there's no live sender.
            # Lazy facade import (test patch surface + breaks circular).
            from landline.claude import try_enqueue_or_send
            token = self._token
            try_enqueue_or_send(
                chat_id,
                html=italic("(Paused.)"),
                direct_fn=lambda body: send_html(token, chat_id, body),
            )
            if self._clear_pause_fn is not None:
                self._clear_pause_fn()

    def _reconcile_session_id(self, result: ClaudeStreamResult) -> None:
        """Update pc's session id from the result, mirror into state, save.

        Load-bearing ordering: pc → state dict → save_state. An interrupted /
        exit-143 turn must still land a persisted snapshot matching pc (the
        source of truth), else stale-by-default clobbers a valid session.
        """
        from landline.claude import _get_persistent_claude
        pc = _get_persistent_claude()
        current_session_id = pc.get_session_id()
        if (
            result.session_id
            and result.session_id != current_session_id
            and is_result_successful(result)
        ):
            pc.set_session_id(result.session_id)
            log(f"Session: {result.session_id[:12]}...")

        # Mirror pc → dict even when result carries no new sid (interrupted
        # turn on an existing session must still reflect pc, not stale-by-default).
        self._state["session_id"] = pc.get_session_id()

        if not result.interrupted:
            self._state["turn_count"] = int(self._state.get("turn_count", 0)) + 1
        save_state(self._state)

    def _record_usage_stats_from(self, result: ClaudeStreamResult) -> None:
        """Record daily usage/cost from a successful turn; log a reply snippet.

        Only on successful (non-interrupted, non-error) turns — interrupts/
        failures don't carry valid accounting, and counting them would double-
        count on the fresh-session retry. Defensively wrapped so a future
        raising refactor of usage_stats can't corrupt finalize.
        """
        if not result.interrupted and is_result_successful(result):
            try:
                from landline.runtime import usage_stats
                usage_stats.record_turn(
                    result_usage=result.result_usage,
                    result_model_usage=result.result_model_usage,
                    total_cost_usd=result.result_total_cost_usd,
                    duration_ms=result.result_duration_ms,
                    dispatched=True,
                )
            except Exception as stats_error:
                log(f"usage_stats.record_turn failed: {stats_error}")

        if result.streamed_text.strip():
            snippet = result.streamed_text[:200]
            if len(result.streamed_text) > 200:
                snippet += "..."
            log_conversation(AGENT_NAME, snippet)

    def _fire_completion_reactions(self, result: ClaudeStreamResult) -> None:
        """Fire completion 👌 batch on the ids 👀'd at classify time.

        Successful turns only. Interrupted → leave 👀 as a persistent "you
        paused this" visual. Failed → leave 👀 so the user knows nothing
        landed. Fire-and-forget with an outer try/except so a future
        raising refactor of reactions can never corrupt finalize.
        """
        if (
            not result.interrupted
            and is_result_successful(result)
            and self._pending_ack_message_ids
            and self._pending_ack_chat_id
        ):
            try:
                from landline.telegram import reactions
                reactions.set_reactions_batch_async(
                    self._token,
                    self._pending_ack_chat_id,
                    list(self._pending_ack_message_ids),
                    config.REACTION_DONE_EMOJI,
                )
            except Exception as reaction_error:
                log(
                    "completion reaction dispatch failed (finalize "
                    "continues): %s" % reaction_error
                )

    def _warn_context_threshold(self, chat_id: str) -> None:
        """Send the one-shot context-usage heads-up on threshold crossing."""
        last_warned = self._state.get("_context_warned_at", 0)
        next_thresholds = [t for t in CONTEXT_WARN_THRESHOLDS if t > last_warned]
        if next_thresholds:
            pct = get_context_percent(self._state.get("session_id"))
            if pct is not None:
                crossed = [t for t in next_thresholds if pct >= t]
                if crossed:
                    self._state["_context_warned_at"] = crossed[-1]
                    save_state(self._state)
                    # Ordered behind the turn's bubbles; direct fallback when
                    # no live sender exists.
                    from landline.claude import try_enqueue_or_send
                    warning = (
                        f"*Heads up: context is at {pct:.0f}% of 1M window. "
                        "Send /new if you want a fresh start.*"
                    )
                    token = self._token
                    send_response = self._send_response
                    try_enqueue_or_send(
                        chat_id,
                        text=warning,
                        direct_fn=lambda body: send_response(token, chat_id, body),
                    )

    def _seed_pc_session_from_state_once(self) -> None:
        """Lazy one-time seed of PersistentClaude from the state dict.

        Runs at the top of send_to_claude. After this, pc is the source of
        truth; state-dict reads are forbidden for session-id decisions.
        Lazy import + swallow — avoids circular import with landline.claude
        and never lets a singleton hiccup crash the dispatch.
        """
        if self._pc_seeded:
            return
        self._pc_seeded = True
        try:
            from landline.claude import _get_persistent_claude
            pc = _get_persistent_claude()
            if pc.get_session_id() is not None:
                # pc already knows — don't clobber.
                return
            seed = self._state.get("session_id")
            if seed:
                pc.set_session_id(seed)
        except Exception as seed_error:
            log(f"persistent claude session seed failed: {seed_error}")

    def _invoke_claude_call(
        self,
        text: str,
        chat_id: str,
        session_id: Optional[str],
        is_new: bool,
        suppress_empty: bool,
    ) -> ClaudeStreamResult:
        """Wrap run_claude_fn with exception handling.

        Bumps the PauseFlag generation and gives the watchdog a closure that
        only honors a pause at this generation. Orchestrator always wires a
        PauseFlag; constructing without one is a programmer error.
        """
        assert self._pause_flag is not None, (
            "ClaudeDispatcher requires a pause_flag — pass one at construction"
        )
        my_generation = self._pause_flag.new_call()
        pf = self._pause_flag
        # Re-anchor a pre-existing pause request to the new generation.
        # Otherwise two bugs collapse: (1) interrupt_check() checks
        # is_requested(my_generation), returns False for a request stranded
        # at the previous generation → watchdog never fires, /pause silently
        # dropped. (2) The level-triggered _event is still set →
        # _wait_for_done_or_pause returns True every 0.5s tick → 100% CPU
        # busy-loop for the whole call. request_pause() re-records
        # _requested_gen at the current generation; safe on a cleared flag.
        if pf.is_set():
            pf.request_pause()

        def interrupt_check() -> bool:
            return pf.is_requested(my_generation)

        try:
            return self._run_claude(
                token=self._token,
                chat_id=chat_id,
                message=text,
                session_id=session_id,
                is_new=is_new,
                suppress_empty_response_notice=suppress_empty,
                shutdown_hook=self._shutdown_hook,
                interrupt_check=interrupt_check,
                pause_flag=pf,
            )
        except Exception as invocation_error:
            log(f"run_claude_streaming raised: {invocation_error}")
            failure_result = ClaudeStreamResult()
            failure_result.error = f"invocation exception: {invocation_error}"
            return failure_result

    def _record_outcome(self, result: ClaudeStreamResult, chat_id: str) -> None:
        """Update the failure tracker based on the Claude call result."""
        if result.interrupted:
            return
        if is_result_successful(result):
            self._failure_tracker.record_success()
            # Successful stream proves auth is back; reset the alert latch
            # so a fresh incident triggers a new alert.
            self._auth_alert_sent = False
            return

        # SIGTERM/SIGINT (daemon restart / launchctl bootout) exit codes:
        # 143/130 (shell 128+sig) or -15/-2 (subprocess "killed-by-signal").
        # Shutdown, not failure — recording would trigger false backoff.
        if result.exit_code in (143, 130, -15, -2):
            log(
                "Claude exited via signal (exit %s) — treating as shutdown, "
                "not a failure" % result.exit_code
            )
            return

        self._failure_tracker.record_failure()
        log(
            f"Claude call failed (streak="
            f"{self._failure_tracker.consecutive_failure_count})"
        )

        if self._failure_tracker.should_send_alert_now():
            self._failure_tracker.mark_alert_sent()
            try:
                self._send_response(
                    self._token, chat_id,
                    "Claude is unavailable right now — message me again "
                    "later and I'll retry then.",
                )
            except Exception as alert_error:
                log(f"Failed to send Claude-unavailable alert: {alert_error}")

        # Auth-expiry alert runs AFTER record_failure so the streak reflects
        # this turn. Decoupled from the in-band "Claude unavailable" (10th
        # failure) — this fires out-of-band on the FIRST OAuth-stderr match
        # so the operator learns about a multi-day outage on turn 1, not 10.
        # See docs/ARCHITECTURE.md "June 2026 auth-expiry outage".
        if _stderr_looks_like_auth_failure(result.stderr_tail):
            self._maybe_send_auth_expiry_alert()

    def _maybe_send_auth_expiry_alert(self) -> None:
        """Fire the one-shot iMessage auth-expiry alert.

        - Latched via ``_auth_alert_sent``; cleared on next successful stream.
        - 6h ``CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS`` floor as defense-in-
          depth; MUST run unconditionally — a fail→success→fail cycle resets
          the latch on success, so gating the floor behind the latch would
          skip it on the second failure and spam a second alert milliseconds
          after the first.
        - Fire-and-forget: send_health_alert spawns a daemon thread. Lazy
          import avoids the landline.claude cycle.
        """
        now = time.time()
        if (
            self._last_auth_alert_at > 0.0
            and now - self._last_auth_alert_at
            < config.CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS
        ):
            return
        try:
            from landline.runtime.notifications import send_health_alert
            import socket as _socket
            started = send_health_alert(
                subject="claude-auth-expired",
                body=(
                    f"[{AGENT_NAME}] Claude auth expired — re-login needed "
                    f"on {_socket.gethostname()}. Headless claude -p jobs "
                    f"may be failing with 401s."
                ),
            )
            if started:
                self._auth_alert_sent = True
                self._last_auth_alert_at = time.time()
                log("Claude auth-expiry alert dispatched (latched)")
        except Exception as alert_error:
            log(f"Auth-expiry alert failed to dispatch: {alert_error}")

    def _start_response_watchdog(self, chat_id: str) -> threading.Timer:
        """Fire a 'still working' message after 60s of no response."""
        def _send_still_working() -> None:
            try:
                # Fires mid-turn when bubbles may already be queued — route
                # via ordered queue so it doesn't race ahead. Direct fallback.
                from landline.claude import try_enqueue_or_send
                token = self._token
                try_enqueue_or_send(
                    chat_id,
                    html=italic("(Still working on your message...)"),
                    direct_fn=lambda body: send_html(token, chat_id, body),
                )
            except Exception:
                pass

        watchdog = threading.Timer(60, _send_still_working)
        watchdog.daemon = True
        watchdog.start()
        return watchdog
