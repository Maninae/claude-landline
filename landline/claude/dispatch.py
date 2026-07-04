"""Claude call lifecycle — rate limiting, backoff, invocation, and finalization.

Decomposed from the original ~160-line _send_coalesced_text_to_claude method
into focused sub-methods, each under 40 lines.
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

# Re-imported so existing patch strings on ``landline.claude.dispatch.<pred>``
# and existing ``from landline.claude.dispatch import <pred>`` sites keep
# resolving after the predicates moved to ``landline.claude.predicates``.
from landline.claude.predicates import (  # noqa: F401
    is_result_successful,
    looks_like_stale_session,
    _stderr_looks_like_auth_failure,
    looks_like_pruned_resume,
)


class ClaudeDispatcher:
    """Manages the Claude call lifecycle: rate limiting, backoff gating,
    invocation with stale-session retry, and response finalization.

    The orchestrator owns a single ClaudeDispatcher instance and calls
    send_to_claude() for each coalesced message batch.
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
        # PauseFlag is the sole interrupt mechanism. The orchestrator always
        # wires one; tests that construct the dispatcher directly must pass
        # one too.
        self._pause_flag = pause_flag
        # Callback to clear the pause flag (= orchestrator._pause_requested.clear).
        # Passed at construction by the orchestrator; None default keeps unit
        # tests that don't exercise the interrupted-clear path simple.
        self._clear_pause_fn: Optional[Callable[[], None]] = clear_pause_fn
        self._backoff_queue: collections.deque = collections.deque(maxlen=20)
        self.last_process_time: float = 0.0
        self.running: bool = True
        # Flips True after the first send_to_claude call seeds
        # PersistentClaude from the persisted state dict. Guarantees the
        # seed is idempotent and does not bypass a sid that pc may already
        # have learned from a stream event under a competing call.
        self._pc_seeded: bool = False
        # Cluster 3: one-shot latch for the "Claude auth expired" iMessage.
        # Flips True on the first stderr-matched failure; cleared on the
        # next successful stream. Only read/written from _record_outcome
        # (single dispatch thread) so no lock is required.
        self._auth_alert_sent: bool = False
        self._last_auth_alert_at: float = 0.0
        # Cluster 3 (reactions): per-call scratch for the message_ids that
        # earned a 👀 during classification. ``send_to_claude`` stashes them
        # at entry and ``_finalize_response`` fires 👌 on the same ids on a
        # successful (non-interrupted, non-error) turn.
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

        ``consumed_paths`` are inject-queue files that produced the prepended
        context in ``text``. They are committed (unlinked) only AFTER the text
        actually reaches a real Claude call — never on the gated/queued path,
        so a daemon death mid-backoff doesn't drop reports on the floor.

        Cluster 3: ``ack_message_ids`` is the list of Telegram message_ids
        that were 👀-acked during classification for this dispatch. Stashed
        on the instance so ``_finalize_response`` can fire 👌 on the same
        ids on a successful (non-interrupted) turn. Cleared at entry so a
        gated/backoff-queued call doesn't carry stale ids into the next
        real call.

        Returns:
            True if the message actually reached ``_invoke_claude_call``
            (i.e. a real Claude call happened, so the pause flag was
            given a chance to be consumed by the watchdog +
            ``_finalize_response``). False if the send was gated by
            failure backoff and the text was stashed on the backoff
            queue — no Claude call happened, and pause-flag consumption
            did NOT run. Callers use this to decide whether the batch's
            deferred /pause is still stranded (see
            ``orchestrator._inject_and_dispatch`` /
            ``_consume_stranded_pause_flag``).
        """
        # Cluster 3: reset pending-ack state at entry so each call starts
        # fresh. The state is only consumed inside _finalize_response of
        # this same call. On the gated/backoff-queued path we carry these
        # ids INTO the queue tuple so a later drain fires 👌 on every
        # message that earned 👀 during the outage.
        self._pending_ack_message_ids = list(ack_message_ids or [])
        self._pending_ack_chat_id = chat_id
        self._seed_pc_session_from_state_once()
        accumulated_paths: List[Path] = list(consumed_paths or [])
        text, drained_paths, drained_ack_ids = (
            self._apply_rate_limit_and_drain_backoff(text, chat_id)
        )
        accumulated_paths.extend(drained_paths)
        # Cluster 3 hardening: the drained queue entries were 👀-acked
        # when they were originally queued; the 👌 for them fires on
        # THIS finalize (the successful call that finally delivers their
        # merged text). Prepend so ordering matches [queued 1..N,
        # current].
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
        # Commit AFTER finalize so the message has truly been handed off to a
        # real Claude call. Even on error/interrupt the stdin write happened,
        # so we don't want to re-inject the same reports next turn.
        # Guard against disk errors here: if commit fails, log it but DO NOT
        # re-raise — otherwise a transient failure would leave the just-injected
        # reports in the queue and re-prepend them on every subsequent turn.
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
        """Enforce minimum spacing between calls and drain any queued messages
        from a prior backoff period into the current text.

        Only entries matching ``chat_id`` are drained — leaving cross-chat
        entries queued (today only one chat is allowlisted, so in practice
        nothing remains, but the guard is cheap insurance). Returns the
        merged text, the list of inject-queue paths inherited from the
        drained entries, and the list of Telegram message_ids that were
        👀-acked when the drained entries were originally queued (Cluster 3:
        the caller extends _pending_ack_message_ids with these so 👌 fires
        on all of them at the successful finalize).
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
        """If Claude is in failure backoff, queue the message and notify the
        user.  Returns True if gated (caller should return early).

        ``consumed_paths`` ride along on the queue tuple — committed only when
        the message is eventually drained and actually dispatched.

        Cluster 3 hardening: ``ack_message_ids`` are the Telegram message_ids
        that received 👀 during classification for this dispatch. They ride
        along on the queue tuple so a later drain fires 👌 on every message
        that earned 👀 during the outage — preserves the "every 👀 gets a
        matching 👌 on success" invariant.
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
        """Backoff entries are stored as
        ``(text, chat_id, paths, ack_message_ids)`` (Cluster 3), but older
        2- and 3-tuple shapes may still appear in tests / hand-populated
        state. Normalise here so callers don't branch."""
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
        """Invoke Claude with history injection. If --resume produces no output
        (stale session), fall back to a fresh session with history context."""
        self._send_typing(self._token, chat_id)

        from landline.claude import _get_persistent_claude
        pc = _get_persistent_claude()
        current_session_id = pc.get_session_id()
        is_new_session = current_session_id is None
        watchdog = self._start_response_watchdog(chat_id)

        # try/finally ensures the watchdog Timer is ALWAYS cancelled — without
        # this, an exception below would leak the Timer and the user would get
        # a spurious "(Still working...)" message after they already received
        # (or failed to receive) the real response.
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
        """Reset session state and retry Claude with a fresh session."""
        from landline.claude import _get_persistent_claude
        pc = _get_persistent_claude()
        stale_display = (pc.get_session_id() or "")[:12]
        log(f"--resume failed for session {stale_display}... — falling back to fresh")
        self._send_response(
            self._token, chat_id, "(Previous session expired, starting fresh.)",
        )
        # Source-of-truth write FIRST, then mirror into the persisted dict.
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
        """Record outcome, persist session state, log conversation, suggest /new.

        Extract-method decomposition: each sub-method's body is the moved
        code verbatim. The call order below IS the finalize contract — do
        not reorder. Session-id ordering (pc updated before state, state
        mirrored before save) lives inside ``_reconcile_session_id``.
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
            # Route through the chat's ordered queue so "(Paused.)" lands AFTER
            # any bubbles still draining from the interrupted turn — not ahead
            # of them. Fall back to a direct send if there's no live sender.
            # Lazy import avoids a circular import with landline.claude;
            # the facade is the patch surface for tests.
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

        Ordering invariant (load-bearing): pc.set_session_id runs BEFORE the
        state-dict mirror, and the mirror runs BEFORE save_state. An
        interrupted / exit-143 turn must still land the persisted snapshot
        matching pc's source-of-truth session id.
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

        # Always mirror pc's current sid into the dict before save_state, so
        # the persisted snapshot matches the source of truth even when the
        # result did not carry a new sid (e.g. an interrupted turn that ran
        # on an existing session — the dict must still reflect pc, not
        # stale-by-default).
        self._state["session_id"] = pc.get_session_id()

        if not result.interrupted:
            self._state["turn_count"] = int(self._state.get("turn_count", 0)) + 1
        save_state(self._state)

    def _record_usage_stats_from(self, result: ClaudeStreamResult) -> None:
        """Record daily usage/cost from a successful turn and log the reply snippet."""
        # Cluster 4: record usage/cost into the daily aggregate on a
        # genuinely successful turn (never on interrupt / failure — those
        # do not carry a valid `result` event's accounting fields, and
        # counting them would double-count on the fresh-session retry
        # that follows). Fire-and-forget: usage_stats.record_turn already
        # swallows all its own exceptions, but wrap defensively so a
        # future refactor that lets it raise can never corrupt finalize
        # (save_state + log_conversation have already run above).
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
        """Send the completion 👌 batch for the ids that were 👀'd at classify time."""
        # Cluster 3: fire completion 👌 on the ids we 👀'd at classify
        # time, but ONLY on a genuinely successful turn — not on
        # interrupted (leave 👀 as a persistent "you paused this"
        # visual), and not on failure (leave 👀 for the user to know
        # nothing landed). The reactions call is fire-and-forget and
        # already swallows all its own exceptions; the outer try/except
        # here is defense-in-depth against a future refactor that lets
        # it raise — a broken reaction path must NEVER corrupt finalize
        # (save_state/log_conversation have already run above).
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
        """Send the one-shot context-usage heads-up when crossing a threshold."""
        last_warned = self._state.get("_context_warned_at", 0)
        next_thresholds = [t for t in CONTEXT_WARN_THRESHOLDS if t > last_warned]
        if next_thresholds:
            pct = get_context_percent(self._state.get("session_id"))
            if pct is not None:
                crossed = [t for t in next_thresholds if pct >= t]
                if crossed:
                    self._state["_context_warned_at"] = crossed[-1]
                    save_state(self._state)
                    # Ordered behind the turn's bubbles (see "(Paused.)" above);
                    # direct send_response fallback when there's no live sender.
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

    # -- internal helpers (not part of the 4-method decomposition) --

    def _seed_pc_session_from_state_once(self) -> None:
        """Lazy one-time seed of PersistentClaude from the persisted state dict.

        Runs at the top of send_to_claude. After this returns the dispatcher
        treats pc as the source of truth and only writes the state dict at
        save time. State-dict reads after this point are forbidden for
        session-id decisions.

        Imported lazily (and exceptions swallowed) for the same reasons as
        the removed _sync_persistent_claude_session_id: avoid a circular
        import with landline.claude and never let a singleton hiccup crash
        the dispatch.
        """
        if self._pc_seeded:
            return
        self._pc_seeded = True
        try:
            from landline.claude import _get_persistent_claude
            pc = _get_persistent_claude()
            if pc.get_session_id() is not None:
                # pc already knows (e.g. a stream event landed before we
                # ran). Don't clobber.
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

        Bumps the PauseFlag generation and passes the watchdog a closure that
        only honors a pause at this generation. The orchestrator always wires
        a PauseFlag; constructing a dispatcher without one is a programmer
        error.
        """
        assert self._pause_flag is not None, (
            "ClaudeDispatcher requires a pause_flag — pass one at construction"
        )
        my_generation = self._pause_flag.new_call()
        pf = self._pause_flag
        # If a pause was requested BEFORE this call started (e.g. queued by
        # the poller's /pause callback in the same batch that produced this
        # dispatch, or held across ``voice_handler``'s ``already_paused_at_
        # start`` branch), re-anchor the request to the new generation. Two
        # bugs collapse into one if we don't:
        #   1. ``interrupt_check()`` uses ``is_requested(my_generation)``,
        #      which returns False for a request stranded at the previous
        #      generation — so the watchdog never fires and the /pause is
        #      silently dropped (the operator never sees "(Paused.)").
        #   2. The level-triggered ``PauseFlag._event`` is still set, so
        #      ``_wait_for_done_or_pause`` returns True on every 0.5s tick
        #      and the watchdog spins in a 100% CPU busy-loop for the full
        #      duration of the Claude call.
        # ``request_pause()`` re-records ``_requested_gen`` at the current
        # generation, closing both holes. Safe if the flag is already
        # cleared (no-op path in that branch below).
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
            # Cluster 3: a single successful stream proves auth is back.
            # Reset the auth-alert latch so a fresh incident (e.g. a
            # separate later expiry) triggers a new alert.
            self._auth_alert_sent = False
            return

        # An external SIGTERM/SIGINT (daemon restart / launchctl bootout) kills
        # Claude with a signal exit code: 143/130 (shell 128+sig convention) or
        # -15/-2 (negative = killed-by-signal in subprocess.returncode). That's a
        # shutdown, not a Claude failure — recording it would trigger false
        # exponential backoff after a few restarts.
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

        # Cluster 3: auth-expiry detection runs AFTER record_failure so the
        # streak count reflects this turn. Intentionally decoupled from the
        # "Claude unavailable" alert above — that fires on the 10th
        # consecutive failure (Telegram, in-band), this fires on the FIRST
        # match of the OAuth-expiry stderr shape (iMessage, out-of-band) so
        # the operator learns about a multi-day outage on turn 1, not turn 10.
        if _stderr_looks_like_auth_failure(result.stderr_tail):
            self._maybe_send_auth_expiry_alert()

    def _maybe_send_auth_expiry_alert(self) -> None:
        """Fire the one-shot iMessage auth-expiry alert (Cluster 3).

        Latched: `_auth_alert_sent` gates re-fires until the next successful
        stream clears it. The `CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS` floor
        is a defensive belt-and-suspenders — if the latch reset path breaks
        elsewhere (bug in _record_outcome's success branch, say), the
        operator still won't get spammed within a 6h window.

        Fire-and-forget: `notifications.send_health_alert` spawns a daemon
        thread and returns immediately, so this never blocks the dispatch
        thread. Imported lazily inside the method to mirror the existing
        `landline.claude._get_persistent_claude` pattern and avoid the import
        cycle with landline.claude.
        """
        # The time-floor MUST run unconditionally — a fail→success→fail
        # cycle resets ``_auth_alert_sent`` to False on the success (see
        # _record_outcome), so gating the floor inside ``if
        # _auth_alert_sent`` skips the floor on the second failure and
        # lets a second alert fire milliseconds after the first (spam).
        # The floor is defense-in-depth against exactly that latch-reset
        # race — apply it whether the latch is set or not.
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
                # This fires DURING a live (slow) turn, when bubbles may already
                # be queued — so route it through the ordered queue too, or it
                # races ahead of them. Direct send_html fallback if no sender.
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
