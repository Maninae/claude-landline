"""TelegramDaemon orchestrator — wires together all extracted modules.

This is the top-level coordinator class that composes:
  poller, lock, commands, inject, state, claude_dispatch, failure_tracker, guard, client.

Helpers split out for clarity:
  pause_flag         — PauseFlag (re-exported below)
  image_cache        — _sweep_telegram_image_cache (re-exported below)
  batch_classifier   — _is_pause_command + classify_updates
  photo_handler      — process_photo_batch + dispatch_photo_group

Re-exports below keep ``landline.orchestrator.<name>`` importable for tests
and other modules that reach into this namespace.
"""

import signal
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from landline.runtime.batch_classifier import (
    _is_pause_command,
    classify_updates,
    extract_chat_id,
)
from landline.telegram import download_file
from landline.claude.dispatch import ClaudeDispatcher, ClaudeStreamResult
from landline.runtime.commands import CommandRouter
from landline import config
from landline.config import (
    COALESCENCE_WINDOW_SECONDS,
    LOCKED_HELP,
    MAX_QUEUED_UPDATES,
    TIMEZONE,
    STARTUP_DELAY,
    USER_NAME,
    WORKSPACE,
)
from landline.media.document import process_document_batch
from landline.media.cache import _sweep_telegram_image_cache, sweep_media_caches
from landline.runtime.inject import drain_inject_queue
from landline.runtime.lock import LockManager
from landline.runtime.logging import log
from landline.telegram.spool import OutboundSpoolReplayer
from landline.claude.pause_flag import PauseFlag
from landline.media.photo import dispatch_photo_group, process_photo_batch
from landline.telegram.poller import BackgroundPoller
from landline.media.voice import process_voice_batch
from landline.telegram import reactions
from landline.runtime.security import keychain_get
from landline.runtime.state import load_state, log_conversation, save_state
from landline.telegram.transport import _send_chunk


# Re-exported public surface — kept here so importers and tests can use
# ``from landline.orchestrator import X`` and ``patch("landline.orchestrator.X")``
# without caring where the implementation lives.
__all__ = [
    "INJECT_QUEUE_DIR",
    "PauseFlag",
    "TelegramDaemon",
    "_is_pause_command",
    "_sweep_telegram_image_cache",
]


INJECT_QUEUE_DIR = WORKSPACE / "cache" / "inject-queue"


def _reset_persistent_claude_for_new() -> None:
    """Kill the live PersistentClaude subprocess and clear its session id.

    Wired into ``CommandRouter`` as ``reset_claude_fn`` so ``/new`` actually
    discards the in-flight Claude session — not just the persisted state.
    Without this, the next dispatch would observe the OLD session id on the
    PersistentClaude singleton (E1 source of truth) and ``--resume`` the same
    conversation, defeating /new.

    Goes through the ``landline.claude`` facade (the test patch surface) rather
    than importing ``landline.persistent_claude`` directly — mirrors the lazy
    import the dispatcher uses in ``_invoke_with_stale_retry`` /
    ``_retry_with_fresh_session`` / ``_finalize_response``. Kill the process
    FIRST, then clear the session id, so a brief window where the subprocess
    is dead but the id still set is preferable to one where pc claims a new
    (nil) session while the old subprocess is still draining.
    """
    from landline.claude import _get_persistent_claude
    pc = _get_persistent_claude()
    try:
        pc.kill()
    finally:
        # Clear the session id even if kill() raised — the next ensure_alive
        # path will detect the dead proc via is_alive==False and respawn,
        # so the session id is the load-bearing reset.
        pc.clear_session()


class TelegramDaemon:
    """Long-polling Telegram bot that routes inbound text to Claude Code."""

    def __init__(
        self,
        run_claude_fn: Callable[..., ClaudeStreamResult],
        shutdown_hook: Any,
        failure_tracker: Any,
        send_response_fn: Callable[[str, str, str], None],
        send_typing_fn: Callable[[str, str], None],
        guard_fn: Callable[[str], bool],
        reject_fn: Callable[[str, str], None],
    ) -> None:
        self.token = keychain_get("telegram-bot-token") or ""
        self.chat_id = keychain_get("telegram-chat-id") or ""
        if not self.token or not self.chat_id:
            log("FATAL: Missing telegram-bot-token or telegram-chat-id in Keychain")
            sys.exit(1)

        self.state: Dict[str, Any] = load_state()
        self.running = True

        self._send_response = send_response_fn
        self._send_typing = send_typing_fn
        self._guard_fn = guard_fn
        self._reject_fn = reject_fn
        self._shutdown_hook = shutdown_hook

        self._lock_manager = LockManager(persist_state_fn=save_state)
        self._lock_manager.restore_from_state(self.state)

        self._command_router = CommandRouter(
            state=self.state,
            lock_manager=self._lock_manager,
            persist_state_fn=save_state,
            reset_claude_fn=_reset_persistent_claude_for_new,
        )

        self._failure_tracker = failure_tracker
        self._background_poller: Optional[BackgroundPoller] = None
        # Cluster 5: background thread that periodically replays pending
        # outbound-spool files. Started in ``run()`` between restart
        # continuation and poller start; stopped in ``_shutdown``.
        self._outbound_spool_replayer: Optional[OutboundSpoolReplayer] = None

        # Cluster 4: rate-limit the poller-staleness check + observability
        # counter for in-process poller replacements. See
        # ``_check_poller_liveness`` / ``_replace_poller_in_place``.
        self._poller_stale_check_last_at: float = 0.0
        self._poller_stale_recovery_count: int = 0

        # Set by the poller thread when /pause is queued. Cleared in two places
        # ONLY: (a) _finalize_response when result.interrupted, (b)
        # _handle_pause_updates when no dispatch is pending in the same batch.
        # NEVER cleared at the start of _invoke_claude_call — that would race
        # with the watchdog when /pause arrives in the same batch as text.
        self._pause_requested = PauseFlag()
        self._deferred_pause_ids: List[int] = []

        # Set by run() per-batch so _advance_update_cursor can record which
        # updates were actually handled. The batch-level catch-all in run()
        # uses this to AVOID bulk-advancing cursors for updates that were
        # never processed — those must be left for Telegram to re-deliver.
        self._batch_processed_ids: Optional[set] = None

        # Cluster 3: per-batch chat_id → [message_id, ...] tracker. The
        # classifier records every 👀-acknowledged message here so the
        # dispatch pass can hand the ids to the dispatcher and fire 👌 on
        # successful finalize. Initialized to {} at the top of
        # _process_update_batch and cleared to None in its finally clause
        # so a mid-batch exception can't leak state into the next batch.
        self._batch_ack_message_ids: Optional[Dict[str, List[int]]] = None

        # Per-batch set of chat_ids that have already received a
        # LOCKED_HELP notice this batch. Prevents cross-type amplification
        # when a mixed batch (photo + voice + document + text) hits the
        # lock gate — each handler runs ``_check_lock_gate`` independently
        # and, without this coalescing, would spam the operator with one
        # LOCKED_HELP per media bucket. Initialized to a fresh set at the
        # top of ``_process_update_batch`` and cleared to None in its
        # finally clause so a mid-batch exception can't leak state.
        self._batch_locked_help_chats: Optional[set] = None

        # Per-batch tracker for the "/pause was deferred but no Claude
        # call ever ran" bail-out class (Findings #1 / #2). When
        # ``_handle_pause_updates`` sees ``dispatch_pending=True`` it
        # leaves ``_pause_requested`` set so an upcoming Claude call
        # can consume it via watchdog+SIGINT (cleared in
        # ``_finalize_response`` on the interrupted result). If a real
        # Claude call never runs — locked-session gate, silent unlock,
        # all-fail download, whisper timeout, unsupported doc,
        # backoff-gated dispatch, or an exception raised before Claude
        # was invoked — the flag would strand and the round-8
        # re-anchor logic in ``ClaudeDispatcher._invoke_claude_call``
        # (``if pf.is_set(): pf.request_pause()``) would fire
        # "(Paused.)" on an unrelated FUTURE turn. Track (a) that
        # /pause was deferred this batch and (b) whether ANY dispatch
        # attempt actually invoked Claude (``send_to_claude`` returned
        # True — i.e. reached ``_invoke_claude_call``, so the
        # watchdog/finalize had a chance to consume the flag), then
        # clean up in the finally of
        # ``_run_batch_classification_and_dispatch``. Cleared to their
        # sentinels in ``_process_update_batch``'s finally so a
        # mid-batch exception can't leak state.
        self._batch_pause_was_deferred: bool = False
        self._batch_dispatch_attempted: bool = False
        self._batch_pause_notify_chat: Optional[str] = None

        self._dispatcher = ClaudeDispatcher(
            token=self.token,
            state=self.state,
            failure_tracker=failure_tracker,
            shutdown_hook=shutdown_hook,
            run_claude_fn=run_claude_fn,
            send_response_fn=send_response_fn,
            send_typing_fn=send_typing_fn,
            pause_flag=self._pause_requested,
            clear_pause_fn=self._pause_requested.clear,
        )

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def _shutdown(self, signum: int, frame: Any) -> None:
        log(f"Shutdown signal received ({signum})")
        self._shutdown_hook.drain_for_shutdown()
        if self._background_poller is not None:
            self._background_poller.signal_stop()
        # Cluster 5: signal the replayer to stop; the current pass will
        # finish before the loop exits (Event.wait). The thread is a
        # daemon-thread so we don't join.
        if self._outbound_spool_replayer is not None:
            self._outbound_spool_replayer.stop()
        self.running = False
        # Also stop the dispatcher so its stale-session retry guard
        # (ClaudeDispatcher.running) doesn't fire a fresh-session retry during
        # shutdown and overwrite the persisted session_id on the way out.
        if self._dispatcher is not None:
            self._dispatcher.running = False

    # -------------------------------------------------------------------------
    # Poller callback (runs on the poller thread)
    # -------------------------------------------------------------------------

    def _on_update_queued(self, update: Dict) -> None:
        """Invoked by the poller thread right after queueing an update.

        Contract: O(1) / non-blocking. Detect '/pause' and set the pause flag
        so the watchdog can interrupt the in-flight Claude call.

        """
        message = update.get("message") or {}
        text = (message.get("text") or "").strip().lower()
        if _is_pause_command(text):
            self._pause_requested.request_pause()
            log("/pause detected — requesting interrupt")

    # -------------------------------------------------------------------------
    # Restart continuation
    # -------------------------------------------------------------------------

    def _handle_restart_continuation(self) -> None:
        """If a restart-continuation trigger file exists, inject its content
        as a synthetic message to Claude so it can resume mid-task.

        Routed through ``_inject_and_dispatch`` so any cron reports queued in
        ``cache/inject-queue/`` during the restart window are prepended too —
        otherwise they'd sit until the operator's next message.

        Two-phase commit (M1): the trigger file is unlinked ONLY AFTER
        ``_inject_and_dispatch`` returns without raising. A dispatch-time
        exception leaves the file in place so the next restart retries —
        otherwise a transient crash silently drops the operator's cross-restart
        instruction (no Telegram update_id exists to replay it).
        """
        trigger = WORKSPACE / "cache" / "restart-continuation.txt"
        if not trigger.exists():
            return
        try:
            msg = trigger.read_text().strip()
        except Exception as e:
            log("Failed to read restart continuation: %s" % e)
            return
        if not msg:
            # Empty payload — safe to remove so we don't retry forever.
            try:
                trigger.unlink()
            except Exception:
                pass
            return
        if self._lock_manager.is_locked:
            # LEAVE the file in place so the payload survives until the next
            # unlock/restart. Unlinking here would permanently lose the operator's
            # continuation message.
            log(
                "Restart continuation skipped — session locked; "
                "will retry on next unlock/restart"
            )
            return
        log("Restart continuation: injecting message to Claude")
        try:
            self._inject_and_dispatch(msg, self.chat_id, update_ids=[])
        except Exception as e:
            # Two-phase commit: dispatch failed — LEAVE the trigger file in
            # place so the next restart retries. Re-raise so the run() loop's
            # existing handlers (and any startup error path) still see this.
            log(
                "Restart continuation dispatch failed (trigger %s preserved "
                "for retry): %s" % (trigger, e)
            )
            raise
        # Dispatch succeeded — commit by unlinking the trigger. A failure
        # here is benign: the next restart will overwrite-then-unlink, and
        # the payload was already delivered to Claude.
        try:
            trigger.unlink()
        except Exception as e:
            log(
                "Failed to unlink restart continuation trigger after "
                "dispatch: %s" % e
            )

    # -------------------------------------------------------------------------
    # Cursor management
    # -------------------------------------------------------------------------

    def _drain_deferred_pause_ids(self) -> None:
        """Advance cursors for /pause updates that were deferred during dispatch."""
        while self._deferred_pause_ids:
            uid = self._deferred_pause_ids.pop(0)
            self._advance_update_cursor(uid)

    def _advance_update_cursor(self, update_id: int) -> None:
        """Advance in-memory cursor, notify poller, and immediately persist
        state to disk so an unclean exit cannot leave Telegram believing the
        update was confirmed while the on-disk cursor still points behind it
        (which would cause duplicate Claude responses on restart).

        ``save_state`` is atomic (tmp+rename), so the extra writes are cheap.
        """
        if update_id > self.state.get("last_update_id", 0):
            self.state["last_update_id"] = update_id
            # Record on the per-batch tracker (if active) so the batch-level
            # exception handler in run() knows which updates were actually
            # processed and does NOT bulk-advance the unprocessed remainder.
            if self._batch_processed_ids is not None:
                self._batch_processed_ids.add(update_id)
            # Durability checkpoint — flush only on a real forward advance so
            # an unclean exit cannot leave Telegram believing the update was
            # confirmed while the on-disk cursor still points behind it.
            save_state(self.state)
        if self._background_poller is not None:
            self._background_poller.advance_processed_cursor(update_id)

    # -------------------------------------------------------------------------
    # Message processing
    # -------------------------------------------------------------------------

    def _handle_non_text_update(
        self, message: Dict, update_id: int, chat_id: str,
    ) -> None:
        # Photos, voice notes, and documents are handled by their own
        # batch methods before this fallback fires. This path only sees
        # unsupported media (sticker/animation/video) or empty messages.
        has_media = any(
            key in message for key in
            ("video", "audio", "voice", "document", "sticker", "animation",
             "video_note")
        )
        if has_media:
            notice = (
                "(I can only process text, photos, voice notes, and "
                "documents (pdf/txt/md/csv/json/log/yaml) for now — "
                "other media received, skipping.)"
            )
        else:
            notice = (
                "(I can only process text, photos, voice notes, and "
                "documents (pdf/txt/md/csv/json/log/yaml) for now.)"
            )
        # Send the skip notice BEFORE advancing the cursor. If the send raises,
        # leave the update un-advanced so Telegram re-delivers it and the user
        # eventually gets the notice — advancing first would silently confirm
        # the update while the user never learns it was dropped.
        try:
            self._send_response(self.token, chat_id, notice)
        except Exception as notify_error:
            log(
                f"Failed to send non-text skip notice for update {update_id} — "
                f"leaving un-advanced for Telegram re-delivery: {notify_error}"
            )
            return
        self._advance_update_cursor(update_id)

    def _process_update_batch(self, updates: List[Dict]) -> None:
        """Classify updates into commands, text, photos, voice notes,
        documents, and /pause, then process each group."""
        # Cluster 3: initialize the per-batch ack tracker. Classifier
        # populates chat_id → [message_id, ...] for every accepted
        # content message so dispatch can pass the ids to the dispatcher
        # for a completion 👌 on successful finalize. Cleared in the
        # finally so mid-batch exceptions can't leak state.
        self._batch_ack_message_ids = {}
        self._batch_locked_help_chats = set()
        self._batch_pause_was_deferred = False
        self._batch_dispatch_attempted = False
        self._batch_pause_notify_chat = None
        try:
            self._run_batch_classification_and_dispatch(updates)
        finally:
            self._batch_ack_message_ids = None
            self._batch_locked_help_chats = None
            self._batch_pause_was_deferred = False
            self._batch_dispatch_attempted = False
            self._batch_pause_notify_chat = None

    def _run_batch_classification_and_dispatch(
        self, updates: List[Dict],
    ) -> None:
        """Inner body of ``_process_update_batch`` — classification and
        the pass-2 dispatch. Extracted to keep the batch-tracker
        try/finally in ``_process_update_batch`` tight and to preserve
        the original sequencing verbatim."""
        # Pass 1: classify all updates (trivial-skip side effects handled
        # inline by the classifier — cursor advance, reject, too-long notice).
        (
            command_updates,
            text_updates,
            photo_updates,
            pause_updates,
            document_updates,
            voice_updates,
        ) = classify_updates(self, updates)

        # Pass 2: handle /pause with full knowledge of whether dispatch will
        # occur in this batch. When dispatch is pending we MUST NOT clear the
        # flag here — the watchdog needs to see it during the upcoming Claude
        # call so it can interrupt mid-stream.
        self._handle_pause_updates(
            pause_updates,
            dispatch_pending=(
                bool(text_updates)
                or bool(photo_updates)
                or bool(document_updates)
                or bool(voice_updates)
            ),
        )

        self._process_commands(command_updates)
        try:
            if photo_updates and self.running:
                self._process_photo_batch(photo_updates)
            if voice_updates and self.running:
                self._process_voice_batch(voice_updates)
            if document_updates and self.running:
                self._process_document_batch(document_updates)
            if text_updates and self.running:
                self._process_text_batch(text_updates)
        finally:
            self._drain_deferred_pause_ids()
            self._consume_stranded_pause_flag()

    def _consume_stranded_pause_flag(self) -> None:
        """Findings #1 / #2: if /pause was deferred in this batch (i.e.
        ``_handle_pause_updates`` saw ``dispatch_pending=True`` and left
        ``_pause_requested`` set for the upcoming Claude call to consume)
        but NO Claude call actually ran this batch — locked-session
        gate, silent unlock, all-fail download, whisper timeout,
        unsupported doc, backoff-gated dispatch (queued for later
        drain), or an exception raised before ``_invoke_claude_call``
        — the flag would strand and the round-8 re-anchor logic in
        ``ClaudeDispatcher._invoke_claude_call`` (``if pf.is_set():
        pf.request_pause()``) would fire "(Paused.)" on the NEXT
        unrelated turn.

        Clear the flag here and, if the session isn't locked, notify the
        user their /pause didn't land on a running turn. When the session
        IS locked, ``_check_lock_gate`` already sent a batch-coalesced
        LOCKED_HELP — sending "(Nothing to pause.)" on top of that would
        be double-noise, so stay silent in that path.

        Load-bearing invariant preserved: when a real Claude call
        actually ran (``send_to_claude`` returned True → set
        ``_batch_dispatch_attempted`` True), the dispatcher/watchdog/
        ``_finalize_response`` own the flag's lifecycle. This helper is
        a no-op in that path so the mocked "dispatched but not
        interrupted" test scenarios (e.g.
        ``test_pause_with_text_before_in_same_batch_does_NOT_send_
        nothing_to_pause``) continue to observe the flag still set on
        their MockClaude callback.
        """
        if not self._batch_pause_was_deferred:
            return
        if self._batch_dispatch_attempted:
            return
        if not self._pause_requested.is_set():
            return
        self._pause_requested.clear()
        if self._lock_manager.is_locked:
            return
        notice_chat = self._batch_pause_notify_chat or self.chat_id
        try:
            self._send_response(
                self.token, notice_chat, "(Nothing to pause.)",
            )
        except Exception as pause_notify_error:
            log(
                "Failed to send deferred /pause notice: %s"
                % pause_notify_error,
            )

    def _handle_pause_updates(
        self,
        pause_updates: List[Tuple[Dict, int, str]],
        dispatch_pending: bool,
    ) -> None:
        """Log /pause, advance cursors, and (only when no dispatch is pending
        in this batch) notify + clear the flag.

        When dispatch IS pending, leave the flag set so the watchdog can
        consume it during the upcoming Claude call.

        Uses a `notified` guard to prevent N copies of LOCKED_HELP or "Nothing
        to pause" when multiple /pause updates arrive in the same batch.
        """
        notified = False
        for message, update_id, chat_id in pause_updates:
            log_conversation(USER_NAME, "/pause")
            if dispatch_pending:
                self._deferred_pause_ids.append(update_id)
                # Record that /pause was deferred THIS batch so the
                # finally in ``_run_batch_classification_and_dispatch``
                # can consume a stranded flag when dispatch never
                # actually reached Claude (Findings #1 / #2). Also
                # remember a chat_id so the cleanup notice targets the
                # same chat that typed /pause.
                self._batch_pause_was_deferred = True
                if self._batch_pause_notify_chat is None:
                    self._batch_pause_notify_chat = chat_id
            else:
                self._advance_update_cursor(update_id)

            if dispatch_pending or notified:
                continue

            if self._lock_manager.is_locked:
                self._send_response(self.token, chat_id, LOCKED_HELP)
                self._pause_requested.clear()
                notified = True
            elif self._pause_requested.is_set():
                self._pause_requested.clear()
                self._send_response(self.token, chat_id, "(Nothing to pause.)")
                notified = True
            # else: flag already consumed (e.g. by _finalize_response) — silent.

    def _process_commands(
        self, command_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Process slash commands in order."""
        for message, update_id, text in command_updates:
            if not self.running:
                break
            chat_id = extract_chat_id(message)
            response = self._command_router.handle(text)
            if response is not None:
                self._send_response(self.token, chat_id, response)
            self._advance_update_cursor(update_id)
            self._dispatcher.last_process_time = time.time()

    def _check_lock_gate(self, chat_id: str, update_ids: List[int]) -> bool:
        """Check lock expiry and send LOCKED_HELP if locked.

        Advances cursors and returns True if the session is locked (caller
        should return early).  Returns False if unlocked (caller proceeds).

        Cross-type coalescing: if a LOCKED_HELP has already been sent to
        this ``chat_id`` earlier in the current batch (tracked on
        ``self._batch_locked_help_chats``), the send is suppressed but the
        cursor still advances — so a mixed batch (photo + voice + document
        + text) delivers exactly one LOCKED_HELP per chat instead of one
        per media bucket. Outside a batch (e.g. defensive callers that
        skip the tracker), the tracker is ``None`` and every call sends.
        """
        self._lock_manager.check_expiry()
        if not self._lock_manager.is_locked:
            return False
        already_sent = (
            isinstance(self._batch_locked_help_chats, set)
            and chat_id in self._batch_locked_help_chats
        )
        # Send LOCKED_HELP BEFORE advancing cursors. If the send raises, leave
        # the updates un-advanced so Telegram re-delivers them — the user must
        # not silently lose visibility that the session is locked.
        if not already_sent:
            try:
                self._send_response(self.token, chat_id, LOCKED_HELP)
            except Exception as notify_error:
                log(
                    f"Failed to send LOCKED_HELP for {len(update_ids)} update(s) — "
                    f"leaving un-advanced for Telegram re-delivery: {notify_error}"
                )
                return True
            if isinstance(self._batch_locked_help_chats, set):
                self._batch_locked_help_chats.add(chat_id)
        for uid in update_ids:
            self._advance_update_cursor(uid)
        self._dispatcher.last_process_time = time.time()
        return True

    def _inject_and_dispatch(
        self,
        text: str,
        chat_id: str,
        update_ids: List[int],
        ack_message_ids: Optional[List[int]] = None,
    ) -> None:
        """Drain inject queue, prepend context, dispatch to Claude, advance cursors.

        Inject files travel WITH the text through ``send_to_claude`` and are
        committed only after the message reaches a real Claude call. If
        ``send_to_claude`` stashes the text on the backoff queue (gated by
        failure backoff), the paths ride along on the queue tuple — so a
        daemon death mid-backoff doesn't drop morning briefs on the floor.

        Cluster 3: ``ack_message_ids`` names ONLY the Telegram message_ids
        actually being dispatched in this turn — passed by each media
        handler from the messages it's dispatching right now. On a
        successful finalize, ``ClaudeDispatcher`` fires 👌 on exactly
        those ids. This partitioning is load-bearing: mixed batches
        (voice+text, photo+text, multi-doc) each get their own dispatch,
        and a failed dispatch (e.g., whisper failure) never lets a later
        successful dispatch cross-pollinate 👌 onto messages that were
        never processed. The per-batch tracker
        (``_batch_ack_message_ids``) is populated by the classifier for
        observability/tests but is NOT read here; the caller owns which
        ids to ack for its own dispatch. Callers with no ids to ack
        (e.g. restart continuation) pass ``None`` / an empty list.

        Additionally, the given ids are popped from the per-batch tracker
        so any observability read reflects the remaining un-dispatched
        ids for the chat.
        """
        inject_prefix, consumed_paths = drain_inject_queue(INJECT_QUEUE_DIR)
        if inject_prefix:
            text = inject_prefix + "\n\n" + text
        ack_ids: List[int] = list(ack_message_ids or [])
        # Pop the dispatched ids off the per-batch tracker so a subsequent
        # dispatch in the same batch (e.g. text after voice) cannot see
        # them and re-👌 messages that don't belong to it.
        if isinstance(self._batch_ack_message_ids, dict) and ack_ids:
            remaining = [
                m for m in self._batch_ack_message_ids.get(chat_id, [])
                if m not in ack_ids
            ]
            if remaining:
                self._batch_ack_message_ids[chat_id] = remaining
            else:
                self._batch_ack_message_ids.pop(chat_id, None)
        # Ownership of ``_batch_dispatch_attempted``: flip True ONLY
        # when ``send_to_claude`` confirms it actually reached
        # ``_invoke_claude_call`` (return True). The watchdog +
        # ``_finalize_response`` are the ONLY consumers of the pause
        # flag downstream, and both only run inside that path. On the
        # backoff-gated path the dispatcher stashes the text on the
        # backoff queue and returns False WITHOUT invoking Claude — so
        # the pause flag would strand, and the round-8 re-anchor logic
        # in ``ClaudeDispatcher._invoke_claude_call`` (``if pf.is_set():
        # pf.request_pause()``) would fire "(Paused.)" on a FUTURE
        # unrelated turn (e.g. after backoff clears and the queued text
        # is drained into a fresh dispatch). Same story if
        # ``send_to_claude`` raises before Claude runs: the exception
        # propagates without setting the flag True, so
        # ``_consume_stranded_pause_flag`` in
        # ``_run_batch_classification_and_dispatch``'s finally can
        # clean up. The pre-fix design (set True before the call)
        # blocked both cleanup paths and stranded /pause across turns.
        claude_invoked = self._dispatcher.send_to_claude(
            text, chat_id,
            consumed_paths=consumed_paths,
            ack_message_ids=ack_ids,
        )
        self._batch_dispatch_attempted = bool(claude_invoked)
        for uid in update_ids:
            self._advance_update_cursor(uid)

    def _process_text_batch(
        self, text_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Coalesce text messages and dispatch to Claude."""
        representative_chat_id = extract_chat_id(text_updates[0][0])
        update_ids = [uid for _, uid, _ in text_updates]

        # message_ids that the classifier fired 👀 on. Used to clear the
        # ack on rejection paths (lock gate, silent unlock) so 👀 never
        # lingers without a matching 👌.
        text_mids: List[int] = [
            msg.get("message_id")
            for msg, _, _ in text_updates
            if isinstance(msg.get("message_id"), int)
        ]

        if self._lock_manager.is_locked and len(text_updates) == 1:
            raw_text = text_updates[0][2]
            if self._lock_manager.try_silent_unlock(raw_text.strip()):
                self._send_response(
                    self.token, representative_chat_id,
                    "Unlocked. Send a message to begin.",
                )
                # The passphrase-typed-directly message is not dispatched
                # to Claude — clear its 👀 so it doesn't sit as an
                # unmatched ack.
                if text_mids:
                    reactions.set_reactions_batch_async(
                        self.token, representative_chat_id, text_mids, None,
                    )
                for uid in update_ids:
                    self._advance_update_cursor(uid)
                self._dispatcher.last_process_time = time.time()
                return

        if self._check_lock_gate(representative_chat_id, update_ids):
            log(f"Sending LOCKED_HELP for batch of {len(text_updates)} text message(s)")
            # Clear 👀 acks on all text messages so they don't linger as
            # false "accepted" signals under the locked-session rejection.
            if text_mids:
                reactions.set_reactions_batch_async(
                    self.token, representative_chat_id, text_mids, None,
                )
            return

        coalesced_text = self._coalesce_messages(text_updates)

        for _, _, individual_text in text_updates:
            log_conversation(USER_NAME, individual_text)

        # Cluster 3: ack only the message_ids being dispatched now — one
        # per accepted text message in this batch. Prevents 👌 leaking
        # onto messages that were dispatched in a different pass
        # (photo/voice/doc) whose success/failure semantics differ.
        text_ack_ids: List[int] = [
            msg.get("message_id")
            for msg, _, _ in text_updates
            if isinstance(msg.get("message_id"), int)
        ]
        self._inject_and_dispatch(
            coalesced_text,
            representative_chat_id,
            update_ids,
            ack_message_ids=text_ack_ids,
        )

    def _process_photo_batch(
        self, photo_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to photo_handler — see ``daemon/photo_handler.py``."""
        process_photo_batch(self, photo_updates)

    def _process_document_batch(
        self, document_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to document_handler — see ``daemon/document_handler.py``."""
        process_document_batch(self, document_updates)

    def _process_voice_batch(
        self, voice_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to voice_handler — see ``daemon/voice_handler.py``."""
        process_voice_batch(self, voice_updates)

    def _dispatch_photo_group(
        self,
        messages: List[Dict],
        update_ids: List[int],
        chat_id: str,
    ) -> None:
        """Delegate to photo_handler — see ``daemon/photo_handler.py``."""
        dispatch_photo_group(self, messages, update_ids, chat_id)

    @staticmethod
    def _coalesce_messages(
        text_updates: List[Tuple[Dict, int, str]],
    ) -> str:
        """Format one or more messages into a single Claude input."""
        def _fmt_ts(message: Dict) -> str:
            unix_ts = message.get("date")
            if unix_ts:
                dt = datetime.fromtimestamp(unix_ts, tz=TIMEZONE)
                return dt.strftime("%Y-%m-%d %H:%M %Z")
            return ""

        if len(text_updates) == 1:
            message, _, text = text_updates[0]
            ts = _fmt_ts(message)
            return f"[{ts}]\n{text}" if ts else text

        formatted_parts: List[str] = []
        for message_index, (message, _, text) in enumerate(text_updates, 1):
            ts = _fmt_ts(message)
            header = f"[message {message_index}]" + (f" [{ts}]" if ts else "")
            formatted_parts.append(f"{header}\n{text}")
        log(f"Coalesced {len(text_updates)} messages into one turn")
        return "\n---\n".join(formatted_parts)

    # -------------------------------------------------------------------------
    # Cluster 5: outbound-spool replay hook
    # -------------------------------------------------------------------------

    def _spool_replay_send(
        self, chat_id: str, chunk: str, html_mode: bool, label: str,
    ) -> Tuple[bool, Optional[int]]:
        """send_fn for the background OutboundSpoolReplayer.

        Bypasses ``_send_with_retry`` (which would re-persist the file into
        a fresh inflight state, defeating the point) and hits ``_send_chunk``
        directly. A single attempt per replay pass — the periodic loop is
        the retry vehicle.
        """
        ok, code, _retry_after = _send_chunk(
            self.token, chat_id, chunk, html_mode,
        )
        return ok, code

    # -------------------------------------------------------------------------
    # Cluster 4: poller staleness detection + in-process replacement
    # -------------------------------------------------------------------------

    def _check_poller_liveness(self) -> None:
        """Called from the main loop (fires only every
        ``POLL_STALE_CHECK_INTERVAL_SECONDS`` to avoid noise).

        If the poller thread hasn't reported a successful poll in
        ``POLL_STALE_ALERT_THRESHOLD_SECONDS``, replace the poller in-process.
        Preserves the dedup set and cursor by handing them to the new instance.

        Rationale for in-process (vs process exit): keeps the persistent
        Claude subprocess and the StreamSender queue backlog alive across
        the swap. See CLAUDE.md "Stuck poller (TCP connection stale)".
        """
        now = time.time()
        if now - self._poller_stale_check_last_at < config.POLL_STALE_CHECK_INTERVAL_SECONDS:
            return
        self._poller_stale_check_last_at = now
        if self._background_poller is None:
            return
        staleness = now - self._background_poller.last_successful_poll()
        if staleness < config.POLL_STALE_ALERT_THRESHOLD_SECONDS:
            return
        log(
            f"Poller stall detected (last successful poll {int(staleness)}s "
            f"ago) — replacing in-process"
        )
        self._replace_poller_in_place(reason=f"stale for {int(staleness)}s")

    def _replace_poller_in_place(self, reason: str) -> None:
        """Swap the current stuck poller for a fresh one, preserving invariants.

        Ordering: (1) signal-stop old poller, (2) snapshot dedup + cursor,
        (3) construct new poller with cursor, (4) hand snapshot to new
        poller, (5) attach fresh ``_on_update_queued`` callback, (6) start
        new poller, (7) let old poller thread die on its next timeout (it's
        daemonized; the fresh urllib request in it will time out within
        ``POLL_TIMEOUT+10`` or hang forever — but it's not our thread
        anymore).

        We use ``signal_stop()`` rather than ``stop()`` — the latter joins
        for up to ``POLL_TIMEOUT+5`` and would stall the main loop on top
        of an already-stalled poller.
        """
        old = self._background_poller
        if old is None:
            return
        old.signal_stop()
        # Snapshot invariants before constructing the replacement so any
        # in-flight thread interaction on ``old`` can't race the handoff.
        with old._last_processed_update_id_lock:
            old_cursor = old._last_processed_update_id
        old_dedup = old.snapshot_dedup_ids()
        # Drain any updates that were already queued on the old poller but
        # not yet consumed by the main loop. Without this, they'd be
        # orphaned: the old poller stops, the main loop only reads the new
        # poller's queue, and Telegram's re-delivery of the same ids would
        # be blocked by the new poller's dedup (loaded from ``old_dedup``).
        # Net result: silently lost updates. Forwarding them below preserves
        # at-least-once across the swap.
        orphaned_updates = old.drain()
        new_poller = BackgroundPoller(
            token=self.token,
            initial_last_processed_update_id=old_cursor,
            on_update_queued=self._on_update_queued,
        )
        # Seed dedup for BOTH the old snapshot AND the forwarded queue's
        # update_ids in a single call. The old snapshot alone is not
        # sufficient: the poller thread may have atomically added an
        # update to (dedup ∪ queue) AFTER ``snapshot_dedup_ids()`` returned
        # but BEFORE ``old.drain()`` — that update lands in the drained
        # payload without ever appearing in ``old_dedup``. If we skipped
        # it here, the new poller's cursor (snapshotted before the update
        # was processed) would ask Telegram for offset=X on the next
        # poll, get X re-delivered, miss the dedup gate, and re-queue X —
        # so the main loop processes X twice (turn N receives its own
        # message twice / a slash command runs twice). Including the
        # forwarded ids in the dedup seed makes the re-delivery a dedup
        # hit and preserves at-most-once processing across the swap.
        forwarded_ids: List[int] = []
        for update in orphaned_updates:
            if not isinstance(update, dict):
                continue
            uid = update.get("update_id")
            if isinstance(uid, int):
                forwarded_ids.append(uid)
        new_poller.load_dedup_ids(list(old_dedup) + forwarded_ids)
        if orphaned_updates:
            forwarded = new_poller.preload_queue(orphaned_updates)
            log(
                f"Poller swap: forwarded {forwarded} in-flight update(s) "
                f"from old poller's queue"
            )
        self._background_poller = new_poller
        new_poller.start()
        self._poller_stale_recovery_count += 1
        log(
            f"Poller replaced (reason={reason}, "
            f"recovery #{self._poller_stale_recovery_count})"
        )

    # -------------------------------------------------------------------------
    # Main loop
    # -------------------------------------------------------------------------

    def run(self) -> None:
        log("Telegram daemon starting (streaming mode)")
        log(f"Session: {self.state.get('session_id') or 'none'}")
        log(f"Lock state: {self._lock_manager.lock_state_label}")
        time.sleep(STARTUP_DELAY)

        # Sweep stale media caches (images + documents) before the poller
        # starts — keeps the daemon's cache dirs bounded without risking
        # deletion of files actively being read by an in-flight Claude turn.
        sweep_media_caches()

        self._background_poller = BackgroundPoller(
            token=self.token,
            initial_last_processed_update_id=self.state["last_update_id"],
            on_update_queued=self._on_update_queued,
        )

        # Handle restart-continuation BEFORE starting the poller so an inbound
        # /pause (or any message) can't race the continuation dispatch and
        # set the pause flag before _invoke_claude_call begins. The
        # continuation path calls send_to_claude via _inject_and_dispatch and
        # does NOT depend on the poller running.
        self._handle_restart_continuation()

        # Cluster 5: start the outbound-spool background replayer AFTER
        # restart continuation and BEFORE the poller. The sync startup
        # flush already ran in ``__main__.main`` (before construction), so
        # this thread only handles files that fail on the next live send.
        # The send_fn closures ``self.token`` so the spool module stays
        # credential-free.
        self._outbound_spool_replayer = OutboundSpoolReplayer(
            send_fn=self._spool_replay_send,
        )
        self._outbound_spool_replayer.start()

        self._background_poller.start()
        log("Background poller started")

        try:
            while self.running:
                updates = self._background_poller.drain(block_timeout_seconds=1.0)
                # Cluster 4: check for silent-TCP-stall recovery on every
                # iteration. The 1s drain timeout guarantees this runs at
                # least once per second when idle; when busy processing, it
                # runs at the top of the next iteration (bounded staleness
                # of one Claude turn duration). The method is internally
                # rate-limited to POLL_STALE_CHECK_INTERVAL_SECONDS.
                self._check_poller_liveness()
                if not updates:
                    continue

                if self._background_poller.has_pending():
                    time.sleep(COALESCENCE_WINDOW_SECONDS)
                    updates.extend(self._background_poller.drain())

                # Cap total drained updates (text + photos + commands + /pause)
                # under one budget so photo spam can't bypass the limit.
                if len(updates) > MAX_QUEUED_UPDATES:
                    overflow = len(updates) - MAX_QUEUED_UPDATES
                    dropped = updates[MAX_QUEUED_UPDATES:]
                    notice_chat = extract_chat_id(
                        dropped[0].get("message") or {}
                    ) or self.chat_id
                    # Send the overflow notice BEFORE advancing cursors for
                    # the dropped updates. If the notice send raises, leave
                    # those updates un-advanced so Telegram re-delivers them
                    # on the next poll instead of silently losing the
                    # messages with zero record.
                    notice_sent = False
                    try:
                        self._send_response(
                            self.token, notice_chat,
                            f"(Received {len(updates)} messages while busy — "
                            f"processing the first {MAX_QUEUED_UPDATES}, "
                            f"dropped {overflow}.)",
                        )
                        notice_sent = True
                    except Exception as overflow_notify_error:
                        log(
                            f"CRITICAL: Failed to send overflow notice — "
                            f"leaving {overflow} dropped updates un-advanced "
                            f"for Telegram re-delivery: {overflow_notify_error}"
                        )
                    if notice_sent:
                        for u in dropped:
                            self._advance_update_cursor(u.get("update_id", 0))
                    updates = updates[:MAX_QUEUED_UPDATES]

                # Track which updates were actually processed so a mid-batch
                # exception doesn't bulk-advance cursors for unhandled
                # messages (which would silently drop them — Telegram never
                # re-delivers a confirmed update).
                self._batch_processed_ids = set()
                try:
                    self._process_update_batch(updates)
                except Exception as batch_error:
                    log(f"_process_update_batch error: {batch_error}")
                    try:
                        self._send_response(
                            self.token, self.chat_id,
                            "(Internal error processing your message — "
                            "it was dropped. Please try again.)",
                        )
                    except Exception as notify_error:
                        log(f"Failed to send error notification: {notify_error}")
                    # Do NOT bulk-advance cursors here. Updates that were
                    # actually handled already advanced their own cursor via
                    # _advance_update_cursor (and were recorded in
                    # _batch_processed_ids). Updates not yet processed must be
                    # left un-advanced so Telegram re-delivers them on the
                    # next poll instead of silently dropping them.
                    unprocessed = [
                        u.get("update_id", 0) for u in updates
                        if u.get("update_id", 0) not in self._batch_processed_ids
                    ]
                    if unprocessed:
                        # Discard from poller dedup set so the next long-poll
                        # actually re-queues them. Without this, the cursor
                        # stays behind (good) but the dedup set blocks
                        # re-delivery — silently dropping the messages.
                        if self._background_poller is not None:
                            self._background_poller.discard_queued_ids(unprocessed)
                        log(
                            f"Batch error: freed {len(unprocessed)} "
                            f"unprocessed update(s) for re-delivery on the next poll"
                        )
                finally:
                    self._batch_processed_ids = None
                # End-of-batch durability checkpoint — session_id and lock
                # fields flushed together. Cursor advances are now persisted
                # individually inside _advance_update_cursor, so this save is
                # belt-and-suspenders for non-cursor state changes.
                save_state(self.state)
        finally:
            self._background_poller.stop()
            save_state(self.state)
            log("Telegram daemon stopped")
