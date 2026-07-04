"""TelegramDaemon orchestrator — wires together all extracted modules.

This is the top-level coordinator class that composes:
  poller, lock, commands, inject, state, claude_dispatch, failure_tracker, guard, client.

The daemon owns all state. Per-batch tracking attributes, dispatcher wiring,
lifecycle, cursor, and the main loop live on the class here. Per-batch
processing logic, restart continuation, and poller staleness handling live
in sibling modules and take ``self`` as ``daemon`` — see the docstrings on
``batch.py``, ``restart.py``, ``poller_health.py``.

Sibling module map:
  batch.py           — classification/dispatch pipeline invoked per drained batch
  restart.py         — restart-continuation trigger-file handling
  poller_health.py   — staleness detection + in-process poller replacement

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
from typing import Any, Callable, Dict, List, Optional, Tuple

# The imports below double as the patch surface for
# ``patch("landline.orchestrator.<name>")``. Sibling modules (batch/restart/
# poller_health) reach back through this module's namespace at call time,
# so names still referenced only by them (download_file, classify_updates,
# drain_inject_queue, log_conversation, load_state, sweep_media_caches,
# BackgroundPoller) are retained here for the seam — do not prune.
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
    MAX_QUEUED_UPDATES,
    STARTUP_DELAY,
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
    # Restart continuation (delegator — see restart.py)
    # -------------------------------------------------------------------------

    def _handle_restart_continuation(self) -> None:
        _restart.handle_restart_continuation(self)

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

    # -------------------------------------------------------------------------
    # Batch pipeline delegators — see batch.py
    # -------------------------------------------------------------------------

    def _process_update_batch(self, updates: List[Dict]) -> None:
        _batch.process_update_batch(self, updates)

    def _run_batch_classification_and_dispatch(
        self, updates: List[Dict],
    ) -> None:
        _batch.run_batch_classification_and_dispatch(self, updates)

    def _consume_stranded_pause_flag(self) -> None:
        _batch.consume_stranded_pause_flag(self)

    def _handle_pause_updates(
        self,
        pause_updates: List[Tuple[Dict, int, str]],
        dispatch_pending: bool,
    ) -> None:
        _batch.handle_pause_updates(self, pause_updates, dispatch_pending)

    def _process_commands(
        self, command_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        _batch.process_commands(self, command_updates)

    def _check_lock_gate(self, chat_id: str, update_ids: List[int]) -> bool:
        return _batch.check_lock_gate(self, chat_id, update_ids)

    def _inject_and_dispatch(
        self,
        text: str,
        chat_id: str,
        update_ids: List[int],
        ack_message_ids: Optional[List[int]] = None,
    ) -> None:
        _batch.inject_and_dispatch(
            self, text, chat_id, update_ids, ack_message_ids=ack_message_ids,
        )

    def _process_text_batch(
        self, text_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        _batch.process_text_batch(self, text_updates)

    def _process_photo_batch(
        self, photo_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to photo_handler — see ``landline/media/photo.py``."""
        process_photo_batch(self, photo_updates)

    def _process_document_batch(
        self, document_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to document_handler — see ``landline/media/document.py``."""
        process_document_batch(self, document_updates)

    def _process_voice_batch(
        self, voice_updates: List[Tuple[Dict, int, str]],
    ) -> None:
        """Delegate to voice_handler — see ``landline/media/voice.py``."""
        process_voice_batch(self, voice_updates)

    def _dispatch_photo_group(
        self,
        messages: List[Dict],
        update_ids: List[int],
        chat_id: str,
    ) -> None:
        """Delegate to photo_handler — see ``landline/media/photo.py``."""
        dispatch_photo_group(self, messages, update_ids, chat_id)

    @staticmethod
    def _coalesce_messages(
        text_updates: List[Tuple[Dict, int, str]],
    ) -> str:
        """Format one or more messages into a single Claude input."""
        return _batch.coalesce_messages(text_updates)

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
    # Cluster 4: poller liveness delegators — see poller_health.py
    # -------------------------------------------------------------------------

    def _check_poller_liveness(self) -> None:
        _poller_health.check_poller_liveness(self)

    def _replace_poller_in_place(self, reason: str) -> None:
        _poller_health.replace_poller_in_place(self, reason)

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


# Sibling module imports live at the bottom to break the circular import:
# batch.py / restart.py / poller_health.py each do
# ``from landline.orchestrator import daemon as _d`` at their top to resolve
# patchable module-scope names (save_state, log_conversation, time, WORKSPACE,
# etc.) at call time through this module's namespace. Placing these imports
# after all module-level bindings above guarantees _d is fully populated by
# the time those sibling modules are loaded.
from landline.orchestrator import batch as _batch  # noqa: E402
from landline.orchestrator import restart as _restart  # noqa: E402
from landline.orchestrator import poller_health as _poller_health  # noqa: E402
