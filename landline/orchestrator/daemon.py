"""TelegramDaemon orchestrator — the coordinator that wires extracted modules.

- Coordinator owns all state; per-batch trackers, dispatcher wiring, lifecycle,
  cursor, and the main loop live on the class here.
- Sibling modules take ``self`` as ``daemon``: ``batch.py`` (per-batch pipeline),
  ``restart.py`` (restart-continuation), ``poller_health.py`` (staleness swap).
- Re-exports below keep ``landline.orchestrator.<name>`` importable for tests
  and other modules that reach into this namespace.
"""

import signal
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

# Imports below double as the patch surface for
# ``patch("landline.orchestrator.<name>")``. Sibling modules reach back through
# this namespace at call time; names referenced only by them are retained here
# for the seam — do not prune.
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


# Public surface for ``from landline.orchestrator import X`` and
# ``patch("landline.orchestrator.X")``.
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

    Wired into ``CommandRouter`` as ``reset_claude_fn`` for ``/new``.

    - Goes through the ``landline.claude`` facade (the test patch surface) —
      mirrors the lazy import the dispatcher uses.
    - Kill FIRST, then clear the session id: a brief window with dead proc +
      stale id is preferable to pc claiming a new (nil) session while the old
      subprocess is still draining.
    - Session-id clear is the load-bearing reset; see docs/ARCHITECTURE.md
      "Session id — single source of truth".
    """
    from landline.claude import _get_persistent_claude
    pc = _get_persistent_claude()
    try:
        pc.kill()
    finally:
        # Clear session id even if kill() raised — next ensure_alive respawns.
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
        # Background outbound-spool replayer. Lifecycle in ``run()`` / ``_shutdown``.
        self._outbound_spool_replayer: Optional[OutboundSpoolReplayer] = None

        # Poller-staleness rate-limit + recovery counter (see poller_health.py).
        self._poller_stale_check_last_at: float = 0.0
        self._poller_stale_recovery_count: int = 0

        # /pause flag: set by the poller thread on classify; cleared ONLY by
        # ``_finalize_response`` on ``result.interrupted`` OR by
        # ``handle_pause_updates`` when no dispatch is pending. NEVER cleared
        # at ``_invoke_claude_call`` entry — races the watchdog.
        self._pause_requested = PauseFlag()
        self._deferred_pause_ids: List[int] = []

        # Per-batch trackers. Initialized in ``process_update_batch``'s try,
        # cleared in its finally so a mid-batch exception can't leak state
        # into the next batch. See ``batch.py``.
        # - _batch_processed_ids: cursor-safety set (batch-error path must not
        #   bulk-advance updates that were never processed).
        self._batch_processed_ids: Optional[set] = None
        # - _batch_ack_message_ids: chat_id → [message_id, ...] populated by
        #   the classifier for 👀→👌 handoff to the dispatcher.
        self._batch_ack_message_ids: Optional[Dict[str, List[int]]] = None
        # - _batch_locked_help_chats: coalesces LOCKED_HELP so a mixed
        #   (photo+voice+doc+text) batch delivers one notice per chat, not
        #   one per media bucket (each handler runs check_lock_gate).
        self._batch_locked_help_chats: Optional[set] = None
        # - _batch_pause_* : "/pause deferred but no Claude call ran" cleanup.
        #   See ``batch.consume_stranded_pause_flag`` + docs/ARCHITECTURE.md
        #   "Deferred /pause — stranded flag".
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

    def _shutdown(self, signum: int, frame: Any) -> None:
        log(f"Shutdown signal received ({signum})")
        self._shutdown_hook.drain_for_shutdown()
        if self._background_poller is not None:
            self._background_poller.signal_stop()
        # Signal spool replayer to stop; current pass finishes; daemon thread.
        if self._outbound_spool_replayer is not None:
            self._outbound_spool_replayer.stop()
        self.running = False
        # Stop the dispatcher so its stale-session retry guard doesn't fire a
        # fresh-session retry during shutdown and overwrite persisted session_id.
        if self._dispatcher is not None:
            self._dispatcher.running = False

    def _on_update_queued(self, update: Dict) -> None:
        """Poller-thread callback: O(1), non-blocking; detects /pause and sets
        the pause flag so the watchdog can interrupt the in-flight Claude call.
        """
        message = update.get("message") or {}
        text = (message.get("text") or "").strip().lower()
        if _is_pause_command(text):
            self._pause_requested.request_pause()
            log("/pause detected — requesting interrupt")

    def _handle_restart_continuation(self) -> None:
        """Delegator — see ``restart.py``."""
        _restart.handle_restart_continuation(self)

    def _drain_deferred_pause_ids(self) -> None:
        """Advance cursors for /pause updates that were deferred during dispatch."""
        while self._deferred_pause_ids:
            uid = self._deferred_pause_ids.pop(0)
            self._advance_update_cursor(uid)

    def _advance_update_cursor(self, update_id: int) -> None:
        """Advance in-memory cursor, notify poller, persist state atomically.

        - Persist-on-advance so an unclean exit cannot leave Telegram
          believing the update was confirmed while the on-disk cursor still
          points behind it (would cause duplicate Claude responses on restart).
        - Records on the per-batch tracker so the batch-error path in ``run()``
          does NOT bulk-advance the unprocessed remainder.
        """
        if update_id > self.state.get("last_update_id", 0):
            self.state["last_update_id"] = update_id
            if self._batch_processed_ids is not None:
                self._batch_processed_ids.add(update_id)
            save_state(self.state)
        if self._background_poller is not None:
            self._background_poller.advance_processed_cursor(update_id)

    def _handle_non_text_update(
        self, message: Dict, update_id: int, chat_id: str,
    ) -> None:
        # Fallback for unsupported media (sticker/animation/video) or empty
        # messages — photo/voice/document have their own batch methods.
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
        # Send BEFORE advancing cursor — a raised send leaves the update
        # un-advanced so Telegram re-delivers rather than silently dropping.
        try:
            self._send_response(self.token, chat_id, notice)
        except Exception as notify_error:
            log(
                f"Failed to send non-text skip notice for update {update_id} — "
                f"leaving un-advanced for Telegram re-delivery: {notify_error}"
            )
            return
        self._advance_update_cursor(update_id)

    # Batch pipeline delegators — see batch.py.

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

    def _spool_replay_send(
        self, chat_id: str, chunk: str, html_mode: bool, label: str,
    ) -> Tuple[bool, Optional[int]]:
        """send_fn for the background OutboundSpoolReplayer.

        Hits ``_send_chunk`` directly, bypassing ``_send_with_retry`` (which
        would re-persist into a fresh inflight state); one attempt per pass
        — the periodic loop is the retry vehicle.
        """
        ok, code, _retry_after = _send_chunk(
            self.token, chat_id, chunk, html_mode,
        )
        return ok, code

    # Poller liveness delegators — see poller_health.py.

    def _check_poller_liveness(self) -> None:
        _poller_health.check_poller_liveness(self)

    def _replace_poller_in_place(self, reason: str) -> None:
        _poller_health.replace_poller_in_place(self, reason)

    def run(self) -> None:
        log("Telegram daemon starting (streaming mode)")
        log(f"Session: {self.state.get('session_id') or 'none'}")
        log(f"Lock state: {self._lock_manager.lock_state_label}")
        time.sleep(STARTUP_DELAY)

        # Sweep before the poller starts so we can never race an in-flight
        # Claude turn reading a cached file.
        sweep_media_caches()

        self._background_poller = BackgroundPoller(
            token=self.token,
            initial_last_processed_update_id=self.state["last_update_id"],
            on_update_queued=self._on_update_queued,
        )

        # Restart-continuation runs BEFORE poller start so an inbound /pause
        # can't race the continuation dispatch and set the flag before
        # _invoke_claude_call begins. Continuation doesn't depend on the poller.
        self._handle_restart_continuation()

        # Spool replayer starts AFTER restart continuation, BEFORE the poller.
        # Sync startup flush already ran in __main__.main; this thread only
        # handles files that fail on the next live send. send_fn closures
        # self.token so the spool module stays credential-free.
        self._outbound_spool_replayer = OutboundSpoolReplayer(
            send_fn=self._spool_replay_send,
        )
        self._outbound_spool_replayer.start()

        self._background_poller.start()
        log("Background poller started")

        try:
            while self.running:
                updates = self._background_poller.drain(block_timeout_seconds=1.0)
                # Silent-TCP-stall recovery check every iteration; method is
                # internally rate-limited to POLL_STALE_CHECK_INTERVAL_SECONDS.
                self._check_poller_liveness()
                if not updates:
                    continue

                if self._background_poller.has_pending():
                    time.sleep(COALESCENCE_WINDOW_SECONDS)
                    updates.extend(self._background_poller.drain())

                # One budget across text/photos/commands/pause so photo spam
                # can't bypass the limit.
                if len(updates) > MAX_QUEUED_UPDATES:
                    overflow = len(updates) - MAX_QUEUED_UPDATES
                    dropped = updates[MAX_QUEUED_UPDATES:]
                    notice_chat = extract_chat_id(
                        dropped[0].get("message") or {}
                    ) or self.chat_id
                    # Send BEFORE advancing dropped cursors — a raised send
                    # leaves them un-advanced for Telegram re-delivery.
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

                # Tracker set so a mid-batch exception can't bulk-advance
                # cursors for unhandled messages (silently dropping them —
                # Telegram never re-delivers a confirmed update).
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
                    # Do NOT bulk-advance here. Processed updates already
                    # advanced their own cursors; unprocessed must be left
                    # un-advanced for Telegram re-delivery.
                    unprocessed = [
                        u.get("update_id", 0) for u in updates
                        if u.get("update_id", 0) not in self._batch_processed_ids
                    ]
                    if unprocessed:
                        # ALSO discard from poller dedup so next long-poll
                        # actually re-queues them (else dedup blocks
                        # re-delivery even with the cursor behind).
                        if self._background_poller is not None:
                            self._background_poller.discard_queued_ids(unprocessed)
                        log(
                            f"Batch error: freed {len(unprocessed)} "
                            f"unprocessed update(s) for re-delivery on the next poll"
                        )
                finally:
                    self._batch_processed_ids = None
                # End-of-batch checkpoint for non-cursor state (session_id,
                # lock fields); cursor advances persist inside
                # _advance_update_cursor, so this is belt-and-suspenders.
                save_state(self.state)
        finally:
            self._background_poller.stop()
            save_state(self.state)
            log("Telegram daemon stopped")


# Sibling imports live at the bottom to break the circular: each sibling
# resolves patchable module-scope names through ``daemon`` at call time, so
# ``_d`` must be fully populated first.
from landline.orchestrator import batch as _batch  # noqa: E402
from landline.orchestrator import restart as _restart  # noqa: E402
from landline.orchestrator import poller_health as _poller_health  # noqa: E402
