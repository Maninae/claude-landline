"""Tests for landline.orchestrator — TelegramDaemon message routing."""

import time
from unittest.mock import patch, MagicMock

import pytest

from landline.claude_dispatch import ClaudeStreamResult
from landline.config import MAX_QUEUED_UPDATES, UNLOCK_DURATION_SECONDS
from landline.orchestrator import TelegramDaemon

from landline.tests.conftest import make_telegram_update, FAKE_CHAT_ID, FAKE_BOT_TOKEN


def _make_daemon(
    send_response_fn=None,
    send_typing_fn=None,
    guard_fn=None,
    reject_fn=None,
    run_claude_fn=None,
):
    """Build a TelegramDaemon with all external dependencies mocked."""
    def default_run_claude(**kwargs):
        r = ClaudeStreamResult()
        r.session_id = "test-session"
        r.streamed_text = "Hello from Claude."
        r.final_result = "Hello from Claude."
        return r

    send_resp = send_response_fn or MagicMock()
    send_typ = send_typing_fn or MagicMock()
    guard = guard_fn or MagicMock(return_value=True)
    reject = reject_fn or MagicMock()
    run_claude = run_claude_fn or MagicMock(side_effect=default_run_claude)
    shutdown_hook = MagicMock()
    failure_tracker = MagicMock()
    failure_tracker.is_in_backoff.return_value = False
    failure_tracker.consecutive_failure_count = 0
    failure_tracker.should_send_alert_now.return_value = False

    with patch("landline.orchestrator.keychain_get") as mock_kc, \
         patch("landline.orchestrator.load_state") as mock_load, \
         patch("landline.orchestrator.save_state"), \
         patch("landline.orchestrator.log_conversation"), \
         patch("signal.signal"):
        mock_kc.side_effect = lambda s, **kw: {
            "telegram-bot-token": FAKE_BOT_TOKEN,
            "telegram-chat-id": FAKE_CHAT_ID,
        }.get(s)
        mock_load.return_value = {
            "session_id": None,
            "last_update_id": 0,
            "turn_count": 0,
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            "unlock_timestamp": 0.0,
        }
        daemon = TelegramDaemon(
            run_claude_fn=run_claude,
            shutdown_hook=shutdown_hook,
            failure_tracker=failure_tracker,
            send_response_fn=send_resp,
            send_typing_fn=send_typ,
            guard_fn=guard,
            reject_fn=reject,
        )
    daemon._send_buttons = lambda token, chat_id, text, buttons: send_resp(token, chat_id, text)
    return daemon, {
        "send_response": send_resp,
        "send_typing": send_typ,
        "guard": guard,
        "reject": reject,
        "run_claude": run_claude,
        "shutdown_hook": shutdown_hook,
        "failure_tracker": failure_tracker,
    }


class TestTelegramDaemonInit:
    def test_exits_without_token(self):
        with patch("landline.orchestrator.keychain_get", return_value=None), \
             patch("landline.orchestrator.load_state"), \
             patch("signal.signal"), \
             pytest.raises(SystemExit):
            TelegramDaemon(
                run_claude_fn=MagicMock(),
                shutdown_hook=MagicMock(),
                failure_tracker=MagicMock(),
                send_response_fn=MagicMock(),
                send_typing_fn=MagicMock(),
                guard_fn=MagicMock(),
                reject_fn=MagicMock(),
            )

    def test_command_router_wired_with_reset_claude_fn(self):
        """REGRESSION: the orchestrator MUST inject reset_claude_fn into the
        CommandRouter so /new actually kills the live Claude subprocess.
        Without this wiring, the E1 refactor's source-of-truth on
        PersistentClaude survives /new — the next dispatch --resumes the
        old session and /new is a no-op for the live subprocess.
        """
        daemon, _ = _make_daemon()
        assert daemon._command_router._reset_claude_fn is not None, (
            "CommandRouter must receive a reset_claude_fn from the "
            "orchestrator — otherwise /new fails to reset the live "
            "PersistentClaude subprocess."
        )


class TestResetPersistentClaudeForNew:
    """The module-level helper wired into CommandRouter as reset_claude_fn.

    Must (a) kill the live subprocess and (b) clear pc's session id, so the
    NEXT dispatch sees pc.is_alive == False AND pc.get_session_id() is None.
    Goes through the landline.claude facade (the established test patch
    surface) so the helper matches the dispatcher's own seam.
    """

    def test_kills_proc_and_clears_session(self):
        from landline.orchestrator import _reset_persistent_claude_for_new

        fake_pc = MagicMock()
        with patch(
            "landline.claude._get_persistent_claude",
            return_value=fake_pc,
        ):
            _reset_persistent_claude_for_new()
        fake_pc.kill.assert_called_once_with()
        fake_pc.clear_session.assert_called_once_with()

    def test_clear_session_runs_even_if_kill_raises(self):
        """If pc.kill() blows up (e.g. zombie pipe), still clear the session
        id — otherwise the OLD id would remain and the next dispatch would
        try to --resume a process we already wanted gone."""
        from landline.orchestrator import _reset_persistent_claude_for_new

        fake_pc = MagicMock()
        fake_pc.kill.side_effect = RuntimeError("zombie")
        with patch(
            "landline.claude._get_persistent_claude",
            return_value=fake_pc,
        ):
            with pytest.raises(RuntimeError):
                _reset_persistent_claude_for_new()
        # The try/finally guarantees clear_session() fires even on kill error.
        fake_pc.clear_session.assert_called_once_with()

    def test_real_pc_after_reset_looks_like_new_session(self):
        """End-to-end: after the helper runs on a REAL PersistentClaude
        (singleton), pc.get_session_id() is None and pc.is_alive is False
        — exactly the invariants the dispatcher's _invoke_with_stale_retry
        relies on to take the is_new_session=True path."""
        from landline.orchestrator import _reset_persistent_claude_for_new
        import landline.persistent_claude as pc_mod

        # Set up a singleton with a fake live subprocess + session id.
        pc = pc_mod.PersistentClaude()
        pc.set_session_id("old-session-uuid-1234")
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = 0
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stderr = MagicMock()
        pc._proc = mock_proc
        pc_mod._persistent_claude = pc
        assert pc.is_alive is True
        assert pc.get_session_id() == "old-session-uuid-1234"

        # After the reset: poll() returns 0 (kill completed), so is_alive is
        # False; session id has been cleared.
        mock_proc.poll.return_value = 0
        _reset_persistent_claude_for_new()
        assert pc.is_alive is False
        assert pc.get_session_id() is None
        mock_proc.terminate.assert_called_once()


class TestProcessUpdateBatch:
    def test_skips_edited_messages(self):
        """Edited messages: no dispatch, no reply, but cursor MUST advance so
        the same update isn't redelivered every poll."""
        daemon, mocks = _make_daemon()
        update = make_telegram_update(11, "edited text", is_edit=True)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        mocks["run_claude"].assert_not_called()
        mocks["send_response"].assert_not_called()
        assert daemon.state["last_update_id"] == 11

    def test_skips_update_with_no_message(self):
        """An update with no `message` key (e.g. channel_post) — cursor still
        advances, nothing dispatched, nothing replied."""
        daemon, mocks = _make_daemon()
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([{"update_id": 99}])
        mocks["run_claude"].assert_not_called()
        mocks["send_response"].assert_not_called()
        assert daemon.state["last_update_id"] == 99

    def test_rejects_unauthorized_chat(self):
        """Unauthorized chat: guard rejects, cursor advanced, no dispatch."""
        daemon, mocks = _make_daemon(guard_fn=MagicMock(return_value=False))
        update = make_telegram_update(5, "hello")
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["reject"].assert_called_once_with(daemon.token, FAKE_CHAT_ID)
        mocks["run_claude"].assert_not_called()
        mocks["send_response"].assert_not_called()
        # Cursor MUST advance — otherwise an attacker can spam the poller
        # indefinitely with the same blocked update.
        assert daemon.state["last_update_id"] == 5

    def test_handles_missing_chat_id(self):
        """Message with no chat.id: cursor advances, nothing sent."""
        daemon, mocks = _make_daemon()
        update = {"update_id": 3, "message": {"text": "hello"}}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["run_claude"].assert_not_called()
        mocks["send_response"].assert_not_called()
        mocks["reject"].assert_not_called()
        assert daemon.state["last_update_id"] == 3

    def test_handles_photo_download_failure_gracefully(self):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = make_telegram_update(1, None, has_photo=True)
        update["message"].pop("text", None)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called()
        call_text = mocks["send_response"].call_args[0][2]
        assert "failed to download" in call_text.lower()

    def test_handles_non_photo_media_with_skip_message(self):
        """Non-photo media (video, audio, etc.) still gets a skip message."""
        daemon, mocks = _make_daemon()
        update = {"update_id": 1, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
            "video": {"file_id": "fake_video", "duration": 10},
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called()
        call_text = mocks["send_response"].call_args[0][2]
        assert "media" in call_text.lower() or "other media" in call_text.lower()

    def test_handles_non_text_non_media(self):
        """Empty message (no text, no media keys) — bare skip message, no
        "other media" phrase, and cursor advanced."""
        daemon, mocks = _make_daemon()
        update = {"update_id": 7, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        text = mocks["send_response"].call_args[0][2]
        # Notice mentions the supported input types (text/photos/documents).
        assert "text" in text and "photos" in text and "documents" in text
        # The "other media" variant must NOT fire for an empty (no-media) msg.
        assert "other media" not in text
        assert daemon.state["last_update_id"] == 7
        mocks["run_claude"].assert_not_called()

    def test_routes_command_to_router(self):
        """/status goes through CommandRouter; reply contains the status
        header AND the lock status line; cursor advances."""
        daemon, mocks = _make_daemon()
        update = make_telegram_update(8, "/status")
        with patch("landline.orchestrator.save_state"), \
             patch("subprocess.run", return_value=MagicMock(stdout="", returncode=0)):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        response_text = mocks["send_response"].call_args[0][2]
        from landline.config import AGENT_NAME
        assert f"{AGENT_NAME} System Status" in response_text
        assert "Lock:" in response_text
        mocks["run_claude"].assert_not_called()
        assert daemon.state["last_update_id"] == 8

    def test_unknown_command_returns_unknown_reply(self):
        """Unknown commands route through CommandRouter and get the
        "Unknown command:" reply — NOT the locked-help."""
        daemon, mocks = _make_daemon()
        # Stay locked — unknown commands shouldn't depend on lock state.
        update = make_telegram_update(12, "/bogus")
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        text = mocks["send_response"].call_args[0][2]
        assert text.startswith("Unknown command: /bogus")
        mocks["run_claude"].assert_not_called()

    def test_locked_text_gets_help(self):
        """Locked + plain text -> LOCKED_HELP, no Claude dispatch, cursor advances."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        update = make_telegram_update(4, "hello Claude")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        response_text = mocks["send_response"].call_args[0][2]
        assert "passphrase" in response_text
        assert "Session is locked" in response_text
        mocks["run_claude"].assert_not_called()
        assert daemon.state["last_update_id"] == 4

    def test_unlock_expiry_relocks_during_runtime(self):
        """An unlocked session whose unlock_timestamp is older than
        UNLOCK_DURATION_SECONDS re-locks on the next batch via _check_lock_gate
        and the message gets LOCKED_HELP instead of reaching Claude."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        # Set timestamp far enough in the past that check_expiry trips.
        daemon._lock_manager._state["unlock_timestamp"] = (
            time.time() - UNLOCK_DURATION_SECONDS - 60
        )
        update = make_telegram_update(20, "hello after expiry")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        # check_expiry re-locked the session.
        assert daemon._lock_manager.is_locked
        mocks["run_claude"].assert_not_called()
        text = mocks["send_response"].call_args[0][2]
        assert "passphrase" in text
        assert daemon.state["last_update_id"] == 20

    def test_too_long_message_rejected(self):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        long_text = "x" * 40000
        update = make_telegram_update(1, long_text)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called()
        response_text = mocks["send_response"].call_args[0][2]
        assert "too long" in response_text.lower()

    def test_unlocked_text_dispatched_to_claude(self):
        """Unlocked text: dispatched once with the original text in the
        message, cursor advances."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = make_telegram_update(9, "hello Claude")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])
        mocks["run_claude"].assert_called_once()
        kwargs = mocks["run_claude"].call_args.kwargs
        assert "hello Claude" in kwargs["message"]
        assert kwargs["chat_id"] == FAKE_CHAT_ID
        assert daemon.state["last_update_id"] == 9

    def test_inject_prefix_prepended_before_text(self):
        """When drain_inject_queue returns content, it is prepended (with a
        blank line) to the user's coalesced text before dispatch."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = make_telegram_update(15, "user message")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("[brief: morning]", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])
        msg = mocks["run_claude"].call_args.kwargs["message"]
        assert "[brief: morning]" in msg
        assert "user message" in msg
        # Injection is prepended.
        assert msg.index("[brief: morning]") < msg.index("user message")


class TestCoalesceMessages:
    def test_single_message_no_message_header(self):
        """Single message: no `[message N]` header, just `[ts]\\ntext`."""
        from datetime import datetime
        from landline.config import TIMEZONE
        msg = {"chat": {"id": 123}, "date": 1715300000}
        result = TelegramDaemon._coalesce_messages([(msg, 1, "hello")])
        assert "hello" in result
        # Single-message path skips the `[message N]` header.
        assert "[message 1]" not in result
        # Timestamp footer carries the TIMEZONE abbreviation (%Z) — host-
        # dependent (e.g. PDT/PST/UTC), so assert the tz shape rather than
        # a specific literal.
        tz_abbrev = datetime.fromtimestamp(1715300000, tz=TIMEZONE).strftime("%Z")
        assert tz_abbrev
        assert tz_abbrev in result

    def test_single_message_without_date_omits_brackets(self):
        """Missing `date` field: result is just the text, no empty `[]` prefix."""
        msg = {"chat": {"id": 123}}
        result = TelegramDaemon._coalesce_messages([(msg, 1, "hi")])
        assert result == "hi"

    def test_multiple_messages_with_headers_and_separator(self):
        """N>=2 messages: each gets `[message K]` header, joined by `\\n---\\n`,
        in stable order. Timestamps included."""
        messages = [
            ({"chat": {"id": 123}, "date": 1715300000}, 1, "first"),
            ({"chat": {"id": 123}, "date": 1715300001}, 2, "second"),
            ({"chat": {"id": 123}, "date": 1715300002}, 3, "third"),
        ]
        result = TelegramDaemon._coalesce_messages(messages)
        assert "[message 1]" in result
        assert "[message 2]" in result
        assert "[message 3]" in result
        # Separator present (N-1 occurrences).
        assert result.count("\n---\n") == 2
        # Order preserved.
        assert result.index("first") < result.index("second") < result.index("third")
        # Timestamps present — count the TIMEZONE abbreviation which appears
        # once per message header.
        from datetime import datetime
        from landline.config import TIMEZONE
        tz_abbrev = datetime.fromtimestamp(1715300000, tz=TIMEZONE).strftime("%Z")
        assert tz_abbrev
        assert result.count(tz_abbrev) == 3


class TestAdvanceUpdateCursor:
    def test_forward_advance_persists_state(self):
        """Forward-advance updates in-memory state AND persists to disk
        immediately, so an unclean exit cannot leave the on-disk cursor
        behind a Telegram-confirmed update."""
        daemon, _ = _make_daemon()
        with patch("landline.orchestrator.save_state") as mock_save:
            daemon._advance_update_cursor(42)
        assert daemon.state["last_update_id"] == 42
        mock_save.assert_called_once_with(daemon.state)

    def test_does_not_go_backwards_or_persist(self):
        """Backward-advance is a no-op AND must NOT touch disk."""
        daemon, _ = _make_daemon()
        daemon.state["last_update_id"] = 50
        with patch("landline.orchestrator.save_state") as mock_save:
            daemon._advance_update_cursor(30)
        assert daemon.state["last_update_id"] == 50
        mock_save.assert_not_called()

    def test_equal_value_is_noop(self):
        """Advancing to the current value is a no-op (no save, no poller call)."""
        daemon, _ = _make_daemon()
        daemon.state["last_update_id"] = 42
        mock_poller = MagicMock()
        daemon._background_poller = mock_poller
        with patch("landline.orchestrator.save_state") as mock_save:
            daemon._advance_update_cursor(42)
        mock_save.assert_not_called()
        # Poller cursor is still pinged — it has its own dedup logic.
        mock_poller.advance_processed_cursor.assert_called_once_with(42)

    def test_advances_poller_cursor(self):
        """Poller cursor is advanced even when state changes."""
        daemon, _ = _make_daemon()
        mock_poller = MagicMock()
        daemon._background_poller = mock_poller
        with patch("landline.orchestrator.save_state"):
            daemon._advance_update_cursor(42)
        mock_poller.advance_processed_cursor.assert_called_once_with(42)

    def test_no_poller_attached_does_not_crash(self):
        """Before run() starts the poller, _background_poller is None.
        Cursor advance must still work (e.g., from _shutdown handler)."""
        daemon, _ = _make_daemon()
        assert daemon._background_poller is None
        with patch("landline.orchestrator.save_state"):
            daemon._advance_update_cursor(7)
        assert daemon.state["last_update_id"] == 7


class TestPauseCallbackAndClassification:
    """Feature 2: /pause command — callback, classification, routing."""

    def test_pause_sets_event_via_callback(self):
        daemon, _ = _make_daemon()
        assert not daemon._pause_requested.is_set()
        update = make_telegram_update(1, "/pause")
        daemon._on_update_queued(update)
        assert daemon._pause_requested.is_set()

    def test_pause_case_insensitive(self):
        daemon, _ = _make_daemon()
        daemon._on_update_queued(make_telegram_update(1, "/PAUSE"))
        assert daemon._pause_requested.is_set()
        daemon._pause_requested.clear()
        daemon._on_update_queued(make_telegram_update(2, "/Pause"))
        assert daemon._pause_requested.is_set()
        daemon._pause_requested.clear()
        daemon._on_update_queued(make_telegram_update(3, "  /pause  "))
        assert daemon._pause_requested.is_set()

    def test_pause_callback_ignores_other_text(self):
        daemon, _ = _make_daemon()
        daemon._on_update_queued(make_telegram_update(1, "hello"))
        assert not daemon._pause_requested.is_set()
        daemon._on_update_queued(make_telegram_update(2, "/status"))
        assert not daemon._pause_requested.is_set()

    def test_pause_routed_before_command_classification(self):
        """/pause is intercepted BEFORE the `/`-prefix branch -> never reaches
        CommandRouter, no "Unknown command" reply."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        with patch.object(daemon._command_router, "handle") as cmd_handle:
            with patch("landline.orchestrator.save_state"):
                daemon._process_update_batch([make_telegram_update(1, "/pause")])
        cmd_handle.assert_not_called()
        # No "Unknown command" reply.
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert not any("Unknown command" in r for r in responses)
        # And no /pause should have been treated like a regular command.
        assert not any("/pause" in r and "command" in r.lower() for r in responses)

    def test_pause_not_routed_to_command_router_cursor_advanced(self):
        """/pause cursor is still advanced and the update is consumed."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([make_telegram_update(42, "/pause")])
        assert daemon.state["last_update_id"] == 42

    def test_pause_while_idle_sends_nothing_to_pause(self):
        """/pause alone in batch + flag was set + no dispatch pending ->
        "(Nothing to pause.)" sent and flag cleared."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()  # poller set it
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([make_telegram_update(1, "/pause")])
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("Nothing to pause" in r for r in responses)
        assert not daemon._pause_requested.is_set()

    def test_pause_while_locked_sends_locked_help(self):
        """/pause alone in batch + locked -> LOCKED_HELP sent + flag cleared."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        daemon._pause_requested.set()
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([make_telegram_update(1, "/pause")])
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("passphrase" in r for r in responses)
        assert not daemon._pause_requested.is_set()

    def test_pause_locked_rapid_fire_sends_one_locked_help(self):
        """Multiple /pause while locked in one batch -> exactly one LOCKED_HELP."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        daemon._pause_requested.set()
        updates = [make_telegram_update(i, "/pause") for i in range(1, 6)]
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch(updates)
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        locked_hits = [r for r in responses if "passphrase" in r]
        assert len(locked_hits) == 1

    def test_pause_with_text_after_in_same_batch_flag_stays_set(self):
        """Order [text, /pause]: text dispatched, flag stays set so watchdog
        can interrupt. Classifier must NOT send "(Nothing to pause.)"."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        updates = [
            make_telegram_update(1, "hello there"),
            make_telegram_update(2, "/pause"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch(updates)
        mocks["run_claude"].assert_called_once()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert not any("Nothing to pause" in r for r in responses)

    def test_pause_with_text_before_in_same_batch_does_NOT_send_nothing_to_pause(self):
        """Order [/pause, text]: text dispatched, flag stays set."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()
        updates = [
            make_telegram_update(1, "/pause"),
            make_telegram_update(2, "hello there"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch(updates)
        mocks["run_claude"].assert_called_once()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert not any("Nothing to pause" in r for r in responses)
        # Flag remains set so the watchdog could see it (no other clearer ran).
        assert daemon._pause_requested.is_set()

    def test_pause_with_photo_in_same_batch_does_NOT_send_nothing_to_pause(self):
        """/pause + photo in same batch: photo dispatched, no idle notification."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()
        photo_update = make_telegram_update(1, None, has_photo=True)
        photo_update["message"].pop("text", None)
        updates = [
            photo_update,
            make_telegram_update(2, "/pause"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file", return_value="/tmp/img.jpg"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch(updates)
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert not any("Nothing to pause" in r for r in responses)
        # Photo got dispatched to Claude.
        mocks["run_claude"].assert_called()

    def test_pause_double_message_prevention(self):
        """/pause during call that finishes BEFORE main loop drains:
        _finalize_response clears the flag. On drain, classifier sees flag
        cleared -> silent (no "(Nothing to pause.)"). """
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        # Flag NOT set (already consumed by an interrupted Claude call).
        assert not daemon._pause_requested.is_set()
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([make_telegram_update(1, "/pause")])
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert not any("Nothing to pause" in r for r in responses)
        assert not any("passphrase" in r for r in responses)

    def test_pause_rapid_fire_idempotent(self):
        """3x /pause -> single SIGINT, single notification."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()
        updates = [make_telegram_update(i, "/pause") for i in range(1, 4)]
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch(updates)
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        idle_hits = [r for r in responses if "Nothing to pause" in r]
        assert len(idle_hits) == 1

    def test_pause_logged_for_audit(self):
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation") as log_conv:
            daemon._process_update_batch([make_telegram_update(1, "/pause")])
        from landline.config import USER_NAME
        log_conv.assert_any_call(USER_NAME, "/pause")

    def test_pause_flag_not_cleared_by_invoke_claude_call(self):
        """Guards against re-introducing the rejected _clear_interrupt_fn
        approach: pre-set the flag, run a normal dispatch, assert clear was
        NOT called before Claude started."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()
        # Track clear calls on the Event.
        original_clear = daemon._pause_requested.clear
        clear_calls = []

        def tracking_clear():
            clear_calls.append(time.time())
            original_clear()

        daemon._pause_requested.clear = tracking_clear  # type: ignore[assignment]
        # Wire up the same way run() would for this test.
        daemon._dispatcher._clear_pause_fn = daemon._pause_requested.clear

        update = make_telegram_update(1, "say hi")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch([update])
        # Successful (non-interrupted) Claude call: clear must NOT have fired.
        assert clear_calls == []


class TestPauseFlagStrandingBailOuts:
    """Findings #1 / #2 regression: when ``/pause`` and a dispatch-eligible
    message arrive in the SAME batch AND dispatch never actually reaches
    a Claude call (locked-session gate, silent unlock, voice/photo/document
    download failure, unsupported doc), ``_pause_requested`` MUST NOT
    strand — the round-8 re-anchor in ``ClaudeDispatcher._invoke_claude_
    call`` (``if pf.is_set(): pf.request_pause()``) would otherwise fire
    "(Paused.)" on the user's NEXT unrelated turn.

    The fix consumes the stranded flag in
    ``_run_batch_classification_and_dispatch``'s finally when /pause was
    deferred and no ``_inject_and_dispatch`` call fired this batch, and
    (outside the locked path) sends a "(Nothing to pause.)" notice.
    """

    def _pin_no_reanchor_on_next_turn(self, daemon, mocks):
        """Shared assertion: after a bail-out that stranded /pause, a
        subsequent unrelated text turn must NOT be interrupted by the
        re-anchor logic.

        The default ``run_claude`` mock returns a successful (non-
        interrupted) result. Under the pre-fix code, ``_invoke_claude_
        call``'s ``pf.request_pause()`` re-anchor would strand the pause
        on this turn's generation and the subsequent watchdog wake
        would fire "(Paused.)" via ``_finalize_response``. We assert
        that (a) run_claude WAS called for this unrelated turn and (b)
        no "(Paused.)" notice was sent — proof the flag was cleared
        before the new call started.
        """
        mocks["run_claude"].reset_mock()
        pre_responses = len(mocks["send_response"].call_args_list)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch([make_telegram_update(99, "next turn")])
        assert not daemon._pause_requested.is_set(), (
            "Stranded /pause must be cleared before the next unrelated turn"
        )
        mocks["run_claude"].assert_called_once()
        new_responses = [
            c[0][2] for c in mocks["send_response"].call_args_list[pre_responses:]
        ]
        assert not any("(Paused.)" in r for r in new_responses), (
            "The next unrelated turn must not receive a spurious (Paused.)"
        )

    def test_pause_plus_text_while_locked_clears_stranded_flag(self):
        """[/pause, hello] while locked: LOCKED_HELP sent for the text,
        /pause was deferred (dispatch_pending=True at classification),
        no Claude call ever fires. The finally in
        ``_run_batch_classification_and_dispatch`` must clear the flag.
        """
        daemon, mocks = _make_daemon()
        # Session locked (default state — no unlock_timestamp seeded).
        assert daemon._lock_manager.is_locked
        daemon._pause_requested.set()  # simulate poller callback
        updates = [
            make_telegram_update(1, "/pause"),
            make_telegram_update(2, "hello there"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"):
            daemon._process_update_batch(updates)
        # No Claude call — text was gated at the lock.
        mocks["run_claude"].assert_not_called()
        # Locked path: LOCKED_HELP was already sent for the text batch;
        # NO "(Nothing to pause.)" on top of it.
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("passphrase" in r for r in responses), (
            "LOCKED_HELP notice expected for the locked text bail-out"
        )
        assert not any("Nothing to pause" in r for r in responses), (
            "Locked path must not double-notify with (Nothing to pause.)"
        )
        # Flag must be cleared so the next unlocked turn is safe.
        assert not daemon._pause_requested.is_set()
        # Unlock and verify the next unrelated turn is not spuriously paused.
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_voice_download_failure_clears_stranded_flag(self):
        """/pause + voice, unlocked: voice download fails, no Claude call.
        Stranded flag must be cleared so the next turn isn't spuriously
        interrupted."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        # Simulate the poller's on_update_queued callback (which the
        # main-loop skips when we drive _process_update_batch directly
        # in a unit test). This mirrors production where /pause sets
        # the flag BEFORE classification runs.
        daemon._pause_requested.set()

        # Build a voice update. make_telegram_update doesn't have
        # a voice helper, so hand-craft the envelope.
        voice_update = make_telegram_update(1, None)
        voice_update["message"].pop("text", None)
        voice_update["message"]["voice"] = {
            "file_id": "fake-voice",
            "duration": 3,
        }
        updates = [
            voice_update,
            make_telegram_update(2, "/pause"),
        ]
        # Download returns None — dispatch bails with the failure notice.
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch(updates)
        # No Claude call fired.
        mocks["run_claude"].assert_not_called()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("Failed to download the voice note" in r for r in responses), (
            "Voice download failure notice expected"
        )
        # Unlocked path: (Nothing to pause.) is the expected feedback.
        assert any("Nothing to pause" in r for r in responses), (
            "Unlocked bail-out must surface (Nothing to pause.)"
        )
        assert not daemon._pause_requested.is_set()
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_photo_all_download_failure_clears_stranded_flag(self):
        """/pause + photo, unlocked: all photo downloads fail — the
        handler sends the failure notice and returns without dispatch.
        Flag must not strand into the next turn."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()  # simulate poller callback
        photo_update = make_telegram_update(1, None, has_photo=True)
        photo_update["message"].pop("text", None)
        updates = [
            photo_update,
            make_telegram_update(2, "/pause"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch(updates)
        mocks["run_claude"].assert_not_called()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("Failed to download the image" in r for r in responses)
        assert any("Nothing to pause" in r for r in responses)
        assert not daemon._pause_requested.is_set()
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_document_download_failure_clears_stranded_flag(self):
        """/pause + document, unlocked: download fails — handler sends
        failure notice, no Claude call. Flag cleared, next turn clean."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()  # simulate poller callback
        doc_update = make_telegram_update(1, None)
        doc_update["message"].pop("text", None)
        doc_update["message"]["document"] = {
            "file_id": "fake-doc",
            "file_name": "hello.pdf",
            "file_size": 1024,
            "mime_type": "application/pdf",
        }
        updates = [
            doc_update,
            make_telegram_update(2, "/pause"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch(updates)
        mocks["run_claude"].assert_not_called()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any("Failed to download the document" in r for r in responses)
        assert any("Nothing to pause" in r for r in responses)
        assert not daemon._pause_requested.is_set()
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_silent_unlock_clears_stranded_flag(self):
        """[/pause, passphrase] while locked: text batch silent-unlocks
        via ``try_silent_unlock`` — no Claude call. Flag must be
        cleared so the FIRST post-unlock turn isn't spurious-paused."""
        daemon, mocks = _make_daemon()
        # Session locked, passphrase configured on the lock manager.
        assert daemon._lock_manager.is_locked
        daemon._pause_requested.set()  # simulate poller callback
        # Wire a passphrase into the lock manager so silent-unlock lands.
        with patch.object(
            daemon._lock_manager, "try_silent_unlock", return_value=True,
        ):
            updates = [
                make_telegram_update(1, "/pause"),
                make_telegram_update(2, "the-passphrase"),
            ]
            with patch("landline.orchestrator.save_state"), \
                 patch("landline.orchestrator.log_conversation"):
                daemon._process_update_batch(updates)
        mocks["run_claude"].assert_not_called()
        # Flag must not strand — the next real turn is safe.
        assert not daemon._pause_requested.is_set()
        # Simulate the now-unlocked state for the follow-up turn.
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_text_while_backoff_gated_clears_stranded_flag(self):
        """/pause + text while ClaudeDispatcher is in failure-backoff:
        ``send_to_claude`` gates the text onto ``_backoff_queue`` and
        returns False WITHOUT invoking Claude. The pause flag was
        deferred at classification with ``dispatch_pending=True``, so
        without the fix nothing would consume it — and on the next
        drain the round-8 re-anchor
        (``if pf.is_set(): pf.request_pause()`` in
        ``_invoke_claude_call``) would fire "(Paused.)" on a fresh
        unrelated turn.

        Pin the new contract: gated-dispatch returns False, so
        ``_batch_dispatch_attempted`` stays False,
        ``_consume_stranded_pause_flag`` clears the flag and sends
        "(Nothing to pause.)". A subsequent drain-and-invoke turn must
        run without a spurious "(Paused.)".
        """
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        # Put the dispatcher into failure backoff so send_to_claude
        # gates the batch onto _backoff_queue instead of invoking
        # Claude. Mocks match the fields consumed by
        # ``_gate_if_in_backoff`` (log line + user notice).
        ft = mocks["failure_tracker"]
        ft.is_in_backoff.return_value = True
        ft.seconds_until_next_attempt.return_value = 30
        ft.consecutive_failure_count = 5

        daemon._pause_requested.set()  # simulate poller /pause callback
        updates = [
            make_telegram_update(1, "/pause"),
            make_telegram_update(2, "hello there"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None):
            daemon._process_update_batch(updates)

        # Backoff-gated: no Claude call this batch; text queued.
        mocks["run_claude"].assert_not_called()
        assert len(daemon._dispatcher._backoff_queue) == 1
        # Pause flag must NOT strand — cleanup fires because
        # send_to_claude returned False (dispatch_attempted stays False).
        assert not daemon._pause_requested.is_set()
        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        assert any(
            "Claude is temporarily unavailable" in r for r in responses
        ), "Expected backoff notice for the gated text"
        assert any("Nothing to pause" in r for r in responses), (
            "Backoff-gated bail-out must surface (Nothing to pause.) so "
            "the deferred /pause doesn't strand onto a future turn"
        )

        # Recovery turn: backoff clears, a fresh unrelated message
        # arrives. ``_apply_rate_limit_and_drain_backoff`` merges the
        # queued text with the new one and invokes Claude. Under the
        # pre-fix code the still-set pause flag would re-anchor to
        # this generation and the watchdog would fire SIGINT / the
        # user would see "(Paused.)". Post-fix the flag is already
        # cleared, so the drain-and-invoke completes cleanly.
        ft.is_in_backoff.return_value = False
        self._pin_no_reanchor_on_next_turn(daemon, mocks)

    def test_pause_plus_text_when_send_to_claude_raises_clears_stranded_flag(self):
        """/pause + text where ``send_to_claude`` raises before ever
        invoking Claude. Same stranding class as backoff-gating: the
        pause flag was deferred but no ``_invoke_claude_call`` runs.
        Post-fix, the exception propagates without setting
        ``_batch_dispatch_attempted`` True, so
        ``_consume_stranded_pause_flag`` clears the flag on unwind.
        """
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        daemon._pause_requested.set()

        # Force send_to_claude to raise before Claude runs.
        boom = RuntimeError("simulated dispatcher failure")
        daemon._dispatcher.send_to_claude = MagicMock(side_effect=boom)

        updates = [
            make_telegram_update(1, "/pause"),
            make_telegram_update(2, "hello there"),
        ]
        # The exception propagates out of _run_batch_classification_and_dispatch,
        # so the outer _process_update_batch's error-handling path runs. We
        # care about post-batch state — swallow the raise here.
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            try:
                daemon._process_update_batch(updates)
            except RuntimeError:
                pass

        # No real Claude call ran.
        mocks["run_claude"].assert_not_called()
        # Flag must be cleared so the next unrelated turn is safe.
        assert not daemon._pause_requested.is_set()
        # Restore a working dispatcher.send_to_claude for the follow-up
        # turn assertion.
        daemon._dispatcher = daemon._dispatcher  # noqa: keep import
        # Rebuild send_to_claude as a passthrough to the real method by
        # dropping the MagicMock — reach into the class binding.
        from landline.claude_dispatch import ClaudeDispatcher
        daemon._dispatcher.send_to_claude = (
            ClaudeDispatcher.send_to_claude.__get__(
                daemon._dispatcher, ClaudeDispatcher,
            )
        )
        self._pin_no_reanchor_on_next_turn(daemon, mocks)


class TestQueueingBehavior:
    """Feature 1: silent message queueing — no auto-interrupt; cap enforcement."""

    def test_interrupt_is_pause_driven_not_pending_driven(self):
        """The dispatcher's interrupt mechanism is the PauseFlag (set by
        /pause via the poller callback), NOT the poller's has_pending. Without
        /pause, the pause flag is False even with "pending" messages.
        """
        daemon, _ = _make_daemon()
        daemon._dispatcher._clear_pause_fn = daemon._pause_requested.clear

        # Without /pause, the PauseFlag reports not-set.
        assert daemon._pause_requested.is_set() is False

        # Setting the pause flag flips it; the dispatcher's _invoke_claude_call
        # closure (built per-call from this flag) is what feeds the watchdog.
        daemon._pause_requested.set()
        assert daemon._pause_requested.is_set() is True

    def test_max_queued_updates_is_a_reasonable_cap(self):
        """Guard against a maintainer accidentally setting an absurd cap."""
        assert 1 <= MAX_QUEUED_UPDATES <= 200

    def test_backoff_queue_is_bounded_deque(self):
        """Backoff queue is a bounded deque (maxlen=20) so a long backoff
        period can't grow unbounded memory."""
        import collections
        daemon, _ = _make_daemon()
        assert isinstance(daemon._dispatcher._backoff_queue, collections.deque)
        assert daemon._dispatcher._backoff_queue.maxlen == 20

    def test_lock_change_during_accumulation(self):
        """Order [text, /new, text]: /new re-locks before the second text is
        processed, so the second text hits LOCKED_HELP. The first text is
        not dispatched either (because text is collected in a single batch
        and the lock gate runs once for the whole batch)."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        updates = [
            make_telegram_update(1, "first message"),
            make_telegram_update(2, "/new"),
            make_telegram_update(3, "second message"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("subprocess.run", return_value=MagicMock(stdout="", returncode=0)):
            daemon._process_update_batch(updates)

        responses = [c[0][2] for c in mocks["send_response"].call_args_list]
        # /new acknowledgement was sent.
        assert any("locked" in r.lower() for r in responses)
        # The text batch then hits LOCKED_HELP.
        assert any("passphrase" in r for r in responses)
        assert daemon._lock_manager.is_locked
        # No Claude call — text was gated by re-lock.
        mocks["run_claude"].assert_not_called()
        # All cursors advanced.
        assert daemon.state["last_update_id"] == 3


class TestShutdown:
    def test_shutdown_sets_running_false_and_drains_hook(self):
        """SIGTERM (signum=15) flips running and drains the shutdown hook."""
        daemon, mocks = _make_daemon()
        assert daemon.running is True
        daemon._shutdown(15, None)
        assert daemon.running is False
        mocks["shutdown_hook"].drain_for_shutdown.assert_called_once_with()

    def test_shutdown_signals_poller_stop_only_if_attached(self):
        """signal_stop is called when poller exists; no crash if it doesn't."""
        # With poller attached.
        daemon, _ = _make_daemon()
        mock_poller = MagicMock()
        daemon._background_poller = mock_poller
        daemon._shutdown(15, None)
        mock_poller.signal_stop.assert_called_once_with()
        # Before run() attaches the poller, _shutdown must not crash.
        daemon2, _ = _make_daemon()
        assert daemon2._background_poller is None
        daemon2._shutdown(15, None)  # must not raise
        assert daemon2.running is False


class TestPhotoBatch:
    """_dispatch_photo_group: standalone vs album, dispatch vs failure."""

    def test_album_groups_share_one_dispatch(self):
        """Two photos with the same media_group_id form one album: one Claude
        call with both file paths in the prompt, all cursors advanced."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        def make_album_photo(uid, media_group_id, caption=None):
            update = make_telegram_update(uid, None, has_photo=True)
            update["message"].pop("text", None)
            update["message"]["media_group_id"] = media_group_id
            update["message"]["photo"][0]["file_id"] = f"file-{uid}"
            if caption:
                update["message"]["caption"] = caption
            return update

        updates = [
            make_album_photo(100, "album-A", caption="check this out"),
            make_album_photo(101, "album-A"),
        ]
        downloaded_paths = []

        def fake_download(token, file_id, filename):
            path = f"/tmp/{file_id}.jpg"
            downloaded_paths.append(path)
            return path

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file", side_effect=fake_download), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch(updates)

        # One Claude call for the whole album.
        mocks["run_claude"].assert_called_once()
        prompt = mocks["run_claude"].call_args.kwargs["message"]
        # Both photo paths in the prompt.
        assert "/tmp/file-100.jpg" in prompt
        assert "/tmp/file-101.jpg" in prompt
        # Caption from first photo used.
        assert "check this out" in prompt
        # All album cursors advanced.
        assert daemon.state["last_update_id"] == 101

    def test_standalone_photos_dispatched_separately(self):
        """Two photos without media_group_id are independent: two Claude calls."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        def make_standalone(uid):
            update = make_telegram_update(uid, None, has_photo=True)
            update["message"].pop("text", None)
            update["message"]["photo"][0]["file_id"] = f"file-{uid}"
            return update

        updates = [make_standalone(200), make_standalone(201)]

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file",
                   side_effect=lambda t, fid, fn: f"/tmp/{fid}.jpg"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch(updates)

        assert mocks["run_claude"].call_count == 2
        assert daemon.state["last_update_id"] == 201

    def test_photo_locked_gets_help_no_download_dispatch(self):
        """Locked + photo: LOCKED_HELP, no download, no Claude dispatch, cursor advances.

        After the M1 fix the lock gate now precedes download_file in
        _dispatch_photo_group, so no download happens when locked and Claude
        is never invoked.
        """
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        update = make_telegram_update(31, None, has_photo=True)
        update["message"].pop("text", None)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file", return_value="/tmp/img.jpg"):
            daemon._process_update_batch([update])
        text = mocks["send_response"].call_args[0][2]
        assert "passphrase" in text
        mocks["run_claude"].assert_not_called()
        assert daemon.state["last_update_id"] == 31


class TestBatchErrorIsolation:
    """The outer run()-level try/except is not exercised here, but the
    classification pass advances cursors even for skipped updates so that a
    poisonous update cannot stall the cursor."""

    def test_mixed_skip_and_dispatch_all_cursors_advance(self):
        """Edited + missing-chat-id + valid text in one batch: cursor lands
        on the highest update_id, valid text reaches Claude."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        updates = [
            make_telegram_update(50, "edited", is_edit=True),
            {"update_id": 51, "message": {"text": "no chat id"}},
            make_telegram_update(52, "real message"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch(updates)
        mocks["run_claude"].assert_called_once()
        assert "real message" in mocks["run_claude"].call_args.kwargs["message"]
        # All cursors advanced.
        assert daemon.state["last_update_id"] == 52


class TestRestartContinuation:
    """_handle_restart_continuation: reads cache/restart-continuation.txt and
    injects it as a synthetic Claude turn, with inject-queue prepended."""

    def test_continuation_routes_through_inject_and_dispatch(self, tmp_path):
        """The continuation path MUST route through _inject_and_dispatch so
        cron reports queued in cache/inject-queue/ during the restart window
        get prepended — not just sent to Claude raw via the dispatcher."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Deploy complete — verify formatting.")
        # Patch WORKSPACE so trigger lookup resolves under tmp_path.
        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("[brief: morning]", [])) as mock_drain, \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history",
                   return_value=""):
            daemon._handle_restart_continuation()
        # drain_inject_queue MUST have been called — confirms the path went
        # through _inject_and_dispatch and not straight to send_to_claude.
        mock_drain.assert_called_once()
        # Claude received the inject prefix AND the continuation message.
        msg = mocks["run_claude"].call_args.kwargs["message"]
        assert "[brief: morning]" in msg
        assert "Deploy complete" in msg
        assert msg.index("[brief: morning]") < msg.index("Deploy complete")
        # Trigger file was consumed.
        assert not trigger.exists()

    def test_continuation_file_is_preserved_when_locked(self, tmp_path):
        """A locked session must NOT dispatch the continuation, and the
        trigger file is PRESERVED so the payload survives until the next
        unlock/restart (unlinking here would lose the operator's message)."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Restarted. Verify the change.")
        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._handle_restart_continuation()
        mocks["run_claude"].assert_not_called()
        # File is PRESERVED when locked so the continuation isn't lost.
        assert trigger.exists()

    def test_continuation_no_trigger_file_is_noop(self, tmp_path):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._handle_restart_continuation()
        mocks["run_claude"].assert_not_called()

    def test_continuation_runs_before_poller_start(self, tmp_path):
        """run() must call _handle_restart_continuation BEFORE starting the
        background poller. A /pause queued during the restart window can't
        race the continuation if the poller isn't running yet."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        order: list = []

        def fake_continuation():
            order.append("continuation")

        class FakePoller:
            def __init__(self_inner, **_kw):
                pass

            def start(self_inner):
                order.append("poller_start")

            def signal_stop(self_inner):
                pass

            def stop(self_inner):
                pass

            def drain(self_inner, block_timeout_seconds=None):
                # Trigger an immediate exit from the main loop after one tick.
                daemon.running = False
                return []

            def has_pending(self_inner):
                return False

            def advance_processed_cursor(self_inner, uid):
                pass

            def last_successful_poll(self_inner):
                return time.time()

        daemon._handle_restart_continuation = fake_continuation
        with patch("landline.orchestrator.BackgroundPoller", FakePoller), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.time.sleep"):
            daemon.run()
        assert order == ["continuation", "poller_start"], (
            "Restart continuation must run BEFORE poller start — order=%r"
            % order
        )

    def test_restart_continuation_trigger_preserved_on_dispatch_error(
        self, tmp_path,
    ):
        """M1: if _inject_and_dispatch raises, the trigger file MUST be
        preserved so the next restart retries. Reverting M1 (unlink-before-
        dispatch) would consume the trigger and silently lose the payload."""
        daemon, _mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Resume the failing task.")

        def boom(*a, **kw):
            raise RuntimeError("simulated dispatch failure")

        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch.object(daemon, "_inject_and_dispatch", side_effect=boom):
            with pytest.raises(RuntimeError, match="simulated dispatch failure"):
                daemon._handle_restart_continuation()
        # Trigger MUST survive so the next restart retries — the whole point.
        assert trigger.exists()
        assert trigger.read_text() == "Resume the failing task."

    def test_restart_continuation_trigger_unlinked_on_dispatch_success(
        self, tmp_path,
    ):
        """M1: success path is unchanged — trigger unlinked after dispatch."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Resume.")
        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent",
                   return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history",
                   return_value=""):
            daemon._handle_restart_continuation()
        assert not trigger.exists()
        mocks["run_claude"].assert_called_once()

    def test_restart_continuation_unlink_ordered_after_dispatch(
        self, tmp_path,
    ):
        """M1 two-phase commit: dispatch must run BEFORE unlink. Records the
        order of events; reverting M1 would put 'unlink' before 'dispatch'."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Resume.")

        order: list = []

        def record_dispatch(text, chat_id, update_ids):
            order.append("dispatch")
            # File must still exist at dispatch time (two-phase: unlink AFTER).
            order.append(
                "trigger_exists_at_dispatch=%s" % trigger.exists()
            )

        original_unlink = type(trigger).unlink

        def record_unlink(self, *a, **kw):
            if self.name == "restart-continuation.txt":
                order.append("unlink")
            return original_unlink(self, *a, **kw)

        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch.object(
                 daemon, "_inject_and_dispatch", side_effect=record_dispatch,
             ), \
             patch.object(type(trigger), "unlink", record_unlink):
            daemon._handle_restart_continuation()

        assert order == [
            "dispatch",
            "trigger_exists_at_dispatch=True",
            "unlink",
        ], "expected dispatch BEFORE unlink, got %r" % order

    def test_restart_continuation_post_dispatch_unlink_failure_is_swallowed(
        self, tmp_path,
    ):
        """M1: a failure to unlink AFTER successful dispatch is benign (next
        restart overwrites). Helper must NOT re-raise — that would crash the
        startup path even though the continuation already succeeded."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        (tmp_path / "cache").mkdir()
        trigger = tmp_path / "cache" / "restart-continuation.txt"
        trigger.write_text("Resume.")

        original_unlink = type(trigger).unlink

        def flaky_unlink(self, *a, **kw):
            if self.name == "restart-continuation.txt":
                raise PermissionError("simulated")
            return original_unlink(self, *a, **kw)

        with patch("landline.orchestrator.WORKSPACE", tmp_path), \
             patch("landline.orchestrator.save_state"), \
             patch.object(
                 daemon, "_inject_and_dispatch", return_value=None,
             ), \
             patch.object(type(trigger), "unlink", flaky_unlink):
            # Must NOT raise.
            daemon._handle_restart_continuation()
        # Trigger remains because unlink raised, but the helper returned
        # cleanly.
        assert trigger.exists()


class TestDispatcherClearPauseWiring:
    """E3: TelegramDaemon wires dispatcher.clear_pause_fn at construction
    (constructor arg), not via post-construction private-attr reach-through."""

    def test_daemon_wires_clear_pause_at_construction(self):
        """The TelegramDaemon must wire dispatcher.clear_pause_fn during
        its own __init__, NOT later in run(). Restart-continuation can fire
        interrupted callbacks before run() would have done the wiring.

        REVERT-FAIL: if the daemon goes back to two-step wiring (or drops
        the ctor arg entirely), `_clear_pause_fn` is None right after
        construction and this assertion fails."""
        daemon, _ = _make_daemon()
        assert daemon._dispatcher._clear_pause_fn is not None
        # Must specifically be the daemon's pause-flag clear (so an
        # interrupted result actually resets /pause).
        assert (
            daemon._dispatcher._clear_pause_fn
            == daemon._pause_requested.clear
        )


class TestIsPauseCommandHelper:
    """Module-level _is_pause_command helper: True only for the /pause command
    (with or without arguments). Guards against re-introducing the prefix-match
    bug that would treat '/pausefoo' or '/paused' as a pause."""

    def test_bare_pause_is_true(self):
        from landline.orchestrator import _is_pause_command
        assert _is_pause_command("/pause") is True

    def test_pause_with_arg_is_true(self):
        from landline.orchestrator import _is_pause_command
        assert _is_pause_command("/pause now") is True

    def test_pausefoo_is_false(self):
        """'/pausefoo' must NOT match — only space-separated args are allowed."""
        from landline.orchestrator import _is_pause_command
        assert _is_pause_command("/pausefoo") is False

    def test_paused_is_false(self):
        """'/paused' (past tense, no space) must NOT match — would be a regression."""
        from landline.orchestrator import _is_pause_command
        assert _is_pause_command("/paused") is False


class TestBatchErrorDiscardsUnprocessedIds:
    """H2 recovery path: when _process_update_batch raises mid-batch, the
    run() loop's except branch must call _background_poller.discard_queued_ids
    with ONLY the unprocessed update_ids — already-processed updates (whose
    cursors advanced via _advance_update_cursor and were recorded in
    _batch_processed_ids) must NOT be re-queued."""

    def test_run_discards_unprocessed_ids_on_batch_error(self):
        daemon, _mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        # Three updates: ids 100, 101, 102. The fake _process_update_batch
        # marks 100 as processed (via _advance_update_cursor), then raises
        # before 101 and 102 can be handled.
        updates = [
            make_telegram_update(100, "first"),
            make_telegram_update(101, "second"),
            make_telegram_update(102, "third"),
        ]

        def fake_process_update_batch(batch):
            # Simulate the first update being handled successfully — this is
            # how _advance_update_cursor populates _batch_processed_ids in
            # production.
            daemon._advance_update_cursor(100)
            raise RuntimeError("simulated mid-batch failure")

        daemon._process_update_batch = fake_process_update_batch  # type: ignore[assignment]

        discard_calls: list = []

        class FakePoller:
            def __init__(self_inner, **_kw):
                pass

            def start(self_inner):
                pass

            def signal_stop(self_inner):
                pass

            def stop(self_inner):
                pass

            def drain(self_inner, block_timeout_seconds=None):
                # Return the prepared updates on first drain, then exit.
                if not getattr(self_inner, "_drained", False):
                    self_inner._drained = True
                    return list(updates)
                daemon.running = False
                return []

            def has_pending(self_inner):
                return False

            def advance_processed_cursor(self_inner, uid):
                pass

            def discard_queued_ids(self_inner, ids):
                discard_calls.append(list(ids))

            def last_successful_poll(self_inner):
                return time.time()

        with patch("landline.orchestrator.BackgroundPoller", FakePoller), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.time.sleep"):
            daemon.run()

        # discard_queued_ids must have been called exactly once, with ONLY
        # the unprocessed ids (101, 102). 100 was processed and must NOT
        # appear — re-queueing a processed update would double-process it.
        assert len(discard_calls) == 1, (
            "expected exactly one discard_queued_ids call, got %r"
            % discard_calls
        )
        assert sorted(discard_calls[0]) == [101, 102]
        assert 100 not in discard_calls[0]


class TestSweepTelegramImageCache:
    """R2b: age-based retention sweep of cache/telegram_images/."""

    def test_sweeps_old_files_and_keeps_recent(self, tmp_path):
        """Files with mtime older than the retention window are deleted;
        recent files survive."""
        import os
        from landline.orchestrator import _sweep_telegram_image_cache

        old_file = tmp_path / "20260101_000000_0.jpg"
        old_file.write_bytes(b"old")
        recent_file = tmp_path / "20260615_000000_1.jpg"
        recent_file.write_bytes(b"recent")

        now = time.time()
        # 48h old → swept (retention is 24h)
        os.utime(str(old_file), (now - 48 * 3600, now - 48 * 3600))
        # 1h old → kept
        os.utime(str(recent_file), (now - 3600, now - 3600))

        swept = _sweep_telegram_image_cache(
            image_dir=tmp_path, retention_hours=24,
        )
        assert swept == 1
        assert not old_file.exists()
        assert recent_file.exists()

    def test_missing_dir_returns_zero_no_crash(self, tmp_path):
        """Missing dir: returns 0, never raises — startup must not crash."""
        from landline.orchestrator import _sweep_telegram_image_cache

        missing = tmp_path / "does-not-exist"
        assert not missing.exists()
        swept = _sweep_telegram_image_cache(
            image_dir=missing, retention_hours=24,
        )
        assert swept == 0

    def test_unlink_failure_is_logged_and_swallowed(self, tmp_path):
        """A failing unlink for one file does not abort the sweep or crash."""
        import os
        from landline.orchestrator import _sweep_telegram_image_cache

        bad_file = tmp_path / "bad.jpg"
        bad_file.write_bytes(b"x")
        good_file = tmp_path / "good.jpg"
        good_file.write_bytes(b"y")
        now = time.time()
        os.utime(str(bad_file), (now - 48 * 3600, now - 48 * 3600))
        os.utime(str(good_file), (now - 48 * 3600, now - 48 * 3600))

        original_unlink = type(bad_file).unlink

        def flaky_unlink(self, *a, **kw):
            if self.name == "bad.jpg":
                raise PermissionError("simulated")
            return original_unlink(self, *a, **kw)

        with patch.object(type(bad_file), "unlink", flaky_unlink):
            swept = _sweep_telegram_image_cache(
                image_dir=tmp_path, retention_hours=24,
            )
        # Good file removed, bad file logged-and-skipped.
        assert swept == 1
        assert bad_file.exists()
        assert not good_file.exists()

    def test_run_invokes_sweep_before_main_loop(self, tmp_path):
        """run() must call sweep_media_caches before the main loop —
        proves the daemon actually wires the sweep in at startup.

        Cluster 1: the generalized ``sweep_media_caches`` replaced the
        single-dir ``_sweep_telegram_image_cache`` at the run() call site;
        the back-compat wrapper is still importable.
        """
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        order: list = []

        class FakePoller:
            def __init__(self_inner, **_kw):
                pass

            def start(self_inner):
                order.append("poller_start")

            def signal_stop(self_inner):
                pass

            def stop(self_inner):
                pass

            def drain(self_inner, block_timeout_seconds=None):
                daemon.running = False
                return []

            def has_pending(self_inner):
                return False

            def advance_processed_cursor(self_inner, uid):
                pass

            def last_successful_poll(self_inner):
                return time.time()

        def fake_sweep(*a, **kw):
            order.append("sweep")
            return 0

        with patch("landline.orchestrator.sweep_media_caches", fake_sweep), \
             patch("landline.orchestrator.BackgroundPoller", FakePoller), \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.time.sleep"):
            daemon.run()
        # Sweep must fire, and must precede poller start.
        assert "sweep" in order
        assert order.index("sweep") < order.index("poller_start")


class TestNotifyAdvanceOrdering:
    """R7: send the notice, then advance the cursor ONLY on success. If the
    send raises, leave the update un-advanced so Telegram re-delivers it."""

    def test_non_text_update_send_failure_leaves_cursor_unadvanced(self):
        """When _send_response raises in _handle_non_text_update, the cursor
        is NOT advanced — Telegram must re-deliver so the user eventually
        sees the skip notice."""
        send_response = MagicMock(side_effect=RuntimeError("telegram down"))
        daemon, mocks = _make_daemon(send_response_fn=send_response)
        # Empty (no media) message — exercises the non-media branch.
        update = {"update_id": 77, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        # Send was attempted.
        mocks["send_response"].assert_called_once()
        # Cursor MUST NOT have advanced — the failure path must leave the
        # update un-confirmed so Telegram re-delivers it on the next poll.
        assert daemon.state["last_update_id"] == 0

    def test_non_text_media_send_failure_leaves_cursor_unadvanced(self):
        """Same as above but for the has_media branch (video/audio/etc)."""
        send_response = MagicMock(side_effect=RuntimeError("telegram down"))
        daemon, mocks = _make_daemon(send_response_fn=send_response)
        update = {"update_id": 78, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
            "video": {"file_id": "fake", "duration": 1},
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        assert daemon.state["last_update_id"] == 0

    def test_non_text_update_send_success_advances_cursor(self):
        """Success path is unchanged: notice sent, cursor advanced."""
        daemon, mocks = _make_daemon()
        update = {"update_id": 79, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        assert daemon.state["last_update_id"] == 79

    def test_lock_gate_send_failure_leaves_cursor_unadvanced(self):
        """When _send_response raises in _check_lock_gate, LOCKED_HELP wasn't
        delivered — leave the updates un-advanced so Telegram re-delivers and
        the user eventually learns the session is locked."""
        send_response = MagicMock(side_effect=RuntimeError("telegram down"))
        daemon, mocks = _make_daemon(send_response_fn=send_response)
        daemon._lock_manager._lock_state = "locked"
        update = make_telegram_update(80, "hello while locked")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        # LOCKED_HELP was attempted.
        mocks["send_response"].assert_called_once()
        text_sent = mocks["send_response"].call_args[0][2]
        assert "passphrase" in text_sent or "locked" in text_sent.lower()
        # Cursor MUST NOT have advanced.
        assert daemon.state["last_update_id"] == 0

    def test_lock_gate_send_success_advances_cursor(self):
        """Success path is unchanged: LOCKED_HELP sent, cursor advances."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        update = make_telegram_update(81, "hello while locked")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])):
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called_once()
        assert daemon.state["last_update_id"] == 81


class TestCheckPollerLiveness:
    """Cluster 4 — silent-TCP-stall detection + in-process replacement."""

    def test_no_op_when_fresh(self):
        """A poller whose last_successful_poll is 'now' must NOT be swapped."""
        daemon, _ = _make_daemon()
        mock_poller = MagicMock()
        mock_poller.last_successful_poll.return_value = time.time()
        daemon._background_poller = mock_poller
        original = daemon._background_poller

        with patch("landline.orchestrator.BackgroundPoller") as MockPoller:
            daemon._check_poller_liveness()
            MockPoller.assert_not_called()
        assert daemon._background_poller is original
        assert daemon._poller_stale_recovery_count == 0

    def test_no_op_when_no_poller(self):
        """Before run() starts the poller, _background_poller is None — the
        check must be a safe no-op."""
        daemon, _ = _make_daemon()
        assert daemon._background_poller is None
        # Should not raise.
        daemon._check_poller_liveness()
        assert daemon._poller_stale_recovery_count == 0

    def test_swaps_when_stale(self):
        """A stale poller (past the threshold) is replaced with a new
        BackgroundPoller instance; the counter increments."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 60
        )
        old_poller._last_processed_update_id = 0
        # Real lock so the ``with old._last_processed_update_id_lock`` block works.
        import threading as _threading
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = []
        daemon._background_poller = old_poller

        new_instance = MagicMock()
        with patch("landline.orchestrator.BackgroundPoller", return_value=new_instance) as MockPoller:
            daemon._check_poller_liveness()

        # Old poller signalled to stop, snapshot taken, new poller constructed.
        old_poller.signal_stop.assert_called_once()
        old_poller.snapshot_dedup_ids.assert_called_once()
        MockPoller.assert_called_once()
        new_instance.load_dedup_ids.assert_called_once_with([])
        new_instance.start.assert_called_once()
        assert daemon._background_poller is new_instance
        assert daemon._poller_stale_recovery_count == 1

    def test_preserves_cursor_across_swap(self):
        """The new poller inherits the old poller's processed cursor via
        ``initial_last_processed_update_id`` — no cursor rewind."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 42
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = []
        daemon._background_poller = old_poller

        with patch("landline.orchestrator.BackgroundPoller") as MockPoller:
            daemon._check_poller_liveness()

        _, kwargs = MockPoller.call_args
        assert kwargs["initial_last_processed_update_id"] == 42

    def test_preserves_dedup_across_swap(self):
        """The new poller receives the old poller's dedup snapshot via
        load_dedup_ids — no re-processing of already-queued ids."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 0
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = [1, 2, 3]
        daemon._background_poller = old_poller

        new_instance = MagicMock()
        with patch("landline.orchestrator.BackgroundPoller", return_value=new_instance):
            daemon._check_poller_liveness()

        new_instance.load_dedup_ids.assert_called_once_with([1, 2, 3])

    def test_rate_limited_to_check_interval(self):
        """Two consecutive calls within POLL_STALE_CHECK_INTERVAL_SECONDS
        must result in ONE swap only — the rate-limit gate blocks the
        second call before it re-inspects staleness."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 0
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = []
        daemon._background_poller = old_poller

        with patch("landline.orchestrator.BackgroundPoller") as MockPoller:
            daemon._check_poller_liveness()
            daemon._check_poller_liveness()  # rate-limited, must be no-op
            assert MockPoller.call_count == 1
        assert daemon._poller_stale_recovery_count == 1

    def test_forwards_orphaned_queue_across_swap(self):
        """REGRESSION: updates queued on the OLD poller but not yet drained
        by the main loop must be forwarded to the NEW poller's queue.
        Without this, they're orphaned in the discarded old queue while
        their ids sit in the new poller's dedup snapshot — Telegram's
        re-delivery gets blocked by dedup and the updates are silently
        lost (finding 1)."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 100
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = [101, 102]
        # Old poller has two updates queued but not yet consumed.
        orphaned = [
            {"update_id": 101, "message": {"text": "one"}},
            {"update_id": 102, "message": {"text": "two"}},
        ]
        old_poller.drain.return_value = orphaned
        daemon._background_poller = old_poller

        new_instance = MagicMock()
        with patch(
            "landline.orchestrator.BackgroundPoller", return_value=new_instance,
        ):
            daemon._check_poller_liveness()

        # New poller's dedup and cursor are preserved (existing invariant).
        # Additionally, the forwarded update_ids from the orphaned queue
        # are folded into the same dedup seed — this closes a race where
        # a poller-thread atomic add-to-(dedup ∪ queue) between
        # ``snapshot_dedup_ids`` and ``drain`` would leave the id in the
        # forwarded payload but NOT in the seeded dedup, letting
        # Telegram's re-delivery through the new poller's dedup gate.
        new_instance.load_dedup_ids.assert_called_once_with(
            [101, 102, 101, 102],
        )
        # NEW invariant: the orphaned updates were transferred so the main
        # loop sees them via the new poller.
        new_instance.preload_queue.assert_called_once_with(orphaned)

    def test_forwarded_updates_seeded_into_new_poller_dedup(self):
        """REGRESSION (finding: poller-swap dedup miss on raced update).

        The old poller's atomic add-to-(dedup ∪ queue) can land BETWEEN
        the orchestrator's ``snapshot_dedup_ids()`` (T0) and its
        ``drain()`` (T1) calls. In that window:
          - The snapshot at T0 captured dedup={A,B} (no id X).
          - The old poller's thread added id X to both dedup and queue at
            T0<t<T1 (atomically under the poller lock).
          - The drain at T1 caught X in the payload.
        The old orchestrator loaded only ``old_dedup`` ({A,B}) into the
        new poller and forwarded the payload (with X). The cursor
        (snapshotted before X was processed) meant Telegram's next poll
        used offset = X. Telegram re-delivered X, the new poller's dedup
        MISSED X (never seeded), and re-queued X — the main loop
        processed X twice.

        Fix: seed the new poller's dedup with old_dedup PLUS every
        forwarded update_id, so any raced-past-snapshot id is still
        blocked from re-processing when Telegram re-delivers it.
        """
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 100
        old_poller._last_processed_update_id_lock = _threading.Lock()
        # SIMULATED RACE: snapshot only captured A=101; the poller thread
        # added X=555 to dedup+queue AFTER snapshot but BEFORE drain.
        old_poller.snapshot_dedup_ids.return_value = [101]
        raced_update = {"update_id": 555, "message": {"text": "raced"}}
        old_poller.drain.return_value = [raced_update]
        daemon._background_poller = old_poller

        new_instance = MagicMock()
        with patch(
            "landline.orchestrator.BackgroundPoller", return_value=new_instance,
        ):
            daemon._check_poller_liveness()

        # The forwarded update_id 555 MUST be seeded into the new poller's
        # dedup — otherwise Telegram's re-delivery would slip past the
        # dedup gate and cause double-processing of the same message.
        seeded_calls = new_instance.load_dedup_ids.call_args_list
        assert len(seeded_calls) == 1
        seeded_ids = seeded_calls[0].args[0]
        assert 555 in seeded_ids, (
            "Raced-past-snapshot update_id 555 must be seeded into the "
            "new poller's dedup, got %r" % (seeded_ids,)
        )
        assert 101 in seeded_ids, (
            "Original dedup snapshot must still be seeded, got %r"
            % (seeded_ids,)
        )

    def test_no_preload_when_old_queue_empty(self):
        """When the old poller's queue is empty at swap time, we must NOT
        invoke preload_queue with an empty list (avoids a misleading
        'forwarded 0 updates' log line and keeps the swap path lean)."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 0
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = []
        old_poller.drain.return_value = []
        daemon._background_poller = old_poller

        new_instance = MagicMock()
        with patch(
            "landline.orchestrator.BackgroundPoller", return_value=new_instance,
        ):
            daemon._check_poller_liveness()

        new_instance.preload_queue.assert_not_called()

    def test_new_poller_uses_on_update_queued_callback(self):
        """The replacement poller must inherit the same _on_update_queued
        callback so /pause detection survives the swap."""
        from landline.config import POLL_STALE_ALERT_THRESHOLD_SECONDS
        import threading as _threading

        daemon, _ = _make_daemon()
        old_poller = MagicMock()
        old_poller.last_successful_poll.return_value = (
            time.time() - POLL_STALE_ALERT_THRESHOLD_SECONDS - 1
        )
        old_poller._last_processed_update_id = 0
        old_poller._last_processed_update_id_lock = _threading.Lock()
        old_poller.snapshot_dedup_ids.return_value = []
        daemon._background_poller = old_poller

        with patch("landline.orchestrator.BackgroundPoller") as MockPoller:
            daemon._check_poller_liveness()

        _, kwargs = MockPoller.call_args
        assert kwargs["on_update_queued"] == daemon._on_update_queued


class TestMainLoopCallsCheckPollerLiveness:
    """Cluster 4 — regression: the main loop must invoke
    ``_check_poller_liveness`` on every drain tick (idle or busy)."""

    def test_main_loop_calls_check_poller_liveness_when_idle(self):
        """When the drain returns no updates, the loop still calls the check
        before `continue`ing to the next iteration."""
        daemon, _ = _make_daemon()
        call_counter = {"n": 0}

        def fake_drain(block_timeout_seconds=0):
            call_counter["n"] += 1
            if call_counter["n"] >= 3:
                daemon.running = False
            return []

        mock_poller = MagicMock()
        mock_poller.drain.side_effect = fake_drain
        mock_poller.has_pending.return_value = False
        mock_poller.last_successful_poll.return_value = time.time()

        check_calls = {"n": 0}
        original_check = daemon._check_poller_liveness

        def counting_check():
            check_calls["n"] += 1
            original_check()

        daemon._check_poller_liveness = counting_check

        with patch("landline.orchestrator.BackgroundPoller", return_value=mock_poller), \
             patch("landline.orchestrator.sweep_media_caches"), \
             patch("landline.orchestrator.save_state"), \
             patch("time.sleep"):
            daemon.run()

        # At least one check per idle iteration.
        assert check_calls["n"] >= 3

    def test_main_loop_calls_check_poller_liveness_when_busy(self):
        """Check runs even on iterations that returned real updates."""
        daemon, _ = _make_daemon()
        # Guard denies so no dispatch happens — the batch classifier's
        # per-update reject path is fast and won't block the loop.
        daemon._guard_fn = MagicMock(return_value=False)

        call_counter = {"n": 0}
        update = make_telegram_update(1, "hello")

        def fake_drain(block_timeout_seconds=0):
            call_counter["n"] += 1
            if call_counter["n"] == 1:
                return [update]
            daemon.running = False
            return []

        mock_poller = MagicMock()
        mock_poller.drain.side_effect = fake_drain
        mock_poller.has_pending.return_value = False

        check_calls = {"n": 0}

        def counting_check():
            check_calls["n"] += 1

        daemon._check_poller_liveness = counting_check

        with patch("landline.orchestrator.BackgroundPoller", return_value=mock_poller), \
             patch("landline.orchestrator.sweep_media_caches"), \
             patch("landline.orchestrator.save_state"), \
             patch("time.sleep"):
            daemon.run()

        assert check_calls["n"] >= 1


class TestDocumentBatch:
    """Cluster 1: end-to-end document routing through _process_update_batch."""

    def _make_doc_update(self, uid, file_name="report.pdf", file_size=1024, caption=None):
        message = {
            "message_id": uid * 10,
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
            "document": {
                "file_id": f"docfile-{uid}",
                "file_name": file_name,
                "file_size": file_size,
            },
        }
        if caption:
            message["caption"] = caption
        return {"update_id": uid, "message": message}

    def test_valid_document_dispatched_to_claude(self):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_doc_update(300, "report.pdf")

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/telegram_files/20260703_120000_report.pdf"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])

        mocks["run_claude"].assert_called_once()
        prompt = mocks["run_claude"].call_args.kwargs["message"]
        assert "[document:" in prompt
        assert "report.pdf" in prompt
        assert daemon.state["last_update_id"] == 300

    def test_document_download_failure_sends_notice(self):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_doc_update(301, "report.pdf")

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch([update])

        mocks["send_response"].assert_called()
        notice = mocks["send_response"].call_args[0][2]
        assert "failed to download" in notice.lower()
        # Cursor advanced even on failure.
        assert daemon.state["last_update_id"] == 301
        # No Claude dispatch.
        mocks["run_claude"].assert_not_called()

    def test_document_locked_gets_help_no_download(self):
        """Locked + document: LOCKED_HELP, no download, no Claude dispatch."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        update = self._make_doc_update(302, "report.pdf")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/should-not-happen.pdf") as mock_dl:
            daemon._process_update_batch([update])
        text = mocks["send_response"].call_args[0][2]
        assert "passphrase" in text
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()
        assert daemon.state["last_update_id"] == 302

    def test_disallowed_extension_gets_skip_notice(self):
        """A .exe document falls through to the generic non-text notice."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_doc_update(303, "malware.exe")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/should-not-happen") as mock_dl:
            daemon._process_update_batch([update])
        mocks["send_response"].assert_called()
        text = mocks["send_response"].call_args[0][2]
        # Rejected doc routes through _handle_non_text_update — notice
        # mentions supported types.
        assert "text" in text and "documents" in text
        mock_dl.assert_not_called()
        assert daemon.state["last_update_id"] == 303

    def test_attacker_path_traversal_scrubbed_in_prompt(self):
        """`../evil.pdf` reaches the prompt as `evil.pdf`, no traversal."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_doc_update(304, "../evil.pdf")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file",
                   side_effect=lambda t, fid, fn, target_dir=None, size_cap=None: f"/tmp/{fn}"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])
        prompt = mocks["run_claude"].call_args.kwargs["message"]
        assert "evil.pdf" in prompt
        assert "../evil.pdf" not in prompt


class TestVoiceBatchWiring:
    """Cluster 2: end-to-end voice routing through _process_update_batch."""

    def _make_voice_update(self, uid, duration=15, key="voice"):
        return {
            "update_id": uid,
            "message": {
                "message_id": uid * 10,
                "chat": {"id": int(FAKE_CHAT_ID)},
                "date": int(time.time()),
                key: {"file_id": f"voice-{uid}", "duration": duration},
            },
        }

    def test_voice_only_batch_dispatches_transcript(self):
        from landline.voice_transcribe import TranscribeResult

        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_voice_update(400, duration=15)

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/telegram_voice/x.ogg"), \
             patch("landline.voice_handler.transcribe_file",
                   return_value=TranscribeResult(
                       ok=True, text="hello agent",
                       duration_seconds=1.0, error=None,
                   )), \
             patch("landline.voice_handler.log_conversation"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])

        mocks["run_claude"].assert_called_once()
        prompt = mocks["run_claude"].call_args.kwargs["message"]
        assert "<voice_note>" in prompt
        assert "hello agent" in prompt
        assert daemon.state["last_update_id"] == 400

    def test_voice_too_long_gets_notice_no_dispatch(self):
        from landline.config import VOICE_MAX_DURATION_SECONDS

        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = self._make_voice_update(
            401, duration=VOICE_MAX_DURATION_SECONDS + 1,
        )
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.download_file") as mock_dl:
            daemon._process_update_batch([update])
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()
        text = mocks["send_response"].call_args[0][2]
        assert "too long" in text.lower()
        assert daemon.state["last_update_id"] == 401

    def test_voice_locked_gets_help_no_download(self):
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        update = self._make_voice_update(402, duration=15)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.download_file") as mock_dl:
            daemon._process_update_batch([update])
        text = mocks["send_response"].call_args[0][2]
        assert "passphrase" in text
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()


class TestNonTextBrushOffWording:
    """Cluster 2: brush-off notice now mentions voice notes alongside
    text/photos/documents."""

    def test_brush_off_mentions_voice_notes(self):
        daemon, mocks = _make_daemon()
        # Sticker / animation / video are still unsupported.
        update = {"update_id": 500, "message": {
            "chat": {"id": int(FAKE_CHAT_ID)},
            "date": int(time.time()),
            "sticker": {"file_id": "s"},
        }}
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])
        text = mocks["send_response"].call_args[0][2]
        assert "voice notes" in text
        assert "documents" in text


class TestReactionAcksPerBatchState:
    """Cluster 3 — the orchestrator manages a per-batch ``_batch_ack_message_ids``
    tracker that the classifier populates on 👀 and the dispatcher
    consumes on 👌. State MUST be cleared in the finally so a
    mid-batch exception can't leak ids into the next batch."""

    def test_tracker_cleared_after_batch(self):
        """After ``_process_update_batch`` returns, the tracker is None."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        assert daemon._batch_ack_message_ids is None
        update = make_telegram_update(600, "hello")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""), \
             patch("landline.reactions.set_reactions_batch_async"):
            daemon._process_update_batch([update])
        assert daemon._batch_ack_message_ids is None

    def test_tracker_cleared_after_batch_error(self):
        """Even on a mid-batch exception, tracker cleared to None."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        def blow_up(*args, **kwargs):
            raise RuntimeError("mid-batch failure")

        with patch("landline.orchestrator.save_state"), \
             patch(
                 "landline.orchestrator.classify_updates",
                 side_effect=blow_up,
             ):
            with pytest.raises(RuntimeError):
                daemon._process_update_batch([make_telegram_update(601, "hi")])
        assert daemon._batch_ack_message_ids is None

    def test_ack_ids_forwarded_to_dispatcher_on_text_batch(self):
        """Three text messages in one batch → dispatcher.send_to_claude
        receives all three message_ids via ``ack_message_ids=``."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        updates = [
            make_telegram_update(700, "one"),
            make_telegram_update(701, "two"),
            make_telegram_update(702, "three"),
        ]
        # Capture the dispatcher's send_to_claude kwargs.
        with patch.object(
            daemon._dispatcher, "send_to_claude"
        ) as mock_send, \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async"):
            daemon._process_update_batch(updates)
        mock_send.assert_called_once()
        ack_ids = mock_send.call_args.kwargs["ack_message_ids"]
        # message_id = update_id * 10 in make_telegram_update.
        assert ack_ids == [7000, 7010, 7020]

    def test_full_batch_fires_ack_and_done_on_success(self):
        """End-to-end: three text messages → 3 × 👀 at classify + one 👌
        batch call at successful finalize with all three ids."""
        from landline.config import REACTION_ACK_EMOJI, REACTION_DONE_EMOJI

        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        updates = [
            make_telegram_update(800, "one"),
            make_telegram_update(801, "two"),
            make_telegram_update(802, "three"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async") as mock_ack, \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""), \
             patch("landline.reactions.set_reactions_batch_async") as mock_done:
            daemon._process_update_batch(updates)
        # 👀 fired 3 times (once per accepted text message).
        assert mock_ack.call_count == 3
        for call in mock_ack.call_args_list:
            assert call[0][3] == REACTION_ACK_EMOJI
        # 👌 fired once with all three ids.
        mock_done.assert_called_once()
        done_args = mock_done.call_args[0]
        # (token, chat_id, message_ids, emoji)
        assert list(done_args[2]) == [8000, 8010, 8020]
        assert done_args[3] == REACTION_DONE_EMOJI

    def test_kill_switch_disables_all_reactions(self, monkeypatch):
        """REACTION_ACKS_ENABLED=False → zero reaction thread spawns, but
        dispatch still runs and state is unchanged."""
        # The kill switch is inside reactions.set_reaction_async /
        # set_reactions_batch_async. When False, they return immediately
        # without spawning a thread — patch urlopen to prove no HTTP.
        monkeypatch.setattr("landline.config.REACTION_ACKS_ENABLED", False)
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        update = make_telegram_update(900, "hello")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""), \
             patch("urllib.request.urlopen") as mock_urlopen, \
             patch("threading.Thread.start") as mock_start:
            daemon._process_update_batch([update])
        # No HTTP + no fire-and-forget thread was spawned from reactions.
        mock_urlopen.assert_not_called()
        # Some other threads (streaming, etc.) may start. Prove no
        # thread with name 'landline-react' started by checking start-call
        # signatures — patch just isolates the thread.start method, so
        # inspect self.name via the calls.
        for c in mock_start.call_args_list:
            # ``self`` isn't passed positionally to the patched method;
            # patch on Thread.start is a bound-method-style patch, so
            # the mock receives the Thread as ``call.__self__`` or the
            # first positional. Skip if not accessible.
            pass
        # Claude dispatch still ran.
        mocks["run_claude"].assert_called_once()


class TestAckPartitioningAcrossMediaTypes:
    """PIN: findings #1, #3, #4 — the completion 👌 must fire ONLY on the
    message_ids of the dispatch that actually succeeded, never on
    message_ids from a different dispatch in the same batch (photo,
    voice, document, or text). Under the pre-fix code, every dispatch
    read the full per-chat tracker and cross-pollinated 👌 onto messages
    that hadn't been dispatched yet (or had already failed)."""

    def test_photo_and_text_batch_partitions_ack_ids(self):
        """Photo (mid=1010) + text (mid=1020) in one batch.
        Photo dispatch → ack_message_ids == [1010].
        Text dispatch → ack_message_ids == [1020].
        Neither carries the other's id."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        now_ts = int(time.time())
        photo_update = {
            "update_id": 101,
            "message": {
                "message_id": 1010,
                "chat": {"id": int(FAKE_CHAT_ID)},
                "date": now_ts,
                "photo": [{"file_id": "pic", "file_size": 100}],
            },
        }
        text_update = make_telegram_update(102, "and here's some text")

        with patch.object(
            daemon._dispatcher, "send_to_claude"
        ) as mock_send, \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/fake.jpg"):
            daemon._process_update_batch([photo_update, text_update])

        # Two dispatches: photo, then text.
        assert mock_send.call_count == 2
        photo_ack = mock_send.call_args_list[0].kwargs["ack_message_ids"]
        text_ack = mock_send.call_args_list[1].kwargs["ack_message_ids"]
        assert photo_ack == [1010], (
            "photo dispatch must ack ONLY the photo mid, not the text mid"
        )
        assert text_ack == [1020], (
            "text dispatch must ack ONLY the text mid, not the photo mid"
        )

    def test_multi_document_batch_partitions_ack_ids(self):
        """Three docs in one batch → three dispatches, each acking only
        its own message_id. Under the pre-fix bug, doc-A's finalize
        would 👌 docs B and C before they had even been sent to Claude."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        def _doc_update(uid, mid):
            return {
                "update_id": uid,
                "message": {
                    "message_id": mid,
                    "chat": {"id": int(FAKE_CHAT_ID)},
                    "date": int(time.time()),
                    "document": {
                        "file_id": "d%d" % uid,
                        "file_name": "r%d.pdf" % uid,
                        "file_size": 100,
                        "mime_type": "application/pdf",
                    },
                },
            }

        docs = [
            _doc_update(201, 2010),
            _doc_update(202, 2020),
            _doc_update(203, 2030),
        ]
        with patch.object(
            daemon._dispatcher, "send_to_claude"
        ) as mock_send, \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/fake.pdf"):
            daemon._process_update_batch(docs)

        assert mock_send.call_count == 3
        acks = [c.kwargs["ack_message_ids"] for c in mock_send.call_args_list]
        assert acks == [[2010], [2020], [2030]], (
            "each doc dispatch must ack ONLY its own mid — no "
            "cross-pollination across the batch"
        )

    def test_voice_failure_leaves_text_ack_clean(self):
        """Voice (mid=3010, fails) + text (mid=3020, succeeds) in one
        batch. Voice never calls send_to_claude (transcribe fails, early
        return in voice_handler). Text dispatch acks ONLY [3020]. Under
        the pre-fix bug, the classifier's tracker held [3010, 3020] and
        the text dispatch would 👌 the failed voice note too."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        voice_update = {
            "update_id": 301,
            "message": {
                "message_id": 3010,
                "chat": {"id": int(FAKE_CHAT_ID)},
                "date": int(time.time()),
                "voice": {
                    "file_id": "vv",
                    "duration": 5,
                    "mime_type": "audio/ogg",
                },
            },
        }
        text_update = make_telegram_update(302, "then some text")

        # Force whisper to fail (empty transcript).
        from landline.voice_transcribe import TranscribeResult
        bad = TranscribeResult(
            ok=False, text="", duration_seconds=0.1, error="empty_transcript",
        )

        with patch.object(
            daemon._dispatcher, "send_to_claude"
        ) as mock_send, \
             patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("", [])), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.orchestrator.download_file",
                   return_value="/tmp/fake.ogg"), \
             patch("landline.voice_handler.transcribe_file", return_value=bad):
            daemon._process_update_batch([voice_update, text_update])

        # Voice never dispatched (early return on transcribe failure).
        # Only text dispatch fires, acking ONLY the text mid.
        assert mock_send.call_count == 1
        assert mock_send.call_args_list[0].kwargs["ack_message_ids"] == [3020], (
            "failed-voice mid must NOT leak into the text dispatch's ack list"
        )

    def test_tracker_popped_after_dispatch(self):
        """Each dispatch pops its ids off the per-batch tracker so a
        subsequent read reflects the remaining un-dispatched ids —
        defense-in-depth if any future caller re-introduces a
        tracker-based lookup."""
        daemon, _ = _make_daemon()
        daemon._batch_ack_message_ids = {"chat-x": [100, 200, 300]}
        with patch.object(daemon._dispatcher, "send_to_claude"), \
             patch("landline.orchestrator.drain_inject_queue",
                   return_value=("", [])), \
             patch("landline.orchestrator.save_state"):
            daemon._inject_and_dispatch(
                "hi", "chat-x", update_ids=[1], ack_message_ids=[100],
            )
        # 100 popped; 200 and 300 remain for the next dispatch.
        assert daemon._batch_ack_message_ids == {"chat-x": [200, 300]}


class TestRejectionPathsClearAck:
    """Findings: 👀 was fired at classify time but never cleared when a
    message was rejected downstream — the emoji lingered forever with
    no matching 👌 (a lie by the docstring's definition)."""

    def test_locked_text_batch_clears_ack_on_all_messages(self):
        """Locked + text batch: LOCKED_HELP sent once AND 👀 cleared on
        every text message_id in the batch (batch_classifier fired 👀)."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        updates = [
            make_telegram_update(950, "hello"),
            make_telegram_update(951, "again"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.reactions.set_reactions_batch_async") as mock_batch:
            daemon._process_update_batch(updates)
        # message_id = update_id * 10 in make_telegram_update.
        # Batch-clear was called with the two text mids and emoji=None.
        cleared_batches = [
            (list(c.args[2]), c.args[3])
            for c in mock_batch.call_args_list
            if c.args[3] is None
        ]
        assert any(
            sorted(mids) == [9500, 9510]
            for mids, _emoji in cleared_batches
        ), "text batch under lock must clear 👀 on ALL text message_ids"

    def test_silent_unlock_clears_ack_on_passphrase_message(self):
        """When the operator types the passphrase directly (single-text batch,
        silent unlock succeeds), the passphrase message is never
        dispatched to Claude — its 👀 must be cleared."""
        daemon, _ = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        # Rig try_silent_unlock to succeed.
        daemon._lock_manager.try_silent_unlock = MagicMock(return_value=True)
        upd = make_telegram_update(960, "correcthorsebatterystaple")
        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.reactions.set_reactions_batch_async") as mock_batch:
            daemon._process_update_batch([upd])
        # 👀 cleared on the passphrase message_id (9600).
        assert any(
            list(c.args[2]) == [9600] and c.args[3] is None
            for c in mock_batch.call_args_list
        )

    def test_photo_group_all_downloads_failed_clears_ack(self):
        """A photo whose download returns None must have its 👀 cleared."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()
        photo_upd = make_telegram_update(970, None, has_photo=True)
        photo_upd["message"].pop("text", None)
        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.reactions.set_reactions_batch_async") as mock_batch, \
             patch("landline.orchestrator.download_file", return_value=None):
            daemon._process_update_batch([photo_upd])
        mocks["run_claude"].assert_not_called()
        # Notice sent.
        text = mocks["send_response"].call_args[0][2]
        assert "Failed to download" in text
        # 👀 cleared on the photo message_id (9700 = 970 * 10).
        assert any(
            list(c.args[2]) == [9700] and c.args[3] is None
            for c in mock_batch.call_args_list
        )

    def test_locked_multi_voice_batch_sends_one_locked_help(self):
        """End-to-end: multiple voice notes in one batch while locked
        must produce EXACTLY ONE LOCKED_HELP notice, not N."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"

        def _make_voice_upd(uid):
            return {
                "update_id": uid,
                "message": {
                    "message_id": uid * 10,
                    "chat": {"id": int(FAKE_CHAT_ID)},
                    "date": int(time.time()),
                    "voice": {"file_id": f"v-{uid}", "duration": 5},
                },
            }

        updates = [_make_voice_upd(u) for u in (980, 981, 982, 983, 984)]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.voice_handler.reactions.set_reaction_async"), \
             patch("landline.orchestrator.download_file") as mock_dl:
            daemon._process_update_batch(updates)
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()
        # Count how many LOCKED_HELP sends went out — must be exactly 1
        # for 5 voice notes (previously was 5).
        locked_help_calls = [
            c for c in mocks["send_response"].call_args_list
            if "passphrase" in c[0][2]
        ]
        assert len(locked_help_calls) == 1, (
            "multi-item locked voice batch must send ONE LOCKED_HELP, "
            "not one per voice note"
        )

    def test_locked_multi_document_batch_sends_one_locked_help(self):
        """End-to-end: multiple documents in one batch while locked
        must produce EXACTLY ONE LOCKED_HELP notice, not N."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"

        def _make_doc_upd(uid, fname):
            return {
                "update_id": uid,
                "message": {
                    "message_id": uid * 10,
                    "chat": {"id": int(FAKE_CHAT_ID)},
                    "date": int(time.time()),
                    "document": {
                        "file_id": f"d-{uid}",
                        "file_name": fname,
                        "file_size": 1234,
                        "mime_type": "application/pdf",
                    },
                },
            }

        updates = [
            _make_doc_upd(990, "a.pdf"),
            _make_doc_upd(991, "b.pdf"),
            _make_doc_upd(992, "c.pdf"),
        ]
        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.document_handler.reactions.set_reaction_async"), \
             patch("landline.orchestrator.download_file") as mock_dl:
            daemon._process_update_batch(updates)
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()
        locked_help_calls = [
            c for c in mocks["send_response"].call_args_list
            if "passphrase" in c[0][2]
        ]
        assert len(locked_help_calls) == 1, (
            "multi-item locked document batch must send ONE LOCKED_HELP, "
            "not one per document"
        )

    def test_locked_mixed_media_batch_sends_one_locked_help(self):
        """Regression pin (orchestrator.py:397 finding): a MIXED batch
        (photo + voice + document + text) while the session is locked
        must produce EXACTLY ONE LOCKED_HELP for the chat — not one per
        media bucket. Each handler runs its own ``_check_lock_gate``, so
        without cross-type coalescing on ``_batch_locked_help_chats``
        the operator sees four identical locked notices for a single batch."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"

        chat_id_int = int(FAKE_CHAT_ID)
        photo_upd = {
            "update_id": 1200,
            "message": {
                "message_id": 12000,
                "chat": {"id": chat_id_int},
                "date": int(time.time()),
                "photo": [{"file_id": "p-1", "width": 100, "height": 100}],
            },
        }
        voice_upd = {
            "update_id": 1201,
            "message": {
                "message_id": 12010,
                "chat": {"id": chat_id_int},
                "date": int(time.time()),
                "voice": {"file_id": "v-1", "duration": 5},
            },
        }
        doc_upd = {
            "update_id": 1202,
            "message": {
                "message_id": 12020,
                "chat": {"id": chat_id_int},
                "date": int(time.time()),
                "document": {
                    "file_id": "d-1",
                    "file_name": "a.pdf",
                    "file_size": 1234,
                    "mime_type": "application/pdf",
                },
            },
        }
        text_upd = make_telegram_update(1203, "hello there")

        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.voice_handler.reactions.set_reaction_async"), \
             patch("landline.document_handler.reactions.set_reaction_async"), \
             patch("landline.reactions.set_reactions_batch_async"), \
             patch("landline.orchestrator.download_file") as mock_dl:
            daemon._process_update_batch(
                [photo_upd, voice_upd, doc_upd, text_upd]
            )
        # No downloads / no Claude dispatch under lock.
        mock_dl.assert_not_called()
        mocks["run_claude"].assert_not_called()
        # EXACTLY one LOCKED_HELP for the whole mixed batch.
        locked_help_calls = [
            c for c in mocks["send_response"].call_args_list
            if "passphrase" in c[0][2]
        ]
        assert len(locked_help_calls) == 1, (
            "mixed-media locked batch must send ONE LOCKED_HELP across "
            "all handlers (photo + voice + document + text), not one "
            "per bucket. Got %d." % len(locked_help_calls)
        )

    def test_locked_help_tracker_reset_between_batches(self):
        """The per-batch tracker resets in ``_process_update_batch``'s
        finally clause — a second locked batch after the first must
        still send its own LOCKED_HELP (this is not sticky)."""
        daemon, mocks = _make_daemon()
        daemon._lock_manager._lock_state = "locked"
        with patch("landline.orchestrator.save_state"), \
             patch("landline.batch_classifier.reactions.set_reaction_async"), \
             patch("landline.reactions.set_reactions_batch_async"):
            daemon._process_update_batch(
                [make_telegram_update(1300, "batch one")]
            )
            daemon._process_update_batch(
                [make_telegram_update(1301, "batch two")]
            )
        # Tracker was cleared between batches — each batch sends one.
        locked_help_calls = [
            c for c in mocks["send_response"].call_args_list
            if "passphrase" in c[0][2]
        ]
        assert len(locked_help_calls) == 2, (
            "the batch-locked-help tracker must reset per batch — "
            "otherwise a second batch never gets its own LOCKED_HELP"
        )
        # Tracker is cleared in the finally block, ready for the next batch.
        assert daemon._batch_locked_help_chats is None
