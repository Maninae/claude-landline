"""E2E test: mock a full Telegram conversation flow.

Simulates the complete lifecycle:
  1. Daemon receives a message while locked -> gets LOCKED_HELP
  2. /unlock with wrong passphrase -> rejection
  3. /unlock with correct passphrase -> unlocked
  4. Text message -> dispatched to Claude, response sent
  5. /status -> status text returned
  6. /new -> session reset, relocked
  7. Text while locked again -> gets LOCKED_HELP
"""

import hashlib
import time
from unittest.mock import patch, MagicMock

import pytest

from landline.claude_dispatch import ClaudeStreamResult
from landline.lock import _normalize_passphrase
from landline.orchestrator import TelegramDaemon

from landline.tests.conftest import make_telegram_update, FAKE_CHAT_ID, FAKE_BOT_TOKEN


PASSPHRASE = "coconut pudding"
PASSPHRASE_HASH = hashlib.sha256(
    _normalize_passphrase(PASSPHRASE).encode("utf-8")
).hexdigest()


def _make_e2e_daemon():
    """Build a fully-wired daemon for E2E testing."""
    sent_messages = []

    def track_send(token, chat_id, text):
        sent_messages.append(text)

    call_count = [0]

    def mock_run_claude(**kwargs):
        call_count[0] += 1
        r = ClaudeStreamResult()
        r.session_id = f"session-{call_count[0]}"
        r.streamed_text = f"Claude response #{call_count[0]}"
        r.final_result = f"Claude response #{call_count[0]}"
        return r

    ft = MagicMock()
    ft.is_in_backoff.return_value = False
    ft.consecutive_failure_count = 0
    ft.should_send_alert_now.return_value = False

    keychain_map = {
        "telegram-bot-token": FAKE_BOT_TOKEN,
        "telegram-chat-id": FAKE_CHAT_ID,
        "telegram-unlock-hash": PASSPHRASE_HASH,
        "telegram-allowed-chat-ids": FAKE_CHAT_ID,
    }

    send_resp_mock = MagicMock(side_effect=track_send)

    with patch("landline.orchestrator.keychain_get", side_effect=lambda s, **kw: keychain_map.get(s)), \
         patch("landline.orchestrator.load_state", return_value={
             "session_id": None,
             "last_update_id": 0,
             "turn_count": 0,
             "failed_unlock_attempts": 0,
             "unlock_lockout_until": 0.0,
             "unlock_timestamp": 0.0,
         }), \
         patch("landline.orchestrator.save_state"), \
         patch("landline.orchestrator.log_conversation"), \
         patch("signal.signal"):
        daemon = TelegramDaemon(
            run_claude_fn=MagicMock(side_effect=mock_run_claude),
            shutdown_hook=MagicMock(),
            failure_tracker=ft,
            send_response_fn=send_resp_mock,
            send_typing_fn=MagicMock(),
            guard_fn=MagicMock(return_value=True),
            reject_fn=MagicMock(),
        )
    daemon._send_buttons = lambda token, chat_id, text, buttons: send_resp_mock(token, chat_id, text)

    return daemon, sent_messages, call_count


class TestE2EConversationFlow:
    def test_full_lifecycle(self):
        daemon, sent, claude_calls = _make_e2e_daemon()
        update_id = [0]

        def next_update(text):
            update_id[0] += 1
            return make_telegram_update(update_id[0], text)

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""), \
             patch("landline.lock.keychain_get", return_value=PASSPHRASE_HASH), \
             patch("subprocess.run", return_value=MagicMock(stdout="", returncode=0)):

            # Step 1: Text while locked -> LOCKED_HELP
            sent.clear()
            daemon._process_update_batch([next_update("hello")])
            assert any("passphrase" in m for m in sent), f"Expected LOCKED_HELP, got: {sent}"

            # Step 2: Wrong passphrase -> LOCKED_HELP (try_silent_unlock fails, falls through)
            sent.clear()
            daemon._process_update_batch([next_update("wrong-pass")])
            assert any("passphrase" in m.lower() for m in sent), f"Expected LOCKED_HELP, got: {sent}"
            assert daemon._lock_manager.is_locked

            # Step 3: Correct passphrase typed directly
            sent.clear()
            daemon._process_update_batch([next_update(PASSPHRASE)])
            assert any("Unlocked" in m for m in sent), f"Expected unlock, got: {sent}"
            assert not daemon._lock_manager.is_locked

            # Step 4: Text message -> Claude response
            sent.clear()
            daemon._process_update_batch([next_update("What's the weather?")])
            assert claude_calls[0] == 1

            # Step 5: /status
            sent.clear()
            daemon._process_update_batch([next_update("/status")])
            assert any("the agent" in m or "Lock" in m for m in sent), f"Expected status, got: {sent}"

            # Step 6: /new -> reset
            sent.clear()
            daemon._process_update_batch([next_update("/new")])
            assert any("locked" in m.lower() for m in sent), f"Expected reset, got: {sent}"
            assert daemon._lock_manager.is_locked
            assert daemon.state["session_id"] is None

            # Step 7: Text while locked again
            sent.clear()
            daemon._process_update_batch([next_update("another message")])
            assert any("passphrase" in m for m in sent), f"Expected LOCKED_HELP, got: {sent}"

    def test_multi_message_coalescing(self):
        """3 text messages in one batch -> single Claude call with all 3
        contents and `[message K]` headers; cursor lands on the last id."""
        daemon, sent, claude_calls = _make_e2e_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        updates = [
            make_telegram_update(1, "first message"),
            make_telegram_update(2, "second message"),
            make_telegram_update(3, "third message"),
        ]

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch(updates)

        assert claude_calls[0] == 1
        coalesced = daemon._dispatcher._run_claude.call_args.kwargs["message"]
        assert "first message" in coalesced
        assert "second message" in coalesced
        assert "third message" in coalesced
        assert "[message 1]" in coalesced
        assert "[message 3]" in coalesced
        assert daemon.state["last_update_id"] == 3

    def test_inject_queue_prepended(self):
        """Inject prefix sits BEFORE the user text and separated by a blank line."""
        daemon, sent, claude_calls = _make_e2e_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        update = make_telegram_update(1, "hi")

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("[injected report: morning brief]", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""):
            daemon._process_update_batch([update])

        assert claude_calls[0] == 1
        message = daemon._dispatcher._run_claude.call_args.kwargs["message"]
        assert "injected report" in message
        assert "hi" in message
        # Order: inject prefix BEFORE user text.
        assert message.index("injected report") < message.index("hi")

    def test_pause_e2e_flow(self):
        """E2E: unlocked text dispatch -> Claude returns interrupted=True ->
        '_(Paused.)_' sent + flag cleared; subsequent queued text becomes the
        next turn (a clean second Claude call)."""
        daemon, sent, claude_calls = _make_e2e_daemon()
        daemon._lock_manager._lock_state = "unlocked"
        daemon._lock_manager._state["unlock_timestamp"] = time.time()

        # Wire the dispatcher the way run() would. The interrupt mechanism is
        # the PauseFlag itself (passed at construction); only the clear
        # callback needs to be wired post-construction.
        daemon._dispatcher._clear_pause_fn = daemon._pause_requested.clear

        # Replace run_claude with one that returns interrupted on the first
        # call and a normal response on the second.
        invocation_results = []
        invocation_count = [0]
        captured_messages = []

        def pause_aware_run_claude(**kwargs):
            invocation_count[0] += 1
            captured_messages.append(kwargs.get("message", ""))
            r = ClaudeStreamResult()
            if invocation_count[0] == 1:
                r.streamed_text = "partial"
                r.interrupted = True
            else:
                r.session_id = "session-after-pause"
                r.streamed_text = "ok, continuing"
                r.final_result = "ok, continuing"
            invocation_results.append(r)
            return r

        daemon._dispatcher._run_claude = MagicMock(side_effect=pause_aware_run_claude)

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.orchestrator.drain_inject_queue", return_value=("", [])), \
             patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history", return_value=""), \
             patch("landline.claude_dispatch.send_html",
                   side_effect=lambda t, c, text: sent.append(text)):
            # Step A: the operator sends text, mock Claude returns interrupted=True
            # (as if a /pause had set the flag and the watchdog SIGINT'd it).
            daemon._pause_requested.set()
            daemon._process_update_batch([make_telegram_update(1, "long task")])

            # Confirm paused message was sent on the interrupted finalize, and
            # the flag has been cleared.
            assert any("Paused" in m for m in sent), f"Expected Paused message, got: {sent}"
            assert not daemon._pause_requested.is_set()
            # Interrupted turn must NOT advance turn_count.
            assert daemon.state.get("turn_count", 0) == 0

            # Step B: the operator sends follow-up text — becomes the next turn.
            sent.clear()
            daemon._process_update_batch([make_telegram_update(2, "now do this")])

        assert invocation_count[0] == 2
        # Second call carried the new text, not a re-send of "long task".
        assert "now do this" in captured_messages[1]
        assert "long task" not in captured_messages[1]
        # Successful second turn bumps turn_count (interrupted call didn't).
        assert daemon.state.get("turn_count", 0) == 1
        # Session id captured from the successful (non-interrupted) turn.
        assert daemon.state.get("session_id") == "session-after-pause"
        # Pause flag remains cleared after the successful follow-up.
        assert not daemon._pause_requested.is_set()

    def test_unauthorized_chat_rejected(self):
        """Foreign chat_id (not on allowlist) -> guard denies, reject_fn fires,
        no Claude dispatch, cursor advances so the attacker can't spam."""
        guard = MagicMock(return_value=False)
        reject = MagicMock()
        run_claude = MagicMock()

        ft = MagicMock()
        ft.is_in_backoff.return_value = False
        ft.consecutive_failure_count = 0

        with patch("landline.orchestrator.keychain_get", side_effect=lambda s, **kw: {
            "telegram-bot-token": FAKE_BOT_TOKEN,
            "telegram-chat-id": FAKE_CHAT_ID,
        }.get(s)), \
             patch("landline.orchestrator.load_state", return_value={
                 "session_id": None, "last_update_id": 0, "turn_count": 0,
                 "failed_unlock_attempts": 0, "unlock_lockout_until": 0.0,
                 "unlock_timestamp": 0.0,
             }), \
             patch("landline.orchestrator.save_state"), \
             patch("signal.signal"):
            daemon = TelegramDaemon(
                run_claude_fn=run_claude,
                shutdown_hook=MagicMock(),
                failure_tracker=ft,
                send_response_fn=MagicMock(),
                send_typing_fn=MagicMock(),
                guard_fn=guard,
                reject_fn=reject,
            )

        update = make_telegram_update(77, "hello", chat_id="999999")
        with patch("landline.orchestrator.save_state"):
            daemon._process_update_batch([update])

        guard.assert_called_with("999999")
        reject.assert_called_once_with(daemon.token, "999999")
        run_claude.assert_not_called()
        # Cursor advanced so we don't redeliver every poll.
        assert daemon.state["last_update_id"] == 77

    def test_passphrase_is_coconut_pudding(self):
        """Sanity check: the hash stored in test fixtures matches the new
        post-refactor passphrase 'coconut pudding'. Catches a future regression
        if someone changes the literal back to 'jelly donuts' without also
        updating the hash."""
        recomputed = hashlib.sha256(
            _normalize_passphrase("coconut pudding").encode("utf-8")
        ).hexdigest()
        assert recomputed == PASSPHRASE_HASH
        # And the legacy passphrase should NOT verify.
        legacy = hashlib.sha256(
            _normalize_passphrase("jelly donuts").encode("utf-8")
        ).hexdigest()
        assert legacy != PASSPHRASE_HASH

    def test_passphrase_plural_variant_rejected(self):
        """Word-specific normalization removed — plural doesn't match singular hash."""
        daemon, sent, _ = _make_e2e_daemon()

        with patch("landline.orchestrator.save_state"), \
             patch("landline.orchestrator.log_conversation"), \
             patch("landline.lock.keychain_get", return_value=PASSPHRASE_HASH):
            daemon._process_update_batch([make_telegram_update(1, "/unlock coconut puddings")])
            assert daemon._lock_manager.is_locked
