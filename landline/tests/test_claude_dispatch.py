"""Tests for landline.claude.dispatch — ClaudeStreamResult, stale detection, ClaudeDispatcher."""

import collections
import time
from unittest.mock import patch, MagicMock, call

import pytest

from landline.claude.dispatch import (
    ClaudeStreamResult,
    is_result_successful,
    looks_like_stale_session,
    looks_like_pruned_resume,
    _stderr_looks_like_auth_failure,
    ClaudeDispatcher,
)
from landline import config as daemon_config
from landline.claude.failure_tracker import ClaudeFailureTracker
from landline.orchestrator import PauseFlag


class TestClaudeStreamResult:
    def test_defaults(self):
        r = ClaudeStreamResult()
        assert r.session_id is None
        assert r.streamed_text == ""
        assert r.final_result is None
        assert r.exit_code is None
        assert r.error is None
        assert r.stderr_tail == ""
        assert r.interrupted is False

    def test_has_content_with_streamed(self):
        r = ClaudeStreamResult()
        r.streamed_text = "hello"
        assert r.has_content is True

    def test_has_content_with_final(self):
        r = ClaudeStreamResult()
        r.final_result = "world"
        assert r.has_content is True

    def test_has_content_false_empty(self):
        r = ClaudeStreamResult()
        assert r.has_content is False

    def test_has_content_false_whitespace(self):
        r = ClaudeStreamResult()
        r.streamed_text = "   \n  "
        r.final_result = "  "
        assert r.has_content is False

    def test_cluster4_usage_fields_default_to_none(self):
        """Usage/cost mirror fields default to None so hand-constructed
        ClaudeStreamResult tests keep working, and downstream code treats
        None as 'no data' (recorded as zero)."""
        r = ClaudeStreamResult()
        assert r.result_usage is None
        assert r.result_model_usage is None
        assert r.result_total_cost_usd is None
        assert r.result_num_turns is None
        assert r.result_duration_ms is None


class TestIsResultSuccessful:
    def test_success_with_content(self):
        r = ClaudeStreamResult()
        r.streamed_text = "response"
        assert is_result_successful(r) is True

    def test_failure_with_error(self):
        r = ClaudeStreamResult()
        r.error = "something broke"
        r.streamed_text = "partial"
        assert is_result_successful(r) is False

    def test_failure_no_content(self):
        r = ClaudeStreamResult()
        assert is_result_successful(r) is False


class TestLooksLikeStaleSession:
    def test_stale_no_content_no_error(self):
        r = ClaudeStreamResult()
        assert looks_like_stale_session(r) is True

    def test_not_stale_with_content(self):
        r = ClaudeStreamResult()
        r.streamed_text = "output"
        assert looks_like_stale_session(r) is False

    def test_not_stale_with_error(self):
        r = ClaudeStreamResult()
        r.error = "crash"
        assert looks_like_stale_session(r) is False

    def test_not_stale_exit_code_143(self):
        """Exit code 143 (SIGTERM) is NOT stale — it's a clean shutdown."""
        r = ClaudeStreamResult()
        r.exit_code = 143
        assert looks_like_stale_session(r) is False

    def test_not_stale_when_interrupted(self):
        """Interrupted sessions are NOT stale."""
        r = ClaudeStreamResult()
        r.interrupted = True
        assert looks_like_stale_session(r) is False

    def test_nonzero_exit_is_not_stale(self):
        """Nonzero exit = process died, not stale session."""
        r = ClaudeStreamResult()
        r.exit_code = 1
        assert looks_like_stale_session(r) is False

    def test_sigkill_is_not_stale(self):
        r = ClaudeStreamResult()
        r.exit_code = 137
        assert looks_like_stale_session(r) is False


class TestClaudeStreamResultLocation:
    """E2 regression tests — ClaudeStreamResult lives in landline.claude.types so the
    streaming<->claude_dispatch cycle pivot stays broken."""

    def test_types_module_is_canonical_home(self):
        """ClaudeStreamResult MUST be importable from landline.claude.types — that
        is the home that breaks the streaming<->claude_dispatch cycle. If
        someone reverts E2 by moving the class back to claude_dispatch,
        this fails because landline.claude.types either won't exist or won't expose
        the class."""
        from landline.claude.types import ClaudeStreamResult as FromTypes
        from landline.claude.dispatch import ClaudeStreamResult as FromDispatch
        # Both names resolve to the SAME class object (claude_dispatch
        # re-imports from landline.types).
        assert FromTypes is FromDispatch

    def test_streaming_does_not_import_claude_dispatch_for_result(self):
        """The streaming module must not reach into claude_dispatch for
        the result type — that edge is the cycle pivot E2 removes."""
        import inspect
        import landline.claude.streaming as streaming_mod
        assert hasattr(streaming_mod, "ClaudeStreamResult")
        src = inspect.getsource(streaming_mod)
        assert "from landline.claude.dispatch import ClaudeStreamResult" not in src
        assert "from landline.claude.types import ClaudeStreamResult" in src

    def test_types_module_is_a_leaf(self):
        """landline.claude.types must not import from sibling package modules — that
        is what keeps it cycle-safe forever. Only `typing` is allowed."""
        import inspect
        import landline.claude.types as types_mod
        src = inspect.getsource(types_mod)
        for line in src.splitlines():
            stripped = line.strip()
            if stripped.startswith("from landline") or stripped.startswith("import landline"):
                raise AssertionError(
                    "landline.claude.types must stay a leaf — found sibling import: " + stripped
                )


class TestClaudeDispatcher:
    def _make_dispatcher(
        self, state=None, failure_tracker=None, run_claude_fn=None,
        send_response_fn=None, send_typing_fn=None,
    ):
        state = state or {"session_id": None, "turn_count": 0}
        ft = failure_tracker or ClaudeFailureTracker()
        hook = MagicMock()

        def default_run_claude(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "new-session"
            r.streamed_text = "response text"
            r.final_result = "response text"
            return r

        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(side_effect=default_run_claude),
            send_response_fn=send_response_fn or MagicMock(),
            send_typing_fn=send_typing_fn or MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_send_to_claude_calls_run_claude(self):
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.streamed_text = "ok"
        r.session_id = "s"
        run_mock.return_value = r
        d = self._make_dispatcher(run_claude_fn=run_mock)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hello", "123")
        assert run_mock.call_count == 1
        kwargs = run_mock.call_args[1]
        # Verify the dispatcher passes the right arguments through to run_claude.
        assert kwargs["token"] == "fake-token"
        assert kwargs["chat_id"] == "123"
        assert "hello" in kwargs["message"]
        assert kwargs["is_new"] is True  # state has no session_id

    def test_send_to_claude_updates_session_id(self):
        state = {"session_id": None, "turn_count": 0}
        d = self._make_dispatcher(state=state)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hello", "123")
        assert state["session_id"] == "new-session"

    def test_increments_turn_count(self):
        state = {"session_id": None, "turn_count": 0}
        d = self._make_dispatcher(state=state)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hello", "123")
        assert state["turn_count"] == 1

    def test_does_not_increment_turn_on_interrupt(self):
        def interrupted_run(**kwargs):
            r = ClaudeStreamResult()
            r.interrupted = True
            r.streamed_text = "partial"
            return r

        state = {"session_id": "s", "turn_count": 5}
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=interrupted_run),
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hi", "123")
        assert state["turn_count"] == 5

    def test_interrupt_does_not_trigger_failure_backoff(self):
        """SECURITY INVARIANT: interrupted results must not record a failure."""
        def interrupted_run(**kwargs):
            r = ClaudeStreamResult()
            r.interrupted = True
            r.streamed_text = "partial"
            return r

        ft = ClaudeFailureTracker()
        d = self._make_dispatcher(
            failure_tracker=ft,
            run_claude_fn=MagicMock(side_effect=interrupted_run),
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hi", "123")
        assert ft.consecutive_failure_count == 0
        assert ft.is_in_backoff() is False

    def test_backoff_gates_message(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.consecutive_failure_count = 5
        send_resp = MagicMock()
        run_mock = MagicMock()
        d = self._make_dispatcher(
            failure_tracker=ft,
            send_response_fn=send_resp,
            run_claude_fn=run_mock,
        )
        d.send_to_claude("hello", "123")
        # When gated, the message goes on the backoff queue as a
        # (text, chat_id, consumed_paths, ack_message_ids) tuple.
        # consumed_paths and ack_message_ids default to [].
        assert len(d._backoff_queue) == 1
        entry = d._backoff_queue[0]
        queued_text, queued_chat, queued_paths, queued_ack_ids = (
            d._unpack_entry(entry)
        )
        assert queued_text == "hello"
        assert queued_chat == "123"
        assert queued_paths == []
        assert queued_ack_ids == []
        # Claude must NOT have been invoked while gated.
        run_mock.assert_not_called()
        # User must have been notified.
        send_resp.assert_called()

    def test_backoff_queue_is_bounded_deque(self):
        """Backoff queue is a deque(maxlen=20) — old messages drop FIFO."""
        d = self._make_dispatcher()
        assert isinstance(d._backoff_queue, collections.deque)
        assert d._backoff_queue.maxlen == 20
        # Verify FIFO eviction at the cap.
        for i in range(25):
            d._backoff_queue.append((f"msg-{i}", "123"))
        assert len(d._backoff_queue) == 20
        # Oldest 5 evicted; first remaining should be msg-5.
        assert d._backoff_queue[0] == ("msg-5", "123")
        assert d._backoff_queue[-1] == ("msg-24", "123")

    def test_backoff_drain_merges_queued_with_current(self):
        """When backoff clears, queued messages are concatenated with the
        current text using the [queued message N] header format."""
        ft = ClaudeFailureTracker()
        # Not in backoff anymore.
        ft.next_attempt_allowed_at_epoch = 0.0
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.streamed_text = "ok"
        r.session_id = "s"
        run_mock.return_value = r
        d = self._make_dispatcher(failure_tracker=ft, run_claude_fn=run_mock)
        d._backoff_queue.append(("earlier message", "123"))
        d._backoff_queue.append(("middle message", "123"))
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("current message", "123")
        # Queue drained.
        assert len(d._backoff_queue) == 0
        sent_text = run_mock.call_args[1]["message"]
        assert "earlier message" in sent_text
        assert "middle message" in sent_text
        assert "current message" in sent_text
        # The three messages should be joined with the separator.
        assert "[queued message 1]" in sent_text
        assert "[queued message 3]" in sent_text

    def test_stale_session_retries_fresh(self):
        call_count = [0]
        calls_seen = []

        def stale_then_fresh(**kwargs):
            call_count[0] += 1
            calls_seen.append({"is_new": kwargs["is_new"], "session_id": kwargs["session_id"]})
            r = ClaudeStreamResult()
            if call_count[0] == 1:
                # Empty result = looks_like_stale_session True.
                return r
            r.session_id = "fresh-session"
            r.streamed_text = "fresh response"
            r.final_result = "fresh response"
            return r

        state = {"session_id": "old-session", "turn_count": 3}
        send_resp = MagicMock()
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=stale_then_fresh),
            send_response_fn=send_resp,
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hello", "123")
        assert call_count[0] == 2
        # First call resumed the old session.
        assert calls_seen[0]["is_new"] is False
        assert calls_seen[0]["session_id"] == "old-session"
        # Second call was a fresh session.
        assert calls_seen[1]["is_new"] is True
        assert calls_seen[1]["session_id"] is None
        assert state["session_id"] == "fresh-session"
        # Turn counter reset by the fallback path, then incremented once.
        assert state["turn_count"] == 1
        # User got the "expired" notice.
        expired_notice = [c for c in send_resp.call_args_list if "expired" in str(c).lower()]
        assert len(expired_notice) == 1

    def test_stale_retry_skipped_when_not_running(self):
        """If self.running is False (during shutdown), no stale-session retry."""
        call_count = [0]

        def empty_result(**kwargs):
            call_count[0] += 1
            return ClaudeStreamResult()

        state = {"session_id": "old-session", "turn_count": 3}
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=empty_result),
        )
        d.running = False
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hello", "123")
        # Only one call — no fallback attempt.
        assert call_count[0] == 1
        # Session state preserved (not reset).
        assert state["session_id"] == "old-session"

    def test_records_failure_on_error(self):
        def error_run(**kwargs):
            r = ClaudeStreamResult()
            r.error = "boom"
            return r

        ft = ClaudeFailureTracker()
        d = self._make_dispatcher(
            failure_tracker=ft,
            run_claude_fn=MagicMock(side_effect=error_run),
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hi", "123")
        assert ft.consecutive_failure_count == 1

    def test_records_success_on_good_result(self):
        ft = ClaudeFailureTracker()
        ft.consecutive_failure_count = 3
        d = self._make_dispatcher(failure_tracker=ft)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d.send_to_claude("hi", "123")
        assert ft.consecutive_failure_count == 0

    def test_context_warning_sent(self):
        state = {"session_id": "s", "turn_count": 0}
        send_resp = MagicMock()
        d = self._make_dispatcher(state=state, send_response_fn=send_resp)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=75.0):
            d.send_to_claude("hi", "123")
        context_calls = [
            c for c in send_resp.call_args_list
            if "context is at" in str(c) and "75" in str(c)
        ]
        assert len(context_calls) == 1
        # 75% crosses the 70 threshold (highest in CONTEXT_WARN_THRESHOLDS).
        assert state.get("_context_warned_at") == 70

    def test_context_warning_not_resent_below_next_threshold(self):
        """Once warned at threshold T, only re-warn after a higher T'."""
        # CONTEXT_WARN_THRESHOLDS = [30, 50, 70]. Already warned at 50.
        state = {"session_id": "s", "turn_count": 0, "_context_warned_at": 50}
        send_resp = MagicMock()
        d = self._make_dispatcher(state=state, send_response_fn=send_resp)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=55.0):
            d.send_to_claude("hi", "123")
        context_calls = [
            c for c in send_resp.call_args_list
            if "context is at" in str(c)
        ]
        # 55% has not crossed the next threshold (70%).
        assert len(context_calls) == 0

    def test_rate_limiting(self):
        from landline.config import RATE_LIMIT_SECONDS
        d = self._make_dispatcher()
        d.last_process_time = time.time()  # just processed -> must throttle
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""), \
             patch("time.sleep") as mock_sleep:
            d.send_to_claude("hi", "123")
        # Verify sleep was called and the duration is bounded by RATE_LIMIT_SECONDS.
        assert mock_sleep.called
        # First call to time.sleep is the rate-limit one.
        slept = mock_sleep.call_args_list[0][0][0]
        assert 0 < slept <= RATE_LIMIT_SECONDS

    def test_no_rate_limit_after_long_idle(self):
        """No throttle sleep when more than RATE_LIMIT_SECONDS has passed."""
        from landline.config import RATE_LIMIT_SECONDS
        d = self._make_dispatcher()
        d.last_process_time = time.time() - (RATE_LIMIT_SECONDS + 10)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""), \
             patch("time.sleep") as mock_sleep:
            d.send_to_claude("hi", "123")
        mock_sleep.assert_not_called()

    def test_inject_history_on_new_session(self):
        state = {"session_id": None, "turn_count": 0}
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.session_id = "s"
        r.streamed_text = "ok"
        run_mock.return_value = r
        d = self._make_dispatcher(state=state, run_claude_fn=run_mock)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value="history here"):
            d.send_to_claude("hello", "123")
        call_kwargs = run_mock.call_args[1]
        assert "history here" in call_kwargs["message"]

    def test_no_history_injection_on_resume(self):
        state = {"session_id": "existing", "turn_count": 3}
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.session_id = "existing"
        r.streamed_text = "ok"
        run_mock.return_value = r
        d = self._make_dispatcher(state=state, run_claude_fn=run_mock)
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history") as mock_hist:
            d.send_to_claude("hello", "123")
        mock_hist.assert_not_called()

    def test_failure_alert_fired_at_threshold(self):
        """After alert_threshold consecutive failures, send a one-time alert."""
        ft = ClaudeFailureTracker(alert_threshold=2, backoff_threshold=100)
        ft.consecutive_failure_count = 1  # one more failure will hit threshold
        send_resp = MagicMock()

        def error_run(**kwargs):
            r = ClaudeStreamResult()
            r.error = "boom"
            return r

        d = self._make_dispatcher(
            failure_tracker=ft,
            run_claude_fn=MagicMock(side_effect=error_run),
            send_response_fn=send_resp,
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hi", "123")
        alert_calls = [
            c for c in send_resp.call_args_list
            if "Claude is unavailable" in str(c)
        ]
        assert len(alert_calls) == 1
        # Idempotent — second failure in same streak must not re-alert.
        send_resp.reset_mock()
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hi", "123")
        alert_calls_2 = [
            c for c in send_resp.call_args_list
            if "Claude is unavailable" in str(c)
        ]
        assert len(alert_calls_2) == 0

    def test_session_id_not_updated_on_failed_result(self):
        """A result with an error must NOT overwrite the stored session_id."""
        def error_run(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "spurious-new-id"
            r.error = "boom"
            return r

        state = {"session_id": "existing-good-id", "turn_count": 3}
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=error_run),
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hi", "123")
        # session_id stays put on failure.
        assert state["session_id"] == "existing-good-id"

    def test_invocation_exception_recorded_as_failure(self):
        """When run_claude raises, the exception is captured and counted."""
        def crashing_run(**kwargs):
            raise RuntimeError("dispatcher should not propagate")

        ft = ClaudeFailureTracker()
        d = self._make_dispatcher(
            failure_tracker=ft,
            run_claude_fn=MagicMock(side_effect=crashing_run),
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            # Must not raise — exception is swallowed and recorded as failure.
            d.send_to_claude("hi", "123")
        assert ft.consecutive_failure_count == 1


class TestFinalizeInterruptedMessage:
    """Tests for the _finalize_response interrupted-message handling.

    Spec: on result.interrupted=True, ALWAYS send "_(Paused.)_" and
    invoke _clear_pause_fn if set. On natural completion, do neither.
    """

    def _make_dispatcher(
        self, send_response_fn=None, run_claude_fn=None,
    ):
        from landline.claude.failure_tracker import ClaudeFailureTracker
        state = {"session_id": "s", "turn_count": 0}
        ft = ClaudeFailureTracker()
        hook = MagicMock()
        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(),
            send_response_fn=send_response_fn or MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_finalize_sends_paused_always_no_streamed_text(self):
        """interrupted=True with EMPTY streamed_text -> paused message still sent."""
        send_html_mock = MagicMock()
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = ""
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html", send_html_mock):
            d._finalize_response(result, "123")
        paused_calls = [
            c for c in send_html_mock.call_args_list
            if "Paused" in str(c)
        ]
        assert len(paused_calls) == 1

    def test_finalize_sends_paused_with_streamed_text(self):
        """interrupted=True with streamed_text -> paused message sent."""
        send_html_mock = MagicMock()
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial response"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html", send_html_mock):
            d._finalize_response(result, "123")
        paused_calls = [
            c for c in send_html_mock.call_args_list
            if "Paused" in str(c)
        ]
        assert len(paused_calls) == 1

    def test_finalize_routes_paused_through_live_sender(self):
        """When a live sender exists, (Paused.) goes through its ORDERED queue
        (sender.status), NOT a direct send_html — so it lands behind the
        interrupted turn's still-draining bubbles. Reverting the dispatcher to a
        direct send_html must fail this test."""
        import landline.claude as claude_mod
        send_html_mock = MagicMock()
        live_sender = MagicMock()
        live_sender.is_closed = False
        live_sender.worker_alive = True
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with claude_mod._senders_lock:
            claude_mod._senders["123"] = live_sender
        try:
            with patch("landline.claude.dispatch.save_state"), \
                 patch("landline.claude.dispatch.log_conversation"), \
                 patch("landline.claude.dispatch.get_context_percent", return_value=None), \
                 patch("landline.claude.dispatch.send_html", send_html_mock):
                d._finalize_response(result, "123")
        finally:
            with claude_mod._senders_lock:
                claude_mod._senders.pop("123", None)
        paused_status = [
            c for c in live_sender.status.call_args_list if "Paused" in str(c)
        ]
        assert len(paused_status) == 1, "Paused must route through the live sender"
        assert not any(
            "Paused" in str(c) for c in send_html_mock.call_args_list
        ), "Paused must NOT be sent directly when a live sender exists"

    def test_finalize_clears_pause_flag_on_interrupted(self):
        """interrupted=True -> _clear_pause_fn called exactly once."""
        clear_mock = MagicMock()
        d = self._make_dispatcher()
        d._clear_pause_fn = clear_mock
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d._finalize_response(result, "123")
        assert clear_mock.call_count == 1

    def test_finalize_does_not_clear_pause_flag_on_natural_completion(self):
        """interrupted=False -> _clear_pause_fn NOT called."""
        clear_mock = MagicMock()
        d = self._make_dispatcher()
        d._clear_pause_fn = clear_mock
        result = ClaudeStreamResult()
        result.interrupted = False
        result.streamed_text = "done"
        result.final_result = "done"
        result.session_id = "s2"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d._finalize_response(result, "123")
        clear_mock.assert_not_called()

    def test_finalize_paused_safe_when_clear_pause_fn_is_none(self):
        """Default _clear_pause_fn=None must not crash the interrupted path."""
        send_html_mock = MagicMock()
        d = self._make_dispatcher()
        # Default: d._clear_pause_fn is None
        assert d._clear_pause_fn is None
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html", send_html_mock):
            d._finalize_response(result, "123")  # must not raise
        paused_calls = [
            c for c in send_html_mock.call_args_list
            if "Paused" in str(c)
        ]
        assert len(paused_calls) == 1

    def test_dispatcher_clear_pause_via_ctor(self):
        """E3 — Constructor arg ``clear_pause_fn`` wires the interrupted-path
        clear without any post-construction reach-through.

        REVERT-FAIL: if the constructor stops accepting clear_pause_fn (or
        drops it on the floor), this test fails because the mock is never
        invoked on result.interrupted=True without any post-construction
        reach-through.
        """
        from landline.claude.failure_tracker import ClaudeFailureTracker
        clear_mock = MagicMock()
        d = ClaudeDispatcher(
            token="fake-token",
            state={"session_id": "s", "turn_count": 0},
            failure_tracker=ClaudeFailureTracker(),
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
            clear_pause_fn=clear_mock,
        )
        # Verify the ctor populated the internal field (no reach-through).
        assert d._clear_pause_fn is clear_mock
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html"):
            d._finalize_response(result, "123")
        assert clear_mock.call_count == 1

    def test_finalize_no_paused_message_on_natural_completion(self):
        """interrupted=False -> no "Paused" message regardless of content."""
        send_resp = MagicMock()
        d = self._make_dispatcher(send_response_fn=send_resp)
        result = ClaudeStreamResult()
        result.interrupted = False
        result.streamed_text = "natural done"
        result.session_id = "s2"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d._finalize_response(result, "123")
        paused_calls = [
            c for c in send_resp.call_args_list
            if "Paused" in str(c)
        ]
        assert len(paused_calls) == 0

    def test_finalize_does_not_increment_turn_on_interrupt(self):
        """interrupted=True -> turn_count is NOT incremented at finalize."""
        d = self._make_dispatcher()
        d._state["turn_count"] = 7
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d._finalize_response(result, "123")
        assert d._state["turn_count"] == 7


class TestInjectCommitUnderBackoff:
    """When send_to_claude is gated by failure backoff, the inject-queue
    files for the current message must NOT be committed (deleted) — they
    ride along on the backoff tuple and are committed only after the
    message is actually drained and dispatched. A daemon death mid-backoff
    must leave the files in place for the next start to re-inject.
    """

    def _make_dispatcher(self, failure_tracker=None, run_claude_fn=None):
        from landline.claude.failure_tracker import ClaudeFailureTracker
        state = {"session_id": None, "turn_count": 0}
        ft = failure_tracker or ClaudeFailureTracker()
        hook = MagicMock()

        def default_run_claude(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "ok-session"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(side_effect=default_run_claude),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_gated_call_does_not_commit_inject_paths(self, tmp_path):
        """In backoff: paths ride along on the queue tuple, NOT unlinked."""
        from landline.claude.failure_tracker import ClaudeFailureTracker
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.consecutive_failure_count = 5
        d = self._make_dispatcher(failure_tracker=ft)

        # Real on-disk files so we can verify they survive the gated call.
        path_a = tmp_path / "a.json"
        path_b = tmp_path / "b.json"
        path_a.write_text("{}")
        path_b.write_text("{}")
        d.send_to_claude("hello", "123", consumed_paths=[path_a, path_b])

        # Files MUST survive.
        assert path_a.exists()
        assert path_b.exists()
        # They are on the backoff tuple.
        assert len(d._backoff_queue) == 1
        _t, _c, queued_paths, _ack_ids = d._unpack_entry(
            d._backoff_queue[0]
        )
        assert queued_paths == [path_a, path_b]

    def test_paths_committed_after_drain_and_dispatch(self, tmp_path):
        """When backoff clears, the drained paths are unlinked after the
        actual Claude call completes."""
        from landline.claude.failure_tracker import ClaudeFailureTracker
        # Start in-backoff, queue a message with paths.
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.consecutive_failure_count = 5
        d = self._make_dispatcher(failure_tracker=ft)

        path_earlier = tmp_path / "earlier.json"
        path_earlier.write_text("{}")
        d.send_to_claude("earlier", "123", consumed_paths=[path_earlier])
        # Earlier file still exists (gated).
        assert path_earlier.exists()

        # Clear backoff for the next call.
        ft.next_attempt_allowed_at_epoch = 0.0

        path_current = tmp_path / "current.json"
        path_current.write_text("{}")

        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("current", "123", consumed_paths=[path_current])

        # Both files are now committed (unlinked).
        assert not path_earlier.exists()
        assert not path_current.exists()
        assert len(d._backoff_queue) == 0

    def test_natural_dispatch_commits_paths(self, tmp_path):
        """A normal (non-backoff) dispatch unlinks consumed paths after the
        Claude call completes."""
        d = self._make_dispatcher()
        path = tmp_path / "report.json"
        path.write_text("{}")
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hi", "123", consumed_paths=[path])
        assert not path.exists()

    def test_no_paths_no_commit_call(self, tmp_path):
        """If consumed_paths is empty or None, no commit_inject_queue call —
        defensive check that we don't accidentally walk an empty list."""
        d = self._make_dispatcher()
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""), \
             patch("landline.claude.dispatch.commit_inject_queue") as mock_commit:
            d.send_to_claude("hi", "123")
        mock_commit.assert_not_called()


class TestBackoffQueueCrossChat:
    """The backoff drain must drain ONLY entries matching the current
    chat_id. Cross-chat entries are left queued. Today only one chat is
    allowlisted, so in practice the drain is full — but the guard is cheap
    insurance against any future multi-chat scenario.
    """

    def _make_dispatcher(self, failure_tracker=None, run_claude_fn=None):
        from landline.claude.failure_tracker import ClaudeFailureTracker
        state = {"session_id": None, "turn_count": 0}
        ft = failure_tracker or ClaudeFailureTracker()
        hook = MagicMock()

        def default_run_claude(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "ok-session"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(side_effect=default_run_claude),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_drain_only_matching_chat_id(self):
        """A backoff queue with entries from two chats is drained selectively
        — the current chat's entry is merged, the other chat's is left queued.
        """
        from landline.claude.failure_tracker import ClaudeFailureTracker
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = 0.0  # not in backoff
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.session_id = "s"
        r.streamed_text = "ok"
        run_mock.return_value = r
        d = self._make_dispatcher(failure_tracker=ft, run_claude_fn=run_mock)
        d._backoff_queue.append(("from chat A", "AAA", []))
        d._backoff_queue.append(("from chat B", "BBB", []))
        d._backoff_queue.append(("also from chat A", "AAA", []))
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("now from A", "AAA")
        sent_text = run_mock.call_args[1]["message"]
        # Both AAA entries merged with the current text.
        assert "from chat A" in sent_text
        assert "also from chat A" in sent_text
        assert "now from A" in sent_text
        # The BBB entry stayed on the queue.
        assert "from chat B" not in sent_text
        assert len(d._backoff_queue) == 1
        leftover_text, leftover_chat, _, _ = d._unpack_entry(
            d._backoff_queue[0]
        )
        assert leftover_text == "from chat B"
        assert leftover_chat == "BBB"

    def test_drain_handles_legacy_two_tuple_entries(self):
        """Backward-compat: hand-populated 2-tuple entries (no paths slot)
        normalise cleanly via _unpack_entry."""
        from landline.claude.failure_tracker import ClaudeFailureTracker
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = 0.0
        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.session_id = "s"
        r.streamed_text = "ok"
        run_mock.return_value = r
        d = self._make_dispatcher(failure_tracker=ft, run_claude_fn=run_mock)
        # Legacy shape — pre-fix queue entries had no paths slot.
        d._backoff_queue.append(("legacy text", "AAA"))
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("current", "AAA")
        sent_text = run_mock.call_args[1]["message"]
        assert "legacy text" in sent_text
        assert "current" in sent_text


class TestSessionIdSingleSource:
    """PersistentClaude is the single source of truth for session_id.

    Tests patch the canonical seam `landline.claude._get_persistent_claude`
    to inject a fake pc and assert on its method calls. Autouse
    `reset_persistent_claude_singleton` in conftest keeps real-singleton
    hygiene for tests that don't patch.
    """

    def _make_dispatcher(self, state=None, run_claude_fn=None,
                         send_response_fn=None):
        state = state or {"session_id": None, "turn_count": 0}
        ft = ClaudeFailureTracker()
        hook = MagicMock()

        def default_run_claude(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "new-session"
            r.streamed_text = "response text"
            r.final_result = "response text"
            return r

        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(side_effect=default_run_claude),
            send_response_fn=send_response_fn or MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    @staticmethod
    def _fake_pc():
        """A fake PersistentClaude whose get/set_session_id model the real
        get-after-set semantics under the same lock guard."""
        pc = MagicMock()
        pc._sid = None

        def _get():
            return pc._sid

        def _set(sid):
            pc._sid = sid

        pc.get_session_id.side_effect = _get
        pc.set_session_id.side_effect = _set
        return pc

    def test_stale_session_retry_clears_pc_and_resumes(self):
        """Stale-session retry must clear pc BEFORE state, then resume with
        is_new=True and session_id=None. After success, pc holds the fresh
        sid and state mirrors it."""
        call_count = [0]
        calls_seen = []

        def stale_then_fresh(**kwargs):
            call_count[0] += 1
            calls_seen.append({
                "is_new": kwargs["is_new"],
                "session_id": kwargs["session_id"],
            })
            r = ClaudeStreamResult()
            if call_count[0] == 1:
                return r  # empty -> looks_like_stale_session True
            r.session_id = "fresh-session"
            r.streamed_text = "fresh response"
            r.final_result = "fresh response"
            return r

        state = {"session_id": "old-session", "turn_count": 3}
        pc = self._fake_pc()
        pc._sid = "old-session"  # pre-seed (simulating prior seed)
        save_state_mock = MagicMock()
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=stale_then_fresh),
        )
        # Skip the seed (pc already has the sid); test the retry path.
        d._pc_seeded = True
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state", save_state_mock), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")

        # Two calls happened.
        assert call_count[0] == 2
        # The retry call was fresh.
        assert calls_seen[1]["is_new"] is True
        assert calls_seen[1]["session_id"] is None

        # pc.set_session_id(None) was called for the clear, then later
        # pc.set_session_id("fresh-session") when finalize observed the new sid.
        set_calls = [c.args[0] for c in pc.set_session_id.call_args_list]
        assert None in set_calls
        assert "fresh-session" in set_calls
        # The None clear happened BEFORE save_state was called for the retry
        # (source-then-cache ordering). save_state was called at least once
        # during the retry path.
        assert save_state_mock.called

        # Final pc state and state-dict mirror.
        assert pc._sid == "fresh-session"
        assert state["session_id"] == "fresh-session"

    def test_interrupt_143_not_mis_detected_as_stale(self):
        """An interrupted turn (exit 143) must NOT trigger stale-session
        retry, and the post-save mirror must reflect pc unchanged."""
        call_count = [0]

        def interrupted_run(**kwargs):
            call_count[0] += 1
            r = ClaudeStreamResult()
            r.exit_code = 143
            r.interrupted = True
            r.streamed_text = "partial"
            return r

        state = {"session_id": "existing-sid", "turn_count": 7}
        pc = self._fake_pc()
        pc._sid = "existing-sid"
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=interrupted_run),
        )
        d._pc_seeded = True  # already seeded for this test
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""), \
             patch("landline.claude.dispatch.send_html"):
            d.send_to_claude("hi", "123")
        # Exactly one run_claude call — no stale retry triggered.
        assert call_count[0] == 1
        # pc.set_session_id was NOT called to clear on the interrupted path.
        assert not any(
            c.args[0] is None for c in pc.set_session_id.call_args_list
        )
        # pc still holds the original sid.
        assert pc._sid == "existing-sid"
        # The post-save mirror line copied pc's sid into state.
        assert state["session_id"] == "existing-sid"

    def test_restart_continuation_seeds_pc_from_stale_state(self):
        """First send_to_claude must seed pc from the state dict (stale-sid
        case) before invoking run_claude."""
        seen_session_ids = []

        def capture_run(**kwargs):
            seen_session_ids.append(kwargs["session_id"])
            r = ClaudeStreamResult()
            r.session_id = "fresh-after-seed"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        state = {"session_id": "stale-sid", "turn_count": 4}
        pc = self._fake_pc()
        # pc has no sid yet (simulating fresh cold-boot process).
        assert pc._sid is None
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=capture_run),
        )
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")
        # The seed populated pc BEFORE run_claude was called; the
        # _invoke_with_stale_retry path then read pc.get_session_id() and
        # passed "stale-sid" through to run_claude.
        assert "stale-sid" in seen_session_ids
        # set_session_id("stale-sid") was called exactly once during seeding.
        seed_calls = [
            c for c in pc.set_session_id.call_args_list
            if c.args[0] == "stale-sid"
        ]
        assert len(seed_calls) == 1

    def test_restart_continuation_absent_session_no_seed(self):
        """If state has no session_id, the seed helper must NOT call
        pc.set_session_id during seeding."""
        state = {"session_id": None, "turn_count": 0}
        pc = self._fake_pc()
        d = self._make_dispatcher(state=state)
        seed_calls_before_run: list = []

        def capture_run(**kwargs):
            # Snapshot the set_session_id calls observed up to this point.
            seed_calls_before_run.extend(
                list(pc.set_session_id.call_args_list)
            )
            r = ClaudeStreamResult()
            r.session_id = "new-sid"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        d._run_claude = MagicMock(side_effect=capture_run)
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")
        # Up to the moment run_claude was first called, the seed helper
        # had not written to pc (nothing to seed).
        assert seed_calls_before_run == []

    def test_fresh_session_seeds_pc(self):
        """A fresh session run that returns a new sid lands in pc and state
        in consistent order."""
        def fresh_run(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "new-sid"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        state = {"session_id": None, "turn_count": 0}
        pc = self._fake_pc()
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=fresh_run),
        )
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")
        assert pc._sid == "new-sid"
        assert state["session_id"] == "new-sid"

    def test_session_id_persisted_round_trip(self):
        """Two successive send_to_claude calls: second call must receive
        the first call's sid with is_new=False, and the seed helper must
        be a no-op the second time (idempotent gate)."""
        calls_seen = []

        def two_call_run(**kwargs):
            calls_seen.append({
                "is_new": kwargs["is_new"],
                "session_id": kwargs["session_id"],
            })
            r = ClaudeStreamResult()
            r.session_id = "round-trip-sid"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        state = {"session_id": None, "turn_count": 0}
        pc = self._fake_pc()
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=two_call_run),
        )
        with patch("landline.claude._get_persistent_claude",
                   return_value=pc) as get_pc_mock, \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("first", "123")
            assert d._pc_seeded is True
            seed_call_count_after_first = get_pc_mock.call_count
            d.send_to_claude("second", "123")
        # First call: fresh session.
        assert calls_seen[0]["is_new"] is True
        assert calls_seen[0]["session_id"] is None
        # Second call resumes the sid pc holds from finalize.
        assert calls_seen[1]["is_new"] is False
        assert calls_seen[1]["session_id"] == "round-trip-sid"
        # The seed helper's _get_persistent_claude call did not re-fire on
        # the second send_to_claude (gated by _pc_seeded). The dispatcher's
        # other call sites still fetch pc on each turn, so the count grows
        # — but the FIRST extra call on the second turn corresponds to
        # _invoke_with_stale_retry, not the seed.
        assert get_pc_mock.call_count > seed_call_count_after_first

    def test_seed_does_not_clobber_pc_already_set(self):
        """Defensive branch: if pc already has a sid, the seed helper must
        return without overwriting it, even when state holds a different sid."""
        state = {"session_id": "state-sid", "turn_count": 0}
        pc = self._fake_pc()
        pc._sid = "pc-sid"  # pc already knows
        d = self._make_dispatcher(state=state)

        def capture_run(**kwargs):
            r = ClaudeStreamResult()
            r.session_id = "pc-sid"
            r.streamed_text = "ok"
            r.final_result = "ok"
            return r

        d._run_claude = MagicMock(side_effect=capture_run)
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")
        # pc.set_session_id was never called with "state-sid" — seed bailed out.
        seed_overwrites = [
            c for c in pc.set_session_id.call_args_list
            if c.args[0] == "state-sid"
        ]
        assert seed_overwrites == []
        # pc still holds its original sid.
        assert pc._sid == "pc-sid"

    def test_finalize_mirrors_pc_into_state_before_save(self):
        """When result.session_id is None (e.g. interrupted with content),
        save_state still receives a dict where session_id matches pc's sid
        — pins the always-mirror invariant."""
        save_state_mock = MagicMock()
        state = {"session_id": None, "turn_count": 3}
        pc = self._fake_pc()
        pc._sid = "live-pc-sid"
        d = self._make_dispatcher(state=state)
        # Bypass seed so the test focuses on finalize-mirror behavior.
        d._pc_seeded = True
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        # result.session_id stays None.
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state", save_state_mock), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html"):
            d._finalize_response(result, "123")
        # save_state was called with state dict mirroring pc.get_session_id().
        assert save_state_mock.called
        saved_dict = save_state_mock.call_args[0][0]
        assert saved_dict["session_id"] == "live-pc-sid"
        # And the actual state dict was mutated to match.
        assert state["session_id"] == "live-pc-sid"


class TestTryEnqueueOrSendCallers:
    """E5 canaries — each dispatcher call site that used the 4-line
    try_enqueue_chat_notice + fallback dance MUST now use the single helper
    ``landline.claude.try_enqueue_or_send``. Reverting any one site to the bool
    pattern silently mis-routes notices; these canaries catch that revert.
    """

    def _make_dispatcher(
        self, state=None, run_claude_fn=None, send_response_fn=None,
    ):
        from landline.claude.failure_tracker import ClaudeFailureTracker
        state = state or {"session_id": "s", "turn_count": 0}
        ft = ClaudeFailureTracker()
        hook = MagicMock()
        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn or MagicMock(),
            send_response_fn=send_response_fn or MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_paused_notice_uses_try_enqueue_or_send(self):
        """The "(Paused.)" branch must call landline.claude.try_enqueue_or_send,
        not the bool primitive."""
        helper_mock = MagicMock()
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.try_enqueue_or_send", helper_mock), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None):
            d._finalize_response(result, "123")
        assert helper_mock.call_count == 1
        call_kwargs = helper_mock.call_args.kwargs
        assert call_kwargs.get("html") is not None
        assert "Paused" in str(call_kwargs.get("html"))
        assert callable(call_kwargs.get("direct_fn"))

    def test_context_warning_uses_try_enqueue_or_send(self):
        """The context-warning branch must call landline.claude.try_enqueue_or_send
        with text=..., not the bool primitive."""
        helper_mock = MagicMock()
        state = {"session_id": "s", "turn_count": 0}
        d = self._make_dispatcher(state=state)
        result = ClaudeStreamResult()
        result.streamed_text = "ok"
        result.final_result = "ok"
        result.session_id = "s"
        with patch("landline.claude.try_enqueue_or_send", helper_mock), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=75.0):
            d._finalize_response(result, "123")
        # 75% crosses the 70 threshold; expect exactly one helper call.
        warning_calls = [
            c for c in helper_mock.call_args_list
            if c.kwargs.get("text") and "context is at" in str(c.kwargs.get("text"))
        ]
        assert len(warning_calls) == 1
        assert callable(warning_calls[0].kwargs.get("direct_fn"))

    def test_still_working_uses_try_enqueue_or_send(self):
        """The watchdog "(Still working...)" branch must route through
        landline.claude.try_enqueue_or_send. Use a Timer test seam (immediate
        fire) so we don't sleep 60s."""
        helper_mock = MagicMock()
        d = self._make_dispatcher()

        class ImmediateTimer:
            def __init__(self, _delay, fn):
                self._fn = fn
                self.daemon = False

            def start(self):
                self._fn()

            def cancel(self):
                pass

        with patch("landline.claude.try_enqueue_or_send", helper_mock), \
             patch("landline.claude.dispatch.threading.Timer", ImmediateTimer):
            watchdog = d._start_response_watchdog("123")
            watchdog.cancel()
        assert helper_mock.call_count == 1
        call_kwargs = helper_mock.call_args.kwargs
        assert call_kwargs.get("html") is not None
        assert "Still working" in str(call_kwargs.get("html"))
        assert callable(call_kwargs.get("direct_fn"))


class TestLooksLikePrunedResume:
    """Stale-resume auto-recovery — the pruned/nonexistent-session predicate.
    Orthogonal to looks_like_stale_session (clean-empty shape); this one
    catches the is_error + no-init shape verified empirically against the
    Claude Code CLI.
    """

    def test_looks_like_pruned_resume_true_for_no_init_is_error(self):
        """Canonical pruned-resume shape: is_error True, saw_init False,
        exit 1, no content, AND the stderr "No conversation found" marker.

        The stderr marker is the load-bearing corroboration: without it,
        the same (is_error + no init) shape is ambiguous with a healthy
        mid-session turn whose ``system/init`` line raised in the pump
        (JSONDecodeError, unhandled _handle_event exception) — see
        looks_like_pruned_resume's docstring. Real pruned-resume always
        emits this marker (verified against the Claude Code CLI), so demanding it
        is safe and blocks the missed-init false-positive.
        """
        r = ClaudeStreamResult()
        r.result_is_error = True
        r.saw_init = False
        r.exit_code = 1
        r.stderr_tail = (
            "No conversation found with session ID: abcd-1234\n"
        )
        assert looks_like_pruned_resume(r) is True

    def test_looks_like_pruned_resume_false_when_missed_init_without_marker(self):
        """REGRESSION for the pump-misses-init path: a JSONDecodeError on
        the ``system/init`` line (or an unhandled _handle_event exception on
        that line) leaves saw_init=False even for a healthy mid-session
        turn whose later ``result`` event set is_error=True. The predicate
        MUST NOT classify this shape as pruned-resume — doing so wipes a
        still-valid server-side session and destroys the whole conversation.
        Real pruned-resume ALWAYS emits the stderr marker; demand it."""
        r = ClaudeStreamResult()
        r.result_is_error = True
        r.saw_init = False
        r.exit_code = 1
        r.stderr_tail = "some unrelated tool error\n"  # no pruned-resume marker
        assert looks_like_pruned_resume(r) is False

    def test_looks_like_pruned_resume_false_when_saw_init(self):
        """Mid-session API error: is_error True, but init DID open the
        turn — must NOT be classified as pruned, or the retry would wipe a
        healthy session's context."""
        r = ClaudeStreamResult()
        r.result_is_error = True
        r.saw_init = True
        r.exit_code = 1
        assert looks_like_pruned_resume(r) is False

    def test_looks_like_pruned_resume_true_when_stderr_marker_only(self):
        """Defense-in-depth fallback: if the result-event path didn't
        populate is_error but the stderr marker is present, still classify
        as pruned so a CLI drift doesn't silently break recovery."""
        r = ClaudeStreamResult()
        r.result_is_error = False
        r.saw_init = False
        r.stderr_tail = (
            "some earlier log line\n"
            "No conversation found with session ID: 1234-abcd\n"
        )
        assert looks_like_pruned_resume(r) is True

    def test_looks_like_pruned_resume_false_when_interrupted(self):
        """Interrupted turns must NEVER trigger pruned-resume — an
        interrupted run has undefined is_error / init state and retrying
        would fight the user's own /pause."""
        r = ClaudeStreamResult()
        r.result_is_error = True
        r.saw_init = False
        r.interrupted = True
        assert looks_like_pruned_resume(r) is False

    def test_looks_like_pruned_resume_false_when_only_no_init_no_error(self):
        """No init AND no is_error AND no stderr marker — just a clean-empty
        shape. That's looks_like_stale_session territory, not pruned-resume."""
        r = ClaudeStreamResult()
        r.result_is_error = False
        r.saw_init = False
        r.stderr_tail = ""
        assert looks_like_pruned_resume(r) is False

    def test_looks_like_pruned_resume_false_when_stderr_is_auth_failure(self):
        """AUTH-EXPIRY COLLISION: an OAuth-expiry 401 also produces
        (is_error=True, saw_init=False) because the CLI aborts BEFORE
        emitting system/init. Classifying it as pruned-resume would
        (a) show the operator the wrong "(Previous session expired…)"
        notice, (b) wipe the still-valid server-side session UUID, and
        (c) delay the real auth alert by an extra failed retry. The
        pruned-resume branch must exclude the auth stderr shape first."""
        for marker in daemon_config.CLAUDE_AUTH_ERROR_MARKERS:
            r = ClaudeStreamResult()
            # Same canonical pruned-resume shape as the True-case above…
            r.result_is_error = True
            r.saw_init = False
            r.exit_code = 1
            # …but the stderr tail matches the auth-expiry shape, so this
            # must NOT be classified as pruned-resume.
            r.stderr_tail = f"HTTP something\n{marker} tail\n"
            assert looks_like_pruned_resume(r) is False, (
                f"marker {marker!r} incorrectly classified as pruned-resume "
                "instead of auth failure"
            )

    def test_looks_like_pruned_resume_false_when_stderr_marker_only_and_auth(self):
        """Belt-and-suspenders defense: even the marker-only fallback path
        (result_is_error=False, saw_init=False) must yield to auth-shape
        stderr — otherwise a CLI drift that produced BOTH markers in
        stderr would still wipe the session on an auth failure."""
        r = ClaudeStreamResult()
        r.result_is_error = False
        r.saw_init = False
        r.stderr_tail = (
            "No conversation found with session ID: 1234-abcd\n"
            "HTTP 401 Invalid authentication credentials\n"
        )
        assert looks_like_pruned_resume(r) is False

    def test_looks_like_stale_session_still_matches_legacy_clean_empty_shape(self):
        """No regression: the existing predicate must still match the
        clean-empty shape unchanged — pruned-resume is additive, not a
        replacement."""
        r = ClaudeStreamResult()
        # No content, no error, exit 0/None, not interrupted.
        assert looks_like_stale_session(r) is True

        r2 = ClaudeStreamResult()
        r2.streamed_text = "some content"
        assert looks_like_stale_session(r2) is False

        r3 = ClaudeStreamResult()
        r3.exit_code = 1
        assert looks_like_stale_session(r3) is False


class TestPrunedResumeRetryIntegration:
    """Regression: the dispatcher must fall back to a fresh session on the
    pruned-resume shape, and must NOT fall back on a mid-session error
    (which also carries is_error=True)."""

    def _make_dispatcher(self, state, run_claude_fn):
        ft = ClaudeFailureTracker()
        hook = MagicMock()
        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=hook,
            run_claude_fn=run_claude_fn,
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    @staticmethod
    def _fake_pc():
        pc = MagicMock()
        pc._sid = None

        def _get():
            return pc._sid

        def _set(sid):
            pc._sid = sid

        pc.get_session_id.side_effect = _get
        pc.set_session_id.side_effect = _set
        return pc

    def test_invoke_with_stale_retry_falls_back_on_pruned_resume(self):
        """REGRESSION: pruned/nonexistent --resume (is_error + no init)
        must trigger fresh-session retry.

        This test fails against pre-Cluster-2 code, which required a clean
        exit with no content to retry — the pruned shape is exit 1 + is_error,
        so the old predicate wouldn't match and the user would just see the
        empty-response notice forever after the session was pruned.
        """
        call_count = [0]
        calls_seen = []

        def pruned_then_fresh(**kwargs):
            call_count[0] += 1
            calls_seen.append({
                "is_new": kwargs["is_new"],
                "session_id": kwargs["session_id"],
            })
            r = ClaudeStreamResult()
            if call_count[0] == 1:
                # Pruned-resume shape (canonical: is_error + no init +
                # exit 1 AND the "No conversation found" stderr marker,
                # which is the load-bearing corroboration that keeps a
                # pump-missed-init mid-session failure from being wiped —
                # see looks_like_pruned_resume's docstring).
                r.result_is_error = True
                r.saw_init = False
                r.exit_code = 1
                r.result_subtype = "error_during_execution"
                r.stderr_tail = (
                    "No conversation found with session ID: pruned-old-sid\n"
                )
                return r
            # Fresh session succeeds.
            r.session_id = "fresh-session"
            r.streamed_text = "fresh response"
            r.final_result = "fresh response"
            return r

        state = {"session_id": "pruned-old-sid", "turn_count": 5}
        pc = self._fake_pc()
        pc._sid = "pruned-old-sid"
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=pruned_then_fresh),
        )
        d._pc_seeded = True  # pre-seeded (simulating a running daemon)
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello after pruning", "123")

        # Retry happened.
        assert call_count[0] == 2
        assert calls_seen[1]["is_new"] is True
        assert calls_seen[1]["session_id"] is None
        # pc was cleared then set to the fresh sid.
        set_args = [c.args[0] for c in pc.set_session_id.call_args_list]
        assert None in set_args
        assert "fresh-session" in set_args
        assert pc._sid == "fresh-session"
        assert state["session_id"] == "fresh-session"

    def test_invoke_with_stale_retry_does_not_fall_back_on_mid_session_is_error(self):
        """MISCLASSIFICATION HAZARD: a mid-session API error (is_error True
        but saw_init True on this turn) must NOT trigger fresh-session retry
        — that would silently wipe a healthy conversation."""
        call_count = [0]

        def mid_session_error(**kwargs):
            call_count[0] += 1
            r = ClaudeStreamResult()
            r.result_is_error = True
            r.saw_init = True  # the load-bearing distinguisher
            r.exit_code = 1
            r.result_subtype = "error_during_execution"
            return r

        state = {"session_id": "healthy-sid", "turn_count": 8}
        pc = self._fake_pc()
        pc._sid = "healthy-sid"
        d = self._make_dispatcher(
            state=state,
            run_claude_fn=MagicMock(side_effect=mid_session_error),
        )
        d._pc_seeded = True
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("mid-session turn", "123")

        # Exactly one call — no retry, because the mid-session shape must
        # not be classified as pruned-resume.
        assert call_count[0] == 1
        # pc.set_session_id(None) must NOT have been called on this path.
        assert not any(
            c.args[0] is None for c in pc.set_session_id.call_args_list
        )
        assert pc._sid == "healthy-sid"
        assert state["session_id"] == "healthy-sid"

    def test_invoke_with_stale_retry_does_not_wipe_session_on_auth_failure(self):
        """AUTH-EXPIRY COLLISION REGRESSION: a Claude CLI 401 (auth expiry)
        emits the same (is_error=True, saw_init=False) shape as a pruned
        --resume, plus an auth marker in stderr. The dispatcher must NOT
        (a) send the operator "(Previous session expired, starting fresh.)",
        (b) clear pc.session_id, or (c) zero state['session_id']/turn_count
        on that shape — all destructive on a still-valid server-side session.
        Only ONE run_claude call must happen; the auth stderr keeps the
        out-of-band auth alert unmolested.
        """
        call_count = [0]
        calls_seen = []

        def auth_failure(**kwargs):
            call_count[0] += 1
            calls_seen.append({
                "is_new": kwargs["is_new"],
                "session_id": kwargs["session_id"],
            })
            r = ClaudeStreamResult()
            # Canonical pruned-resume shape…
            r.result_is_error = True
            r.saw_init = False
            r.exit_code = 1
            r.result_subtype = "error_during_execution"
            # …but with the auth-expiry stderr marker. Must NOT trigger
            # fresh-session fallback.
            r.stderr_tail = "HTTP 401 Invalid authentication credentials\n"
            return r

        send_response_mock = MagicMock()
        state = {"session_id": "still-valid-sid", "turn_count": 5}
        pc = self._fake_pc()
        pc._sid = "still-valid-sid"
        ft = ClaudeFailureTracker()
        d = ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ft,
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(side_effect=auth_failure),
            send_response_fn=send_response_mock,
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )
        d._pc_seeded = True
        with patch("landline.claude._get_persistent_claude", return_value=pc), \
             patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history",
                   return_value=""), \
             patch(
                 "landline.runtime.notifications.send_health_alert", return_value=True,
             ) as mock_alert:
            d.send_to_claude("test turn during auth outage", "123")

        # Exactly ONE run_claude call — no fresh-session retry.
        assert call_count[0] == 1
        assert calls_seen[0]["is_new"] is False
        assert calls_seen[0]["session_id"] == "still-valid-sid"

        # Session UUID preserved end-to-end.
        assert pc._sid == "still-valid-sid"
        assert state["session_id"] == "still-valid-sid"
        # pc.set_session_id(None) MUST NOT have been called — that's the
        # destructive wipe we're guarding against.
        assert not any(
            c.args[0] is None for c in pc.set_session_id.call_args_list
        )

        # The misleading "(Previous session expired…)" notice MUST NOT
        # have been sent.
        sent_bodies = [c.args[2] for c in send_response_mock.call_args_list]
        assert not any(
            "Previous session expired" in body for body in sent_bodies
        ), (
            "dispatcher sent the pruned-resume notice on an auth failure, "
            "misleading the operator and wiping the session"
        )

        # Out-of-band auth alert still fires on turn 1.
        mock_alert.assert_called_once()
        _, kwargs = mock_alert.call_args
        assert kwargs.get("subject") == "claude-auth-expired"


class TestClaudeAuthExpiryAlert:
    """Claude CLI auth-expiry detection.

    Regression against a multi-day silent auth outage (June 2026): every
    headless `claude -p` call 401'd silently for two days before it was
    noticed. These tests pin the shape of the one-shot iMessage alert that
    fires on the FIRST failure whose stderr matches the OAuth-expiry
    pattern, independent of the failure_tracker "Claude unavailable" alert
    (which fires on the 10th consecutive failure).
    """

    def _make_dispatcher(self, failure_tracker=None):
        ft = failure_tracker or ClaudeFailureTracker()
        return ClaudeDispatcher(
            token="fake-token",
            state={"session_id": None, "turn_count": 0},
            failure_tracker=ft,
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    @staticmethod
    def _auth_failure_result(stderr_tail="HTTP 401 Invalid authentication"):
        r = ClaudeStreamResult()
        r.exit_code = 1
        r.stderr_tail = stderr_tail
        # No content — is_result_successful will return False.
        return r

    @staticmethod
    def _success_result():
        r = ClaudeStreamResult()
        r.streamed_text = "healthy response"
        r.final_result = "healthy response"
        r.session_id = "healthy-sid"
        return r

    def test_stderr_looks_like_auth_failure_true_on_401(self):
        assert _stderr_looks_like_auth_failure(
            "some log\nHTTP 401 Invalid authentication credentials\n"
        ) is True

    def test_stderr_looks_like_auth_failure_false_on_empty(self):
        assert _stderr_looks_like_auth_failure("") is False
        assert _stderr_looks_like_auth_failure(None) is False  # type: ignore[arg-type]

    def test_stderr_looks_like_auth_failure_case_insensitive(self):
        assert _stderr_looks_like_auth_failure("INVALID AUTHENTICATION") is True
        assert _stderr_looks_like_auth_failure("Please Run /login") is True

    def test_stderr_looks_like_auth_failure_false_on_unrelated(self):
        assert _stderr_looks_like_auth_failure("connection refused") is False
        assert _stderr_looks_like_auth_failure("segfault") is False

    def test_auth_expiry_alert_fires_once_on_first_auth_failure(self):
        """First 401-shaped failure dispatches send_health_alert exactly
        once with subject='claude-auth-expired'."""
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(self._auth_failure_result(), "123")
        assert mock_alert.call_count == 1
        _, kwargs = mock_alert.call_args
        assert kwargs.get("subject") == "claude-auth-expired"
        assert "auth expired" in kwargs.get("body", "").lower()
        assert d._auth_alert_sent is True
        assert d._last_auth_alert_at > 0.0

    def test_auth_expiry_alert_does_not_refire_within_latch(self):
        """Repeated failures with the same auth shape must dispatch the
        alert exactly ONCE — the latch (and belt-and-suspenders time floor)
        blocks re-fires until the next success."""
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            for _ in range(5):
                d._record_outcome(self._auth_failure_result(), "123")
        assert mock_alert.call_count == 1

    def test_auth_expiry_alert_latch_resets_on_success(self):
        """A successful stream clears the ``_auth_alert_sent`` latch. A
        fresh auth failure AFTER that success triggers a SECOND alert —
        but ONLY after the belt-and-suspenders time floor
        (CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS) has elapsed. Together the
        two guards mean: within the same 6h window a fail→success→fail
        cycle NEVER re-fires (that's the spam pattern — a stray auth
        marker in the cumulative CC stderr tail combined with a latch
        reset would otherwise repeat the alert milliseconds apart).
        After the floor has passed AND the latch is clear, a genuine new
        incident does re-alert.
        """
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 1
            assert d._auth_alert_sent is True
            first_alert_at = d._last_auth_alert_at

            # A rapid success then failure inside the 6h floor MUST NOT
            # re-fire — this is the exact fail→success→fail spam pattern
            # the floor exists to block.
            d._record_outcome(self._success_result(), "123")
            assert d._auth_alert_sent is False
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 1, (
                "second alert fired within the 6h floor — floor is not "
                "gating latch-reset spam"
            )
            # And the floor timestamp must NOT have been bumped by the
            # blocked attempt (otherwise the floor would slide forward
            # forever on repeated fails).
            assert d._last_auth_alert_at == first_alert_at

            # Simulate the 6h floor elapsing: rewind the last-alert
            # timestamp beyond the floor. A REAL new incident now
            # re-alerts as intended.
            from landline import config as daemon_config
            d._last_auth_alert_at = (
                time.time()
                - daemon_config.CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS
                - 10
            )
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 2
            assert d._auth_alert_sent is True

    @pytest.mark.parametrize("marker", list(daemon_config.CLAUDE_AUTH_ERROR_MARKERS))
    def test_auth_expiry_alert_matches_multiple_marker_shapes(self, marker):
        """Every configured marker string, embedded in a synthetic stderr
        tail, must trigger the alert. Guards against a marker being added
        to config but silently unused."""
        d = self._make_dispatcher()
        stderr_tail = f"[cc stderr]\nsome prefix {marker} some suffix\n"
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(self._auth_failure_result(stderr_tail), "123")
        assert mock_alert.call_count == 1, (
            f"marker {marker!r} did not fire the alert"
        )

    def test_auth_expiry_alert_does_not_fire_on_non_auth_failure(self):
        """A generic failure (e.g. 'connection refused') must NOT trigger
        the auth alert, but MUST still record the failure — regression
        against over-matching the stderr shape."""
        ft = ClaudeFailureTracker()
        d = self._make_dispatcher(failure_tracker=ft)
        result = self._auth_failure_result(stderr_tail="connection refused")
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(result, "123")
        mock_alert.assert_not_called()
        assert d._auth_alert_sent is False
        # Failure tracker still recorded the failure — the two paths are
        # independent.
        assert ft.consecutive_failure_count == 1

    def test_auth_expiry_alert_survives_send_health_alert_exception(self):
        """If send_health_alert raises, _record_outcome must NOT propagate,
        and the latch must stay False so the next failure retries the
        alert (rather than being silently swallowed forever)."""
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert",
            side_effect=RuntimeError("imsg broke"),
        ) as mock_alert:
            # Must not raise.
            d._record_outcome(self._auth_failure_result(), "123")
            assert d._auth_alert_sent is False
            # Next failure retries because the latch is still False.
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 2

    def test_auth_expiry_alert_not_latched_when_send_returns_false(self):
        """If send_health_alert returns False (Keychain missing owner
        handle), the latch must NOT set — otherwise the daemon silently
        forfeits the alert for the rest of the incident."""
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=False,
        ):
            d._record_outcome(self._auth_failure_result(), "123")
        assert d._auth_alert_sent is False
        assert d._last_auth_alert_at == 0.0

    def test_stderr_bare_401_substring_does_not_trigger_auth_alert(self):
        """REGRESSION: PersistentClaude's stderr tail is cumulative across
        the process's life. Any prior line with the digit sequence 401 in
        it — a port like ``4014``, ``processed 401 files``, a pid/hash
        ending in ...401... — must NOT trip the auth detector. Historically
        a bare ``"401"`` substring marker was in CLAUDE_AUTH_ERROR_MARKERS
        and fired an ``claude-auth-expired`` iMessage on every such
        false-positive line. The anchored HTTP-401 markers replace it.
        """
        # None of these strings represent an actual auth failure. Each
        # contains the digits 401 as a false-positive substring.
        for stderr in (
            "port 4014 already in use\n",
            "processed 401 files\n",
            "pid=1234015401\n",
            "hash=deadbeef401cafe\n",
            "elapsed=4014ms\n",
        ):
            assert _stderr_looks_like_auth_failure(stderr) is False, (
                "bare-401-substring false positive on: %r" % stderr
            )
        # And a real HTTP 401 line still matches (via the anchored form).
        assert _stderr_looks_like_auth_failure(
            "HTTP 401 Unauthorized\n"
        ) is True

    def test_stderr_bare_401_false_positive_does_not_dispatch_alert(self):
        """End-to-end: a mixed stderr tail with a legitimate non-auth
        failure (e.g. tool timeout, exit code 1) BUT also a stray ``401``
        digit substring anywhere in the ~8KB cumulative buffer must NOT
        dispatch the auth iMessage. Guards against the substring bug
        reappearing at the config OR predicate layer."""
        d = self._make_dispatcher()
        result = self._auth_failure_result(
            stderr_tail="tool timed out\nlistening on port 4014\n"
        )
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(result, "123")
        mock_alert.assert_not_called()
        assert d._auth_alert_sent is False

    def test_auth_alert_time_floor_blocks_rapid_refire_regardless_of_latch(self):
        """REGRESSION for the ``if self._auth_alert_sent:``-gated floor
        bug: the 6h ``CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS`` floor must
        be checked whether the latch is set or not. A fail→success→fail
        cycle previously reset ``_auth_alert_sent`` to False, so the
        second failure's re-fire skipped the time-floor branch entirely
        and dispatched a second iMessage milliseconds after the first.
        The floor now runs unconditionally, so a rapid re-fire is blocked
        even after the latch has been cleared by a success."""
        d = self._make_dispatcher()
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            # Fire the first alert normally.
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 1
            assert d._auth_alert_sent is True
            first_alert_at = d._last_auth_alert_at
            assert first_alert_at > 0.0

            # Simulate the latch reset that used to skip the floor:
            # clear the latch WITHOUT going through _record_outcome so we
            # test the guard in isolation.
            d._auth_alert_sent = False

            # Now try to fire again immediately (well within 6h).
            d._record_outcome(self._auth_failure_result(), "123")
            assert mock_alert.call_count == 1, (
                "second alert fired within the 6h time floor — the floor "
                "is being skipped when _auth_alert_sent is False"
            )
            # Timestamp must not slide forward on a blocked attempt.
            assert d._last_auth_alert_at == first_alert_at

    def test_first_auth_failure_alerts_before_ten_failure_threshold(self):
        """REGRESSION for the June 2026 incident: the auth alert fires on
        the very first auth-shaped failure — independently of the
        existing 10-consecutive-failure ``Claude unavailable`` Telegram
        alert. the operator must learn about a multi-day outage on turn 1, not on
        turn 10."""
        ft = ClaudeFailureTracker()
        d = self._make_dispatcher(failure_tracker=ft)
        with patch(
            "landline.runtime.notifications.send_health_alert", return_value=True,
        ) as mock_alert:
            d._record_outcome(self._auth_failure_result(), "123")
        # Auth alert did fire on the very first turn.
        mock_alert.assert_called_once()
        # But the failure_tracker's Claude-unavailable Telegram alert must
        # NOT have gated yet — that path fires only at the 10-consecutive
        # threshold, and the two are independent by design.
        assert ft.consecutive_failure_count == 1
        assert ft.should_send_alert_now() is False


class TestReactionCompletionAcks:
    """Dispatcher fires 👌 (completion) on the message_ids that earned a 👀
    at classify time, but ONLY on genuinely successful turns
    (non-interrupted, non-error).

    Coverage:
      - success → 👌 dispatched with the exact ids and DONE emoji
      - interrupted → no 👌 (leave 👀 as "you paused this")
      - error/empty result → no 👌
      - a reactions call that raises MUST NOT corrupt finalize
      - send_to_claude kwarg wires ids onto the dispatcher instance
    """

    def _make_dispatcher(self, state=None, send_response_fn=None):
        state = state if state is not None else {
            "session_id": "s", "turn_count": 0,
        }
        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ClaudeFailureTracker(),
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(),
            send_response_fn=send_response_fn or MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_send_to_claude_stashes_ack_ids(self):
        """The ``ack_message_ids`` kwarg lands on the instance so
        _finalize_response can consume it. Regression against a future
        rename that silently drops the kwarg."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = ["stale"]
        d._pending_ack_chat_id = "stale"
        # Force early return so we don't run the full call — the entry
        # stash lines run BEFORE the seed/gate/invoke path.
        with patch.object(d, "_seed_pc_session_from_state_once", side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError):
                d.send_to_claude(
                    "hi", "chat-1", ack_message_ids=[11, 22, 33],
                )
        assert d._pending_ack_message_ids == [11, 22, 33]
        assert d._pending_ack_chat_id == "chat-1"

    def test_send_to_claude_default_ack_ids_are_empty(self):
        """Callers that don't pass ack_message_ids get an empty stash
        (not a leaked list from a previous call)."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [1, 2, 3]  # stale
        with patch.object(d, "_seed_pc_session_from_state_once", side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError):
                d.send_to_claude("hi", "chat-1")
        assert d._pending_ack_message_ids == []

    def test_finalize_fires_done_reaction_on_success(self):
        from landline.config import REACTION_DONE_EMOJI
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [101, 202]
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()
        result.streamed_text = "hello back"
        result.final_result = "hello back"
        result.session_id = "s"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.telegram.reactions.set_reactions_batch_async") as mock_batch:
            d._finalize_response(result, "chat-1")
        mock_batch.assert_called_once()
        args = mock_batch.call_args[0]
        # (token, chat_id, message_ids, emoji)
        assert args[0] == "fake-token"
        assert args[1] == "chat-1"
        assert list(args[2]) == [101, 202]
        assert args[3] == REACTION_DONE_EMOJI

    def test_finalize_skips_done_reaction_on_interrupted(self):
        """Interrupted → leave 👀 as the persistent "you paused this"
        cue. NO 👌 fired."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [101]
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html"), \
             patch("landline.telegram.reactions.set_reactions_batch_async") as mock_batch:
            d._finalize_response(result, "chat-1")
        mock_batch.assert_not_called()

    def test_finalize_skips_done_reaction_on_error(self):
        """Failed turn (not interrupted) → NO 👌; leave 👀 so the user
        knows nothing landed."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [101]
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()
        result.error = "something broke"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.telegram.reactions.set_reactions_batch_async") as mock_batch:
            d._finalize_response(result, "chat-1")
        mock_batch.assert_not_called()

    def test_finalize_skips_done_reaction_on_empty_result(self):
        """Empty result (no content, no error, not interrupted) is also
        NOT a success — no 👌."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [101]
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()  # no content, no error
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.telegram.reactions.set_reactions_batch_async") as mock_batch:
            d._finalize_response(result, "chat-1")
        mock_batch.assert_not_called()

    def test_finalize_skips_done_reaction_when_no_pending_ids(self):
        """No ids stashed → no 👌 (nothing to react to)."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = []
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()
        result.streamed_text = "hi"
        result.final_result = "hi"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.telegram.reactions.set_reactions_batch_async") as mock_batch:
            d._finalize_response(result, "chat-1")
        mock_batch.assert_not_called()

    def test_finalize_survives_reactions_raising(self):
        """A reaction dispatch that raises MUST NOT corrupt the finalize
        path — save_state/log_conversation already ran, and the caller
        of finalize must return normally."""
        d = self._make_dispatcher()
        d._pending_ack_message_ids = [101]
        d._pending_ack_chat_id = "chat-1"
        result = ClaudeStreamResult()
        result.streamed_text = "hi"
        result.final_result = "hi"
        result.session_id = "s"
        with patch("landline.claude.dispatch.save_state") as mock_save, \
             patch("landline.claude.dispatch.log_conversation") as mock_logconv, \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch(
                 "landline.telegram.reactions.set_reactions_batch_async",
                 side_effect=RuntimeError("nope"),
             ):
            # Must not raise.
            d._finalize_response(result, "chat-1")
        # save_state and log_conversation still ran (before the reaction
        # dispatch attempt).
        assert mock_save.called
        assert mock_logconv.called

    def test_ack_ids_survive_backoff_queue_drain(self):
        """PIN (finding #4): messages queued during Claude backoff must
        still receive 👌 when the drained batch eventually finalizes.

        Repro:
          1. Backoff active → send_to_claude(m1, ack=[11]) → gated + queued.
          2. Backoff clears; next send_to_claude(m2, ack=[22]) drains the
             queue, merges [m1 || m2], and finalizes successfully.
          3. The completion reaction batch MUST include BOTH 11 and 22.
             Under the pre-fix code only [22] was fired — every message
             queued during the outage kept 👀 forever without 👌.
        """
        from landline.config import REACTION_DONE_EMOJI
        from landline.claude.failure_tracker import ClaudeFailureTracker

        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.consecutive_failure_count = 5

        run_mock = MagicMock()
        r = ClaudeStreamResult()
        r.streamed_text = "ok"
        r.final_result = "ok"
        r.session_id = "s"
        run_mock.return_value = r

        d = ClaudeDispatcher(
            token="fake-token",
            state={"session_id": "s", "turn_count": 0},
            failure_tracker=ft,
            shutdown_hook=MagicMock(),
            run_claude_fn=run_mock,
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

        # Step 1: gated call while backoff active. m1 goes on the queue
        # with its ack_message_ids=[11].
        d.send_to_claude("m1", "chat-1", ack_message_ids=[11])
        assert len(d._backoff_queue) == 1
        run_mock.assert_not_called()

        # Step 2: backoff clears. Next call drains, merges, finalizes.
        ft.next_attempt_allowed_at_epoch = 0.0
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch(
                 "landline.claude.dispatch.get_context_percent",
                 return_value=None,
             ), \
             patch(
                 "landline.claude.dispatch.read_recent_conversation_history",
                 return_value="",
             ), \
             patch(
                 "landline.telegram.reactions.set_reactions_batch_async",
             ) as mock_batch:
            d.send_to_claude("m2", "chat-1", ack_message_ids=[22])

        # Both messages merged into one Claude call.
        sent_text = run_mock.call_args[1]["message"]
        assert "m1" in sent_text
        assert "m2" in sent_text
        # Queue drained.
        assert len(d._backoff_queue) == 0

        # 👌 fires on BOTH the queued id and the current id.
        mock_batch.assert_called_once()
        args = mock_batch.call_args[0]
        # (token, chat_id, message_ids, emoji)
        assert args[0] == "fake-token"
        assert args[1] == "chat-1"
        assert sorted(args[2]) == [11, 22], (
            "backoff-queued ack ids were dropped — the finding-#4 "
            "regression is back"
        )
        assert args[3] == REACTION_DONE_EMOJI

    def test_gated_call_stashes_ack_ids_on_queue_tuple(self):
        """Direct-observation pin: when gated, the queue tuple's 4th slot
        holds the ack_message_ids so a later drain can find them."""
        from landline.claude.failure_tracker import ClaudeFailureTracker
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.consecutive_failure_count = 5
        d = ClaudeDispatcher(
            token="fake-token",
            state={"session_id": "s", "turn_count": 0},
            failure_tracker=ft,
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )
        d.send_to_claude("m1", "chat-1", ack_message_ids=[7, 8, 9])
        assert len(d._backoff_queue) == 1
        _, _, _, queued_ack_ids = d._unpack_entry(d._backoff_queue[0])
        assert queued_ack_ids == [7, 8, 9]


class TestUsageStatsRecording:
    """Dispatcher persists usage/cost per turn to the daily aggregate
    on genuinely successful turns only.

    Coverage:
      - success → usage_stats.record_turn called once with dispatched=True
      - interrupted → NOT called
      - error → NOT called
      - empty result (no content, no error) → NOT called
      - record_turn raising must NOT propagate; save_state + log_conversation
        already ran and finalize must return.
    """

    def _make_dispatcher(self, state=None):
        state = state if state is not None else {
            "session_id": "s", "turn_count": 0,
        }
        return ClaudeDispatcher(
            token="fake-token",
            state=state,
            failure_tracker=ClaudeFailureTracker(),
            shutdown_hook=MagicMock(),
            run_claude_fn=MagicMock(),
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=PauseFlag(),
        )

    def test_success_calls_record_turn_with_dispatched_true(self):
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.streamed_text = "hi"
        result.final_result = "hi"
        result.session_id = "s"
        result.result_usage = {"input_tokens": 10, "output_tokens": 20}
        result.result_model_usage = {"m": {"input_tokens": 10, "output_tokens": 20}}
        result.result_total_cost_usd = 0.005
        result.result_duration_ms = 300
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.runtime.usage_stats.record_turn") as mock_record:
            d._finalize_response(result, "chat-1")
        mock_record.assert_called_once()
        kwargs = mock_record.call_args.kwargs
        assert kwargs["dispatched"] is True
        assert kwargs["result_usage"] == {"input_tokens": 10, "output_tokens": 20}
        assert kwargs["result_model_usage"] == {"m": {"input_tokens": 10, "output_tokens": 20}}
        assert abs(kwargs["total_cost_usd"] - 0.005) < 1e-9
        assert kwargs["duration_ms"] == 300

    def test_interrupted_does_not_record_turn(self):
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.interrupted = True
        result.streamed_text = "partial"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.send_html"), \
             patch("landline.runtime.usage_stats.record_turn") as mock_record:
            d._finalize_response(result, "chat-1")
        mock_record.assert_not_called()

    def test_error_result_does_not_record_turn(self):
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.error = "boom"
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.runtime.usage_stats.record_turn") as mock_record:
            d._finalize_response(result, "chat-1")
        mock_record.assert_not_called()

    def test_empty_result_does_not_record_turn(self):
        d = self._make_dispatcher()
        result = ClaudeStreamResult()  # no content, no error, not interrupted
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.runtime.usage_stats.record_turn") as mock_record:
            d._finalize_response(result, "chat-1")
        mock_record.assert_not_called()

    def test_record_turn_raising_does_not_propagate(self):
        """A broken stats sidecar must NEVER crash finalize. save_state and
        log_conversation must still run; finalize must return normally."""
        d = self._make_dispatcher()
        result = ClaudeStreamResult()
        result.streamed_text = "hi"
        result.final_result = "hi"
        result.session_id = "s"
        with patch("landline.claude.dispatch.save_state") as mock_save, \
             patch("landline.claude.dispatch.log_conversation") as mock_logconv, \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch(
                 "landline.runtime.usage_stats.record_turn",
                 side_effect=RuntimeError("io simulated"),
             ):
            # Must not raise.
            d._finalize_response(result, "chat-1")
        # save_state ran BEFORE the record_turn attempt, so it must have
        # fired regardless.
        assert mock_save.called
        # log_conversation runs AFTER record_turn — but our try/except
        # around record_turn must swallow the exception so log_conversation
        # still runs.
        assert mock_logconv.called


class TestPreExistingPauseReAnchor:
    """Regression pins for the voice_handler.py:229 / claude_dispatch.py
    ``_invoke_claude_call`` bug: when a pause was requested BEFORE
    ``_invoke_claude_call`` ran (poller's ``/pause`` callback fires in
    the same batch as a media dispatch, or ``voice_handler`` filtered
    the flag off whisper and dispatched the transcript), ``new_call()``
    bumps the generation and strands the request at the previous gen.
    Two failures collapse:

    1. Silent /pause drop: ``interrupt_check`` uses ``is_requested(
       my_generation)``, which is False for the stranded request, so
       the watchdog never fires and the operator never sees "(Paused.)".
    2. 100% CPU busy-loop: the level-triggered ``PauseFlag._event`` is
       still set, so ``_wait_for_done_or_pause`` returns True on every
       tick and the watchdog spins for the full Claude call.

    Fix: ``_invoke_claude_call`` re-anchors the request at the current
    generation (``pf.request_pause()``) so ``is_requested(
    my_generation)`` returns True and the watchdog interrupts on the
    first tick.
    """

    def _make_dispatcher(self, run_claude_fn, pause_flag):
        return ClaudeDispatcher(
            token="fake-token",
            state={"session_id": None, "turn_count": 0},
            failure_tracker=ClaudeFailureTracker(),
            shutdown_hook=MagicMock(),
            run_claude_fn=run_claude_fn,
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=pause_flag,
        )

    def test_pre_existing_pause_is_honored_at_new_generation(self):
        """A pause requested BEFORE send_to_claude must fire the watchdog
        interrupt on the FIRST ``interrupt_check`` at the new generation
        — not be stranded at the previous generation."""
        pf = PauseFlag()
        # Simulate the "flag was set before dispatch" state: poller
        # callback fires request_pause() at generation 0, then
        # voice_handler filters whisper's pause_flag to None and
        # dispatches the transcript to Claude.
        pf.request_pause()
        interrupt_observations = []

        def run_claude(**kwargs):
            interrupt_check = kwargs.get("interrupt_check")
            # First check inside run_claude — must return True because
            # the fix re-anchored the request at my_generation.
            interrupt_observations.append(interrupt_check())
            r = ClaudeStreamResult()
            r.interrupted = True
            r.streamed_text = "partial"
            return r

        d = self._make_dispatcher(
            run_claude_fn=MagicMock(side_effect=run_claude),
            pause_flag=pf,
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("please transcribe this", "123")
        # The pre-existing pause was honored at the new generation.
        assert interrupt_observations == [True], (
            "pre-existing pause was stranded at the old generation — "
            "interrupt_check should return True at the new generation"
        )

    def test_pre_existing_pause_does_not_starve_wait_loop(self):
        """Belt-and-suspenders: after re-anchor, the PauseFlag's
        level-triggered Event still reflects "a pause is currently
        requested," but is now anchored at ``my_generation`` — so a
        watchdog observing ``pause_flag.wait(0) → True`` and looping
        back to ``interrupt_check`` will EXIT (not busy-spin) because
        the check returns True. Prior bug: ``wait(0)`` True forever,
        ``interrupt_check`` False forever, 100% CPU."""
        pf = PauseFlag()
        pf.request_pause()  # stranded at generation 0
        d = self._make_dispatcher(
            run_claude_fn=MagicMock(return_value=ClaudeStreamResult()),
            pause_flag=pf,
        )
        # Manually walk through what _invoke_claude_call does BEFORE
        # run_claude is called: new_call() + re-anchor.
        my_generation = pf.new_call()

        # Reproduce the load-bearing block from _invoke_claude_call.
        if pf.is_set():
            pf.request_pause()

        # POST-fix state: the level-triggered Event is set, AND the
        # generation-guarded request is anchored at my_generation, so
        # ``interrupt_check`` returns True — the watchdog would break
        # out of its wait loop on the very next iteration.
        assert pf.wait(0) is True, "Event was cleared by re-anchor — regressed"
        assert pf.is_requested(my_generation) is True, (
            "pause request was not re-anchored to the new generation — "
            "watchdog will busy-loop until Claude finishes"
        )

    def test_no_pre_existing_pause_leaves_flag_untouched(self):
        """When no pause was pending at call entry, the re-anchor path
        must NOT phantom-request a pause. ``is_requested`` stays False
        so the watchdog runs to completion normally."""
        pf = PauseFlag()
        assert pf.is_set() is False  # baseline

        interrupt_observations = []

        def run_claude(**kwargs):
            interrupt_observations.append(kwargs["interrupt_check"]())
            r = ClaudeStreamResult()
            r.streamed_text = "ok"
            r.session_id = "s"
            return r

        d = self._make_dispatcher(
            run_claude_fn=MagicMock(side_effect=run_claude),
            pause_flag=pf,
        )
        with patch("landline.claude.dispatch.save_state"), \
             patch("landline.claude.dispatch.log_conversation"), \
             patch("landline.claude.dispatch.get_context_percent", return_value=None), \
             patch("landline.claude.dispatch.read_recent_conversation_history", return_value=""):
            d.send_to_claude("hello", "123")
        # No spurious interrupt.
        assert interrupt_observations == [False]
        assert pf.is_set() is False
