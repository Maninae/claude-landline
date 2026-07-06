"""Tests for landline.runtime.commands — CommandRouter, _parse_command, _status_text."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from landline.runtime.commands import _parse_command, _status_text, CommandRouter
from landline.runtime.lock import LockManager


class TestParseCommand:
    def test_simple_command(self):
        cmd, arg = _parse_command("/unlock secretpass")
        assert cmd == "/unlock"
        assert arg == "secretpass"

    def test_command_no_arg(self):
        cmd, arg = _parse_command("/status")
        assert cmd == "/status"
        assert arg == ""

    def test_lowercases_command(self):
        cmd, arg = _parse_command("/UNLOCK pass")
        assert cmd == "/unlock"

    def test_strips_whitespace(self):
        cmd, arg = _parse_command("  /new  ")
        assert cmd == "/new"
        assert arg == ""

    def test_empty_string(self):
        cmd, arg = _parse_command("")
        assert cmd == ""
        assert arg == ""

    def test_preserves_arg_case(self):
        cmd, arg = _parse_command("/unlock MySecret")
        assert arg == "MySecret"

    def test_multi_word_arg(self):
        cmd, arg = _parse_command("/unlock my secret phrase")
        assert cmd == "/unlock"
        assert arg == "my secret phrase"

    def test_non_command_text(self):
        cmd, arg = _parse_command("hello world")
        assert cmd == "hello"
        assert arg == "world"


class TestStatusText:
    def _make_lock_manager(self):
        persist = MagicMock()
        lm = LockManager(persist)
        lm.restore_from_state({
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            "unlock_timestamp": 0.0,
        })
        return lm

    def test_contains_header(self, no_subprocess, tmp_workspace):
        from landline.config import AGENT_NAME
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert f"{AGENT_NAME} System Status" in text

    def test_contains_session_info(self, no_subprocess, tmp_workspace):
        lm = self._make_lock_manager()
        state = {"session_id": "abcdefghijklmnop", "turn_count": 5}
        text = _status_text(state, lm, tmp_workspace)
        assert "abcdefghijkl..." in text
        assert "5 turns" in text

    def test_no_session_shows_none(self, no_subprocess, tmp_workspace):
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert "none" in text

    def test_contains_lock_status(self, no_subprocess, tmp_workspace):
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert "Lock:" in text

    def test_shows_morning_brief(self, no_subprocess, tmp_workspace, monkeypatch):
        # MORNING_BRIEF_GLOB is opt-in — set it to exercise the briefs branch.
        monkeypatch.setattr(
            "landline.runtime.commands.MORNING_BRIEF_GLOB",
            "briefs_morning/morning-*.md",
        )
        brief_dir = tmp_workspace / "briefs_morning"
        brief_dir.mkdir(parents=True, exist_ok=True)
        brief = brief_dir / "morning-2026-05-10.md"
        brief.write_text("test brief")
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert "morning-2026-05-10.md" in text


class TestCommandRouter:
    def _make_router(
        self,
        state=None,
        persist_fn=None,
        workspace=None,
        reset_claude_fn=None,
    ):
        state = state or {"session_id": None, "turn_count": 0}
        persist_fn = persist_fn or MagicMock()
        workspace = workspace or Path("/tmp/test-workspace")
        lm = LockManager(persist_fn)
        lm.restore_from_state({
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            "unlock_timestamp": 0.0,
        })
        return CommandRouter(
            state, lm, persist_fn, workspace,
            reset_claude_fn=reset_claude_fn,
        )

    def test_non_command_returns_none(self):
        router = self._make_router()
        assert router.handle("hello world") is None

    def test_unknown_command(self):
        router = self._make_router()
        result = router.handle("/foobar")
        assert "Unknown command" in result

    def test_new_resets_session(self):
        state = {"session_id": "abc", "turn_count": 5}
        persist = MagicMock()
        router = self._make_router(state=state, persist_fn=persist)
        result = router.handle("/new")
        assert "locked" in result.lower()
        assert state["session_id"] is None
        assert state["turn_count"] == 0

    def test_new_relocks(self):
        """/new must re-lock even if the session was unlocked at the time."""
        import time as _time
        persist = MagicMock()
        state = {
            "session_id": None,
            "turn_count": 0,
            "unlock_timestamp": _time.time(),
        }
        router = self._make_router(state=state, persist_fn=persist)
        # Manually unlock so we can verify reset() flips it back.
        router._lock_manager.restore_from_state(state)
        assert router._lock_manager.is_locked is False
        result = router.handle("/new")
        assert "locked" in result.lower()
        assert router._lock_manager.is_locked is True

    def test_new_preserves_lockout_counters(self):
        """SECURITY: /new must NOT clear lockout state — a locked-out user
        cannot reset their way out of the cooldown."""
        import time as _time
        persist = MagicMock()
        state = {"session_id": "abc", "turn_count": 2}
        router = self._make_router(state=state, persist_fn=persist)
        router._lock_manager._failed_unlock_attempts = 4
        router._lock_manager._unlock_lockout_until = _time.time() + 200
        router.handle("/new")
        assert router._lock_manager._failed_unlock_attempts == 4
        assert router._lock_manager._unlock_lockout_until > _time.time()

    def test_status_returns_text(self, no_subprocess, tmp_workspace):
        from landline.config import AGENT_NAME
        state = {"session_id": None, "turn_count": 0}
        router = self._make_router(state=state, workspace=tmp_workspace)
        result = router.handle("/status")
        assert result is not None
        assert AGENT_NAME in result

    def test_unlock_is_unknown_command(self):
        """/unlock is removed — passphrase is typed directly after /new."""
        router = self._make_router()
        result = router.handle("/unlock something")
        assert "Unknown command" in result

    def test_pause_not_handled_by_router(self):
        """/pause is intercepted in the orchestrator BEFORE reaching the router.
        If it ever does reach the router, it should fall through to the
        unknown-command branch — which is harmless, but documents that the
        router intentionally has no /pause handler."""
        router = self._make_router()
        result = router.handle("/pause")
        assert "Unknown command" in result

    def test_command_routing_is_case_insensitive(self, no_subprocess, tmp_workspace):
        """/STATUS, /Status, /status — all dispatch to the status handler."""
        from landline.config import AGENT_NAME
        state = {"session_id": None, "turn_count": 0}
        router = self._make_router(state=state, workspace=tmp_workspace)
        r1 = router.handle("/STATUS")
        r2 = router.handle("/Status")
        assert AGENT_NAME in r1
        assert AGENT_NAME in r2

    def test_new_clears_context_warned_at(self):
        state = {"session_id": "x", "turn_count": 3, "_context_warned_at": 50}
        router = self._make_router(state=state)
        router.handle("/new")
        assert "_context_warned_at" not in state

    def test_new_persists_state(self):
        persist = MagicMock()
        state = {"session_id": "x", "turn_count": 1}
        router = self._make_router(state=state, persist_fn=persist)
        router.handle("/new")
        assert persist.called

    def test_new_invokes_reset_claude_fn(self):
        """REGRESSION: /new must reset the live PersistentClaude subprocess,
        not just the persisted state dict. Without this, pc still owns the
        OLD session id (E1 source of truth) and the next dispatch --resumes
        the same conversation — defeating /new entirely.

        Verifies the injected callback is invoked exactly once after the
        existing state-dict + lock reset path runs.
        """
        reset_claude = MagicMock()
        persist = MagicMock()
        state = {"session_id": "abc", "turn_count": 5}
        router = self._make_router(
            state=state, persist_fn=persist, reset_claude_fn=reset_claude,
        )
        router.handle("/new")
        reset_claude.assert_called_once_with()
        # The existing state-dict reset must STILL fire (the regression
        # didn't break this; the new callback runs IN ADDITION to it).
        assert state["session_id"] is None
        assert state["turn_count"] == 0

    def test_new_reset_claude_fn_runs_after_persist(self):
        """The state reset must be persisted BEFORE the live proc is killed,
        so a crash mid-kill cannot leave behind a state file that still
        points at the old session. (LockManager.reset() ALSO persists, so
        persist may fire multiple times — the load-bearing invariant is
        that reset_claude is the LAST callback to run.)"""
        call_order = []
        persist = MagicMock(side_effect=lambda s: call_order.append("persist"))
        reset_claude = MagicMock(side_effect=lambda: call_order.append("reset"))
        state = {"session_id": "abc", "turn_count": 2}
        router = self._make_router(
            state=state, persist_fn=persist, reset_claude_fn=reset_claude,
        )
        router.handle("/new")
        # At least one persist call before the reset; reset is last.
        assert "persist" in call_order
        assert call_order[-1] == "reset"
        assert call_order.index("persist") < call_order.index("reset")

    def test_new_without_reset_claude_fn_is_back_compat(self):
        """Constructing CommandRouter without reset_claude_fn (the default)
        must still succeed and run /new cleanly — preserves the prior
        contract for tests that don't exercise the new path."""
        state = {"session_id": "abc", "turn_count": 5}
        router = self._make_router(state=state, reset_claude_fn=None)
        result = router.handle("/new")
        assert "locked" in result.lower()
        assert state["session_id"] is None

    def test_new_reset_claude_fn_exception_does_not_break_response(self):
        """A singleton hiccup inside reset_claude_fn must never block the operator
        from receiving the locked confirmation — the callback runs inside
        a try/except so /new still returns NEW_RESPONSE_TEXT.
        """
        reset_claude = MagicMock(side_effect=RuntimeError("singleton blew up"))
        state = {"session_id": "abc", "turn_count": 5}
        router = self._make_router(state=state, reset_claude_fn=reset_claude)
        result = router.handle("/new")
        assert "locked" in result.lower()
        reset_claude.assert_called_once_with()
        # State reset still happened (the persisted-state path runs BEFORE
        # the reset callback, so it's never affected by reset failure).
        assert state["session_id"] is None
        assert state["turn_count"] == 0


class TestStatusSubprocessTimeouts:
    """/status subprocess timeouts must be tightened to 3s each so the
    worst-case orchestrator stall is 6s, not 10s."""

    def _make_lock_manager(self):
        persist = MagicMock()
        lm = LockManager(persist)
        lm.restore_from_state({
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            "unlock_timestamp": 0.0,
        })
        return lm

    def test_status_launchctl_timeout_is_3s(self, no_subprocess, tmp_workspace):
        """The launchctl subprocess in /status must cap at 3s — head-of-line
        blocking on the single-threaded orchestrator loop."""
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        _status_text(state, lm, tmp_workspace)

        run_mock = no_subprocess["run"]
        launchctl_calls = [
            c for c in run_mock.call_args_list
            if c.args and c.args[0] and c.args[0][0] == "launchctl"
        ]
        assert launchctl_calls, "expected /status to invoke launchctl"
        # Builder must NOT loosen this back to 5s.
        assert launchctl_calls[0].kwargs.get("timeout") == 3

    def test_status_git_log_timeout_is_3s(self, no_subprocess, tmp_workspace):
        """The git log subprocess in /status must cap at 3s — same
        head-of-line concern as the launchctl call."""
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        _status_text(state, lm, tmp_workspace)

        run_mock = no_subprocess["run"]
        git_calls = [
            c for c in run_mock.call_args_list
            if c.args and c.args[0] and c.args[0][0] == "git"
        ]
        assert git_calls, "expected /status to invoke git log"
        assert git_calls[0].kwargs.get("timeout") == 3


class TestStatusUsageStatsLine:
    """/status appends today's usage/cost line when data exists, and
    degrades gracefully to no line + no crash when the stats file is
    missing (fresh install)."""

    def _make_lock_manager(self):
        persist = MagicMock()
        lm = LockManager(persist)
        lm.restore_from_state({
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            "unlock_timestamp": 0.0,
        })
        return lm

    def test_fresh_install_omits_today_line_and_does_not_raise(
        self, no_subprocess, tmp_workspace,
    ):
        # isolate_usage_stats_file fixture points USAGE_STATS_FILE at a
        # tmp path that does NOT exist yet — the fresh-install scenario.
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert "Today:" not in text
        # The Session: line always renders — proves /status succeeded.
        assert "Session:" in text

    def test_after_record_turn_status_shows_today_line_with_notional(
        self, no_subprocess, tmp_workspace,
    ):
        from landline.runtime import usage_stats
        usage_stats.record_turn(
            result_usage={"input_tokens": 1000, "output_tokens": 2000},
            result_model_usage=None,
            total_cost_usd=0.0123,
            duration_ms=100,
            dispatched=True,
        )
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        text = _status_text(state, lm, tmp_workspace)
        assert "Today: 1 turns" in text
        assert "1000 in" in text
        assert "2000 out" in text
        assert "notional" in text

    def test_broken_format_status_line_never_crashes_status(
        self, no_subprocess, tmp_workspace,
    ):
        """Any exception from format_status_line must be swallowed so
        /status never fails — the operator loses the number, not
        diagnostics."""
        lm = self._make_lock_manager()
        state = {"session_id": None, "turn_count": 0}
        with patch(
            "landline.runtime.usage_stats.format_status_line",
            side_effect=RuntimeError("boom"),
        ):
            # Must not raise.
            text = _status_text(state, lm, tmp_workspace)
        assert "Today:" not in text
        assert "Session:" in text


class TestDoctorCommand:
    """/doctor — detached spawn of the configured doctor script."""

    def _make_router(self, workspace=None, locked=False):
        import time as _time
        state = {"session_id": None, "turn_count": 0}
        persist_fn = MagicMock()
        lm = LockManager(persist_fn)
        lm.restore_from_state({
            "failed_unlock_attempts": 0,
            "unlock_lockout_until": 0.0,
            # A fresh (recent) unlock timestamp restores UNLOCKED; 0.0 → LOCKED.
            "unlock_timestamp": 0.0 if locked else _time.time(),
        })
        return CommandRouter(
            state, lm, persist_fn, workspace or Path("/tmp/test-workspace"),
        )

    def test_locked_session_refuses_doctor(self, monkeypatch, tmp_path):
        script = tmp_path / "doctor.sh"
        script.write_text("#!/bin/bash\n")
        monkeypatch.setattr(
            "landline.runtime.commands.DOCTOR_SCRIPT", str(script)
        )
        router = self._make_router(workspace=tmp_path, locked=True)
        with patch("landline.runtime.commands.subprocess.Popen") as popen:
            result = router.handle("/doctor anything")
        assert "unlock" in result.lower()
        popen.assert_not_called()

    def test_unconfigured_returns_setup_guidance(self, monkeypatch):
        monkeypatch.setattr("landline.runtime.commands.DOCTOR_SCRIPT", None)
        router = self._make_router()
        with patch("landline.runtime.commands.subprocess.Popen") as popen:
            result = router.handle("/doctor something is broken")
        assert "doctor_script" in result
        popen.assert_not_called()

    def test_missing_script_reports_not_found(self, monkeypatch, tmp_path):
        missing = tmp_path / "nope.sh"
        monkeypatch.setattr(
            "landline.runtime.commands.DOCTOR_SCRIPT", str(missing)
        )
        router = self._make_router()
        with patch("landline.runtime.commands.subprocess.Popen") as popen:
            result = router.handle("/doctor")
        assert "not found" in result
        popen.assert_not_called()

    def test_spawns_detached_with_issue_text_as_single_argv(
        self, monkeypatch, tmp_path
    ):
        script = tmp_path / "doctor.sh"
        script.write_text("#!/bin/bash\n")
        monkeypatch.setattr(
            "landline.runtime.commands.DOCTOR_SCRIPT", str(script)
        )
        router = self._make_router(workspace=tmp_path)
        with patch("landline.runtime.commands.subprocess.Popen") as popen:
            result = router.handle("/doctor morning brief came in empty")
        assert "dispatched" in result.lower()
        (argv,), kwargs = popen.call_args
        assert argv == [str(script), "morning brief came in empty"]
        assert kwargs["start_new_session"] is True
        assert kwargs["cwd"] == str(tmp_path)

    def test_no_issue_text_spawns_with_bare_argv(self, monkeypatch, tmp_path):
        script = tmp_path / "doctor.sh"
        script.write_text("#!/bin/bash\n")
        monkeypatch.setattr(
            "landline.runtime.commands.DOCTOR_SCRIPT", str(script)
        )
        router = self._make_router(workspace=tmp_path)
        with patch("landline.runtime.commands.subprocess.Popen") as popen:
            result = router.handle("/doctor")
        assert "dispatched" in result.lower()
        (argv,), _kwargs = popen.call_args
        assert argv == [str(script)]

    def test_spawn_failure_returns_error_not_raise(self, monkeypatch, tmp_path):
        script = tmp_path / "doctor.sh"
        script.write_text("#!/bin/bash\n")
        monkeypatch.setattr(
            "landline.runtime.commands.DOCTOR_SCRIPT", str(script)
        )
        router = self._make_router(workspace=tmp_path)
        with patch(
            "landline.runtime.commands.subprocess.Popen",
            side_effect=OSError("no fds"),
        ):
            result = router.handle("/doctor anything")
        assert "Failed to launch" in result
