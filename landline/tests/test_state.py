"""Tests for landline.runtime.state — state persistence, conversation logging, context %."""

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


class TestLoadState:
    def test_returns_defaults_when_no_file(self, tmp_workspace):
        missing = tmp_workspace / "cache" / "nofile.json"
        with patch("landline.runtime.state.STATE_FILE", missing), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] is None
        assert state["last_update_id"] == 0
        assert state["turn_count"] == 0
        assert state["failed_unlock_attempts"] == 0
        assert state["unlock_lockout_until"] == 0.0
        assert state["unlock_timestamp"] == 0.0
        # Missing file is the normal first-run path — no backup, no log noise.
        assert not missing.with_suffix(missing.suffix + ".corrupt").exists()
        mock_log.assert_not_called()

    def test_loads_existing_state(self, tmp_state_file):
        saved = {
            "session_id": "abc-123",
            "last_update_id": 42,
            "turn_count": 5,
            "failed_unlock_attempts": 2,
            "unlock_lockout_until": 100.0,
            "unlock_timestamp": 99.0,
        }
        tmp_state_file.write_text(json.dumps(saved))
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] == "abc-123"
        assert state["last_update_id"] == 42
        assert state["turn_count"] == 5

    def test_fills_missing_keys_with_defaults(self, tmp_state_file):
        tmp_state_file.write_text(json.dumps({"session_id": "x"}))
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] == "x"
        assert state["last_update_id"] == 0
        assert state["turn_count"] == 0

    def test_handles_corrupt_json(self, tmp_state_file):
        """Corrupt JSON: must return defaults, back up the original bytes
        to a ``.corrupt`` sibling, and log loudly. Silent reset would erase
        last_update_id / unlock_lockout_until / session_id — that's the bug
        this whole branch is here to prevent."""
        corrupt_bytes = "not json {{{"
        tmp_state_file.write_text(corrupt_bytes)
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] is None
        assert state["last_update_id"] == 0
        # Backup sibling exists with the original bytes preserved.
        backup = tmp_state_file.with_suffix(tmp_state_file.suffix + ".corrupt")
        assert backup.exists()
        assert backup.read_text() == corrupt_bytes
        # Original was moved out of the way — next load_state hits the
        # missing-file path instead of looping on the same corruption.
        assert not tmp_state_file.exists()
        # Something was logged loudly.
        assert mock_log.called
        logged = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "corrupt" in logged.lower()

    def test_handles_truncated_json(self, tmp_state_file):
        """Mid-write truncation (disk full, abrupt shutdown): same contract
        as ``test_handles_corrupt_json`` — back up + log + defaults."""
        truncated = '{"session_id": "abc-123", "last_update_id": 42'
        tmp_state_file.write_text(truncated)
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] is None
        assert state["last_update_id"] == 0
        backup = tmp_state_file.with_suffix(tmp_state_file.suffix + ".corrupt")
        assert backup.exists()
        assert backup.read_text() == truncated
        assert mock_log.called

    def test_overwrites_existing_corrupt_backup(self, tmp_state_file):
        """A second corruption shouldn't fail because ``.corrupt`` already
        exists — the latest corruption is what's worth keeping for debugging,
        and we don't want load_state to crash the daemon's startup."""
        backup = tmp_state_file.with_suffix(tmp_state_file.suffix + ".corrupt")
        backup.write_text("older corruption")
        latest = "newer corruption {{{"
        tmp_state_file.write_text(latest)
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.log"):
            from landline.runtime.state import load_state
            state = load_state()
        assert state["session_id"] is None
        assert backup.exists()
        assert backup.read_text() == latest


class TestSaveState:
    def test_writes_json_atomically(self, tmp_state_file):
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import save_state
            save_state({"session_id": "xyz", "last_update_id": 10})
        data = json.loads(tmp_state_file.read_text())
        assert data["session_id"] == "xyz"
        assert data["last_update_id"] == 10

    def test_creates_parent_dirs(self, tmp_workspace):
        deep_file = tmp_workspace / "deep" / "nested" / "state.json"
        with patch("landline.runtime.state.STATE_FILE", deep_file):
            from landline.runtime.state import save_state
            save_state({"session_id": None})
        assert deep_file.exists()

    def test_handles_write_error(self, tmp_workspace):
        """OSError on save_state must not crash the daemon; nothing should be written."""
        bad_path = tmp_workspace / "readonly" / "state.json"
        (tmp_workspace / "readonly").mkdir()
        # 0o555 (not 0o444): the execute bit must stay on so exists()/stat()
        # on children still work — only writes should fail.
        os.chmod(str(tmp_workspace / "readonly"), 0o555)
        try:
            with patch("landline.runtime.state.STATE_FILE", bad_path):
                from landline.runtime.state import save_state
                # The contract: save_state swallows OSError, no exception leaks.
                save_state({"key": "val"})
            assert not bad_path.exists()
            # C2 - tmp is preserved on disk-full / write-time OSError so an
            # operator can inspect it. This specific failure mode raises an
            # OSError on os.open() *before* tmp is ever created, so it never
            # appears on disk regardless. The forensic preservation contract
            # is covered by ``test_save_state_disk_full_preserves_tmp``.
            tmp_sibling = bad_path.with_suffix(bad_path.suffix + ".tmp")
            assert not tmp_sibling.exists()  # because os.open() failed pre-creation
        finally:
            os.chmod(str(tmp_workspace / "readonly"), 0o755)

    def test_replaces_existing_file_atomically(self, tmp_state_file):
        """save_state must overwrite existing state, not append or fail."""
        tmp_state_file.write_text('{"session_id": "old"}')
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import save_state
            save_state({"session_id": "new"})
        data = json.loads(tmp_state_file.read_text())
        assert data == {"session_id": "new"}

    def test_cleans_up_tmp_file(self, tmp_state_file):
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import save_state
            save_state({"session_id": "a"})
        tmp_file = tmp_state_file.with_suffix(tmp_state_file.suffix + ".tmp")
        assert not tmp_file.exists()


class TestLogConversation:
    def test_creates_log_file(self, tmp_workspace):
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "hello world")
        log_files = list((tmp_workspace / "memory" / "daily").glob("*_telegram.md"))
        assert len(log_files) == 1

    def test_appends_role_and_text(self, tmp_workspace):
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "first message")
            log_conversation("the agent", "response here")
        log_files = list((tmp_workspace / "memory" / "daily").glob("*_telegram.md"))
        content = log_files[0].read_text()
        assert "**the operator**" in content
        assert "first message" in content
        assert "**the agent**" in content
        assert "response here" in content

    def test_writes_header_on_empty_file(self, tmp_workspace):
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "test")
        log_files = list((tmp_workspace / "memory" / "daily").glob("*_telegram.md"))
        content = log_files[0].read_text()
        assert "# Telegram Conversation" in content


class TestReadRecentConversationHistory:
    def test_returns_empty_when_no_log(self, tmp_workspace):
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import read_recent_conversation_history
            result = read_recent_conversation_history()
        assert result == ""

    def test_returns_recent_lines(self, tmp_workspace):
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "# Telegram Conversation\n\n"
            "**the operator** (10:00): hello\n\n"
            "**the agent** (10:01): hi back\n\n"
        )
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import read_recent_conversation_history
            result = read_recent_conversation_history()
        assert "hello" in result
        assert "hi back" in result

    def test_truncates_with_preamble(self, tmp_workspace):
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        lines = ["# Header\n"]
        for i in range(100):
            lines.append(f"**the operator** (10:{i:02d}): msg {i}\n")
        log_path.write_text("\n".join(lines))
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import read_recent_conversation_history
            result = read_recent_conversation_history(max_turns=5)
        assert "omitted" in result
        assert "<system>" in result
        # max_turns=5 → keep last 10 lines, omit 100 - 10 = 90.
        assert "90 earlier messages omitted" in result
        # First retained message should be msg 90, last should be msg 99.
        assert "msg 90" in result
        assert "msg 99" in result
        # Truncated messages must not appear.
        assert "msg 0)" not in result
        assert "msg 50" not in result

    def test_no_truncation_preamble_when_under_limit(self, tmp_workspace):
        """If conversation fits in max_turns, no 'omitted' notice should appear."""
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            "# Header\n"
            "**the operator** (10:00): hi\n"
            "**the agent** (10:01): hello\n"
        )
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import read_recent_conversation_history
            result = read_recent_conversation_history(max_turns=20)
        assert "omitted" not in result
        assert "<system>" in result

    def test_returns_empty_when_log_has_no_conversation_lines(self, tmp_workspace):
        """A log file with only header/comment lines (no ``**Role**`` prefixes)
        should return empty, not a malformed preamble."""
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("# Telegram Conversation\n\nSome prose, no messages yet.\n")
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import read_recent_conversation_history
            result = read_recent_conversation_history()
        assert result == ""

    def test_handles_read_error(self, tmp_workspace):
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("content")
        os.chmod(str(log_path), 0o000)
        try:
            with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
                from landline.runtime.state import read_recent_conversation_history
                result = read_recent_conversation_history()
            assert result == ""
        finally:
            os.chmod(str(log_path), 0o644)


class TestGetContextPercent:
    def test_returns_none_for_no_session(self):
        from landline.runtime.state import get_context_percent
        assert get_context_percent(None) is None
        assert get_context_percent("") is None

    def test_returns_none_when_file_missing(self, tmp_workspace):
        with patch("landline.runtime.state.PROJECT_DIR", tmp_workspace / "nonexistent"):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("some-session-id")
        assert result is None

    def test_parses_usage_from_jsonl(self, tmp_workspace):
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        entry = {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 500000,
                    "cache_read_input_tokens": 100000,
                    "cache_creation_input_tokens": 50000,
                }
            }
        }
        session_file.write_text(json.dumps(entry) + "\n")
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        assert result is not None
        assert abs(result - 65.0) < 0.1

    def test_returns_none_for_no_usage_data(self, tmp_workspace):
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        entry = {"type": "user", "message": {"content": "hello"}}
        session_file.write_text(json.dumps(entry) + "\n")
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        assert result is None

    def test_uses_last_assistant_message(self, tmp_workspace):
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        lines = []
        for tokens in [100000, 200000, 300000]:
            entry = {
                "type": "assistant",
                "message": {
                    "usage": {
                        "input_tokens": tokens,
                        "cache_read_input_tokens": 0,
                        "cache_creation_input_tokens": 0,
                    }
                }
            }
            lines.append(json.dumps(entry))
        session_file.write_text("\n".join(lines) + "\n")
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        assert result is not None
        assert abs(result - 30.0) < 0.1

    def test_ignores_corrupt_jsonl_lines(self, tmp_workspace):
        """A malformed JSON line in the tail must not break the entire read.

        The function uses try/except per line — verify that a good usage
        entry mixed with garbage still returns a percentage.
        """
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        good = {
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 250000,
                    "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0,
                }
            }
        }
        session_file.write_text(
            "not json {{{\n"
            + json.dumps(good) + "\n"
            + "more garbage\n"
        )
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        assert result is not None
        assert abs(result - 25.0) < 0.1

    def test_reads_only_tail_of_large_file(self, tmp_workspace):
        """For a file > SESSION_JSONL_TAIL_BYTES, get_context_percent should
        see the LAST usage entry within the tail, not the first one in the
        file."""
        from landline.config import SESSION_JSONL_TAIL_BYTES as TAIL_BYTES
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        early = {
            "type": "assistant",
            "message": {"usage": {
                "input_tokens": 900000,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            }},
        }
        late = {
            "type": "assistant",
            "message": {"usage": {
                "input_tokens": 100000,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            }},
        }
        # Stuff the file so `early` is well outside the tail window.
        padding = "x" * (TAIL_BYTES * 2)
        session_file.write_text(
            json.dumps(early) + "\n"
            + padding + "\n"
            + json.dumps(late) + "\n"
        )
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        # Should reflect the late entry (10%), not the early one (90%).
        assert result is not None
        assert abs(result - 10.0) < 0.5

    def test_skips_user_entries(self, tmp_workspace):
        """get_context_percent must only count assistant messages (which carry
        usage data) — user entries should be ignored even if they also have a
        ``usage`` key in their message body."""
        project_dir = tmp_workspace / "project"
        project_dir.mkdir()
        session_file = project_dir / "test-session.jsonl"
        user_entry = {
            "type": "user",
            "message": {"usage": {
                "input_tokens": 999999,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            }},
        }
        assistant_entry = {
            "type": "assistant",
            "message": {"usage": {
                "input_tokens": 200000,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
            }},
        }
        session_file.write_text(
            json.dumps(user_entry) + "\n"
            + json.dumps(assistant_entry) + "\n"
        )
        with patch("landline.runtime.state.PROJECT_DIR", project_dir):
            from landline.runtime.state import get_context_percent
            result = get_context_percent("test-session")
        assert result is not None
        assert abs(result - 20.0) < 0.1


class TestDailyLogPermissions:
    """B4 - daily-log + state-file PII permissions."""

    def test_log_conversation_creates_file_at_0o600(self, tmp_workspace):
        """A fresh daily log must land at 0o600. Reverting os.open + fchmod
        to plain open(..., 'a') falls back to umask-default 0o644 - fails."""
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "hello")
        log_files = list((tmp_workspace / "memory" / "daily").glob("*_telegram.md"))
        assert len(log_files) == 1
        mode = os.stat(str(log_files[0])).st_mode & 0o777
        assert mode == 0o600, f"daily log mode is {oct(mode)}, expected 0o600"

    def test_log_conversation_creates_dir_at_0o700(self, tmp_workspace):
        """The memory/daily dir must be 0o700. Reverting the explicit dir-chmod
        leaves 0o755 from mkdir default - fails."""
        # Pre-create dir at a loose mode to verify the chmod tightens it.
        daily_dir = tmp_workspace / "memory" / "daily"
        os.chmod(str(daily_dir), 0o755)
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "hello")
        mode = os.stat(str(daily_dir)).st_mode & 0o777
        assert mode == 0o700, f"daily dir mode is {oct(mode)}, expected 0o700"

    def test_log_conversation_tightens_existing_loose_file(self, tmp_workspace):
        """An EXISTING loose 0o644 daily log must be tightened to 0o600 when
        log_conversation appends to it (the fchmod-on-open path). Reverting
        leaves the existing file at its prior mode - fails."""
        from datetime import datetime
        from landline.config import TIMEZONE

        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        log_path = tmp_workspace / "memory" / "daily" / f"{today}_telegram.md"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("# pre-existing content\n")
        os.chmod(str(log_path), 0o644)
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import log_conversation
            log_conversation("the operator", "appended turn")
        mode = os.stat(str(log_path)).st_mode & 0o777
        assert mode == 0o600, (
            f"loose existing log was not tightened: mode is {oct(mode)}"
        )

    def test_secure_daily_logs_backfills_existing(self, tmp_workspace):
        """Pre-existing 0644 files + 0755 dir must be tightened by the backfill.
        Reverting the function body or its wire-up - backfill never runs - fails."""
        daily_dir = tmp_workspace / "memory" / "daily"
        os.chmod(str(daily_dir), 0o755)
        legacy_file = daily_dir / "2024-01-01_telegram.md"
        legacy_file.write_text("legacy contents")
        os.chmod(str(legacy_file), 0o644)
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import secure_daily_logs
            secure_daily_logs()
        assert os.stat(str(legacy_file)).st_mode & 0o777 == 0o600
        assert os.stat(str(daily_dir)).st_mode & 0o777 == 0o700

    def test_secure_daily_logs_skips_non_telegram_files(self, tmp_workspace):
        """The daemon must NOT chmod other tools' files in memory/daily/.
        Only *_telegram.md is in scope."""
        daily_dir = tmp_workspace / "memory" / "daily"
        os.chmod(str(daily_dir), 0o755)
        tg_file = daily_dir / "2024-01-01_telegram.md"
        tg_file.write_text("tg")
        os.chmod(str(tg_file), 0o644)
        journal = daily_dir / "2024-01-01.md"
        journal.write_text("journal entry")
        os.chmod(str(journal), 0o644)
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import secure_daily_logs
            secure_daily_logs()
        assert os.stat(str(tg_file)).st_mode & 0o777 == 0o600
        # Non-telegram file is untouched (only the dir chmod tightens its
        # ambient exposure).
        assert os.stat(str(journal)).st_mode & 0o777 == 0o644

    def test_secure_daily_logs_is_idempotent(self, tmp_workspace):
        """Second call must not raise and must leave modes unchanged."""
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import secure_daily_logs
            secure_daily_logs()
            secure_daily_logs()
        daily_dir = tmp_workspace / "memory" / "daily"
        assert os.stat(str(daily_dir)).st_mode & 0o777 == 0o700

    def test_secure_daily_logs_no_dir(self, tmp_path):
        """No memory/daily dir - return cleanly. Protects fresh-machine path."""
        # tmp_path has no memory/daily under it.
        with patch("landline.runtime.state.WORKSPACE", tmp_path):
            from landline.runtime.state import secure_daily_logs
            secure_daily_logs()  # must not raise

    def test_secure_daily_logs_backcompat_wrapper_still_chmods_daily(
        self, tmp_workspace
    ):
        """Cluster 1 back-compat: ``secure_daily_logs`` is a wrapper for
        ``secure_workspace_paths``; the daily-log tightening it used to do
        directly must still happen through the wrapper."""
        daily_dir = tmp_workspace / "memory" / "daily"
        os.chmod(str(daily_dir), 0o755)
        legacy_file = daily_dir / "2024-01-01_telegram.md"
        legacy_file.write_text("legacy contents")
        os.chmod(str(legacy_file), 0o644)
        with patch("landline.runtime.state.WORKSPACE", tmp_workspace):
            from landline.runtime.state import secure_daily_logs
            secure_daily_logs()
        assert os.stat(str(legacy_file)).st_mode & 0o777 == 0o600
        assert os.stat(str(daily_dir)).st_mode & 0o777 == 0o700

    def test_save_state_writes_at_0o600(self, tmp_state_file):
        """State file must land at 0o600 (defense-in-depth even inside cache/)."""
        with patch("landline.runtime.state.STATE_FILE", tmp_state_file):
            from landline.runtime.state import save_state
            save_state({"session_id": "abc"})
        mode = os.stat(str(tmp_state_file)).st_mode & 0o777
        assert mode == 0o600, f"state file mode is {oct(mode)}, expected 0o600"

    def test_state_module_does_not_use_os_umask(self):
        """Sentinel: parse daemon/state.py source as text and assert
        ``os.umask`` does not appear. Reverting to a umask-based design - fails.
        Catches the most dangerous regression class even if a future edit
        forgets the fchmod."""
        import landline.runtime.state as state_mod
        source = Path(state_mod.__file__).read_text()
        # Strip comments before checking (allow umask in commentary only).
        code_only = "\n".join(
            ln for ln in source.splitlines() if "#" not in ln or ln.split("#", 1)[0].strip()
        )
        # Stricter: ensure no os.umask( call exists as an executable statement.
        assert "os.umask(" not in source, (
            "landline.runtime.state must NOT call os.umask - the daemon is multi-threaded "
            "and os.umask is process-wide (races concurrent file creation in "
            "poller/sender threads). Use os.open + fchmod instead."
        )


class TestSaveStateDurability:
    """C1 - parent-dir fsync after os.replace."""

    def test_save_state_fsyncs_parent_dir(self, tmp_state_file):
        """After os.replace, save_state must fsync the parent directory fd so
        the rename is durable across hard crash / power loss. Without this,
        the dirent can revert to pre-rename state on remount even though the
        tmp file's bytes are on disk."""
        fsynced_modes = []
        real_fsync = os.fsync

        def recording_fsync(fd):
            try:
                mode = os.fstat(fd).st_mode
            except OSError:
                mode = 0
            fsynced_modes.append(mode)
            return real_fsync(fd)

        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.os.fsync", side_effect=recording_fsync):
            from landline.runtime.state import save_state
            save_state({"session_id": "durable"})

        dir_fsyncs = [m for m in fsynced_modes if stat.S_ISDIR(m)]
        assert dir_fsyncs, (
            "save_state must fsync the parent directory after os.replace "
            "(POSIX durability: directory-metadata must reach disk for the "
            f"rename to survive a crash). Saw fsync modes: {[oct(m) for m in fsynced_modes]}"
        )

    def test_save_state_dir_fsync_failure_is_swallowed(self, tmp_state_file):
        """Some filesystems (BSDs, some FUSE) don't support fsync on a
        directory fd and return OSError. The daemon must keep running - the
        rename already happened before the dir fsync, so the state IS on
        disk; the dir fsync is a durability nice-to-have only."""
        real_fsync = os.fsync

        def selective_fsync(fd):
            try:
                mode = os.fstat(fd).st_mode
            except OSError:
                mode = 0
            if stat.S_ISDIR(mode):
                raise OSError("ENOTSUP: directory fsync not supported")
            return real_fsync(fd)

        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.os.fsync", side_effect=selective_fsync), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import save_state
            # Must not raise.
            save_state({"session_id": "ok-on-bsd"})

        assert tmp_state_file.exists()
        assert json.loads(tmp_state_file.read_text())["session_id"] == "ok-on-bsd"
        # We logged the skip (metadata only, no PII).
        log_messages = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "parent-dir fsync skipped" in log_messages


class TestSaveStateForensics:
    """C2 - tmp file preserved on disk-full / write-time OSError."""

    def test_save_state_disk_full_preserves_tmp(self, tmp_state_file):
        """If fsync raises OSError (disk full, ENOSPC, EIO), the tmp file must
        be LEFT IN PLACE for forensic inspection, and the log line must
        mention the tmp path so an operator can find it from logs alone."""
        tmp_sibling = tmp_state_file.with_suffix(tmp_state_file.suffix + ".tmp")
        real_fsync = os.fsync

        def selective_fsync(fd):
            try:
                mode = os.fstat(fd).st_mode
            except OSError:
                mode = 0
            # Fail only on the regular file fsync (the tmp). Dir fsync, if
            # it ever runs in the failure path, is not the injection point.
            if stat.S_ISREG(mode):
                raise OSError(28, "No space left on device")
            return real_fsync(fd)

        with patch("landline.runtime.state.STATE_FILE", tmp_state_file), \
             patch("landline.runtime.state.os.fsync", side_effect=selective_fsync), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import save_state
            save_state({"session_id": "forensic-evidence", "last_update_id": 7})

        # (a) tmp still exists for forensics
        assert tmp_sibling.exists(), "tmp file must be preserved for forensics"
        # (b) it contains the bytes we tried to persist (post-write, pre-fsync)
        assert "forensic-evidence" in tmp_sibling.read_text()
        # (c) log mentions both the error and the tmp path
        assert mock_log.called
        msg = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "OSError" in msg
        assert str(tmp_sibling) in msg
        # (d) durable file was not touched
        assert not tmp_state_file.exists()


class TestTailBytesConstantsConsumed:
    """C3 - landline.runtime.state imports the two windows from landline.config, no locals."""

    def test_state_uses_config_tail_bytes_constants_no_local_redefinition(self):
        """landline.runtime.state must not redefine the tail-bytes constants locally.
        The two windows live in landline.config; landline.runtime.state imports them. A
        local redefinition (HISTORY_TAIL_BYTES = ..., TAIL_BYTES = ...) would
        silently re-introduce the drift this item exists to prevent."""
        from landline.runtime import state
        from landline import config
        # The old local names must be gone.
        assert not hasattr(state, "HISTORY_TAIL_BYTES"), (
            "landline.runtime.state.HISTORY_TAIL_BYTES re-appeared - must come from "
            "landline.config.CONVERSATION_LOG_TAIL_BYTES instead."
        )
        assert not hasattr(state, "TAIL_BYTES"), (
            "landline.runtime.state.TAIL_BYTES re-appeared - must come from "
            "landline.config.SESSION_JSONL_TAIL_BYTES instead."
        )
        # state imports the config names (binding check - they're in state's
        # module namespace via the ``from landline.config import ...`` statement).
        assert state.CONVERSATION_LOG_TAIL_BYTES is config.CONVERSATION_LOG_TAIL_BYTES
        assert state.SESSION_JSONL_TAIL_BYTES is config.SESSION_JSONL_TAIL_BYTES


class TestProjectDirDerivation:
    """C4 - derive PROJECT_DIR from WORKSPACE, env override, no host coupling."""

    def test_encode_cc_project_dir_matches_claude_code_rule(self):
        """Direct call to the helper - the encoding rule is /` and `.` both
        replaced with `-`. Reverted code has no helper - fails at import."""
        from landline.runtime.state import encode_cc_project_dir
        assert encode_cc_project_dir(Path("/Users/testuser/workspace")) == "-Users-testuser-workspace"
        assert encode_cc_project_dir(Path("/Users/alice/.agent-ws")) == "-Users-alice--agent-ws"
        assert encode_cc_project_dir(Path("/Users/x/proj.name")) == "-Users-x-proj-name"
        assert encode_cc_project_dir(Path("/srv/work")) == "-srv-work"

    def test_project_dir_matches_helper_for_workspace(self):
        """PROJECT_DIR derives from WORKSPACE via the helper. On the operator's host
        without the env override, this matches Path.home() / ".claude" /
        "projects" / encode_cc_project_dir(WORKSPACE)."""
        if os.environ.get("LANDLINE_CC_PROJECT_DIR"):
            pytest.skip("env override set in test environment")
        from landline.runtime.state import PROJECT_DIR, encode_cc_project_dir
        from landline.config import WORKSPACE
        expected = Path.home() / ".claude" / "projects" / encode_cc_project_dir(WORKSPACE)
        assert PROJECT_DIR == expected

    def test_project_dir_honors_env_override(self, tmp_path):
        """Spawn a fresh interpreter with LANDLINE_CC_PROJECT_DIR set; subprocess
        prints the override. Hardcoded path ignores the env var - fails."""
        # Repo root is two levels up from this test file
        # (landline/tests/test_state.py → landline/ → repo root). Point the
        # subprocess's PYTHONPATH at it so ``import landline.state`` works
        # regardless of how ``pytest`` was invoked.
        repo_root = Path(__file__).resolve().parent.parent.parent

        override = str(tmp_path / "cc-fake-c4")
        env = dict(os.environ)
        env["LANDLINE_CC_PROJECT_DIR"] = override
        env["PYTHONPATH"] = (
            str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")
        )
        probe = (
            "from landline.runtime.state import PROJECT_DIR; "
            "import sys; sys.stdout.write(str(PROJECT_DIR))"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == override


class TestSecureWorkspacePaths:
    """Cluster 1 — top-level workspace-sensitive dirs get 0o700 at startup."""

    def _mk_workspace(self, root, dirs):
        for d in dirs:
            (root / d).mkdir(parents=True, exist_ok=True)
            os.chmod(str(root / d), 0o755)

    def test_secure_workspace_paths_chmods_each_declared_dir(self, tmp_path):
        """Every dir in WORKSPACE_SENSITIVE_DIRS present under WORKSPACE
        must end at 0o700. Reverting the WORKSPACE_SENSITIVE_DIRS loop
        leaves them at 0o755 and this fails."""
        from landline.config import WORKSPACE_SENSITIVE_DIRS

        self._mk_workspace(tmp_path, WORKSPACE_SENSITIVE_DIRS)
        # memory/daily must exist for the daily-log tightening branch.
        (tmp_path / "memory" / "daily").mkdir(parents=True, exist_ok=True)

        with patch("landline.runtime.state.WORKSPACE", tmp_path):
            from landline.runtime.state import secure_workspace_paths
            secure_workspace_paths()

        for d in WORKSPACE_SENSITIVE_DIRS:
            mode = os.stat(str(tmp_path / d)).st_mode & 0o777
            assert mode == 0o700, (
                "workspace-sensitive dir %s ended at %s, expected 0o700"
                % (d, oct(mode))
            )

    def test_secure_workspace_paths_skips_missing_dir_without_raise(
        self, tmp_path
    ):
        """A missing dir (fresh checkout without cache/ yet) must be a
        no-op — the daemon must never fail to start because one dir
        happens to be absent."""
        # Only memory/ + memory/daily/ exist; cache/inbox/outbox/logs absent.
        (tmp_path / "memory" / "daily").mkdir(parents=True)
        os.chmod(str(tmp_path / "memory"), 0o755)

        with patch("landline.runtime.state.WORKSPACE", tmp_path), \
             patch("landline.runtime.state.log"):  # silence the "skipping" info line
            from landline.runtime.state import secure_workspace_paths
            secure_workspace_paths()  # must not raise

        # The dir that DID exist was tightened.
        mode = os.stat(str(tmp_path / "memory")).st_mode & 0o777
        assert mode == 0o700

    def test_secure_workspace_paths_swallows_chmod_oserror(self, tmp_path):
        """A single stuck NFS mount / permission error must be logged and
        skipped, not fatal. Emulate by patching os.chmod to raise on one
        specific path."""
        from landline.config import WORKSPACE_SENSITIVE_DIRS

        self._mk_workspace(tmp_path, WORKSPACE_SENSITIVE_DIRS)
        (tmp_path / "memory" / "daily").mkdir(parents=True, exist_ok=True)

        real_chmod = os.chmod
        stuck = str(tmp_path / "cache")

        def selective_chmod(path, mode):
            if path == stuck:
                raise OSError("EPERM: mock NFS mount is read-only")
            return real_chmod(path, mode)

        with patch("landline.runtime.state.WORKSPACE", tmp_path), \
             patch("landline.runtime.state.os.chmod", side_effect=selective_chmod), \
             patch("landline.runtime.state.log") as mock_log:
            from landline.runtime.state import secure_workspace_paths
            secure_workspace_paths()  # must not raise

        # The stuck path was logged (failure surfaced), and the others
        # were still chmodded.
        logged = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "cache" in logged
        # memory/ (which came first) succeeded.
        assert os.stat(str(tmp_path / "memory")).st_mode & 0o777 == 0o700
