"""Tests for landline.logging — rotating file logger + test seam."""

import re
from unittest.mock import patch, MagicMock

from landline.logging import log


class TestLog:
    def test_log_does_not_print_to_stdout(self, capsys):
        """log() must not write to stdout — the rotating handler is canonical."""
        log("test message")
        captured = capsys.readouterr()
        assert captured.out == "", (
            "A2 regression: log() printed to stdout. "
            "The print() call must be gone."
        )

    def test_log_format_full(self):
        """Format must be: [YYYY-MM-DD HH:MM:SS] message — anchor for log parsers."""
        with patch("landline.logging._get_logger") as mock_get:
            mock_logger = MagicMock()
            mock_get.return_value = mock_logger
            log("hello")
        forwarded = mock_logger.info.call_args[0][0]
        match = re.match(
            r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] hello$", forwarded
        )
        assert match is not None, f"Bad log format: {forwarded!r}"

    def test_log_handles_empty_string(self):
        with patch("landline.logging._get_logger") as mock_get:
            mock_logger = MagicMock()
            mock_get.return_value = mock_logger
            log("")
        forwarded = mock_logger.info.call_args[0][0]
        # Empty message still gets a valid timestamp prefix.
        assert re.match(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] $", forwarded)

    def test_log_handles_special_characters(self):
        with patch("landline.logging._get_logger") as mock_get:
            mock_logger = MagicMock()
            mock_get.return_value = mock_logger
            log("line1\nline2\ttab")
        forwarded = mock_logger.info.call_args[0][0]
        assert "line1\nline2\ttab" in forwarded

    def test_log_does_not_write_anything_to_stdout_with_special_chars(self, capsys):
        """No print path remains for any payload — special chars included."""
        log("line1\nline2\ttab")
        assert capsys.readouterr().out == ""

    def test_log_survives_logger_error_writes_to_stderr(self, capsys):
        """If logger.info raises, log() must fall back to stderr — not print, not silent."""
        with patch("landline.logging._get_logger") as mock_get:
            mock_logger = MagicMock()
            mock_logger.info.side_effect = Exception("disk full")
            mock_get.return_value = mock_logger
            log("should still surface")
        captured = capsys.readouterr()
        assert "should still surface" in captured.err, (
            "A2 regression: per-call logger failure should fall back to stderr, "
            "not stdout and not be silent."
        )
        assert captured.out == ""

    def test_log_forwards_to_file_logger(self):
        """Each log() must call logger.info() once with the formatted line."""
        with patch("landline.logging._get_logger") as mock_get:
            mock_logger = MagicMock()
            mock_get.return_value = mock_logger
            log("forwarded message")
        assert mock_logger.info.call_count == 1
        forwarded = mock_logger.info.call_args[0][0]
        assert "forwarded message" in forwarded
        assert re.match(
            r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] forwarded message$",
            forwarded,
        )

    def test_get_logger_handler_failure_writes_to_stderr(self, capsys):
        """If RotatingFileHandler construction fails, the failure must surface on stderr."""
        from landline import logging as _dlog
        _dlog._reset_logger_for_tests()
        with patch(
            "landline.logging.logging.handlers.RotatingFileHandler",
            side_effect=PermissionError("denied"),
        ):
            log("anything")
        captured = capsys.readouterr()
        assert "failed to create RotatingFileHandler" in captured.err, (
            "A2 regression: silent except in _get_logger swallowed handler "
            "init failure; must write to stderr."
        )

    def test_get_logger_handler_failure_does_not_raise(self):
        """Handler init failure must not propagate — the daemon must keep running."""
        from landline import logging as _dlog
        _dlog._reset_logger_for_tests()
        with patch(
            "landline.logging.logging.handlers.RotatingFileHandler",
            side_effect=PermissionError("denied"),
        ):
            # Must not raise.
            log("anything")


class TestLogTestSeam:
    def test_log_does_not_touch_real_LOG_FILE(self):
        """Calling log() under the autouse seam must never create OR grow
        the real production daemon.log.

        Two distinct regression modes are covered:
          (1) create-from-absent: a regression that writes an empty file at
              LOG_FILE when it didn't exist before.
          (2) grow-existing: a regression that appends to an already-existing
              LOG_FILE.
        Both must fail this test with a message that names the mode.
        """
        from landline import config as _cfg
        real_log = _cfg.LOG_FILE
        existed_before = real_log.exists()
        size_before = real_log.stat().st_size if existed_before else None
        log("seam smoke test")
        existed_after = real_log.exists()
        size_after = real_log.stat().st_size if existed_after else None
        # Mode (1): file did not exist; regression created it (possibly empty).
        assert existed_after == existed_before, (
            "A1 seam regressed (create-from-absent): real daemon.log at "
            "{0} did not exist before the test but exists after. "
            "Check daemon/logging.py env override and conftest autouse fixture.".format(real_log)
        )
        # Mode (2): file existed; regression appended to it.
        if existed_before:
            assert size_after == size_before, (
                "A1 seam regressed (grow-existing): real daemon.log at "
                "{0} grew from {1} to {2} bytes during a test. "
                "Check env override + conftest fixture.".format(real_log, size_before, size_after)
            )

    def test_LOG_FILE_is_lazy_at_import(self):
        """Importing landline.logging must not build the singleton — the seam
        only works if handler construction is deferred until first log().

        Uses importlib.reload() to simulate a fresh import without disturbing
        the autouse fixture's state for other tests, then asserts _LOGGER is
        None BEFORE any reset call (so the assertion is genuinely
        revert-sensitive — calling _reset_logger_for_tests() first would set
        _LOGGER=None and mask a future eager-build regression).
        """
        import importlib
        from landline import logging as _dlog
        # Reload to re-run module top-level code as if freshly imported.
        # This is the load-bearing step: if a regression moves handler
        # construction to module top-level, _LOGGER will be non-None here.
        importlib.reload(_dlog)
        try:
            assert _dlog._LOGGER is None, (
                "A1 lazy-at-import invariant violated: landline.logging built "
                "the logger singleton at import time. Handler construction "
                "must remain inside _get_logger() (called on first log())."
            )
        finally:
            # Restore clean state for the autouse fixture's post-yield reset.
            _dlog._reset_logger_for_tests()

    def test_env_override_redirects_handler(self, tmp_path, monkeypatch):
        """log() under LANDLINE_DAEMON_LOG=<tmp> must write into <tmp>, not LOG_FILE."""
        from landline import logging as _dlog
        target = tmp_path / "redirected.log"
        monkeypatch.setenv("LANDLINE_DAEMON_LOG", str(target))
        _dlog._reset_logger_for_tests()
        _dlog.log("redirected line")
        # Force flush by closing the singleton's handlers
        for h in _dlog._LOGGER.handlers:
            h.flush()
        assert target.exists()
        assert "redirected line" in target.read_text(encoding="utf-8")
