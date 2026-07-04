"""Tests for landline.voice_transcribe — Cluster 2 (whisper wrapper).

Covers the subprocess contract, timeout non-raising, cleanup, and the
privacy invariant that transcript text NEVER reaches the daemon log.
"""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from landline.config import (
    VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS,
    WHISPER_BIN,
)
from landline.voice_transcribe import TranscribeResult, transcribe_file


def _make_completed(returncode=0, stdout="", stderr=""):
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


def _prime_output_txt(text: str):
    """Return a side_effect for subprocess.run that writes ``<stem>.txt``
    into the ``--output_dir`` passed to whisper, mirroring the CLI's own
    behavior. Returns a rc=0 CompletedProcess."""
    def _side_effect(cmd, *a, **kw):
        # Find --output_dir <path>
        try:
            idx = cmd.index("--output_dir")
            output_dir = cmd[idx + 1]
        except (ValueError, IndexError):
            return _make_completed(returncode=0)
        # Write a canned .txt file inside output_dir
        try:
            with open(
                os.path.join(output_dir, "audio.txt"), "w", encoding="utf-8"
            ) as f:
                f.write(text)
        except Exception:
            pass
        return _make_completed(returncode=0)
    return _side_effect


class TestTranscribeFileSuccess:
    def test_returns_ok_with_transcript(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"fake-audio")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt("hello world"),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/whisper",
                language="en", timeout_seconds=90,
            )
        assert result.ok is True
        assert result.text == "hello world"
        assert result.error is None

    def test_cli_shape_uses_configured_flags(self, tmp_path, monkeypatch):
        """Assert the exact CLI shape whisper is invoked with — flags
        drift silently otherwise."""
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"fake")
        captured = {"cmd": None}

        def _capture(cmd, *a, **kw):
            captured["cmd"] = list(cmd)
            # Write empty transcript so we hit the ok-but-empty path;
            # we're only inspecting the CLI here.
            idx = cmd.index("--output_dir")
            output_dir = cmd[idx + 1]
            with open(
                os.path.join(output_dir, "audio.txt"), "w", encoding="utf-8"
            ) as f:
                f.write("nonempty")
            return _make_completed(returncode=0)

        with patch(
            "landline.voice_transcribe.subprocess.run", side_effect=_capture,
        ):
            transcribe_file(
                audio, model="base", model_dir="/tmp/models",
                language="en", timeout_seconds=90,
            )
        cmd = captured["cmd"]
        assert cmd is not None
        assert cmd[0] == WHISPER_BIN
        # Required flag pairs must all be present with the exact values.
        for flag, expected in [
            ("--model", "base"),
            ("--model_dir", "/tmp/models"),
            ("--language", "en"),
            ("--task", "transcribe"),
            # fp16 MUST be title-cased 'False' — 'false' is silently
            # ignored by whisper's ast.literal_eval parsing.
            ("--fp16", "False"),
            ("--output_format", "txt"),
            ("--verbose", "False"),
        ]:
            i = cmd.index(flag)
            assert cmd[i + 1] == expected, (
                "flag %s expected %r, got %r" % (flag, expected, cmd[i + 1])
            )

    def test_transcript_truncated_when_over_cap(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        huge = "a" * (VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS + 500)
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt(huge),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is True
        assert len(result.text) == VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS


class TestTranscribeFileFailure:
    def test_timeout_returns_ok_false(self, tmp_path, monkeypatch):
        """Load-bearing don't-wedge-dispatch guarantee: on
        subprocess.TimeoutExpired the wrapper MUST return TranscribeResult
        rather than raising."""
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="whisper", timeout=90),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is False
        assert result.error == "timeout"
        assert result.text == ""

    def test_nonzero_exit_returns_ok_false_with_exit_error(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            return_value=_make_completed(
                returncode=1, stderr="ffmpeg missing",
            ),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is False
        assert result.error is not None
        assert result.error.startswith("exit 1")

    def test_missing_whisper_binary_returns_ok_false(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=FileNotFoundError("no such file"),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is False
        assert result.error == "whisper_missing"

    def test_empty_transcript_treated_as_failure(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt(""),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is False
        assert result.error == "empty_transcript"


class TestPrivacyDiscipline:
    """Transcripts are the operator's voice content. The module MUST NOT log the
    transcript text on any code path."""

    def test_transcript_never_appears_in_log(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        secret = "PRIVATE_TRANSCRIPT_speaker_says_this_particular_thing"
        log_messages = []
        with patch(
            "landline.voice_transcribe.log",
            side_effect=lambda m: log_messages.append(m),
        ), patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt(secret),
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert result.ok is True
        for msg in log_messages:
            assert secret not in msg, (
                "PRIVACY: transcript leaked into log line: %r" % msg
            )


class TestCleanup:
    """Whichever branch the wrapper exits from, the whisper temp dir is
    rmtree'd. Prevents cache/telegram_voice/ from accreting whisper_*
    scratch dirs."""

    def _count_whisper_tmpdirs(self, voice_dir: Path) -> int:
        return sum(
            1 for p in voice_dir.iterdir()
            if p.is_dir() and p.name.startswith("whisper_")
        )

    def test_success_cleans_tmpdir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt("clean me up"),
        ):
            transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert self._count_whisper_tmpdirs(tmp_path) == 0

    def test_timeout_cleans_tmpdir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="whisper", timeout=90),
        ):
            transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert self._count_whisper_tmpdirs(tmp_path) == 0

    def test_nonzero_exit_cleans_tmpdir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.run",
            return_value=_make_completed(returncode=2, stderr="boom"),
        ):
            transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
            )
        assert self._count_whisper_tmpdirs(tmp_path) == 0


class TestPauseInterrupt:
    """Regression pin for the "whisper starves dispatch loop" finding:
    when the caller passes a ``pause_flag`` and it becomes set during
    whisper, the subprocess MUST be killed within one polling interval
    and the wrapper MUST return ``error="paused"`` (NOT ``ok=True``,
    NOT ``error="timeout"``). Without this, a 30-90s whisper run
    starves every subsequent batch (text, photos, /pause itself) on
    the single-threaded dispatch loop.
    """

    class _FakeProc:
        """Popen stand-in that stays 'alive' until ``kill()`` is
        called, then reports rc=-9 on communicate. Simulates whisper
        being SIGKILL'd mid-transcribe."""

        def __init__(self):
            self.returncode = None  # None = still running
            self._killed = False
            self.stdout = None
            self.stderr = None

        def communicate(self, timeout=None):
            if self._killed:
                # Post-kill drain: pipes closed, no stderr, rc=-9.
                self.returncode = -9
                return ("", "")
            # Still 'running' → timeout on every poll until kill fires.
            import subprocess
            raise subprocess.TimeoutExpired(cmd="whisper", timeout=timeout)

        def kill(self):
            self._killed = True

    def test_pause_flag_set_during_whisper_kills_and_returns_paused(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")

        fake_proc = self._FakeProc()

        class _PauseFlag:
            """Fires True on the 3rd is_set() call, simulating /pause
            arriving mid-whisper."""

            def __init__(self):
                self.calls = 0

            def is_set(self):
                self.calls += 1
                return self.calls >= 3

        pause_flag = _PauseFlag()

        with patch(
            "landline.voice_transcribe.subprocess.Popen",
            return_value=fake_proc,
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
                pause_flag=pause_flag,
            )

        assert result.ok is False
        assert result.error == "paused"
        assert result.text == ""
        # The subprocess was killed (not left running).
        assert fake_proc._killed is True

    def test_pause_flag_never_set_lets_whisper_complete(
        self, tmp_path, monkeypatch,
    ):
        """With a pause_flag that stays False, the interruptible path
        completes normally and delivers a transcript — proving the
        pause branch doesn't false-trigger on quiescent flags."""
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")

        class _CompleteOnFirstPoll:
            def __init__(self):
                self.returncode = 0
                self._output_dir = None
                self._polls = 0

            def communicate(self, timeout=None):
                self._polls += 1
                # First poll: complete immediately. Write a canned
                # transcript into the output dir the wrapper made for
                # us — we sniff it out of TELEGRAM_VOICE_DIR.
                for entry in tmp_path.iterdir():
                    if entry.is_dir() and entry.name.startswith("whisper_"):
                        (entry / "audio.txt").write_text(
                            "quiescent flag", encoding="utf-8",
                        )
                        break
                return ("", "")

            def kill(self):
                # Should NEVER be called on the quiescent path.
                raise AssertionError(
                    "kill() called on the quiescent-pause_flag path"
                )

        fake_proc = _CompleteOnFirstPoll()

        class _FalsePauseFlag:
            def is_set(self):
                return False

        with patch(
            "landline.voice_transcribe.subprocess.Popen",
            return_value=fake_proc,
        ):
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
                pause_flag=_FalsePauseFlag(),
            )

        assert result.ok is True
        assert result.error is None
        assert result.text == "quiescent flag"

    def test_pause_flag_none_uses_subprocess_run_path(
        self, tmp_path, monkeypatch,
    ):
        """Backward-compat: without a ``pause_flag`` the wrapper MUST
        still take the historic ``subprocess.run`` path so callers
        without a daemon (and every existing test in this file) keep
        working."""
        monkeypatch.setattr(
            "landline.voice_transcribe.TELEGRAM_VOICE_DIR", tmp_path,
        )
        audio = tmp_path / "audio.ogg"
        audio.write_bytes(b"x")
        with patch(
            "landline.voice_transcribe.subprocess.Popen",
        ) as mock_popen, patch(
            "landline.voice_transcribe.subprocess.run",
            side_effect=_prime_output_txt("no pause path"),
        ) as mock_run:
            result = transcribe_file(
                audio, model="base", model_dir="/tmp/w",
                language="en", timeout_seconds=90,
                # pause_flag omitted deliberately
            )
        mock_popen.assert_not_called()
        assert mock_run.call_count == 1
        assert result.ok is True
        assert result.text == "no pause path"
