"""Local voice-note transcription via the whisper CLI.

Pure functional wrapper: no daemon state; only side effects are the
subprocess call and a scratch tempdir inside the voice cache.

- Interruptibility (load-bearing): callers pass the daemon's ``PauseFlag``
  as ``pause_flag=``. Whisper runs under ``Popen`` with a polling loop
  that kills the process on pause and returns ``error="paused"``. Without
  this a 30-90s run would starve the single-threaded dispatch loop.
  ``pause_flag=None`` falls back to ``subprocess.run`` (historic path,
  used by tests and non-daemon callers).
- Privacy (load-bearing): MUST NOT log transcript text. Only metadata
  (char count, elapsed, error class) — log lines end up in daily memory
  and leaking defeats the point of local transcription.
"""

import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, NamedTuple, Optional

from landline.config import (
    MEDIA_CACHE_DIR_MODE,
    TELEGRAM_VOICE_DIR,
    VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS,
    WHISPER_BIN,
)
from landline.runtime.logging import log


class TranscribeResult(NamedTuple):
    """Result of a whisper subprocess run.

    Attributes:
        ok: True iff whisper returned rc=0 AND we read a transcript.
        text: Stripped transcript (possibly truncated to
            VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS). Empty on failure.
        duration_seconds: Wall-clock time the subprocess spent (informational).
        error: Short error class + tail on failure; None on success.
            Special values: ``"timeout"``, ``"whisper_missing"``,
            ``"empty_transcript"``, ``"paused"`` (pause_flag observed
            during interruptible run).
    """

    ok: bool
    text: str
    duration_seconds: Optional[float]
    error: Optional[str]


class _PauseInterrupted(Exception):
    """Interruptible runner's signal that ``pause_flag`` became set.

    Caught in ``transcribe_file`` → ``TranscribeResult(error="paused")``.
    """


# 200ms poll: /pause latency ≤200ms, dispatch wakes 300-450x per 60-90s run.
_WHISPER_POLL_INTERVAL_SECONDS = 0.2


def _ensure_voice_dir() -> None:
    """Create the voice cache dir at 0o700 if missing.

    Mirrors ``download_file``'s dir-prep (no umask; explicit chmod).
    """
    try:
        TELEGRAM_VOICE_DIR.mkdir(
            parents=True, exist_ok=True, mode=MEDIA_CACHE_DIR_MODE,
        )
        try:
            os.chmod(str(TELEGRAM_VOICE_DIR), MEDIA_CACHE_DIR_MODE)
        except OSError:
            pass
    except Exception as e:
        log(
            f"voice_transcribe: failed to prepare {TELEGRAM_VOICE_DIR} "
            f"(exc={type(e).__name__}): {e}"
        )


def _read_transcript_from(output_dir: str) -> str:
    """Read whisper's ``<audio_stem>.txt`` output; return "" on read error.

    Exactly one ``.txt`` per invocation, so the first match wins.
    """
    try:
        for entry in os.listdir(output_dir):
            if entry.endswith(".txt"):
                with open(os.path.join(output_dir, entry), "r", encoding="utf-8") as f:
                    return f.read()
    except Exception:
        return ""
    return ""


def _pause_is_set(pause_flag: Optional[Any]) -> bool:
    """Defensive ``pause_flag.is_set()`` — a broken flag must NEVER kill
    transcription (would be misread as a real user pause and drop voice).
    """
    if pause_flag is None:
        return False
    try:
        return bool(pause_flag.is_set())
    except Exception:
        return False


def _run_whisper_interruptible(
    cmd,
    timeout_seconds: int,
    pause_flag: Any,
    started_at: float,
):
    """Run whisper via ``Popen`` with a polling wait; interruptible on /pause.

    Returns:
        A ``CompletedProcess``-shaped object on normal exit (matches
        ``subprocess.run``'s shape so the outer handler shares one branch).

    Raises:
        ``subprocess.TimeoutExpired`` on wall-clock timeout,
        ``_PauseInterrupted`` on pause request,
        ``FileNotFoundError`` when whisper is missing.

    - On timeout or pause: ``proc.kill()`` + ``communicate(timeout=2)``.
    - SIGKILL only to whisper; ffmpeg (on macOS) exits on broken pipe —
      accepted trade against process-group complexity.
    """
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        raise
    except Exception:
        raise

    stderr_bytes = ""
    while True:
        try:
            stdout, stderr = proc.communicate(
                timeout=_WHISPER_POLL_INTERVAL_SECONDS,
            )
            stderr_bytes = stderr or ""
            # Matches subprocess.run's return shape (see docstring).
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=proc.returncode,
                stdout=stdout or "",
                stderr=stderr_bytes,
            )
        except subprocess.TimeoutExpired:
            elapsed = time.time() - started_at
            # Wall-clock cap: fires even if pause_flag never does, so a
            # wedged whisper (torch/model/ffmpeg lockup) can't hold forever.
            if elapsed >= timeout_seconds:
                _kill_and_drain(proc)
                raise subprocess.TimeoutExpired(
                    cmd=cmd, timeout=timeout_seconds,
                )
            if _pause_is_set(pause_flag):
                _kill_and_drain(proc)
                raise _PauseInterrupted()
            continue


def _kill_and_drain(proc) -> None:
    """Best-effort ``kill()`` + drain pipes; NEVER raises.

    Drain prevents the tempdir cleanup from racing a still-writing whisper.
    """
    try:
        proc.kill()
    except Exception:
        pass
    try:
        proc.communicate(timeout=2)
    except Exception:
        pass


def transcribe_file(
    audio_path: Path,
    model: str,
    model_dir: str,
    language: str,
    timeout_seconds: int,
    pause_flag: Optional[Any] = None,
) -> TranscribeResult:
    """Transcribe ``audio_path`` with whisper. NEVER raises.

    Args:
        pause_flag: object with ``.is_set() -> bool`` or None. When set,
            whisper runs under ``Popen`` + 200ms polling and returns
            ``error="paused"`` on interrupt. None → ``subprocess.run``
            (historic path used by existing test patches).

    Returns:
        ``TranscribeResult(ok=False, error=<short str>)`` on ANY failure
        (timeout / non-zero / missing binary / pause). Caller picks the
        user-facing notice.

    - Cleans up its tempdir in ``finally`` so a mid-transcribe SIGTERM
      within launchd's grace window still leaves the voice cache empty.
    """
    _ensure_voice_dir()
    started_at = time.time()
    # Fresh output dir per invocation (single-threaded dispatch; tmpdir
    # makes cleanup a single ``rmtree``).
    try:
        output_dir = tempfile.mkdtemp(
            prefix="whisper_", dir=str(TELEGRAM_VOICE_DIR),
        )
    except Exception as e:
        return TranscribeResult(
            ok=False,
            text="",
            duration_seconds=None,
            error=f"{type(e).__name__}: {e}",
        )

    try:
        cmd = [
            WHISPER_BIN,
            "--model", model,
            "--model_dir", model_dir,
            "--language", language,
            "--task", "transcribe",
            # Title-cased "False" — whisper parses via ast.literal_eval;
            # lowercase "false" is silently ignored.
            "--fp16", "False",
            "--output_format", "txt",
            "--output_dir", output_dir,
            "--verbose", "False",
            str(audio_path),
        ]
        try:
            if pause_flag is None:
                # Historic path: keeps existing subprocess.run test patches
                # working and gives non-daemon callers the simplest shape.
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                )
            else:
                proc = _run_whisper_interruptible(
                    cmd, timeout_seconds, pause_flag, started_at,
                )
        except subprocess.TimeoutExpired:
            elapsed = time.time() - started_at
            log(
                f"voice_transcribe: timeout after {elapsed:.1f}s "
                f"(cap {timeout_seconds}s)"
            )
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=elapsed,
                error="timeout",
            )
        except FileNotFoundError:
            log("voice_transcribe: whisper binary not found at %s" % WHISPER_BIN)
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=time.time() - started_at,
                error="whisper_missing",
            )
        except _PauseInterrupted:
            elapsed = time.time() - started_at
            # Metadata-only — never log audio_path.
            log(
                f"voice_transcribe: interrupted by /pause after {elapsed:.1f}s"
            )
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=elapsed,
                error="paused",
            )
        except Exception as e:
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=time.time() - started_at,
                error=f"{type(e).__name__}: {e}",
            )

        elapsed = time.time() - started_at
        if proc.returncode != 0:
            stderr_tail = (proc.stderr or "")[-200:].replace("\n", " ")
            log(
                f"voice_transcribe: exit {proc.returncode} after {elapsed:.1f}s"
            )
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=elapsed,
                error=f"exit {proc.returncode}: {stderr_tail}",
            )

        text = _read_transcript_from(output_dir).strip()
        if not text:
            log(
                f"voice_transcribe: whisper exit 0 but empty transcript "
                f"({elapsed:.1f}s)"
            )
            return TranscribeResult(
                ok=False,
                text="",
                duration_seconds=elapsed,
                error="empty_transcript",
            )

        if len(text) > VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS:
            text = text[:VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS]
        # Metadata-only — NEVER include ``text``.
        log(
            "voice_transcribe: whisper OK: %d chars in %.1fs"
            % (len(text), elapsed)
        )
        return TranscribeResult(
            ok=True,
            text=text,
            duration_seconds=elapsed,
            error=None,
        )
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)
