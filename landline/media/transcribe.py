"""Local voice-note transcription via the whisper CLI.

Pure functional wrapper around the configured whisper binary — no daemon
state, no side effects beyond the subprocess call and a scratch temp dir
inside the voice cache.

**Interruptibility (load-bearing):** the caller passes the daemon's
``PauseFlag`` as ``pause_flag=``. When set, ``transcribe_file`` polls it
between short ``proc.wait()`` slices and kills whisper on request,
returning ``error="paused"``. Without this, a 30-90s whisper run would
starve every other batch (text, photos, /pause) for the full duration
because dispatch is single-threaded — the operator's "sent voice, sent
/pause" sequence would sit locked out for a minute before the pause is
even observed. When ``pause_flag`` is ``None``, we fall back to the
historic ``subprocess.run`` path — used by existing tests and any
non-daemon caller where interruption isn't required.

Privacy discipline (load-bearing): this module MUST NOT log transcript
text. Only metadata (char count, elapsed seconds, error class) may be
logged. Log lines can be read by future agents and copied into daily
memory; leaking the transcript defeats the point of doing transcription
locally in the first place.
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
    """Raised by the interruptible whisper runner when the caller's
    ``pause_flag`` becomes set. Caught in ``transcribe_file`` and
    translated to ``TranscribeResult(error="paused")`` — the caller's
    voice_handler drops the dispatch and lets the pause path route the
    notice.
    """


# Poll cadence for the interruptible runner. 200ms strikes the balance:
# fast enough that /pause feels responsive (worst-case ~200ms observed
# latency between set-flag and whisper-kill), slow enough that a
# 60-90s transcription only wakes the dispatch thread 300-450 times
# total. Not exposed via config — this is a pure implementation detail
# of the polling loop.
_WHISPER_POLL_INTERVAL_SECONDS = 0.2


def _ensure_voice_dir() -> None:
    """Create the voice cache dir if missing, at 0o700 mode. Mirrors
    ``download_file``'s dir-prep pattern (no umask; explicit chmod).
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
    """Read whisper's ``<basename>.txt`` output from ``output_dir``.

    Whisper writes ``<audio_stem>.txt`` next to any other ``--output_format``
    files. There's exactly one ``.txt`` per invocation, so scan-and-read
    the first match. Returns "" on any read error.
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
    """Read ``pause_flag.is_set()`` defensively — a broken pause_flag
    must NEVER kill transcription mid-flight (which would be misread as
    a real user pause and drop the voice note)."""
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
    """Run whisper via ``Popen`` with a polling wait loop.

    Returns a ``CompletedProcess``-shaped object on normal exit (with
    ``returncode`` and ``stderr`` populated). Raises
    ``subprocess.TimeoutExpired`` on wall-clock timeout,
    ``_PauseInterrupted`` on pause request, ``FileNotFoundError`` if
    the whisper binary is missing (mirrors ``subprocess.run``'s shape
    so the outer handler can share one branch).

    Kill semantics: on timeout OR pause we ``proc.kill()`` and
    ``proc.communicate(timeout=2)`` to drain pipes. Whisper on
    macOS uses ffmpeg internally; kill delivers SIGKILL to the
    whisper process only, leaving ffmpeg to exit on broken pipe —
    an accepted trade against the extra complexity of process groups.
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
            # subprocess.CompletedProcess is a NamedTuple-like object
            # exposing ``.returncode`` / ``.stderr`` — matches the
            # subprocess.run return shape used by the non-interruptible
            # path.
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=proc.returncode,
                stdout=stdout or "",
                stderr=stderr_bytes,
            )
        except subprocess.TimeoutExpired:
            elapsed = time.time() - started_at
            # Wall-clock cap: enforce even if pause_flag never fires so
            # a wedged whisper (torch import hang, corrupt model, ffmpeg
            # lockup) can't hold the loop forever.
            if elapsed >= timeout_seconds:
                _kill_and_drain(proc)
                raise subprocess.TimeoutExpired(
                    cmd=cmd, timeout=timeout_seconds,
                )
            if _pause_is_set(pause_flag):
                _kill_and_drain(proc)
                raise _PauseInterrupted()
            # Neither timeout nor pause → keep waiting.
            continue


def _kill_and_drain(proc) -> None:
    """Best-effort ``kill()`` + ``communicate(timeout=2)`` to drain
    pipes so the temp dir cleanup doesn't race a still-writing whisper.
    NEVER raises."""
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
    """Transcribe ``audio_path`` with the whisper CLI. Never raises.

    Runs whisper synchronously; on ANY failure mode (timeout, non-zero
    exit, missing binary, corrupt model, pause requested, etc.) returns
    ``TranscribeResult(ok=False, ...)`` with a short ``error`` string.
    The caller (voice_handler) uses that string to pick a user-facing
    notice and MUST NOT re-raise.

    ``pause_flag``: optional object with ``.is_set() -> bool``. When
    provided, whisper runs under ``Popen`` with a 200ms polling loop
    that kills the process on pause and returns ``error="paused"``.
    When ``None`` (or missing), whisper runs via ``subprocess.run`` —
    the historic path that existing tests patch.

    Cleans up its output tmpdir in ``finally``, so a mid-transcribe SIGTERM
    within launchd's grace window still leaves the voice cache empty.
    """
    _ensure_voice_dir()
    started_at = time.time()
    # Fresh output dir per invocation. Dispatch is single-threaded, so
    # collisions are impossible in practice; the tmpdir also makes cleanup
    # a single rmtree.
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
            # False (title-cased) is the correct disable form — whisper
            # parses this as a Python bool via ast.literal_eval. Lowercase
            # 'false' is silently ignored.
            "--fp16", "False",
            "--output_format", "txt",
            "--output_dir", output_dir,
            "--verbose", "False",
            str(audio_path),
        ]
        try:
            if pause_flag is None:
                # Non-interruptible fallback — historic path. Kept so
                # existing test patches on ``landline.media.transcribe.
                # subprocess.run`` continue to apply, and so any future
                # caller without a pause_flag observes the simplest
                # possible shape.
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
            # Metadata-only log line — never mention audio_path (a file
            # name reveals nothing but is still user-adjacent metadata
            # we don't need in the log).
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
        # Metadata-only log — NEVER include ``text``.
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
