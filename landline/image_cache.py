"""Media cache sweep — age-based retention for ``cache/telegram_images/``
and ``cache/telegram_files/``.

Inbound photos and documents are downloaded into daemon-owned cache
directories and handed to Claude as file paths during the turn. Per-turn
deletion is unsafe because Claude reads the file DURING the turn and may
reference it across tool calls; an age-based sweep at daemon startup is the
simplest robust approach.

These are app cache files in the daemon's own dirs (not user documents), so
``path.unlink()`` / ``shutil.rmtree()`` is correct here — ``trash`` is
reserved for files the user might want to recover.

Whisper cleanup (privacy invariant): ``voice_transcribe.transcribe_file``
mkdtemp's ``whisper_XXX/`` subdirectories under ``TELEGRAM_VOICE_DIR`` and
relies on ``finally: shutil.rmtree(...)`` for cleanup. That ``finally`` does
NOT run under SIGKILL / launchd force-kill / OS crash — leaving the subdir
plus a plaintext ``<audio_stem>.txt`` transcript on disk indefinitely. So
``_sweep_dir`` also recurses into stale sub-directories (older than
``retention_hours``) and blows them away with ``shutil.rmtree``. Without
this, transcripts leak past daemon restarts and violate the stated privacy
invariant in ``voice_transcribe.py``.
"""

import shutil
import time
from typing import Any, Iterable, Optional

from landline.config import (
    MEDIA_CACHE_DIRS,
    MEDIA_CACHE_RETENTION_HOURS,
    TELEGRAM_IMAGE_DIR,
    TELEGRAM_IMAGE_RETENTION_HOURS,
)
from landline.logging import log


def _sweep_dir(
    image_dir: Any,
    retention_hours: float,
) -> int:
    """Delete stale files AND stale sub-directories in ``image_dir`` and
    return the count swept.

    - Regular files older than ``retention_hours`` are ``unlink()``-ed.
    - Sub-directories whose own mtime is older than ``retention_hours``
      are ``rmtree``-ed entirely (covers whisper's mkdtemp tmpdirs when
      the ``finally`` cleanup didn't run — see module docstring). Each
      removed subdir counts as a single sweep, regardless of how many
      files it contained.
    - Symlinks are ``unlink()``-ed by age (never followed).

    Failures (missing dir, permission errors) are logged and swallowed so a
    flaky filesystem can never crash daemon startup.
    """
    swept = 0
    try:
        if not image_dir.exists():
            return 0
        cutoff = time.time() - (retention_hours * 3600.0)
        for entry in image_dir.iterdir():
            try:
                # Symlinks: never follow — remove by age like a file.
                if entry.is_symlink():
                    if entry.lstat().st_mtime < cutoff:
                        entry.unlink()
                        swept += 1
                    continue
                if entry.is_file():
                    if entry.stat().st_mtime < cutoff:
                        entry.unlink()
                        swept += 1
                    continue
                if entry.is_dir():
                    # Directory mtime updates on child add/remove, so a
                    # subdir that hasn't been touched in retention_hours
                    # is provably stale from the daemon's POV.
                    if entry.stat().st_mtime < cutoff:
                        shutil.rmtree(str(entry), ignore_errors=True)
                        # ``rmtree(ignore_errors=True)`` may leave the
                        # dir if a child is permission-denied; recount
                        # only if it's actually gone.
                        if not entry.exists():
                            swept += 1
                    continue
            except Exception as per_entry_error:
                # PRIVACY: never log ``entry.name`` (sanitized-but-still-
                # sensitive user filenames like ``private_medical_records.pdf``
                # or ``<ts>_voice_note.oga``) and never render the exception
                # ``__str__`` (an OSError embeds the FULL absolute path, e.g.
                # ``[Errno 13] Permission denied: '.../<ts>_<filename>'``).
                # Both would land in the 25MB rotating daemon.log — the exact
                # leak surface document_handler / voice_handler / telegram_
                # download were carefully hardened against (their reject/fail
                # paths log metadata only). Metadata-only shape: which cache
                # dir + error class.
                log(
                    "Media cache sweep: failed to remove one entry in %s "
                    "(error type: %s)" % (
                        image_dir.name,
                        type(per_entry_error).__name__,
                    )
                )
        log(
            f"Media cache sweep: removed {swept} entry(ies) older than "
            f"{retention_hours}h from {image_dir}"
        )
    except Exception as sweep_error:
        # Never let a sweep failure crash daemon startup.
        log(f"Media cache sweep failed (non-fatal) for {image_dir}: {sweep_error}")
    return swept


def _sweep_telegram_image_cache(
    image_dir: Any = TELEGRAM_IMAGE_DIR,
    retention_hours: float = TELEGRAM_IMAGE_RETENTION_HOURS,
) -> int:
    """Back-compat wrapper — sweep a single dir (defaults to the image cache).

    Retained so ``landline.orchestrator._sweep_telegram_image_cache`` remains
    importable and existing tests keep passing. The generalized entrypoint
    used at startup is ``sweep_media_caches`` below.
    """
    return _sweep_dir(image_dir, retention_hours)


def sweep_media_caches(
    dirs: Iterable[Any] = MEDIA_CACHE_DIRS,
    retention_hours: Optional[float] = None,
) -> int:
    """Sweep every media cache directory. Returns the total files removed.

    Each directory is swept with its own retention window looked up from
    ``config.MEDIA_CACHE_RETENTION_HOURS`` (image / document / voice each
    have their own knob so the operator can tune voice-note privacy independently
    of image/PDF retention). Dirs not in the map fall back to the image
    retention default — safe for callers that pass ad-hoc dirs from tests.

    An explicit ``retention_hours`` argument overrides the per-dir lookup
    for the whole batch — used by the multi-dir sweep tests that want to
    pin one retention value across a batch.

    Failures in one directory are logged and do not abort the others.
    """
    total = 0
    for d in dirs:
        try:
            if retention_hours is not None:
                hours = retention_hours
            else:
                hours = MEDIA_CACHE_RETENTION_HOURS.get(
                    d, TELEGRAM_IMAGE_RETENTION_HOURS,
                )
            total += _sweep_dir(d, hours)
        except Exception as loop_error:
            # Defensive — _sweep_dir already swallows its own errors, but a
            # weird Path implementation could raise before we get there.
            log(f"Media cache sweep loop failed for {d}: {loop_error}")
    return total
