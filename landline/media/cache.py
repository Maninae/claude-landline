"""Media cache sweep — age-based retention for the daemon's own cache dirs.

- Inbound photos / documents are handed to Claude as file paths during a
  turn; per-turn deletion is unsafe (cross-tool references). An age-based
  sweep at startup is the robust option.
- These are app cache files (not user documents), so ``unlink`` /
  ``rmtree`` is correct — ``trash`` is reserved for recoverable files.
- Whisper cleanup (privacy invariant): ``transcribe.transcribe_file`` uses
  a ``finally: rmtree`` on its ``whisper_XXX/`` tempdir, which does NOT
  run under SIGKILL / launchd force-kill / OS crash. So ``_sweep_dir``
  also blows away stale sub-directories with ``rmtree`` — otherwise
  transcripts leak past restarts, violating ``transcribe.py``'s invariant.
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
from landline.runtime.logging import log


def _sweep_dir(
    image_dir: Any,
    retention_hours: float,
) -> int:
    """Delete stale files AND stale sub-dirs in ``image_dir``; return count.

    - Files older than ``retention_hours`` → ``unlink``.
    - Sub-dirs whose own mtime is older than ``retention_hours`` →
      ``rmtree`` (each subdir counts as one, regardless of contents).
    - Symlinks → ``unlink`` by age, never followed.
    - Failures (missing dir / perm error) are logged and swallowed so a
      flaky FS can never crash daemon startup.
    """
    swept = 0
    try:
        if not image_dir.exists():
            return 0
        cutoff = time.time() - (retention_hours * 3600.0)
        for entry in image_dir.iterdir():
            try:
                if entry.is_symlink():
                    # Never follow — remove by age like a file.
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
                    # Dir mtime updates on child add/remove → untouched-since
                    # retention is provably stale.
                    if entry.stat().st_mtime < cutoff:
                        shutil.rmtree(str(entry), ignore_errors=True)
                        # ``ignore_errors=True`` may leave the dir behind on
                        # a permission-denied child; only recount if gone.
                        if not entry.exists():
                            swept += 1
                    continue
            except Exception as per_entry_error:
                # PRIVACY: NEVER log ``entry.name`` (sanitized-but-sensitive
                # filenames) or ``exc.__str__`` (OSError embeds the full
                # absolute path). Metadata-only shape: cache dir + error class.
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
        # Never let a sweep failure crash startup.
        log(f"Media cache sweep failed (non-fatal) for {image_dir}: {sweep_error}")
    return swept


def _sweep_telegram_image_cache(
    image_dir: Any = TELEGRAM_IMAGE_DIR,
    retention_hours: float = TELEGRAM_IMAGE_RETENTION_HOURS,
) -> int:
    """Back-compat wrapper — sweep a single dir (defaults to image cache).

    Kept so ``landline.orchestrator._sweep_telegram_image_cache`` stays
    importable and existing tests keep passing. Startup uses ``sweep_media_caches``.
    """
    return _sweep_dir(image_dir, retention_hours)


def sweep_media_caches(
    dirs: Iterable[Any] = MEDIA_CACHE_DIRS,
    retention_hours: Optional[float] = None,
) -> int:
    """Sweep every media cache directory; return total files removed.

    Args:
        dirs: cache directories to sweep.
        retention_hours: overrides the per-dir lookup for the whole batch
            (used by multi-dir sweep tests).

    - Each dir's retention comes from ``MEDIA_CACHE_RETENTION_HOURS`` so
      voice-note privacy tunes independently of image/PDF retention.
    - Dirs not in the map fall back to the image default.
    - Per-directory failures are logged and don't abort the others.
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
            # Defensive — _sweep_dir swallows its own errors, but a weird
            # Path impl could raise before we get there.
            log(f"Media cache sweep loop failed for {d}: {loop_error}")
    return total
