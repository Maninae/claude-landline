"""Inject queue — just-in-time context prepended to the next Claude turn.

Cron jobs and other scripts drop JSON files into the inject-queue directory.
Before each Claude call, the daemon drains all queued files and formats them
as a context block prepended to the user's message.

Two-phase commit: ``drain_inject_queue`` reads the files and returns the
text plus the list of paths that produced it, but does NOT delete anything.
After the caller has handed the message off to Claude successfully, it calls
``commit_inject_queue`` to unlink the consumed files.  If the Claude call
crashes, times out, or is in backoff, the files remain on disk and get
re-injected on the next attempt — the operator never silently loses a morning brief
or news report.

Files that are malformed (bad JSON or bad UTF-8 bytes) are unlinked
immediately so they don't keep failing on every drain. Transient I/O errors
(``OSError``: EACCES/EIO) are the opposite — the file is left on disk and
retried next drain, so a momentarily-unreadable good report is never lost.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from landline.config import INJECT_TIMESTAMP_FORMAT, TIMEZONE, USER_NAME
from landline.logging import log


def drain_inject_queue(queue_dir: Path) -> Tuple[str, List[Path]]:
    """Read all queued inject files and build a formatted context block.

    Returns a ``(text, consumed_paths)`` tuple.  ``text`` is empty when
    nothing is queued or every file is corrupt; ``consumed_paths`` is the
    list of well-formed files whose content was incorporated into ``text``
    and that should be deleted after a successful Claude dispatch via
    :func:`commit_inject_queue`.

    The caller is responsible for checking lock state before calling —
    inject is only active when unlocked.
    """
    if not queue_dir.exists():
        return "", []
    def _sort_key(p):
        # Sort by mtime (when the producer finished writing the file). Filename
        # tiebreak gives deterministic order on filesystem-mtime ties. If stat
        # raises (file vanished between glob and stat), sort it last so the
        # subsequent read_text routes through the existing OSError branch and
        # leaves it for retry — never silently drop it from the batch.
        try:
            return (p.stat().st_mtime, p.name)
        except OSError:
            return (float("inf"), p.name)

    items = sorted(queue_dir.glob("*.json"), key=_sort_key)
    if not items:
        return "", []

    summary_parts: List[str] = []
    content_blocks: List[str] = []
    consumed_paths: List[Path] = []
    for path in items:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except OSError as e:
            # Transient I/O / permission error — DO NOT unlink. A good report
            # file briefly unreadable (EACCES/EIO) is not corrupt; leave it on
            # disk so the next drain retries it.
            log(f"[inject] I/O error reading {path.name} (leaving for retry): {e}")
            continue
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Malformed payload (bad JSON or bad UTF-8 bytes): unlink now so it
            # doesn't jam the queue and drop every later message on each drain.
            # UnicodeDecodeError is NOT an OSError, so without this it would
            # propagate up and re-fail forever — the infinite-reprocess trap.
            log(f"[inject] Bad queue file {path.name}: {e}")
            try:
                path.unlink()
            except Exception:
                pass
            continue

        label = data.get("label", "cron")
        content = data.get("content")
        if not isinstance(content, str):
            # Malformed/missing content — skip this entry rather than crashing dispatch.
            log(
                "[inject] skipping %s: 'content' is not a string (%s)"
                % (path.name, type(content).__name__)
            )
            try:
                path.unlink()
            except Exception:
                pass
            continue
        ts_str = ""
        stem = path.stem
        if len(stem) >= 15:
            try:
                ts = datetime.strptime(stem[:15], INJECT_TIMESTAMP_FORMAT).replace(
                    tzinfo=TIMEZONE,
                )
                ts_str = " (" + ts.strftime("%H:%M %Z") + ")"
            except (ValueError, IndexError):
                pass
        summary_parts.append(f"{label}{ts_str}")
        if content.strip():
            time_val = ts_str.strip(" ()")
            content_blocks.append(
                f'<injected-report name="{label}" time="{time_val}">\n'
                f"{content.strip()}\n"
                f"</injected-report>"
            )
        consumed_paths.append(path)

    if not summary_parts:
        return "", []
    joined = ", ".join(summary_parts)
    header = f"[Reports delivered to {USER_NAME} since last message: {joined}]"
    if content_blocks:
        header += "\n\n" + "\n\n".join(content_blocks)
    log(
        f"[inject] Prepending {len(summary_parts)} report(s), "
        f"{len(content_blocks)} with content"
    )
    return header, consumed_paths


def commit_inject_queue(paths: List[Path]) -> None:
    """Delete inject-queue files that were successfully handed to Claude.

    Called after :func:`drain_inject_queue` returned paths AND the message
    was accepted by Claude (stdin write returned).  Already-missing files
    are tolerated — the goal is best-effort cleanup, not strict accounting.
    """
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"[inject] Failed to commit {path.name}: {e}")
