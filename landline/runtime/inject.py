"""Inject queue — just-in-time context prepended to the next Claude turn.

Cron jobs and other scripts drop JSON files into the inject-queue directory;
the daemon drains them before each Claude call and prepends them to the
user's message.

- Two-phase commit: `drain_inject_queue` returns text + paths but does NOT
  unlink. `commit_inject_queue` runs only after Claude accepted the stdin
  write, so a crashed / backed-off dispatch re-injects on retry — the operator
  never silently loses a morning brief or news report.
- Malformed files (bad JSON / bad UTF-8) are unlinked on first read so they
  can't jam the queue. Transient I/O errors (OSError EACCES/EIO) are left on
  disk for the next drain — a momentarily-unreadable good report is never lost.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from landline.config import INJECT_TIMESTAMP_FORMAT, TIMEZONE, USER_NAME
from landline.runtime.logging import log


def drain_inject_queue(queue_dir: Path) -> Tuple[str, List[Path]]:
    """Read all queued inject files and build the formatted context block.

    Args:
        queue_dir: directory holding `*.json` inject files.
    Returns:
        `(text, consumed_paths)`: `text` empty when nothing queued or every
        file is corrupt; `consumed_paths` are the paths the caller must pass
        to `commit_inject_queue` after Claude accepted the stdin write.

    - Caller must check lock state first — inject only runs when unlocked.
    """
    if not queue_dir.exists():
        return "", []
    def _sort_key(p):
        # mtime = producer's finish time; filename tiebreak for determinism.
        # stat() raising (vanished file) sorts last so the read_text below
        # routes through the OSError branch — never silently drop.
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
            # Transient I/O — DO NOT unlink; retry next drain.
            log(f"[inject] I/O error reading {path.name} (leaving for retry): {e}")
            continue
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Malformed payload — unlink now so it doesn't jam the queue and
            # drop every later message forever (the infinite-reprocess trap:
            # UnicodeDecodeError isn't an OSError, so it re-fails on every read).
            log(f"[inject] Bad queue file {path.name}: {e}")
            try:
                path.unlink()
            except Exception:
                pass
            continue

        label = data.get("label", "cron")
        content = data.get("content")
        if not isinstance(content, str):
            # Malformed/missing content — skip, don't crash dispatch.
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
    """Delete inject-queue files after Claude accepted the stdin write.

    Best-effort: missing files are tolerated.
    """
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            log(f"[inject] Failed to commit {path.name}: {e}")
