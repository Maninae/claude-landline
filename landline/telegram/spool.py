"""Disk-backed outbound spool for Telegram send chunks — at-least-once delivery.

Transport persist-first: chunk → disk BEFORE ``_send_chunk``; success unlinks;
retry-exhaustion renames to ``pending`` for periodic + startup replay.

File layout (under ``config.SPOOL_DIR``, mode 0o700):

    {created_epoch_ns}-{uid8}-{state}.json     mode 0o600

State is ``pending`` (queue-visible) or ``inflight-<pid>`` (a specific
daemon process is retrying it; orphaned files from a dead pid are
reclaimed at startup).

- Concurrency: send-path owns each ``inflight-<pid>`` file; replay thread
  only touches ``pending``, renaming to its own ``inflight-<pid>`` for the
  retry. ``os.rename`` on POSIX is atomic — the two paths cannot collide.
- Startup reclaim runs BEFORE the poller and any StreamSender worker.
- Per-boot ordering via ``created_epoch_ns`` prefix. Cross-boot ordering
  between spooled and freshly-enqueued chunks is NOT preserved
  (at-least-once trumps ordering here).
- Replay 400 → unlink (unfixable); anything else non-2xx → return to pending.
- Corrupt payload → log + unlink.
"""

import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from landline.config import (
    SPOOL_DIR,
    SPOOL_DIR_MODE,
    SPOOL_FILE_MODE,
    SPOOL_MAX_AGE_SECONDS,
    SPOOL_MAX_FILES,
    SPOOL_REPLAY_INTERVAL_SECONDS,
    SPOOL_REPLAY_MIN_AGE_SECONDS,
)
from landline.runtime.logging import log


# send_fn signature: (chat_id, chunk, html_mode, label) → (ok, http_code).
# Callers close over the token so this module stays credential-free.
SendFn = Callable[[str, str, bool, str], Tuple[bool, Optional[int]]]


def _spool_filename(created_epoch_ns: int, uid: str, state: str) -> str:
    return "%d-%s-%s.json" % (created_epoch_ns, uid, state)


def _parse_spool_filename(name: str) -> Optional[Tuple[int, str, str]]:
    """Parse ``{created_ns}-{uid8}-{state}.json`` → parts, or None if foreign.

    ``state`` is ``pending`` or ``inflight-<pid>``; callers inspect the prefix.
    """
    if not name.endswith(".json"):
        return None
    stem = name[:-len(".json")]
    parts = stem.split("-", 2)
    if len(parts) != 3:
        return None
    try:
        created_ns = int(parts[0])
    except ValueError:
        return None
    return created_ns, parts[1], parts[2]


def ensure_spool_dir() -> Path:
    """Create ``SPOOL_DIR`` at 0o700; idempotent.

    Called at startup and defensively before each ``persist`` (in case an
    external process pruned the dir).
    """
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(str(SPOOL_DIR), SPOOL_DIR_MODE)
    except OSError as e:
        log("outbound_spool: chmod %s failed: %r" % (SPOOL_DIR, e))
    return SPOOL_DIR


def persist(chat_id: str, chunk: str, html_mode: bool, label: str) -> str:
    """Persist an outbound chunk. Returns spool_id (absolute path).

    Two-phase: pending 0o600 + fsync, then rename to ``inflight-<pid>`` so
    the replayer won't see it while send-path is retrying. On I/O failure
    the caller degrades to best-effort (un-persisted send).
    """
    ensure_spool_dir()
    now_ns = time.time_ns()
    uid = uuid.uuid4().hex[:8]
    payload = {
        "chat_id": chat_id,
        "chunk": chunk,
        "html_mode": bool(html_mode),
        "label": label,
        "created_at": time.time(),
        "attempts": 0,
    }
    pending_name = _spool_filename(now_ns, uid, "pending")
    inflight_name = _spool_filename(now_ns, uid, "inflight-%d" % os.getpid())
    pending_path = SPOOL_DIR / pending_name
    inflight_path = SPOOL_DIR / inflight_name

    # 0o600 write + fsync; O_EXCL guards against uuid collision.
    fd = os.open(
        str(pending_path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        SPOOL_FILE_MODE,
    )
    # Once the inode exists, ANY subsequent failure (fsync/chmod/rename can
    # all raise under disk pressure) MUST unlink before propagating —
    # otherwise a swallowed OSError + successful network send leaks a
    # pending file that the replayer double-delivers.
    try:
        try:
            os.write(fd, json.dumps(payload).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        # Belt-and-suspenders 0o600 enforce (macOS umask race). chmod
        # failure is cosmetic — DO NOT unlink on it (that would drop a
        # persisted chunk on benign perm jitter).
        try:
            os.chmod(str(pending_path), SPOOL_FILE_MODE)
        except OSError:
            pass
        os.rename(str(pending_path), str(inflight_path))
    except BaseException:
        # Unlink the leaked pending BEFORE re-raising.
        try:
            os.unlink(str(pending_path))
        except OSError:
            pass
        raise
    return str(inflight_path)


def mark_success(spool_id: str) -> None:
    """Unlink the file after a successful send. Idempotent."""
    try:
        os.unlink(spool_id)
    except FileNotFoundError:
        pass
    except OSError as e:
        log("outbound_spool: mark_success unlink failed for %s: %r" %
            (spool_id, e))


def discard(spool_id: str) -> None:
    """Unlink the spool file for ``spool_id`` regardless of its current state.

    Args:
        spool_id: absolute path returned by ``persist``.

    - Used when a superseding variant is being delivered instead (e.g.
      ``send_response`` switching to plain-text after HTML failed) —
      without this, the replayer double-delivers.
    - Matches by ``(created_ns, uid)`` prefix so it works whether the file
      is currently ``inflight-<pid>`` or already renamed back to ``pending``.
    - Idempotent for missing files.
    """
    try:
        path = Path(spool_id)
        parsed = _parse_spool_filename(path.name)
        if parsed is None:
            return
        created_ns, uid, _state = parsed
        try:
            entries = list(path.parent.iterdir())
        except FileNotFoundError:
            return
        for entry in entries:
            parts = _parse_spool_filename(entry.name)
            if parts is None:
                continue
            if parts[0] != created_ns or parts[1] != uid:
                continue
            try:
                os.unlink(str(entry))
            except FileNotFoundError:
                pass
            except OSError as e:
                log("outbound_spool: discard unlink failed for %s: %r" %
                    (entry.name, e))
    except OSError as e:
        log("outbound_spool: discard failed for %s: %r" % (spool_id, e))


def mark_failed(spool_id: str) -> None:
    """Rename ``inflight-<pid>`` → ``pending`` for the next replay pass.

    Called by ``_send_with_retry`` after in-call retries are exhausted.
    Idempotent when the file is already gone.
    """
    try:
        path = Path(spool_id)
        parsed = _parse_spool_filename(path.name)
        if parsed is None:
            return
        created_ns, uid, _state = parsed
        pending_name = _spool_filename(created_ns, uid, "pending")
        pending_path = path.parent / pending_name
        os.rename(str(path), str(pending_path))
    except FileNotFoundError:
        pass
    except OSError as e:
        log("outbound_spool: mark_failed rename failed for %s: %r" %
            (spool_id, e))


def startup_reclaim_orphaned_inflight() -> int:
    """Rename orphaned ``inflight-<pid>`` files back to ``pending``.

    The owning pid died with the previous daemon, so no live retry is in
    progress. Runs once at startup BEFORE ``replay_all``. Returns the count
    of renamed files (for the startup log line).
    """
    ensure_spool_dir()
    count = 0
    try:
        entries = list(SPOOL_DIR.iterdir())
    except OSError as e:
        log("outbound_spool: reclaim scan failed: %r" % e)
        return 0
    for entry in entries:
        parsed = _parse_spool_filename(entry.name)
        if parsed is None:
            continue
        created_ns, uid, state = parsed
        if not state.startswith("inflight-"):
            continue
        pending_name = _spool_filename(created_ns, uid, "pending")
        try:
            os.rename(str(entry), str(SPOOL_DIR / pending_name))
            count += 1
        except FileNotFoundError:
            continue
        except OSError as e:
            log("outbound_spool: reclaim rename failed for %s: %r" %
                (entry.name, e))
    return count


def _list_pending_sorted() -> List[Tuple[int, Path]]:
    """Enumerate ``pending`` files, sorted by ``created_epoch_ns`` ascending."""
    try:
        entries = list(SPOOL_DIR.iterdir())
    except OSError as e:
        log("outbound_spool: replay scan failed: %r" % e)
        return []
    pending: List[Tuple[int, Path]] = []
    for entry in entries:
        parsed = _parse_spool_filename(entry.name)
        if parsed is None:
            continue
        created_ns, _uid, state = parsed
        if state != "pending":
            continue
        pending.append((created_ns, entry))
    pending.sort(key=lambda x: x[0])
    return pending


def _apply_soft_cap(pending: List[Tuple[int, Path]]) -> List[Tuple[int, Path]]:
    """If pending exceeds ``SPOOL_MAX_FILES``, drop the oldest excess."""
    if len(pending) <= SPOOL_MAX_FILES:
        return pending
    excess = len(pending) - SPOOL_MAX_FILES
    dropped = pending[:excess]
    for _, path in dropped:
        try:
            os.unlink(str(path))
        except OSError:
            pass
    log("outbound_spool: soft cap exceeded; dropped %d oldest pending file(s)" %
        excess)
    return pending[excess:]


def replay_all(send_fn: SendFn) -> None:
    """Attempt to replay every pending spool file, oldest-first.

    Per-file flow:
      - Read + JSON-parse. Corrupt payload → log + unlink (unfixable).
      - Age > SPOOL_MAX_AGE_SECONDS → log + unlink (stale is worse than lost).
      - Age < SPOOL_REPLAY_MIN_AGE_SECONDS → skip (probably still in-flight).
      - Rename to ``inflight-<pid>``, call ``send_fn``, then:
          ok True                     → unlink (success)
          ok False, code == 400       → unlink (unfixable — bad payload)
          ok False, any other code    → rename back to ``pending`` for the
                                        next pass
    """
    ensure_spool_dir()
    now = time.time()
    pending = _apply_soft_cap(_list_pending_sorted())

    for _created_ns, entry in pending:
        try:
            with open(str(entry), "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            continue
        except OSError as e:
            log("outbound_spool: read failed for %s: %r" % (entry.name, e))
            continue

        try:
            payload = json.loads(raw.decode("utf-8", errors="replace"))
        except (json.JSONDecodeError, ValueError):
            log("outbound_spool: corrupt payload %s, unlinking" % entry.name)
            try:
                os.unlink(str(entry))
            except OSError:
                pass
            continue

        created_at = payload.get("created_at", now)
        try:
            age = now - float(created_at)
        except (TypeError, ValueError):
            age = 0.0
        if age > SPOOL_MAX_AGE_SECONDS:
            log(
                "outbound_spool: dropping stale file %s (age=%ds > %ds)"
                % (entry.name, int(age), SPOOL_MAX_AGE_SECONDS)
            )
            try:
                os.unlink(str(entry))
            except OSError:
                pass
            continue
        if age < SPOOL_REPLAY_MIN_AGE_SECONDS:
            # Too young — may still be inflight on the primary send-path.
            continue

        parsed = _parse_spool_filename(entry.name)
        if parsed is None:
            continue
        p_created_ns, uid, _state = parsed
        inflight_name = _spool_filename(
            p_created_ns, uid, "inflight-%d" % os.getpid(),
        )
        inflight_path = SPOOL_DIR / inflight_name
        try:
            os.rename(str(entry), str(inflight_path))
        except FileNotFoundError:
            continue
        except OSError as e:
            log("outbound_spool: replay rename failed for %s: %r" %
                (entry.name, e))
            continue

        chat_id = payload.get("chat_id", "")
        chunk = payload.get("chunk", "")
        html_mode = bool(payload.get("html_mode", False))

        # Bump attempts counter for future log lines (best-effort).
        payload["attempts"] = int(payload.get("attempts", 0)) + 1

        try:
            ok, code = send_fn(chat_id, chunk, html_mode, "spool replay")
        except Exception as e:
            log("outbound_spool: replay send raised for %s: %r" %
                (inflight_path.name, e))
            ok, code = False, None

        if ok:
            try:
                os.unlink(str(inflight_path))
            except OSError:
                pass
            continue

        # Unfixable 4xx (bad token / bot kicked / chat gone) → drop.
        # Retrying would burn ~1440 calls/file/day for no chance of success.
        # Only 429 + 5xx round-trip to the retry branch below.
        if code in (400, 401, 403, 404):
            log(
                "outbound_spool: dropping %d (unfixable) for %s"
                % (code, inflight_path.name)
            )
            try:
                os.unlink(str(inflight_path))
            except OSError:
                pass
            continue

        # Retryable → rename back to pending for the next pass.
        pending_name = _spool_filename(p_created_ns, uid, "pending")
        try:
            os.rename(str(inflight_path), str(SPOOL_DIR / pending_name))
        except OSError as e:
            log("outbound_spool: return-to-pending failed for %s: %r" %
                (inflight_path.name, e))


class OutboundSpoolReplayer:
    """Background thread running ``replay_all`` on a fixed interval.

    Started by ``TelegramDaemon.run()`` between ``_handle_restart_continuation``
    and ``_background_poller.start()``. Stopped by the shutdown handler
    (level-triggered Event; the current pass finishes before exit).
    """

    def __init__(self, send_fn: SendFn) -> None:
        self._send_fn = send_fn
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop,
            name="OutboundSpoolReplayer",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                replay_all(self._send_fn)
            except Exception as e:
                log("outbound_spool: replay pass raised: %r" % e)
            # Event.wait returns True on set → shutdown latency ≤ this sleep.
            self._stop.wait(SPOOL_REPLAY_INTERVAL_SECONDS)
