"""Disk-backed outbound spool for Telegram send chunks (Cluster 5).

Provides at-least-once persistence for outbound sends. The transport layer
persists a chunk to disk BEFORE calling ``_send_chunk``; on success the file
is unlinked; on retry-exhaustion the file is renamed back to a ``pending``
state so a periodic replay pass (and a synchronous startup pass) can retry it
on the next daemon boot.

File layout (under ``config.SPOOL_DIR``, mode 0o700):

    {created_epoch_ns}-{uid8}-{state}.json     mode 0o600

where ``state`` is either ``pending`` (queue-visible) or ``inflight-<pid>``
(a specific daemon process is retrying it right now — orphaned inflight
files from a dead pid are reclaimed at startup).

Concurrency invariants:
- The primary send-path (``_send_with_retry``) is the only writer for a
  file in its ``inflight-<pid>`` state; ``os.rename`` on POSIX is atomic.
- The background replay thread only touches files in ``pending`` state,
  and renames them to ``inflight-<pid>`` for the duration of its own retry
  — so send-path and replay-path cannot collide on the same file.
- Startup reclaim is called BEFORE the poller starts and BEFORE any
  StreamSender worker is constructed, so no live sends can interleave.

Design notes:
- Order is preserved per-daemon-boot via the ``created_epoch_ns`` prefix
  (sorted ascending). Cross-boot order between spooled and freshly-enqueued
  chunks is NOT preserved — at-least-once trumps ordering for this daemon's
  use case (the operator wants no lost messages more than perfect ordering).
- 400s from replay are unfixable (malformed payload) and get unlinked;
  every other non-2xx is treated as retryable and returned to pending.
- Corrupt spool files (unparseable JSON) are logged and unlinked.
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
from landline.logging import log


# The send_fn passed to ``replay_all`` accepts (chat_id, chunk, html_mode,
# label) and returns (ok, http_code). Callers close over the token (either
# self.token from TelegramDaemon or a keychain lookup in ``_send_chunk_raw``)
# so the spool module never handles credentials directly.
SendFn = Callable[[str, str, bool, str], Tuple[bool, Optional[int]]]


# -----------------------------------------------------------------------------
# Filename helpers
# -----------------------------------------------------------------------------

def _spool_filename(created_epoch_ns: int, uid: str, state: str) -> str:
    return "%d-%s-%s.json" % (created_epoch_ns, uid, state)


def _parse_spool_filename(name: str) -> Optional[Tuple[int, str, str]]:
    """Parse ``{created_ns}-{uid8}-{state}.json`` into its parts.

    Returns None for names that don't match the spool shape (so foreign
    files in the dir are safely ignored). ``state`` may be ``pending`` or
    ``inflight-<pid>`` — callers inspect the prefix.
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


# -----------------------------------------------------------------------------
# Directory & write primitives
# -----------------------------------------------------------------------------

def ensure_spool_dir() -> Path:
    """Create SPOOL_DIR at 0o700 idempotently.

    Called at startup (from ``__main__.main``) and defensively before each
    ``persist`` in case the dir was pruned by an external process.
    """
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(str(SPOOL_DIR), SPOOL_DIR_MODE)
    except OSError as e:
        log("outbound_spool: chmod %s failed: %r" % (SPOOL_DIR, e))
    return SPOOL_DIR


def persist(chat_id: str, chunk: str, html_mode: bool, label: str) -> str:
    """Persist an outbound chunk. Returns the spool_id (absolute path).

    Two-phase write: create the pending file at mode 0o600 + fsync, then
    rename to the ``inflight-<pid>`` state so the background replayer will
    NOT see it while the send-path is retrying. On any I/O failure the
    caller (``_send_with_retry``) swallows the exception and proceeds
    without persistence — at-least-once degrades to best-effort on
    disk-full rather than fail-closed on the send.
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

    # 0o600 write + fsync; O_EXCL guards against a uuid collision.
    fd = os.open(
        str(pending_path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        SPOOL_FILE_MODE,
    )
    # Once the pending file exists on disk, any subsequent failure (fsync,
    # chmod, rename) MUST unlink it before propagating — otherwise the
    # caller (``_send_with_retry_tracked``) swallows the OSError, proceeds
    # without a spool_id, delivers the chunk successfully over the network,
    # and the leaked pending file is picked up by the next replay pass as a
    # duplicate. Verified: fsync/chmod/rename can all raise on disk pressure
    # (ENOSPC, EIO) after the O_EXCL open already committed the inode.
    try:
        try:
            os.write(fd, json.dumps(payload).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        # Belt-and-suspenders: on macOS a race can leave the file at
        # umask-modified perms. Enforce 0o600 explicitly. chmod failure
        # is non-fatal — the O_EXCL open above already applied
        # SPOOL_FILE_MODE, so a chmod race is cosmetic; do NOT unlink on
        # chmod failure or we would drop a persisted chunk on a benign perm
        # jitter.
        try:
            os.chmod(str(pending_path), SPOOL_FILE_MODE)
        except OSError:
            pass
        os.rename(str(pending_path), str(inflight_path))
    except BaseException:
        # Unlink the leaked pending file BEFORE re-raising so replay can
        # never see a half-persisted duplicate. Suppress the unlink's own
        # errors — the original exception is what the caller needs.
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

    Used when the caller has decided the logical message this file represents
    is being superseded by an alternate variant (e.g. ``send_response``'s
    plain-text fallback for a chunk whose HTML variant just failed) and the
    original must NOT be replayed. Without this, the failed HTML variant sits
    in ``pending`` state alongside the freshly-persisted plain-text variant,
    and both get delivered by the next replay pass → user sees the same
    logical message twice.

    Matches by ``created_ns`` + ``uid`` prefix so it works whether the file
    is currently ``inflight-<pid>`` or has already been renamed to
    ``pending`` by ``mark_failed``. Idempotent for missing files.
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
    """Rename ``inflight-<pid>`` back to ``pending``.

    Called by ``_send_with_retry`` after all in-call retries are exhausted;
    the periodic replay thread will pick it up next pass. Idempotent for
    the missing-file case.
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


# -----------------------------------------------------------------------------
# Startup reclaim
# -----------------------------------------------------------------------------

def startup_reclaim_orphaned_inflight() -> int:
    """Rename any ``inflight-<pid>`` files back to ``pending``.

    The pid that owned each file died with the previous daemon process, so
    no live retry is in progress. Called once at startup BEFORE
    ``replay_all`` so the initial replay pass sees the reclaimed files.
    Returns the count of renamed files (for the startup log line).
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


# -----------------------------------------------------------------------------
# Replay pass
# -----------------------------------------------------------------------------

def _list_pending_sorted() -> List[Tuple[int, Path]]:
    """Enumerate ``pending`` files, sorted by created_epoch_ns ascending."""
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
    """If pending exceeds SPOOL_MAX_FILES, drop the oldest excess (keep newest)."""
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
            # Too young — may still be in-flight on the primary send-path.
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

        # Increment attempts counter and rewrite the payload so a future
        # log line can name it; best-effort.
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

        # Any 4xx that we cannot recover from by retrying belongs to the
        # unlink branch, not the retry branch. Retrying a bad-token 401 or
        # a bot-kicked-from-chat 403 or a chat-not-found 404 just burns
        # ~1440 API calls per file per 24h (SPOOL_MAX_AGE_SECONDS) with
        # zero chance of success. 429s (rate limit) and 5xx (transient
        # server errors) are the only genuinely-retryable non-2xx codes,
        # so we default to "drop" for the rest.
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

        # Retryable failure — rename back to pending for the next pass.
        pending_name = _spool_filename(p_created_ns, uid, "pending")
        try:
            os.rename(str(inflight_path), str(SPOOL_DIR / pending_name))
        except OSError as e:
            log("outbound_spool: return-to-pending failed for %s: %r" %
                (inflight_path.name, e))


# -----------------------------------------------------------------------------
# Background replayer
# -----------------------------------------------------------------------------

class OutboundSpoolReplayer:
    """Background thread that runs ``replay_all`` on a fixed interval.

    Started by ``TelegramDaemon.run()`` after ``_handle_restart_continuation``
    and before ``_background_poller.start()``. Stopped by the daemon
    shutdown handler via ``stop()`` (level-triggered Event; the current
    replay pass finishes before the loop exits).
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
            # Event.wait returns True on set — bounds shutdown latency to the
            # remainder of the current sleep window.
            self._stop.wait(SPOOL_REPLAY_INTERVAL_SECONDS)
