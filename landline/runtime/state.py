"""State persistence and conversation logging for the daemon.

Handles atomic JSON state saves, flock-guarded conversation log appends,
and reading recent conversation history for session continuity.
"""

import fcntl
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from landline.config import (
    AGENT_NAME,
    CONTEXT_WINDOW_TOKENS,
    CONVERSATION_LOG_TAIL_BYTES,
    DAILY_LOG_DIR_MODE,
    DAILY_LOG_FILE_MODE,
    TIMEZONE,
    SESSION_JSONL_TAIL_BYTES,
    STATE_FILE,
    STATE_FILE_MODE,
    USER_NAME,
    WORKSPACE,
    WORKSPACE_SENSITIVE_DIR_MODE,
    WORKSPACE_SENSITIVE_DIRS,
)
from landline.runtime.logging import log


def encode_cc_project_dir(workspace: Path) -> str:
    """Encode a workspace path the way Claude Code names its projects dir.

    Claude Code stores per-project JSONL transcripts under
    ``~/.claude/projects/<encoded>/<session_id>.jsonl`` where ``<encoded>``
    is the absolute workspace path with ``/`` and ``.`` both replaced by
    ``-``. Verified empirically: a workspace like ``/Users/alice/.agent-ws``
    encodes to ``-Users-alice--agent-ws`` (the double dash comes from the
    ``.`` in ``.agent-ws``).
    """
    return str(workspace).replace("/", "-").replace(".", "-")


# C4 - override via LANDLINE_CC_PROJECT_DIR for tests / sandboxed runs; otherwise
# derive from WORKSPACE so the daemon works on any host.
# Read at import-time only (invariant: no per-call env reads for paths).
_PROJECT_DIR_OVERRIDE = os.environ.get("LANDLINE_CC_PROJECT_DIR")
PROJECT_DIR: Path = (
    Path(_PROJECT_DIR_OVERRIDE)
    if _PROJECT_DIR_OVERRIDE
    else Path.home() / ".claude" / "projects" / encode_cc_project_dir(WORKSPACE)
)

_save_state_lock = threading.Lock()


def secure_workspace_paths() -> None:
    """One-shot startup backfill: chmod daemon-owned workspace dirs + PII files.

    Two-layer tightening, both idempotent:

    1. Daily-log PII: ``memory/daily/`` is chmodded to 0o700 and every
       ``*_telegram.md`` file inside to 0o600. These logs contain the
       unredacted user<->agent conversation.
    2. Workspace-sensitive top-level dirs (config.WORKSPACE_SENSITIVE_DIRS):
       ``memory/``, ``cache/``, ``inbox/``, ``outbox/``, ``logs/`` receive
       ``WORKSPACE_SENSITIVE_DIR_MODE`` (0o700). We do NOT recurse — that
       would touch files owned by other tools (search indexes, cron
       artifacts) and is out of scope. The top-level chmod is what stops a
       drive-by ``ls <workspace>/memory`` from another local user.

    Per-entry errors are logged and swallowed; a single stuck NFS/SMB mount
    or missing dir must never block the daemon from starting.
    """
    # Layer 2 first (workspace-sensitive dirs). Runs before layer 1 so that
    # the memory/ chmod happens before we touch memory/daily/, in case a
    # future extension wants to observe the pre-tightening state on the
    # inner dir.
    for dir_name in WORKSPACE_SENSITIVE_DIRS:
        target = WORKSPACE / dir_name
        if not target.exists():
            log(f"secure_workspace_paths: {dir_name}/ missing, skipping")
            continue
        try:
            os.chmod(str(target), WORKSPACE_SENSITIVE_DIR_MODE)
        except OSError as ws_error:
            log(
                f"secure_workspace_paths: failed to chmod {target}: "
                f"{ws_error!r}"
            )

    # Layer 1: daily-log PII (preserves the pre-cluster behaviour that
    # `secure_daily_logs` covered).
    daily_dir = WORKSPACE / "memory" / "daily"
    if not daily_dir.exists():
        return
    try:
        os.chmod(str(daily_dir), DAILY_LOG_DIR_MODE)
    except OSError as dir_error:
        log(f"secure_workspace_paths: failed to chmod {daily_dir}: {dir_error!r}")
    for path in daily_dir.glob("*_telegram.md"):
        try:
            os.chmod(str(path), DAILY_LOG_FILE_MODE)
        except OSError as file_error:
            log(
                f"secure_workspace_paths: failed to chmod {path.name}: "
                f"{file_error!r}"
            )


def secure_daily_logs() -> None:
    """Back-compat wrapper — delegates to :func:`secure_workspace_paths`.

    ``daemon/__main__.py`` still imports this name. The rename is a
    surface-only shuffle; behaviour is a strict superset of the pre-cluster
    version (daily-log tightening is preserved; workspace-sensitive dir
    chmodding is added).
    """
    secure_workspace_paths()


def load_state() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "session_id": None,
        "last_update_id": 0,
        "turn_count": 0,
        "failed_unlock_attempts": 0,
        "unlock_lockout_until": 0.0,
        "unlock_timestamp": 0.0,
    }
    try:
        raw = STATE_FILE.read_text()
    except FileNotFoundError:
        # First run / missing file is expected — silently return defaults.
        return dict(defaults)
    except Exception as read_error:
        # Read failed on an existing file. Silently resetting would lose
        # last_update_id (Telegram re-delivers a backlog), unlock_lockout_until
        # (defeats brute-force lockout), and session_id (loses Claude
        # continuity). Back up the file and log loudly instead.
        _backup_corrupt_state(read_error)
        return dict(defaults)
    try:
        state = json.loads(raw)
    except Exception as parse_error:
        _backup_corrupt_state(parse_error)
        return dict(defaults)
    for key, default_value in defaults.items():
        state.setdefault(key, default_value)
    return state


def _backup_corrupt_state(error: BaseException) -> None:
    """Rename a corrupt STATE_FILE to a ``.corrupt`` sibling and log loudly.

    Uses ``os.replace`` so the original bytes are preserved at the backup path
    and the original is removed (next ``load_state`` will hit the missing-file
    path, not the same corruption again). If a prior ``.corrupt`` already
    exists, it is overwritten — we keep this simple rather than versioning.
    Any failure during the backup itself is logged but swallowed so the daemon
    can still start.
    """
    backup = STATE_FILE.with_suffix(STATE_FILE.suffix + ".corrupt")
    try:
        os.replace(STATE_FILE, backup)
        log(
            f"load_state: corrupt state file {STATE_FILE} ({error!r}); "
            f"backed up to {backup}, returning defaults"
        )
    except OSError as backup_error:
        log(
            f"load_state: corrupt state file {STATE_FILE} ({error!r}); "
            f"backup to {backup} also failed ({backup_error!r}); returning defaults"
        )


def save_state(state: Dict[str, Any]) -> None:
    """Atomic save: write to a temp file, then os.replace onto the target.

    OSError is caught and logged rather than propagated — keeping the daemon
    alive is preferred over crashing on a transient disk-full condition.
    """
    with _save_state_lock:
        tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            # B4 - race-free 0o600 creation: the mode arg to os.open is umask-
            # subject, so we MUST follow with fchmod to guarantee final mode.
            fd = os.open(
                str(tmp),
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                STATE_FILE_MODE,
            )
            os.fchmod(fd, STATE_FILE_MODE)
            with os.fdopen(fd, "w") as f:
                f.write(json.dumps(state, indent=2))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, STATE_FILE)
            # C1 - durably persist the rename itself (POSIX: fsync the parent
            # dir to flush directory-metadata so the new dirent survives crash/
            # power-loss). Best-effort: some filesystems (BSD, some FUSE) don't
            # support fsync on a directory fd and return OSError - don't crash
            # the daemon over it.
            try:
                dir_fd = os.open(str(STATE_FILE.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError as dir_fsync_error:
                log(f"save_state: parent-dir fsync skipped ({dir_fsync_error!r})")
        except OSError as save_state_error:
            # C2 - leave the tmp in place for forensics: disk-full self-limits
            # (next save overwrites it via truncate), and the bytes that didn't
            # make it onto STATE_FILE are the only evidence of the failure for
            # an operator to inspect.
            log(
                f"save_state OSError: {save_state_error}; "
                f"tmp left at {tmp} for inspection"
            )


def log_conversation(role: str, text: str) -> None:
    """Append a conversation turn to today's telegram log file.

    B4 - creates the file (and parent dir) with restrictive permissions
    (file 0o600, dir 0o700) via os.open + os.fchmod - process-wide umask
    is intentionally NOT touched (would race with concurrent file creation
    in poller/sender threads).

    Uses advisory fcntl.flock on the open file descriptor so two writers
    can't interleave bytes.
    """
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    log_path = WORKSPACE / "memory" / "daily" / f"{today}_telegram.md"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Idempotent: harmless re-chmod after the first call each session.
        try:
            os.chmod(str(log_path.parent), DAILY_LOG_DIR_MODE)
        except OSError as dir_chmod_error:
            log(f"log_conversation: dir chmod failed: {dir_chmod_error!r}")
        ts = datetime.now(TIMEZONE).strftime("%H:%M")
        # Race-free 0o600 creation: the mode arg to os.open is umask-subject,
        # so we MUST follow with fchmod to guarantee the final mode. Also
        # tightens any pre-existing loose file mode (0644 backfill case).
        fd = os.open(
            str(log_path),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            DAILY_LOG_FILE_MODE,
        )
        os.fchmod(fd, DAILY_LOG_FILE_MODE)
        with os.fdopen(fd, "a") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            if os.fstat(f.fileno()).st_size == 0:
                f.write(
                    f"# Telegram Conversation: {today}\n\n"
                    "- **Source**: telegram-daemon\n\n## Messages\n\n"
                )
            f.write(f"**{role}** ({ts}): {text}\n\n")
    except Exception as e:
        log(f"Log write error: {e}")


def _read_file_tail(log_path: Path, tail_bytes: int) -> str:
    """Read up to the last ``tail_bytes`` of ``log_path`` as decoded text.

    Seeks near the end of the file rather than loading the whole thing.
    On a partial-line boundary (which is the common case), the first line
    of the returned text may be a fragment — callers must drop it.
    """
    with open(log_path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        if size <= tail_bytes:
            f.seek(0)
        else:
            f.seek(size - tail_bytes)
        raw = f.read()
    return raw.decode("utf-8", errors="replace")


def read_recent_conversation_history(max_turns: int = 20) -> str:
    """Read recent conversation from today's telegram log for session continuity.

    When a session resets mid-day (via /new or stale-session fallback), the
    new Claude session has no context. This reads the last N turns from today's
    telegram log and returns them as injectable context.

    To stay fast on active days (the log can grow well past 500KB), only the
    tail of the file is read.  When truncation occurs at the tail boundary,
    the result is trimmed so the kept slice starts with a user turn — never
    a dangling agent response without the prompt that produced it.
    """
    today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
    log_path = WORKSPACE / "memory" / "daily" / f"{today}_telegram.md"
    if not log_path.exists():
        return ""
    try:
        tail_text = _read_file_tail(log_path, CONVERSATION_LOG_TAIL_BYTES)
        # If we seeked into the middle of a line, drop the leading fragment.
        try:
            file_size = log_path.stat().st_size
        except OSError:
            file_size = len(tail_text.encode("utf-8", errors="replace"))
        tail_was_truncated = file_size > CONVERSATION_LOG_TAIL_BYTES

        lines = tail_text.splitlines()
        if tail_was_truncated and lines:
            # First line may be a partial fragment of a longer line.
            lines = lines[1:]

        content_lines = [line for line in lines if line.startswith("**")]
        if not content_lines:
            return ""

        # Slice to at most max_turns*2 (one line per role).
        max_lines = max_turns * 2
        recent = content_lines[-max_lines:] if len(content_lines) > max_lines else list(content_lines)

        # Pair alignment: keep complete user+agent pairs.  If we truncated and
        # the first kept line is an agent response, drop it so the slice starts
        # with the user turn it answers.
        truncated_by_count = len(content_lines) > max_lines
        if (truncated_by_count or tail_was_truncated) and recent:
            if recent[0].startswith(f"**{AGENT_NAME}"):
                recent = recent[1:]

        omitted_count = len(content_lines) - len(recent) if truncated_by_count else 0

        preamble = (
            f"<system>\n"
            f"This is the last {max_turns} turns of the most recent session. "
            f"There may be more important context that has been truncated. "
            f"When in doubt, read the original at: {log_path}\n"
            f"</system>"
        )
        if truncated_by_count and omitted_count > 0:
            preamble += f"\n[... {omitted_count} earlier messages omitted ...]"
        return preamble + "\n\n" + "\n".join(recent)
    except Exception as history_read_error:
        log(f"Failed to read conversation history: {history_read_error}")
        return ""


def get_context_percent(session_id: Optional[str]) -> Optional[float]:
    """Read the last assistant message's usage from the session JSONL tail.

    Only reads the final 32KB of the file (not the whole thing), then scans
    backwards for the last assistant message with usage data.
    Returns context usage as a percentage of CONTEXT_WINDOW_TOKENS,
    or None if the session file doesn't exist or has no usage data.
    """
    if not session_id:
        return None
    path = PROJECT_DIR / f"{session_id}.jsonl"
    if not path.exists():
        return None
    try:
        size = path.stat().st_size
        with open(path, "rb") as f:
            f.seek(max(0, size - SESSION_JSONL_TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="replace")
        last_usage = None
        for line in tail.splitlines():
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if entry.get("type") != "assistant":
                continue
            usage = entry.get("message", {}).get("usage")
            if usage:
                last_usage = usage
        if not last_usage:
            return None
        context_used = (
            last_usage.get("input_tokens", 0)
            + last_usage.get("cache_read_input_tokens", 0)
            + last_usage.get("cache_creation_input_tokens", 0)
        )
        return (context_used / CONTEXT_WINDOW_TOKENS) * 100
    except Exception as e:
        log(f"get_context_percent error: {e}")
        return None
