"""Persistent Claude subprocess — long-lived stream-json process manager.

One Claude Code child accepting many turns via NDJSON on stdin. Spawns /
respawns on demand, drains stderr in a background thread, exposes
SIGINT-based interrupt and clean kill.
"""

import collections
import json
import os
import shutil
import signal
import subprocess
import threading
import uuid
from typing import Deque, List, Optional

from landline.config import (
    CLAUDE,
    CLAUDE_MODEL,
    CLAUDE_PERMISSION_MODE,
    STDERR_BUFFER_MAX,
    WORKSPACE,
)
from landline.runtime.logging import log


def _resolve_claude_binary() -> str:
    """Concrete path to invoke as the Claude CLI.

    - Absolute paths pass through (launchd's minimal PATH).
    - Bare names resolve via ``shutil.which`` at spawn time so a missing
      binary raises a clear ``RuntimeError`` instead of Popen's opaque
      ``FileNotFoundError``.
    """
    if os.path.isabs(str(CLAUDE)):
        return str(CLAUDE)
    resolved = shutil.which(str(CLAUDE))
    if resolved is None:
        raise RuntimeError(
            "landline: claude binary %r not found on PATH; set 'claude_binary' "
            "in landline.json to an absolute path" % (str(CLAUDE),)
        )
    return resolved


class PersistentClaude:
    """Manages a long-lived Claude Code subprocess with stream-json I/O."""

    def __init__(self) -> None:
        self._proc: Optional[subprocess.Popen] = None
        self._session_id: Optional[str] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._stderr_buf: Deque[str] = collections.deque()
        self._stderr_total_len: int = 0
        self._stderr_lock = threading.Lock()
        self._lock = threading.Lock()

    @property
    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def session_id(self) -> Optional[str]:
        # Back-compat shim; new code should call ``get_session_id()``.
        return self.get_session_id()

    def get_session_id(self) -> Optional[str]:
        """Cached session id (None if no session yet).

        Lock-guarded against concurrent ``_spawn`` / ``set_session_id``
        writers so a reader can't observe a torn value.
        """
        with self._lock:
            return self._session_id

    def _spawn(self, session_id: Optional[str] = None, is_new: bool = False) -> subprocess.Popen:
        cmd: List[str] = [_resolve_claude_binary(), "-p"]
        if CLAUDE_MODEL is not None:
            cmd.extend(["--model", CLAUDE_MODEL])
        cmd.extend([
            "--permission-mode", CLAUDE_PERMISSION_MODE,
            "--output-format", "stream-json",
            "--input-format", "stream-json",
            "--verbose",
        ])
        if is_new:
            new_sid = str(uuid.uuid4())
            cmd.extend(["--session-id", new_sid])
            self._session_id = new_sid
        elif session_id:
            cmd.extend(["--resume", session_id])

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=str(WORKSPACE),
        )

        self._stderr_buf = collections.deque()
        self._stderr_total_len = 0
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, args=(proc,), daemon=True,
        )
        self._stderr_thread.start()

        return proc

    def _drain_stderr(self, proc: subprocess.Popen) -> None:
        try:
            assert proc.stderr is not None
            for line in proc.stderr:
                with self._stderr_lock:
                    self._stderr_buf.append(line)
                    self._stderr_total_len += len(line)
                    while self._stderr_total_len > STDERR_BUFFER_MAX and len(self._stderr_buf) > 1:
                        dropped = self._stderr_buf.popleft()
                        self._stderr_total_len -= len(dropped)
        except Exception as e:
            # Log before exiting so a silently-broken drainer isn't invisible.
            # Common benign cause: watchdog closed stderr after process died.
            log(f"_drain_stderr exited: {e}")

    def clear_session(self) -> None:
        # Funnel through set_session_id so all writes take the same lock.
        self.set_session_id(None)

    def set_session_id(self, sid: Optional[str]) -> None:
        """Single writer entry point for the session id.

        Called by the dispatcher to publish a new id, clear on stale-resume,
        and lazy-seed from persisted state on first dispatch.
        """
        with self._lock:
            self._session_id = sid

    def ensure_alive(self, session_id: Optional[str] = None, force_new: bool = False) -> subprocess.Popen:
        """Ensure a live process exists, spawning or respawning as needed."""
        with self._lock:
            if force_new:
                if self.is_alive:
                    self.kill()
                self._session_id = None
                log("Spawning persistent Claude (new session)")
                self._proc = self._spawn(is_new=True)
                return self._proc

            if self.is_alive:
                assert self._proc is not None
                return self._proc

            if session_id:
                log(f"Spawning persistent Claude (resume {session_id[:12]}...)")
                self._proc = self._spawn(session_id=session_id)
            elif self._session_id:
                log(f"Respawning persistent Claude (resume {self._session_id[:12]}...)")
                self._proc = self._spawn(session_id=self._session_id)
            else:
                log("Spawning persistent Claude (new session)")
                self._proc = self._spawn(is_new=True)

            return self._proc

    def send_message(self, text: str) -> None:
        if not self.is_alive or self._proc is None:
            raise RuntimeError("Process not alive")
        msg = json.dumps({
            "type": "user",
            "message": {"role": "user", "content": text},
            "session_id": "",
            "parent_tool_use_id": None,
        })
        assert self._proc.stdin is not None
        self._proc.stdin.write(msg + "\n")
        self._proc.stdin.flush()

    def interrupt(self) -> None:
        # Snapshot ``self._proc`` under the lock, then release before
        # signalling. Otherwise a concurrent respawn could swap the proc
        # between ``is_alive`` and ``os.kill`` → SIGINT the fresh process.
        with self._lock:
            proc = self._proc
            if proc is None or proc.poll() is not None:
                return
            pid = proc.pid
        log("Sending SIGINT to persistent Claude")
        try:
            os.kill(pid, signal.SIGINT)
        except Exception as e:
            log(f"SIGINT failed: {e}")

    def kill(self) -> None:
        if self._proc is not None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=5)
            except Exception:
                try:
                    self._proc.kill()
                    self._proc.wait(timeout=3)
                except Exception:
                    pass
            self._close_pipes()

    def _close_pipes(self) -> None:
        if self._proc is not None:
            for stream in (self._proc.stdin, self._proc.stdout, self._proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except Exception:
                        pass

    def get_stderr_tail(self) -> str:
        with self._stderr_lock:
            return "".join(self._stderr_buf)[-STDERR_BUFFER_MAX:]


_persistent_claude: Optional[PersistentClaude] = None
_persistent_claude_lock = threading.Lock()


def _get_persistent_claude() -> PersistentClaude:
    global _persistent_claude
    if _persistent_claude is None:
        with _persistent_claude_lock:
            if _persistent_claude is None:
                _persistent_claude = PersistentClaude()
    return _persistent_claude
