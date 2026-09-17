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
import time
import uuid
from typing import Deque, List, Optional

from landline.config import (
    CLAUDE,
    CLAUDE_MODEL,
    CLAUDE_PERMISSION_MODE,
    CLAUDE_STDIN_WRITE_TIMEOUT_SECONDS,
    STDERR_BUFFER_MAX,
    WORKSPACE,
)
from landline.runtime.logging import log


# Escalation grace windows for _terminate_with_escalation. Extracted from
# kill()'s original 5/3 literals so operators can grep the ceilings; the
# streaming.py interrupt branch now shares them too (its post-SIGTERM wait
# was 3s before this consolidation — unified to 5s, a slightly gentler grace
# before SIGKILL, and it runs on the watchdog thread, not the main loop).
_TERM_GRACE_SECONDS = 5
_KILL_GRACE_SECONDS = 3
# Post-timeout join for the stdin writer thread: give the (now-unblocked,
# closed-fd) writer a beat to observe the closed pipe and exit. Bounded so
# a laggard never blocks send_message from returning; it is a daemon
# thread and will exit with the process regardless.
_STDIN_WRITER_JOIN_GRACE_SECONDS = 1.0


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
        """Write one NDJSON turn on stdin, bounded by
        ``CLAUDE_STDIN_WRITE_TIMEOUT_SECONDS``.

        A full pipe (Claude stopped draining stdin) would freeze the main
        loop here indefinitely — BEFORE the streaming watchdog is armed,
        so the existing silence caps never fire. F3 wraps the write on a
        helper thread and, on timeout, kills the child (closes stdin,
        releasing the blocked writer) and raises so the dispatcher records
        a failure and can retry with a fresh process.
        """
        if not self.is_alive or self._proc is None:
            raise RuntimeError("Process not alive")
        msg = json.dumps({
            "type": "user",
            "message": {"role": "user", "content": text},
            "session_id": "",
            "parent_tool_use_id": None,
        })
        assert self._proc.stdin is not None
        stdin = self._proc.stdin
        payload = msg + "\n"

        # Container mutated by the helper thread so the caller can see any
        # exception the write itself raised (BrokenPipeError, OSError...).
        error_holder: List[Optional[BaseException]] = [None]

        def _do_write() -> None:
            try:
                stdin.write(payload)
                stdin.flush()
            except BaseException as write_error:
                error_holder[0] = write_error

        writer = threading.Thread(
            target=_do_write, daemon=True, name="claude-stdin-write",
        )
        writer.start()
        writer.join(CLAUDE_STDIN_WRITE_TIMEOUT_SECONDS)
        if writer.is_alive():
            log(
                "stdin write did not complete in %ss — killing Claude"
                % CLAUDE_STDIN_WRITE_TIMEOUT_SECONDS
            )
            try:
                self.kill()
            except Exception as kill_error:
                log(f"kill after stdin-write timeout failed: {kill_error}")
            # Give the (now-unblocked, closed-fd) writer thread a beat to
            # observe the closed pipe and exit before we return; it's a
            # daemon thread either way, so a laggard won't block shutdown.
            writer.join(_STDIN_WRITER_JOIN_GRACE_SECONDS)
            raise RuntimeError(
                "stdin write timed out after "
                f"{CLAUDE_STDIN_WRITE_TIMEOUT_SECONDS}s"
            )
        if error_holder[0] is not None:
            raise error_holder[0]

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

    def _terminate_with_escalation(
        self,
        proc: subprocess.Popen,
        term_grace: float = _TERM_GRACE_SECONDS,
        kill_grace: float = _KILL_GRACE_SECONDS,
    ) -> None:
        """Escalate SIGTERM -> wait -> SIGKILL -> wait; LOUD ERROR if alive.

        A child stuck in a C-level syscall (hung MCP socket, runaway subagent
        holding fds) can survive SIGTERM and SIGKILL both. Two bounded waits
        with a final ERROR log make that pathology visible instead of
        silently swallowed under a bare ``except: pass``. Never raises: every
        step's exception is logged and the escalation continues to the next.

        Called by ``kill()`` and by ``streaming.py``'s interrupt-branch
        SIGKILL escalation — one dialect across both call sites so an
        operator greps a single wording for this class of failure.
        """
        start = time.time()
        try:
            proc.terminate()
        except Exception as term_err:
            log(f"SIGTERM failed: {term_err}")
        try:
            proc.wait(timeout=term_grace)
            return
        except Exception:
            # SIGTERM did not land within ``term_grace`` — escalate.
            pass
        try:
            proc.kill()
        except Exception as kill_err:
            log(f"SIGKILL failed: {kill_err}")
        try:
            proc.wait(timeout=kill_grace)
            return
        except Exception:
            # Still alive after SIGKILL escalation — this is a loud
            # operational event, so a single ERROR line names pid, poll()
            # and elapsed seconds. Mirrors the wording dialect used in
            # streaming.py's watchdog SIGKILL branch.
            elapsed = time.time() - start
            log(
                "ERROR: Claude PID %s did not exit after SIGKILL "
                "escalation (poll=%s, elapsed=%.1fs)" % (
                    getattr(proc, "pid", "?"),
                    proc.poll(),
                    elapsed,
                )
            )

    def kill(self) -> None:
        if self._proc is not None:
            self._terminate_with_escalation(self._proc)
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
