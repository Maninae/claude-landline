"""Entry point for the Landline Telegram daemon.

Usage: python3 -m landline (run from your agent workspace, or set LANDLINE_WORKSPACE)

Handles PID locking, fatal crash reporting, and launchd restart pacing.
"""

import fcntl
import os
import signal
import sys
import time
import traceback

from landline.telegram import spool as outbound_spool
from landline.claude import ClaudeStreamShutdownHook, run_claude_streaming
from landline.telegram import send_response, send_typing
from landline.config import AGENT_NAME, FATAL_CRASH_PAUSE_SECONDS, STATE_FILE, WORKSPACE
from landline.claude.failure_tracker import ClaudeFailureTracker
from landline.runtime.guard import is_allowed, reject_message
from landline.runtime.logging import log
from landline.orchestrator import TelegramDaemon
from landline.runtime.state import secure_daily_logs


_PID_LOCK_FILE = WORKSPACE / "cache" / "telegram-daemon.pid"


def _acquire_singleton_lock():
    """Acquire an exclusive flock on the PID file to prevent dual instances.
    Returns the open file handle (must stay open for the lock to hold).
    Exits with code 0 if another instance holds the lock."""
    _PID_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(_PID_LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log("Another daemon instance is already running — exiting")
        lock_fd.close()
        sys.exit(0)
    lock_fd.write(str(os.getpid()))
    lock_fd.flush()
    return lock_fd


def _emit_fatal_crash_report(fatal_traceback_text: str) -> None:
    crash_report_message = (
        "FATAL: unhandled exception in main(); pausing before exit to avoid "
        "launchd crash loop\n" + fatal_traceback_text
    )
    try:
        log(crash_report_message)
    except Exception:
        try:
            sys.stderr.write(crash_report_message + "\n")
            sys.stderr.flush()
        except Exception:
            pass


def _pause_before_launchd_restart() -> None:
    try:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    except Exception:
        pass
    time.sleep(FATAL_CRASH_PAUSE_SECONDS)


def main() -> None:
    lock_fd = _acquire_singleton_lock()
    try:
        log("=" * 40)
        log(f"Landline ({AGENT_NAME}) — Telegram daemon (streaming, modular)")
        log(f"Workspace: {WORKSPACE}")
        log(f"State: {STATE_FILE}")
        log("=" * 40)

        secure_daily_logs()

        # Reclaim orphaned inflight spool files from a prior process (pid
        # died with it) so the background replayer sees them as pending.
        # Cheap dir scan + rename — no network I/O.
        # Do NOT add a synchronous `replay_all` pass here; see
        # ARCHITECTURE.md "Outbound spool" — it can block startup for
        # tens of minutes when Telegram is unreachable. Background
        # `OutboundSpoolReplayer` covers replay from `TelegramDaemon.run`.
        outbound_spool.ensure_spool_dir()
        reclaimed = outbound_spool.startup_reclaim_orphaned_inflight()
        if reclaimed:
            log(
                f"Reclaimed {reclaimed} orphaned inflight spool file(s) "
                f"from previous run"
            )

        shutdown_hook = ClaudeStreamShutdownHook()
        failure_tracker = ClaudeFailureTracker()

        daemon = TelegramDaemon(
            run_claude_fn=run_claude_streaming,
            shutdown_hook=shutdown_hook,
            failure_tracker=failure_tracker,
            send_response_fn=send_response,
            send_typing_fn=send_typing,
            guard_fn=is_allowed,
            reject_fn=reject_message,
        )
        daemon.run()
    except KeyboardInterrupt:
        raise
    except SystemExit as system_exit_exception:
        system_exit_code = system_exit_exception.code
        if system_exit_code is None or system_exit_code == 0:
            raise
        fatal_traceback_text = traceback.format_exc()
        _emit_fatal_crash_report(fatal_traceback_text)
        _pause_before_launchd_restart()
        raise
    except Exception:
        fatal_traceback_text = traceback.format_exc()
        _emit_fatal_crash_report(fatal_traceback_text)
        _pause_before_launchd_restart()
        raise
    finally:
        lock_fd.close()


if __name__ == "__main__":
    main()
