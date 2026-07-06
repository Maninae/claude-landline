"""Command handlers for /new, /status, /doctor, and unknown commands.

Accepts its dependencies explicitly — no coupling to the orchestrator class.
"""

import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from landline.config import (
    AGENT_NAME,
    DOCTOR_SCRIPT,
    LAUNCHD_LABEL_PREFIX,
    MORNING_BRIEF_GLOB,
    WORKSPACE,
)
from landline.runtime.lock import LockManager
from landline.runtime.logging import log


def _parse_command(text: str) -> Tuple[str, str]:
    """Split a message into (command, argument). Returns ("", "") if empty."""
    stripped = text.strip()
    parts = stripped.split(None, 1)
    if not parts:
        return ("", "")
    return (parts[0].lower(), parts[1] if len(parts) > 1 else "")


def _status_text(
    state: Dict[str, Any],
    lock_manager: LockManager,
    workspace: Path,
) -> str:
    """Build the /status response. Runs subprocesses for system info."""
    lines = [f"**{AGENT_NAME} System Status**\n"]

    try:
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True, text=True, timeout=3,
        )
        jobs = [
            line for line in result.stdout.splitlines()
            if LAUNCHD_LABEL_PREFIX in line
        ]
        running = sum(1 for job in jobs if job.split()[0] != "-")
        lines.append(f"Scheduled jobs: {len(jobs)} loaded, {running} currently running")
    except Exception:
        lines.append("Scheduled jobs: unable to check")

    # Defensive: a bad glob or inaccessible dir must never break /status —
    # operator loses the brief line, not their diagnostics.
    if MORNING_BRIEF_GLOB:
        try:
            briefs = sorted(workspace.glob(MORNING_BRIEF_GLOB))
            if briefs:
                lines.append(f"Last morning brief: {briefs[-1].name}")
        except Exception as brief_error:
            log(f"/status: morning brief glob failed: {brief_error}")

    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%s (%cr)"],
            capture_output=True, text=True, timeout=3, cwd=str(workspace),
        )
        if result.stdout.strip():
            lines.append(f"Last backup: {result.stdout.strip()}")
    except Exception:
        pass

    session_id = state.get("session_id")
    turns = state.get("turn_count", 0)
    session_id_display = (session_id[:12] + "...") if session_id else "none"
    lines.append(f"Session: {session_id_display} ({turns} turns)")

    # Today's usage/cost line if any; missing file → "". Defensive so a
    # broken stats file never breaks /status.
    try:
        from landline.runtime import usage_stats
        stats_line = usage_stats.format_status_line()
        if stats_line:
            lines.append(stats_line)
    except Exception as stats_error:
        log(f"/status: usage_stats.format_status_line failed: {stats_error}")

    lines.append(lock_manager.unlock_status_line())

    return "\n".join(lines)


class CommandRouter:
    """Routes slash commands to their handlers.

    Instantiated once at daemon startup with its dependencies.  The
    orchestrator calls handle() for every message starting with '/'.
    """

    def __init__(
        self,
        state: Dict[str, Any],
        lock_manager: LockManager,
        persist_state_fn: Callable[[Dict[str, Any]], None],
        workspace: Path = WORKSPACE,
        reset_claude_fn: Optional[Callable[[], None]] = None,
    ) -> None:
        self._state = state
        self._lock_manager = lock_manager
        self._persist_state = persist_state_fn
        self._workspace = workspace
        # Reset callback: kills the live PersistentClaude child + clears its
        # session id so the next dispatch spawns a fresh Claude. Optional for
        # test back-compat; production always wires it (else /new would leave
        # the subprocess on the old session).
        self._reset_claude_fn = reset_claude_fn

    def handle(self, text: str) -> Optional[str]:
        """Process a slash command. Returns reply text, or None if not a command."""
        cmd, arg = _parse_command(text)
        if not cmd.startswith("/"):
            return None

        if cmd == "/new":
            return self._handle_new()

        if cmd == "/status":
            return _status_text(self._state, self._lock_manager, self._workspace)

        if cmd == "/doctor":
            return self._handle_doctor(arg)

        return f"Unknown command: {cmd}"

    def _handle_doctor(self, issue_text: str) -> str:
        """Launch the configured doctor script detached and ack immediately.

        The doctor is a separate diagnostic session with its own logging and
        report delivery — the router only spawns it. Detached via
        ``start_new_session`` so a daemon restart can't kill a run in flight,
        and streams go to DEVNULL so the child can never block on a dead pipe.
        The operator's issue text rides as a single argv element (no shell).

        Lock-gated: unlike /status, the doctor can CHANGE the system (it
        applies safe fixes), so a locked session may not launch it.
        """
        if self._lock_manager.is_locked:
            return "🩺 /doctor is available after unlock."
        if not DOCTOR_SCRIPT:
            return (
                "🩺 /doctor isn't configured. Set \"doctor_script\" in "
                "landline.json to an executable that runs the diagnostic "
                "session (see docs/SETUP.md)."
            )
        script = Path(DOCTOR_SCRIPT)
        if not script.exists():
            return f"🩺 doctor_script not found: {script}"
        argv = [str(script)]
        if issue_text:
            argv.append(issue_text)
        try:
            subprocess.Popen(
                argv,
                cwd=str(self._workspace),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as spawn_error:
            log(f"/doctor: spawn failed: {spawn_error}")
            return f"🩺 Failed to launch the doctor: {spawn_error}"
        # PII rule: log the dispatch and the text's size, never the text.
        log(f"/doctor dispatched ({len(issue_text)} chars of issue text)")
        return (
            "🩺 Doctor session dispatched. The report will arrive here when "
            "it finishes (typically a few minutes)."
        )

    def _handle_new(self) -> str:
        """Reset session state, re-lock, and reset the live Claude subprocess.

        - `PersistentClaude` owns the live session id (single source of truth),
          so resetting `state` alone would leave the singleton on the old sid
          and the next `--resume` would keep the same conversation. Must call
          `reset_claude_fn` too.
        """
        self._state["session_id"] = None
        self._state["turn_count"] = 0
        self._state.pop("_context_warned_at", None)
        self._lock_manager.reset()
        self._persist_state(self._state)
        # Order matters: persist first, then kill — a crash during proc-kill
        # can't leave state pointing at the dead session. Swallow errors so a
        # singleton hiccup never blocks the operator's locked confirmation.
        if self._reset_claude_fn is not None:
            try:
                self._reset_claude_fn()
            except Exception as reset_error:
                log(f"/new: reset_claude_fn raised: {reset_error}")
        return NEW_RESPONSE_TEXT


NEW_RESPONSE_TEXT = "🔒 Session locked. Enter the passphrase to start."
