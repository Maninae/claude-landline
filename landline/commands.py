"""Command handlers for /new, /status, and unknown commands.

Accepts its dependencies explicitly — no coupling to the orchestrator class.
"""

import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from landline.config import (
    AGENT_NAME,
    LAUNCHD_LABEL_PREFIX,
    MORNING_BRIEF_GLOB,
    WORKSPACE,
)
from landline.lock import LockManager
from landline.logging import log


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

    # Wrapped defensively for the same reason as the usage_stats block below:
    # a bad glob value (e.g. an absolute pathlib pattern raises
    # NotImplementedError) or an inaccessible directory must never break the
    # whole /status reply — the operator loses the brief line, not their
    # diagnostics.
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

    # Cluster 4: append today's usage/cost line if we have any data. When
    # usage-stats.json is missing (fresh install) format_status_line
    # returns "" and /status still succeeds. Wrapped defensively so a
    # broken stats file never breaks /status — the operator loses the
    # number, not their diagnostics.
    try:
        from landline import usage_stats
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
        # Injected callback that resets the live PersistentClaude subprocess
        # (kill the child + clear its session id) so the next dispatch spawns
        # a brand-new Claude session. Optional + defaults to None purely for
        # back-compat with tests that construct CommandRouter directly without
        # exercising the reset path. In production this is ALWAYS wired by
        # the orchestrator — without it, /new would silently fail to reset
        # the live subprocess (the bug this parameter fixes).
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

        return f"Unknown command: {cmd}"

    def _handle_new(self) -> str:
        """Reset session state, re-lock, and reset the live Claude subprocess.

        The persisted state reset alone is NOT sufficient — PersistentClaude
        owns the live session id (E1 refactor moved ownership there). Without
        invoking ``reset_claude_fn``, the next dispatch would still see the
        OLD session id on the singleton and `--resume` the same conversation,
        defeating /new entirely.
        """
        self._state["session_id"] = None
        self._state["turn_count"] = 0
        self._state.pop("_context_warned_at", None)
        self._lock_manager.reset()
        self._persist_state(self._state)
        # Reset the live PersistentClaude AFTER state has been persisted, so a
        # crash during the proc-kill can't leave behind a state file that still
        # thinks the old session is live. Wrapped in try/except so a singleton
        # hiccup never blocks the operator from getting the locked confirmation
        # back.
        if self._reset_claude_fn is not None:
            try:
                self._reset_claude_fn()
            except Exception as reset_error:
                log(f"/new: reset_claude_fn raised: {reset_error}")
        return NEW_RESPONSE_TEXT


NEW_RESPONSE_TEXT = "🔒 Session locked. Enter the passphrase to start."
