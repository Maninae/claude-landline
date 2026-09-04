"""Command handlers for /new, /status, /doctor, and unknown commands.

Accepts its dependencies explicitly — no coupling to the orchestrator class.
"""

import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from landline.config import (
    AGENT_NAME,
    CONTEXT_WINDOW_TOKENS,
    DOCTOR_SCRIPT,
    LAUNCHD_LABEL_PREFIX,
    MORNING_BRIEF_GLOB,
    PROFILE_NAME,
    WORKSPACE,
)
from landline.runtime.lock import LockManager
from landline.runtime.logging import log
from landline.runtime.state import get_context_percent, get_session_age_seconds


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
    """Build the /status response. Runs subprocesses for system info.

    - Header surfaces the optional cosmetic ``PROFILE_NAME`` in parentheses so
      an operator running multiple daemons on one Mac (per-profile plist,
      per-profile keychain account, per-profile workspace — see
      ``docs/SETUP.md`` → "Running multiple daemons") can tell them apart at
      a glance. Purely visual; never an auth/security surface.
    """
    if PROFILE_NAME:
        header = f"**{AGENT_NAME} System Status** ({PROFILE_NAME})\n"
    else:
        header = f"**{AGENT_NAME} System Status**\n"
    lines = [header]

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


def _fmt_tokens(count: int) -> str:
    """Human-readable token count: 1_000_000 -> '1.0M', 620_400 -> '620.4k'."""
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        rounded_k = round(count / 1_000, 1)
        # e.g. 999_999 rounds to 1000.0k — promote to '1.0M' rather than show 'k'.
        if rounded_k >= 1000.0:
            return f"{count / 1_000_000:.1f}M"
        return f"{rounded_k:.1f}k"
    return str(count)


def _fmt_age(seconds: float) -> str:
    """Compact age: 45 -> '45s', 320 -> '5m', 3900 -> '1h 5m', 273600 -> '3d 4h'."""
    total = max(0, int(seconds))
    if total < 60:
        return f"{total}s"
    minutes = total // 60
    if minutes < 60:
        return f"{minutes}m"
    hours, rem_min = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {rem_min}m" if rem_min else f"{hours}h"
    days, rem_hours = divmod(hours, 24)
    return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"


def _context_text(state: Dict[str, Any]) -> str:
    """Build the /context response: the live session's context-window usage.

    Deterministic and agent-free — reuses ``get_context_percent`` (the same
    number the heartbeat's context warnings read from the session JSONL tail),
    so it matches the CC status bar without spending an agent turn. The second
    line shows the turn count and how long the session has been alive. Degrades
    to a friendly notice when there is no active session or no usage yet.
    """
    session_id = state.get("session_id")
    if not session_id:
        return "📊 No active session yet. Send a message to start one."
    pct = get_context_percent(session_id)
    if pct is None:
        return "📊 No usage recorded yet (fresh session)."
    used_tokens = int(round(pct / 100.0 * CONTEXT_WINDOW_TOKENS))
    # Round once so the displayed % and the health bucket can never disagree.
    pct_display = int(round(pct))
    turns = state.get("turn_count", 0)
    turn_word = "turn" if turns == 1 else "turns"
    # Coarse 3-color health view at 50 / 70%. (The heartbeat fires its own
    # warnings at CONTEXT_WARN_THRESHOLDS = [30, 50, 70]; this dot is a rougher
    # at-a-glance bucket, not a 1:1 mirror of those thresholds.)
    if pct_display < 50:
        health = "🟢 healthy"
    elif pct_display < 70:
        health = "🟡 getting full"
    else:
        health = "🔴 near limit"
    second_line = f"{turns} {turn_word}"
    age = get_session_age_seconds(session_id)
    if age is not None:
        second_line += f" · {_fmt_age(age)} old"
    return (
        f"📊 Context: {_fmt_tokens(used_tokens)} / "
        f"{_fmt_tokens(CONTEXT_WINDOW_TOKENS)} ({pct_display}%)  {health}\n"
        f"{second_line}"
    )


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

        if cmd == "/context":
            return _context_text(self._state)

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
