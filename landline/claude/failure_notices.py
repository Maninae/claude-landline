"""Per-turn failure notices — what the user hears when a Claude turn fails.

Pure policy + strings, no I/O. ``ClaudeDispatcher._record_outcome`` asks
``failed_turn_notice_text`` which in-band line (if any) a failed turn earns
and does the sending; this module stays a leaf next to ``types.py``.

- Exactly ONE in-band line per failed turn. Precedence: the failure
  tracker's threshold alert ("Claude is unavailable ...", sent by the
  dispatcher on the turn it fires) > the silence-watchdog freeze notice >
  the generic resend notice. A notice streaming already enqueued
  (``result.failure_notice_sent``) counts as that turn's line.
- A silence-watchdog kill ALWAYS earns its freeze notice: the user waited
  out the whole cap and the message is gone; silence here is the
  Oct-2026 tccd-wedge incident (4 silently dropped messages in 2 days).
- Per-turn (not first-of-streak) by design: the daemon never retries on
  its own, so every failed turn is a message the user just sent. Rate is
  bounded by the user; a first-only latch would re-create silent drops on
  failures 2..alert_threshold-1 that land between backoff windows.
- Shutdown exits and interrupted turns never reach this policy
  (``_record_outcome`` returns before consulting it), EXCEPT that a
  watchdog kill is checked before the shutdown-exit test: ``pc.kill()``
  sends SIGTERM first, so a frozen Claude often dies with exit 143 / -15,
  the same codes a real shutdown produces.
- A watchdog kill after partial reply text is success-shaped (content
  arrived) and gets ``PARTIAL_FREEZE_NOTICE`` instead of a failure line.
"""

from typing import Optional

from landline import config
from landline.claude.types import ClaudeStreamResult


SILENCE_KILL_NOTICE_TEMPLATE = (
    "I froze for {minutes} minutes with no output and had to restart, so "
    "your last message wasn't handled. Please resend it. If this keeps "
    "happening, the Mac itself is probably struggling."
)

# Appended to either freeze notice from the 2nd consecutive watchdog kill on
# the same session: a session that keeps freezing on resume is often the
# culprit, and /new is the user's own way out.
REPEATED_FREEZE_HINT = " If it keeps freezing, send /new to start a fresh session."

FAILED_TURN_NOTICE = (
    "(That one failed before I could reply, so your last message wasn't "
    "handled. Please resend it.)"
)

# Watchdog kill AFTER some reply text streamed. The turn stays success-shaped
# (the user has a partial answer, the tracker is not tripped), but they must
# know the tail is missing.
PARTIAL_FREEZE_NOTICE = (
    "(I froze partway through and had to restart. The reply above may be "
    "incomplete; resend if anything is missing.)"
)

SILENCE_KILL_ALERT_SUBJECT = "claude-silence-kill"


def silence_kill_minutes(result: ClaudeStreamResult) -> int:
    """Whole minutes of the silence cap that killed this turn (never 0).

    Falls back to ``config.CLAUDE_TIMEOUT`` when the result carries no cap
    (defensive; the watchdog always stamps one).
    """
    seconds = result.silence_timeout_seconds
    if seconds is None:
        seconds = config.CLAUDE_TIMEOUT
    return max(1, int(round(seconds / 60.0)))


def failed_turn_notice_text(
    result: ClaudeStreamResult, repeated_freeze: bool = False,
) -> Optional[str]:
    """The in-band line this failed turn earns, or None if streaming already
    sent one. See the module docstring for precedence.

    Args:
        repeated_freeze: 2nd+ consecutive watchdog kill on the same session;
            appends ``REPEATED_FREEZE_HINT`` to the freeze notice.
    """
    if result.killed_by_silence_watchdog:
        notice = SILENCE_KILL_NOTICE_TEMPLATE.format(
            minutes=silence_kill_minutes(result),
        )
        return notice + REPEATED_FREEZE_HINT if repeated_freeze else notice
    if result.failure_notice_sent:
        return None
    return FAILED_TURN_NOTICE


def partial_freeze_notice_text(repeated_freeze: bool = False) -> str:
    """The success-shaped freeze line, with the /new hint on a repeat."""
    return PARTIAL_FREEZE_NOTICE + REPEATED_FREEZE_HINT if repeated_freeze else PARTIAL_FREEZE_NOTICE


def silence_kill_alert_body(result: ClaudeStreamResult, hostname: str) -> str:
    """Operator-facing body for the out-of-band ``claude-silence-kill`` alert."""
    return (
        f"Claude produced no output for {silence_kill_minutes(result)} min "
        f"and the silence watchdog killed it on {hostname}. The user's "
        f"message was dropped and they were asked to resend. Repeated kills "
        f"usually mean the Mac is overloaded (check load and tccd)."
    )
