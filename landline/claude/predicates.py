"""Result-shape predicates for the Claude call lifecycle.

Pure classifiers of ``ClaudeStreamResult`` outcome (success / stale-session /
auth-failure stderr / pruned-resume). Hoisted out of ``landline.claude.dispatch``
so the dispatcher file stays about lifecycle orchestration and these read
(and test) in isolation.
"""

from landline import config
from landline.claude.types import ClaudeStreamResult


def is_result_successful(result: ClaudeStreamResult) -> bool:
    if result.error:
        return False
    return result.has_content


def looks_like_stale_session(result: ClaudeStreamResult) -> bool:
    """True if the result suggests --resume hit a dead session.

    Stale = clean exit (0 or None) with no content. ANY nonzero exit means
    the process died — misclassifying a crash as stale silently wipes the
    conversation.
    """
    if result.error:
        return False
    if result.interrupted:
        return False
    if result.exit_code is not None and result.exit_code != 0:
        return False
    return not result.has_content


def _stderr_looks_like_auth_failure(stderr_tail: str) -> bool:
    """True if the CC stderr tail matches an OAuth-expiry shape.

    Match generously (case-insensitive) — Anthropic-CLI strings drift; false
    positive = one ignorable iMessage, false negative = multi-day silent
    outage. See docs/ARCHITECTURE.md "June 2026 auth-expiry outage".
    """
    if not stderr_tail:
        return False
    lowered = stderr_tail.lower()
    return any(marker.lower() in lowered
               for marker in config.CLAUDE_AUTH_ERROR_MARKERS)


def looks_like_pruned_resume(result: ClaudeStreamResult) -> bool:
    """True if the result matches the pruned/nonexistent-session shape.

    Empirically-verified shape: bare ``result`` with ``is_error=true`` +
    ``subtype=error_during_execution``, NO preceding ``system/init``, exit 1,
    stderr contains "No conversation found with session ID: <uuid>".

    - Distinguisher vs mid-session API error (which also emits is_error):
      mid-session ``saw_init=True``; pruned-resume ``saw_init=False``.
      Load-bearing — a false positive wipes conversation context.
    - Orthogonal to ``looks_like_stale_session`` (clean-empty shape).
    - Auth-expiry collision: 401 also produces is_error + no-init. Detecting
      the auth stderr shape first prevents a destructive session wipe on an
      auth outage.
    - Corroborating stderr marker required: is_error + no-init alone is
      ambiguous with "pump missed init" (JSONDecodeError on the init line
      leaves saw_init False for a healthy turn). No marker → preserve the
      session ("(no response — exit N)" is recoverable; a wipe is not).

    See docs/ARCHITECTURE.md "Stale-resume vs mid-session-error discriminator".
    """
    if result.interrupted:
        return False
    if _stderr_looks_like_auth_failure(result.stderr_tail or ""):
        return False
    if result.saw_init:
        return False
    tail = (result.stderr_tail or "")
    return any(m in tail for m in config.STALE_RESUME_STDERR_MARKERS)
