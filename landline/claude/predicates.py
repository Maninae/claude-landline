"""Result-shape predicates for the Claude call lifecycle.

These pure functions read a ``ClaudeStreamResult`` and classify the shape of
the outcome — success, stale-session, auth-failure stderr, pruned-resume —
without any side effects. Hoisted out of ``landline.claude.dispatch`` so the
dispatcher's file stays about lifecycle orchestration and these predicates
can be read (and tested) in isolation.
"""

from landline import config
from landline.claude.types import ClaudeStreamResult


def is_result_successful(result: ClaudeStreamResult) -> bool:
    if result.error:
        return False
    return result.has_content


def looks_like_stale_session(result: ClaudeStreamResult) -> bool:
    """True if the result suggests --resume hit a dead session.

    A stale session is specifically a clean exit (code 0 or None) with no
    content. ANY nonzero exit code means the process died, not that the
    session was pruned. Misclassifying a crash as stale silently wipes
    the conversation.
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

    Cluster 3: a multi-day silent auth outage (June 2026) had every
    ``claude -p`` call 401ing without surfacing anywhere. Match generously
    (case-insensitive): the exact strings are Anthropic-CLI-owned and
    change without notice, and the cost of a false positive is one
    iMessage the operator can ignore — strictly better than missing a
    multi-day outage.
    """
    if not stderr_tail:
        return False
    lowered = stderr_tail.lower()
    return any(marker.lower() in lowered
               for marker in config.CLAUDE_AUTH_ERROR_MARKERS)


def looks_like_pruned_resume(result: ClaudeStreamResult) -> bool:
    """True if the result matches the pruned/nonexistent-session shape.

    Verified empirically against the Claude Code CLI: resuming a pruned uuid emits a bare
    ``result`` event with ``subtype=error_during_execution``,
    ``is_error=true``, NO preceding ``system/init``, and the process exits
    code 1. Stderr contains ``No conversation found with session ID: <uuid>``.

    Distinguisher from a mid-session API error (which also emits is_error):
    the mid-session case DID see an init on this turn (``saw_init=True``);
    the pruned-resume case did not. This is the load-bearing predicate that
    keeps a genuine mid-session failure from being wiped into a fresh
    session (which would destroy conversation context).

    Orthogonal to ``looks_like_stale_session``: that predicate catches the
    clean-empty shape (exit 0 / None, no content). This one catches the
    is_error + no-init shape (and, as belt-and-suspenders defense-in-depth,
    the stderr-marker shape if the result-event path didn't populate
    is_error — e.g. process death before result emit).

    Cluster 2/3 collision guard: a Claude CLI auth failure ALSO produces the
    is_error + no-init shape (401 happens before any system/init event). If
    we classified an auth expiry as a pruned resume we would (a) show the
    operator the misleading "(Previous session expired, starting fresh.)"
    notice, (b) wipe the still-valid server-side session UUID, and (c)
    delay the real Cluster 3 auth alert by an extra failed retry. Detect
    the auth stderr shape first and hand the result to Cluster 3 unmolested.
    """
    if result.interrupted:
        return False
    # Auth-expiry stderr shape must NOT be treated as pruned-resume; it is
    # Cluster 3's territory and wiping the session on it is destructive.
    if _stderr_looks_like_auth_failure(result.stderr_tail or ""):
        return False
    if result.saw_init:
        return False
    # Corroborating evidence required: is_error + no-init ALONE is ambiguous
    # with the "pump missed init" path — a JSONDecodeError on the system/init
    # line (or an exception inside _handle_event on that line) leaves
    # saw_init=False on the handle even for a healthy mid-session turn that
    # later failed. Wiping the still-valid server-side session in that case
    # destroys the whole conversation. Real pruned-resume ALWAYS emits
    # "No conversation found with session ID" (or "session not found") into
    # stderr (verified empirically against the Claude Code CLI); demand the marker before we
    # decide to nuke a session. If saw_init=False + is_error but no marker,
    # fall back to preserving the session (pre-Cluster-2 behavior) — the
    # operator sees the "(no response — exit N)" notice and can retry,
    # which is strictly recoverable, vs. an irreversible session wipe.
    tail = (result.stderr_tail or "")
    return any(m in tail for m in config.STALE_RESUME_STDERR_MARKERS)
