"""Shared result types for the Claude streaming pipeline.

- Breaks the import cycle between `landline.claude.streaming` (produces
  `ClaudeStreamResult`) and `landline.claude.dispatch` (consumes it).
- Keep this module a LEAF: do not import from the rest of `landline.claude` here.
- Predicates that read a `ClaudeStreamResult` (`is_result_successful`,
  `looks_like_stale_session`) live in `landline.claude.dispatch` — dispatcher
  policy, not type structure.
"""

from typing import Any, Dict, Optional


class ClaudeStreamResult:
    """Result container for a Claude streaming call."""

    def __init__(self) -> None:
        self.session_id: Optional[str] = None
        self.streamed_text: str = ""
        self.final_result: Optional[str] = None
        self.exit_code: Optional[int] = None
        self.error: Optional[str] = None
        self.stderr_tail: str = ""
        self.interrupted: bool = False
        # Usage/cost mirror of the terminal `result` event's optional
        # accounting fields. Populated by `landline.claude.streaming` from
        # `TurnHandle`. None defaults keep existing tests + dispatcher
        # branches that ignore usage working unchanged.
        self.result_usage: Optional[Dict[str, Any]] = None
        self.result_model_usage: Optional[Dict[str, Any]] = None
        self.result_total_cost_usd: Optional[float] = None
        self.result_num_turns: Optional[int] = None
        self.result_duration_ms: Optional[int] = None
        # Stale-resume-recovery signal: pump-observed `is_error` / `subtype`
        # plus whether this turn's block ever opened with `system/init`.
        # `looks_like_pruned_resume` uses all three to catch the pruned
        # `--resume` shape without false-positiving on mid-session API
        # errors (which DO see init on the same turn).
        self.result_is_error: bool = False
        self.result_subtype: Optional[str] = None
        self.saw_init: bool = False
        # Set by the streaming watchdog when IT killed the process for
        # silence (any-event or owned-progress cap). `silence_timeout_seconds`
        # is the cap that fired, so the dispatcher's user notice can say how
        # long the freeze was without re-deriving which clock tripped.
        self.killed_by_silence_watchdog: bool = False
        self.silence_timeout_seconds: Optional[float] = None
        # True once streaming has already enqueued an in-band notice about
        # this failed turn ("(Empty response from Claude.)" / "(Claude
        # returned no response ...)"). The dispatcher's per-turn failure
        # notice checks this so the user never gets two lines for one turn.
        self.failure_notice_sent: bool = False

    @property
    def has_content(self) -> bool:
        has_streamed = bool(self.streamed_text.strip())
        has_final = bool((self.final_result or "").strip())
        return has_streamed or has_final

    def summary_for_log(self, stderr_tail_chars: int = 500) -> str:
        """One-line metadata summary for daemon log lines (no message text).

        Content is reported as lengths only; the stderr tail is CC's own
        diagnostics and is the one free-text field included.
        """
        tail = (self.stderr_tail or "")[-stderr_tail_chars:]
        return (
            "exit=%s num_turns=%s duration_ms=%s saw_init=%s is_error=%s "
            "subtype=%s streamed_chars=%d final_chars=%d interrupted=%s "
            "watchdog_kill=%s error=%r stderr_tail=%r" % (
                self.exit_code, self.result_num_turns, self.result_duration_ms,
                self.saw_init, self.result_is_error, self.result_subtype,
                len(self.streamed_text), len(self.final_result or ""),
                self.interrupted, self.killed_by_silence_watchdog,
                self.error, tail,
            )
        )
