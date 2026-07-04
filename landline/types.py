"""Shared result types for the Claude streaming pipeline.

This module exists to break the import cycle between `landline.streaming`
(which produces `ClaudeStreamResult`) and `landline.claude_dispatch`
(which consumes it). Keep this module a LEAF: do not import from the
rest of `daemon/` here.

Predicates that read a `ClaudeStreamResult` (e.g. `is_result_successful`,
`looks_like_stale_session`) live in `landline.claude_dispatch` — they
encode dispatcher policy, not type structure.
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
        # Cluster 4 (usage/cost stats): mirror of the pump-observed values
        # from the terminal `result` event's optional accounting fields.
        # Populated by ``landline.streaming`` from ``TurnHandle``. Safe
        # defaults (None) so existing tests that build ClaudeStreamResult
        # by hand and existing dispatcher branches that ignore usage keep
        # working unchanged.
        self.result_usage: Optional[Dict[str, Any]] = None
        self.result_model_usage: Optional[Dict[str, Any]] = None
        self.result_total_cost_usd: Optional[float] = None
        self.result_num_turns: Optional[int] = None
        self.result_duration_ms: Optional[int] = None
        # Cluster 2 (stale-resume auto-recovery): surface the pump's
        # observation of the terminal `result` event's is_error flag /
        # subtype, plus whether this turn's block ever opened with a
        # `system/init` event. Together they let
        # ``landline.claude_dispatch.looks_like_pruned_resume`` catch the
        # empirically-verified pruned/nonexistent --resume shape without
        # false-positiving on mid-session API errors (which DO see init on
        # the same turn). All defaults are safe: existing tests that build
        # ClaudeStreamResult by hand keep working unchanged.
        self.result_is_error: bool = False
        self.result_subtype: Optional[str] = None
        self.saw_init: bool = False

    @property
    def has_content(self) -> bool:
        has_streamed = bool(self.streamed_text.strip())
        has_final = bool((self.final_result or "").strip())
        return has_streamed or has_final
