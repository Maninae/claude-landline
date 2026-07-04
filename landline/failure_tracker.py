"""Consecutive-failure tracker + exponential backoff for Claude subprocess calls.

The daemon consults this state machine before and after each Claude call:
  - record_success / record_failure track a consecutive-failure count.
  - After backoff_threshold failures, is_in_backoff gates Claude calls behind
    an exponentially growing cooldown.
  - After alert_threshold failures, should_send_alert_now fires exactly once
    per failure streak.

State is in-memory on purpose: a process restart that resets counters is
harmless (the next few messages rebuild the streak if Claude is still broken).
"""

import time
from typing import Optional

from landline.config import (
    CLAUDE_FAILURE_ALERT_THRESHOLD,
    CLAUDE_FAILURE_BACKOFF_BASE_SECONDS,
    CLAUDE_FAILURE_BACKOFF_CAP_SECONDS,
    CLAUDE_FAILURE_BACKOFF_THRESHOLD,
)


class ClaudeFailureTracker:
    """In-memory state machine for consecutive Claude failures + backoff."""

    def __init__(
        self,
        backoff_threshold: int = CLAUDE_FAILURE_BACKOFF_THRESHOLD,
        alert_threshold: int = CLAUDE_FAILURE_ALERT_THRESHOLD,
        backoff_base_seconds: float = CLAUDE_FAILURE_BACKOFF_BASE_SECONDS,
        backoff_cap_seconds: float = CLAUDE_FAILURE_BACKOFF_CAP_SECONDS,
    ) -> None:
        self.backoff_threshold = backoff_threshold
        self.alert_threshold = alert_threshold
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds

        self.consecutive_failure_count = 0
        self.next_attempt_allowed_at_epoch = 0.0
        self.alert_sent_for_current_failure_streak = False

    def record_success(self) -> None:
        self.consecutive_failure_count = 0
        self.next_attempt_allowed_at_epoch = 0.0
        self.alert_sent_for_current_failure_streak = False

    def record_failure(self) -> None:
        self.consecutive_failure_count += 1
        if self.consecutive_failure_count >= self.backoff_threshold:
            backoff_seconds = self._compute_backoff_seconds()
            self.next_attempt_allowed_at_epoch = time.time() + backoff_seconds

    def _compute_backoff_seconds(self) -> float:
        exponent_past_threshold = (
            self.consecutive_failure_count - self.backoff_threshold
        )
        uncapped_backoff_seconds = (
            self.backoff_base_seconds * (2 ** exponent_past_threshold)
        )
        return min(uncapped_backoff_seconds, self.backoff_cap_seconds)

    def seconds_until_next_attempt(self, now_epoch: Optional[float] = None) -> float:
        current_epoch = now_epoch if now_epoch is not None else time.time()
        remaining_seconds = self.next_attempt_allowed_at_epoch - current_epoch
        return max(0.0, remaining_seconds)

    def is_in_backoff(self, now_epoch: Optional[float] = None) -> bool:
        return self.seconds_until_next_attempt(now_epoch) > 0.0

    def should_send_alert_now(self) -> bool:
        if self.alert_sent_for_current_failure_streak:
            return False
        return self.consecutive_failure_count >= self.alert_threshold

    def mark_alert_sent(self) -> None:
        self.alert_sent_for_current_failure_streak = True
