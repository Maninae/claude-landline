"""Tests for landline.failure_tracker — ClaudeFailureTracker state machine."""

import time
from unittest.mock import patch

import pytest

from landline.failure_tracker import (
    ClaudeFailureTracker,
    CLAUDE_FAILURE_BACKOFF_THRESHOLD,
    CLAUDE_FAILURE_ALERT_THRESHOLD,
    CLAUDE_FAILURE_BACKOFF_BASE_SECONDS,
    CLAUDE_FAILURE_BACKOFF_CAP_SECONDS,
)


class TestInitialState:
    def test_starts_with_zero_failures(self):
        ft = ClaudeFailureTracker()
        assert ft.consecutive_failure_count == 0

    def test_not_in_backoff_initially(self):
        ft = ClaudeFailureTracker()
        assert ft.is_in_backoff() is False

    def test_no_alert_initially(self):
        ft = ClaudeFailureTracker()
        assert ft.should_send_alert_now() is False


class TestRecordSuccess:
    def test_resets_failure_count(self):
        ft = ClaudeFailureTracker()
        ft.consecutive_failure_count = 5
        ft.record_success()
        assert ft.consecutive_failure_count == 0

    def test_clears_backoff(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 1000
        ft.record_success()
        assert ft.is_in_backoff() is False

    def test_clears_alert_flag(self):
        ft = ClaudeFailureTracker()
        ft.alert_sent_for_current_failure_streak = True
        ft.record_success()
        assert ft.alert_sent_for_current_failure_streak is False


class TestRecordFailure:
    def test_increments_failure_count(self):
        ft = ClaudeFailureTracker()
        ft.record_failure()
        assert ft.consecutive_failure_count == 1
        ft.record_failure()
        assert ft.consecutive_failure_count == 2

    def test_no_backoff_below_threshold(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_BACKOFF_THRESHOLD - 1):
            ft.record_failure()
        assert ft.is_in_backoff() is False

    def test_backoff_at_threshold(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_BACKOFF_THRESHOLD):
            ft.record_failure()
        assert ft.is_in_backoff() is True

    def test_backoff_at_threshold_uses_base_seconds(self):
        """At exactly the threshold, exponent is 0 → backoff == base seconds."""
        ft = ClaudeFailureTracker(
            backoff_threshold=3,
            backoff_base_seconds=30.0,
            backoff_cap_seconds=10_000.0,
        )
        for _ in range(3):
            ft.record_failure()
        remaining = ft.seconds_until_next_attempt()
        # Allow tiny clock drift between record_failure and the read.
        assert 29.0 < remaining <= 30.0

    def test_backoff_doubles_each_step_past_threshold(self):
        """Verify true exponential growth: base * 2^(n - threshold)."""
        ft = ClaudeFailureTracker(
            backoff_threshold=2,
            backoff_base_seconds=10.0,
            backoff_cap_seconds=100_000.0,
        )
        backoffs = []
        for _ in range(5):
            ft.record_failure()
            backoffs.append(ft.seconds_until_next_attempt())
        # n=1: below threshold, no backoff
        assert backoffs[0] == 0.0
        # n=2 (threshold): exponent 0 → 10s
        assert 9.0 < backoffs[1] <= 10.0
        # n=3: exponent 1 → 20s
        assert 19.0 < backoffs[2] <= 20.0
        # n=4: exponent 2 → 40s
        assert 39.0 < backoffs[3] <= 40.0
        # n=5: exponent 3 → 80s
        assert 79.0 < backoffs[4] <= 80.0

    def test_backoff_capped(self):
        ft = ClaudeFailureTracker()
        for _ in range(50):
            ft.record_failure()
        remaining = ft.seconds_until_next_attempt()
        assert remaining <= CLAUDE_FAILURE_BACKOFF_CAP_SECONDS + 1
        # And it should actually be AT the cap given 50 failures (would otherwise be huge).
        assert remaining > CLAUDE_FAILURE_BACKOFF_CAP_SECONDS - 1

    def test_recording_failure_does_not_reset_alert_flag(self):
        """Once alert_sent is True, additional failures must not re-arm the alert."""
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD):
            ft.record_failure()
        ft.mark_alert_sent()
        ft.record_failure()
        ft.record_failure()
        assert ft.should_send_alert_now() is False


class TestSecondsUntilNextAttempt:
    def test_zero_when_not_in_backoff(self):
        ft = ClaudeFailureTracker()
        assert ft.seconds_until_next_attempt() == 0.0

    def test_positive_when_in_backoff(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 100
        assert ft.seconds_until_next_attempt() > 0

    def test_zero_when_backoff_expired(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() - 1
        assert ft.seconds_until_next_attempt() == 0.0

    def test_accepts_now_override(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = 1000.0
        assert ft.seconds_until_next_attempt(now_epoch=900.0) == 100.0
        assert ft.seconds_until_next_attempt(now_epoch=1100.0) == 0.0


class TestIsInBackoff:
    def test_false_when_no_backoff_set(self):
        ft = ClaudeFailureTracker()
        assert ft.is_in_backoff() is False

    def test_true_when_active(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() + 60
        assert ft.is_in_backoff() is True

    def test_false_when_expired(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = time.time() - 1
        assert ft.is_in_backoff() is False

    def test_accepts_now_override(self):
        ft = ClaudeFailureTracker()
        ft.next_attempt_allowed_at_epoch = 1000.0
        assert ft.is_in_backoff(now_epoch=999.0) is True
        assert ft.is_in_backoff(now_epoch=1001.0) is False


class TestAlertThreshold:
    def test_no_alert_below_threshold(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD - 1):
            ft.record_failure()
        assert ft.should_send_alert_now() is False

    def test_alert_at_threshold(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD):
            ft.record_failure()
        assert ft.should_send_alert_now() is True

    def test_alert_fires_only_once(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD):
            ft.record_failure()
        assert ft.should_send_alert_now() is True
        ft.mark_alert_sent()
        assert ft.should_send_alert_now() is False

    def test_alert_resets_on_success(self):
        ft = ClaudeFailureTracker()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD):
            ft.record_failure()
        ft.mark_alert_sent()
        ft.record_success()
        for _ in range(CLAUDE_FAILURE_ALERT_THRESHOLD):
            ft.record_failure()
        assert ft.should_send_alert_now() is True


class TestCustomThresholds:
    def test_custom_backoff_threshold(self):
        ft = ClaudeFailureTracker(backoff_threshold=1)
        ft.record_failure()
        assert ft.is_in_backoff() is True

    def test_custom_alert_threshold(self):
        ft = ClaudeFailureTracker(alert_threshold=2)
        ft.record_failure()
        ft.record_failure()
        assert ft.should_send_alert_now() is True

    def test_custom_backoff_base(self):
        ft = ClaudeFailureTracker(backoff_threshold=1, backoff_base_seconds=10)
        ft.record_failure()
        remaining = ft.seconds_until_next_attempt()
        assert 5 < remaining <= 11

    def test_custom_backoff_cap(self):
        ft = ClaudeFailureTracker(backoff_threshold=1, backoff_cap_seconds=60)
        for _ in range(100):
            ft.record_failure()
        remaining = ft.seconds_until_next_attempt()
        assert remaining <= 61


class TestConfigCanonicalSource:
    """E6 - failure_tracker must not redefine the tunables locally.

    The four CLAUDE_FAILURE_* names are imported from landline.config; a
    local redefinition in failure_tracker.py would shadow the config
    source of truth and silently desync tuner-edited values from runtime
    behaviour. Identity (is) check catches a re-shadow; CPython's
    small-int caching is irrelevant because we are asserting binding
    identity (both modules resolve to the same name).
    """

    def test_failure_tracker_consts_come_from_config(self):
        from landline import config, failure_tracker

        assert (
            failure_tracker.CLAUDE_FAILURE_BACKOFF_THRESHOLD
            is config.CLAUDE_FAILURE_BACKOFF_THRESHOLD
        )
        assert (
            failure_tracker.CLAUDE_FAILURE_ALERT_THRESHOLD
            is config.CLAUDE_FAILURE_ALERT_THRESHOLD
        )
        assert (
            failure_tracker.CLAUDE_FAILURE_BACKOFF_BASE_SECONDS
            is config.CLAUDE_FAILURE_BACKOFF_BASE_SECONDS
        )
        assert (
            failure_tracker.CLAUDE_FAILURE_BACKOFF_CAP_SECONDS
            is config.CLAUDE_FAILURE_BACKOFF_CAP_SECONDS
        )
