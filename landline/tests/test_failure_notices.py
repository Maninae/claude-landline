"""Tests for landline.claude.failure_notices — pure per-turn notice policy."""

from landline import config as daemon_config
from landline.claude.failure_notices import (
    FAILED_TURN_NOTICE,
    PARTIAL_FREEZE_NOTICE,
    SILENCE_KILL_ALERT_SUBJECT,
    SILENCE_KILL_NOTICE_TEMPLATE,
    failed_turn_notice_text,
    silence_kill_alert_body,
    silence_kill_minutes,
)
from landline.claude.types import ClaudeStreamResult


def _silence_kill(seconds):
    r = ClaudeStreamResult()
    r.killed_by_silence_watchdog = True
    r.silence_timeout_seconds = seconds
    return r


class TestSilenceKillMinutes:
    def test_production_caps(self):
        assert silence_kill_minutes(_silence_kill(600)) == 10
        assert silence_kill_minutes(_silence_kill(3600)) == 60

    def test_never_reports_zero(self):
        assert silence_kill_minutes(_silence_kill(20)) == 1

    def test_falls_back_to_config_timeout(self):
        assert silence_kill_minutes(_silence_kill(None)) == round(
            daemon_config.CLAUDE_TIMEOUT / 60
        )


class TestFailedTurnNoticeText:
    def test_silence_kill_wins_even_if_streaming_sent_a_line(self):
        r = _silence_kill(600)
        r.failure_notice_sent = True
        assert failed_turn_notice_text(r) == SILENCE_KILL_NOTICE_TEMPLATE.format(minutes=10)

    def test_generic_failure_gets_resend_notice(self):
        assert failed_turn_notice_text(ClaudeStreamResult()) == FAILED_TURN_NOTICE

    def test_none_when_streaming_already_notified(self):
        r = ClaudeStreamResult()
        r.failure_notice_sent = True
        assert failed_turn_notice_text(r) is None


class TestStrings:
    def test_no_em_dashes_in_new_user_facing_strings(self):
        for s in (
            SILENCE_KILL_NOTICE_TEMPLATE,
            FAILED_TURN_NOTICE,
            PARTIAL_FREEZE_NOTICE,
            silence_kill_alert_body(_silence_kill(600), "host"),
        ):
            assert "—" not in s

    def test_alert_body_names_minutes_and_host(self):
        body = silence_kill_alert_body(_silence_kill(600), "mini.local")
        assert "10 min" in body
        assert "mini.local" in body
        assert SILENCE_KILL_ALERT_SUBJECT == "claude-silence-kill"
