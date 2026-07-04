"""Tests for landline.runtime.usage_stats — Cluster 4 daily aggregate.

Scenarios covered:
    * record_turn writes today's bucket with correct token counts,
      dispatched/unsolicited attribution, and per-model breakdown.
    * Multiple record_turn calls same day aggregate additively.
    * format_status_line returns '' on no-data, and a line with 'notional'
      when there is data.
    * Retention prunes buckets older than USAGE_STATS_RETENTION_DAYS.
    * Corrupt JSON is renamed to .corrupt sibling and a fresh dict is used.
    * Concurrent record_turn calls from many threads aggregate correctly
      (proves the module-level _lock serialises writes).
    * Missing fields (result_usage=None, total_cost_usd=None) are recorded
      as zeros without a KeyError.
    * File is created with 0o600 mode.
"""

import json
import os
import threading
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from landline import config
from landline.runtime import usage_stats


# ---------------------------------------------------------------------------
# Per-test setup: point USAGE_STATS_FILE at a scratch path
# ---------------------------------------------------------------------------

@pytest.fixture()
def stats_path(tmp_path, monkeypatch):
    path = tmp_path / "usage-stats.json"
    monkeypatch.setattr("landline.config.USAGE_STATS_FILE", path)
    monkeypatch.setattr("landline.runtime.usage_stats.USAGE_STATS_FILE", path)
    return path


class TestRecordTurn:
    def test_first_dispatched_write_creates_file_with_bucket(self, stats_path):
        usage_stats.record_turn(
            result_usage={
                "input_tokens": 100,
                "output_tokens": 200,
                "cache_read_input_tokens": 10,
                "cache_creation_input_tokens": 5,
            },
            result_model_usage={
                "claude-opus-4-8": {"input_tokens": 100, "output_tokens": 200},
            },
            total_cost_usd=0.0123,
            duration_ms=1500,
            dispatched=True,
        )

        assert stats_path.exists()
        data = json.loads(stats_path.read_text())
        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = data["days"][today]
        assert bucket["turns_dispatched"] == 1
        assert bucket["turns_unsolicited"] == 0
        assert bucket["input_tokens"] == 100
        assert bucket["output_tokens"] == 200
        assert bucket["cache_read_input_tokens"] == 10
        assert bucket["cache_creation_input_tokens"] == 5
        assert bucket["duration_ms_sum"] == 1500
        assert bucket["total_cost_usd_notional"] == pytest.approx(0.0123)
        assert bucket["by_model"]["claude-opus-4-8"] == {
            "input_tokens": 100, "output_tokens": 200,
        }

    def test_file_mode_is_0o600(self, stats_path):
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        mode = os.stat(str(stats_path)).st_mode & 0o777
        assert mode == 0o600

    def test_dispatched_and_unsolicited_are_separate(self, stats_path):
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        usage_stats.record_turn(None, None, None, None, dispatched=False)
        usage_stats.record_turn(None, None, None, None, dispatched=False)

        today = datetime.now(config.TIMEZONE).date().isoformat()
        data = json.loads(stats_path.read_text())
        bucket = data["days"][today]
        assert bucket["turns_dispatched"] == 1
        assert bucket["turns_unsolicited"] == 2

    def test_same_day_aggregation_is_additive(self, stats_path):
        usage_stats.record_turn(
            result_usage={"input_tokens": 10, "output_tokens": 20},
            result_model_usage=None,
            total_cost_usd=0.001,
            duration_ms=100,
            dispatched=True,
        )
        usage_stats.record_turn(
            result_usage={"input_tokens": 30, "output_tokens": 40},
            result_model_usage=None,
            total_cost_usd=0.002,
            duration_ms=200,
            dispatched=True,
        )
        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = json.loads(stats_path.read_text())["days"][today]
        assert bucket["turns_dispatched"] == 2
        assert bucket["input_tokens"] == 40
        assert bucket["output_tokens"] == 60
        assert bucket["duration_ms_sum"] == 300
        assert bucket["total_cost_usd_notional"] == pytest.approx(0.003)

    def test_missing_fields_recorded_as_zero_no_keyerror(self, stats_path):
        # None result_usage + None total_cost_usd + None duration_ms
        # must still bump the turn count and not raise.
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = json.loads(stats_path.read_text())["days"][today]
        assert bucket["turns_dispatched"] == 1
        assert bucket["input_tokens"] == 0
        assert bucket["output_tokens"] == 0
        assert bucket["total_cost_usd_notional"] == 0.0
        assert bucket["duration_ms_sum"] == 0

    def test_partial_result_usage_uses_zero_for_missing_keys(self, stats_path):
        # Only some token keys present — others recorded as zero.
        usage_stats.record_turn(
            result_usage={"input_tokens": 5},
            result_model_usage=None,
            total_cost_usd=None,
            duration_ms=None,
            dispatched=True,
        )
        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = json.loads(stats_path.read_text())["days"][today]
        assert bucket["input_tokens"] == 5
        assert bucket["output_tokens"] == 0
        assert bucket["cache_read_input_tokens"] == 0

    def test_model_label_is_truncated_to_max(self, stats_path):
        long_label = "x" * (config.USAGE_STATS_MODEL_LABEL_MAX + 20)
        usage_stats.record_turn(
            result_usage=None,
            result_model_usage={long_label: {"input_tokens": 3, "output_tokens": 4}},
            total_cost_usd=None,
            duration_ms=None,
            dispatched=True,
        )
        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = json.loads(stats_path.read_text())["days"][today]
        # Every key in by_model must be <= the cap.
        for key in bucket["by_model"]:
            assert len(key) <= config.USAGE_STATS_MODEL_LABEL_MAX


class TestFormatStatusLine:
    def test_returns_empty_when_no_data(self, stats_path):
        assert usage_stats.format_status_line() == ""

    def test_returns_line_with_notional_when_data(self, stats_path):
        usage_stats.record_turn(
            result_usage={"input_tokens": 1234, "output_tokens": 4321},
            result_model_usage=None,
            total_cost_usd=0.0123,
            duration_ms=1000,
            dispatched=True,
        )
        line = usage_stats.format_status_line()
        assert "Today:" in line
        assert "1 turns" in line
        assert "1234 in" in line
        assert "4321 out" in line
        assert "notional" in line
        # Never leaks a raw dollar figure without the guardrail label.
        assert "$" in line and "notional" in line

    def test_dispatched_plus_unsolicited_rolled_into_total_turns(self, stats_path):
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        usage_stats.record_turn(None, None, None, None, dispatched=False)
        line = usage_stats.format_status_line()
        assert "3 turns" in line


class TestRetention:
    def test_prunes_days_older_than_retention(self, stats_path, monkeypatch):
        # Hand-seed 40 days of buckets — the newest 30 stay, oldest 10 go.
        today = datetime.now(config.TIMEZONE).date()
        seed = {"days": {}}
        for delta in range(40):
            day = (today - timedelta(days=delta)).isoformat()
            seed["days"][day] = {
                "turns_dispatched": delta,
                "turns_unsolicited": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0,
                "total_cost_usd_notional": 0.0,
                "duration_ms_sum": 0,
                "by_model": {},
            }
        stats_path.write_text(json.dumps(seed))

        # A fresh record_turn triggers _prune during save.
        usage_stats.record_turn(None, None, None, None, dispatched=True)

        data = json.loads(stats_path.read_text())
        remaining = data["days"].keys()
        cutoff = today - timedelta(days=config.USAGE_STATS_RETENTION_DAYS)
        cutoff_str = cutoff.isoformat()
        for key in remaining:
            assert key >= cutoff_str, f"stale key {key} survived pruning"

    def test_prune_leaves_non_iso_keys_alone(self, stats_path):
        # Defensive: a hand-authored sentinel key that doesn't look like an
        # ISO date must survive pruning (see _prune's shape check).
        seed = {"days": {"sentinel-key": {"turns_dispatched": 1}}}
        stats_path.write_text(json.dumps(seed))
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        data = json.loads(stats_path.read_text())
        assert "sentinel-key" in data["days"]


class TestCorruptRecovery:
    def test_malformed_json_backs_up_and_starts_fresh(self, stats_path):
        stats_path.write_text("{not valid json")
        # Should NOT raise.
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        # Corrupt sibling exists with the original bytes.
        corrupt = stats_path.with_suffix(stats_path.suffix + ".corrupt")
        assert corrupt.exists()
        assert corrupt.read_text() == "{not valid json"
        # Fresh file exists with today's bucket only.
        today = datetime.now(config.TIMEZONE).date().isoformat()
        data = json.loads(stats_path.read_text())
        assert list(data["days"].keys()) == [today]

    def test_missing_file_is_not_corrupt(self, stats_path):
        # A brand new install must NOT create a .corrupt file.
        assert not stats_path.exists()
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        corrupt = stats_path.with_suffix(stats_path.suffix + ".corrupt")
        assert not corrupt.exists()

    def test_unexpected_shape_is_treated_as_corrupt(self, stats_path):
        # A JSON file that is valid but the wrong shape (e.g. a list, or
        # missing "days") must be backed up and replaced, not silently
        # KeyError on the next record.
        stats_path.write_text('["not a dict"]')
        usage_stats.record_turn(None, None, None, None, dispatched=True)
        corrupt = stats_path.with_suffix(stats_path.suffix + ".corrupt")
        assert corrupt.exists()


class TestConcurrency:
    def test_ten_threads_ten_contributions_each_add_correctly(self, stats_path):
        # Prove _lock serialises: 10 threads x 10 record_turn calls each
        # must produce 100 dispatched turns and 100_000 input_tokens.
        def worker():
            for _ in range(10):
                usage_stats.record_turn(
                    result_usage={"input_tokens": 100, "output_tokens": 1},
                    result_model_usage=None,
                    total_cost_usd=None,
                    duration_ms=None,
                    dispatched=True,
                )

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        today = datetime.now(config.TIMEZONE).date().isoformat()
        bucket = json.loads(stats_path.read_text())["days"][today]
        assert bucket["turns_dispatched"] == 100
        assert bucket["input_tokens"] == 10000
        assert bucket["output_tokens"] == 100


class TestNoRaiseOnBrokenLoad:
    def test_record_turn_never_raises_even_if_load_fails(
        self, stats_path, monkeypatch,
    ):
        # Any unexpected exception inside _load_unlocked must be swallowed
        # so the pump / dispatcher never crash on a broken stats file.
        def boom(*_a, **_kw):
            raise RuntimeError("simulated io failure")

        monkeypatch.setattr(
            "landline.runtime.usage_stats._load_unlocked", boom,
        )
        # Should NOT raise.
        usage_stats.record_turn(
            result_usage={"input_tokens": 1, "output_tokens": 1},
            result_model_usage=None,
            total_cost_usd=0.01,
            duration_ms=10,
            dispatched=True,
        )


class TestTodaySummaryEmpty:
    def test_empty_bucket_returns_empty_line(self, stats_path):
        # Even when the file exists with a bucket that has 0 turns, the
        # status line must be empty — no misleading "Today: 0 turns".
        empty_bucket = usage_stats._empty_bucket()
        today = datetime.now(config.TIMEZONE).date().isoformat()
        stats_path.write_text(json.dumps({"days": {today: empty_bucket}}))
        assert usage_stats.format_status_line() == ""


if __name__ == "__main__":
    unittest.main()
