"""Cluster 4: daily aggregate of Claude persistent-stream usage/cost data.

the operator is on flat-rate Max, so any dollar figure surfaced here is labelled
"notional" — the aggregate is a signal of intensity, not a bill.

Schema (JSON, stored at ``config.USAGE_STATS_FILE`` at ``0o600``)::

    {
      "days": {
        "2026-07-03": {
          "turns_dispatched": 4,
          "turns_unsolicited": 1,
          "input_tokens": 1234,
          "output_tokens": 4321,
          "cache_read_input_tokens": 0,
          "cache_creation_input_tokens": 0,
          "total_cost_usd_notional": 0.0123,
          "duration_ms_sum": 45000,
          "by_model": {
            "claude-opus-4-8": {"input_tokens": 12, "output_tokens": 34}
          }
        }
      }
    }

Recording is single-writer per process but the pump-thread AND the
dispatcher thread can both call in, so a module-level ``threading.Lock``
serialises reads and writes.

Design notes:
    * Attribution split: dispatched turns and unsolicited (background
      subagent completions / ``run_in_background`` Bash tasks) are counted
      separately so the operator can distinguish "my messages" cost from
      "background stuff" cost. The /status one-liner rolls them into a
      single "Today: N turns" but the JSON keeps the split for future
      breakdowns.
    * Known attribution race (documented in stream_pump.py): a background
      turn that starts in the sub-second window between ``register_turn``
      and the dispatched turn's ``system/init`` can swap attribution. That
      moves ONE turn between the dispatched/unsolicited buckets — accepted;
      matches the CLAUDE.md "do not fix with counting" invariant.
    * Retention: buckets older than ``USAGE_STATS_RETENTION_DAYS`` are
      pruned on save. ISO ``YYYY-MM-DD`` string compare gives correct
      ordering without parsing.
    * Corrupt-file recovery mirrors ``landline.state.load_state``: rename
      to a ``.corrupt`` sibling, log loudly, and proceed with a fresh dict.
"""

import json
import os
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from landline import config
from landline.config import (
    TIMEZONE,
    USAGE_STATS_FILE,
    USAGE_STATS_FILE_MODE,
    USAGE_STATS_MODEL_LABEL_MAX,
    USAGE_STATS_RETENTION_DAYS,
)
from landline.logging import log


_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Load / save (both must run under ``_lock``)
# ---------------------------------------------------------------------------

def _empty_data() -> Dict[str, Any]:
    return {"days": {}}


def _load_unlocked() -> Dict[str, Any]:
    """Read USAGE_STATS_FILE from disk. Missing / malformed => fresh dict.

    On a malformed file, rename to a ``.corrupt`` sibling before returning
    defaults — same defensive pattern as ``landline.state.load_state`` so a
    partial write cannot silently zero the aggregate on every subsequent
    read.
    """
    path = USAGE_STATS_FILE
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return _empty_data()
    except Exception as read_error:
        _backup_corrupt(read_error)
        return _empty_data()
    try:
        data = json.loads(raw)
    except Exception as parse_error:
        _backup_corrupt(parse_error)
        return _empty_data()
    if not isinstance(data, dict) or not isinstance(data.get("days"), dict):
        _backup_corrupt(ValueError("unexpected top-level shape"))
        return _empty_data()
    return data


def _backup_corrupt(error: BaseException) -> None:
    """Rename USAGE_STATS_FILE to a ``.corrupt`` sibling and log loudly.

    Any failure inside the backup itself is logged but swallowed so the
    daemon keeps going — worst case we lose the corrupt bytes.
    """
    path = USAGE_STATS_FILE
    backup = path.with_suffix(path.suffix + ".corrupt")
    try:
        os.replace(str(path), str(backup))
        log(
            f"usage_stats: corrupt file {path} ({error!r}); "
            f"backed up to {backup}, starting fresh"
        )
    except OSError as backup_error:
        log(
            f"usage_stats: corrupt file {path} ({error!r}); backup to "
            f"{backup} also failed ({backup_error!r}); starting fresh"
        )


def _save_unlocked(data: Dict[str, Any]) -> None:
    """Atomic write of the aggregate to USAGE_STATS_FILE at ``0o600``.

    Uses the same race-free ``os.open`` + ``os.fchmod`` + ``os.replace``
    pattern as ``landline.state.save_state`` — process-wide ``os.umask`` is
    forbidden across the daemon (races with the poller/sender threads).
    """
    path = USAGE_STATS_FILE
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(
            str(tmp),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            USAGE_STATS_FILE_MODE,
        )
        os.fchmod(fd, USAGE_STATS_FILE_MODE)
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(data, indent=2, sort_keys=True))
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(tmp), str(path))
    except OSError as save_error:
        log(f"usage_stats save failed: {save_error}")


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

def _prune(data: Dict[str, Any]) -> None:
    """Drop day buckets older than USAGE_STATS_RETENTION_DAYS.

    ISO ``YYYY-MM-DD`` lexicographic ordering is correct — no strptime
    needed. Any key that isn't a parseable date is left alone (defensive:
    a hand-authored JSON file shouldn't lose custom sentinels).
    """
    today = datetime.now(TIMEZONE).date()
    cutoff = today - timedelta(days=USAGE_STATS_RETENTION_DAYS)
    cutoff_str = cutoff.isoformat()
    days = data.get("days", {})
    drop = []
    for key in days:
        # Simple ISO shape check — length 10, dashes at 4/7.
        if len(key) != 10 or key[4] != "-" or key[7] != "-":
            continue
        if key < cutoff_str:
            drop.append(key)
    for key in drop:
        days.pop(key, None)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _empty_bucket() -> Dict[str, Any]:
    return {
        "turns_dispatched": 0,
        "turns_unsolicited": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_input_tokens": 0,
        "cache_creation_input_tokens": 0,
        "total_cost_usd_notional": 0.0,
        "duration_ms_sum": 0,
        "by_model": {},
    }


def _coerce_int(value: Any) -> int:
    """Return ``value`` as ``int`` or 0 for anything else (None/str/dict)."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _coerce_float(value: Any) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def record_turn(
    result_usage: Optional[Dict[str, Any]],
    result_model_usage: Optional[Dict[str, Any]],
    total_cost_usd: Optional[float],
    duration_ms: Optional[int],
    dispatched: bool,
) -> None:
    """Aggregate one turn's usage into today's bucket and persist.

    None-safe on every argument — a turn that reports no usage still bumps
    the turn count (dispatched vs unsolicited) so the /status "N turns
    today" line stays honest even when tokens are missing. Never raises;
    logs and swallows any disk/JSON failure so a broken aggregate cannot
    corrupt the finalize path.
    """
    try:
        with _lock:
            data = _load_unlocked()
            days = data.setdefault("days", {})
            today = datetime.now(TIMEZONE).date().isoformat()
            bucket = days.setdefault(today, _empty_bucket())

            if dispatched:
                bucket["turns_dispatched"] = int(
                    bucket.get("turns_dispatched", 0)) + 1
            else:
                bucket["turns_unsolicited"] = int(
                    bucket.get("turns_unsolicited", 0)) + 1

            if isinstance(result_usage, dict):
                for token_key in (
                    "input_tokens",
                    "output_tokens",
                    "cache_read_input_tokens",
                    "cache_creation_input_tokens",
                ):
                    bucket[token_key] = int(
                        bucket.get(token_key, 0)
                    ) + _coerce_int(result_usage.get(token_key))

            bucket["total_cost_usd_notional"] = float(
                bucket.get("total_cost_usd_notional", 0.0)
            ) + _coerce_float(total_cost_usd)
            bucket["duration_ms_sum"] = int(
                bucket.get("duration_ms_sum", 0)
            ) + _coerce_int(duration_ms)

            if isinstance(result_model_usage, dict):
                by_model = bucket.setdefault("by_model", {})
                for raw_label, per_model in result_model_usage.items():
                    if not isinstance(raw_label, str):
                        continue
                    label = raw_label[:USAGE_STATS_MODEL_LABEL_MAX]
                    if not isinstance(per_model, dict):
                        continue
                    slot = by_model.setdefault(
                        label, {"input_tokens": 0, "output_tokens": 0},
                    )
                    slot["input_tokens"] = int(
                        slot.get("input_tokens", 0)
                    ) + _coerce_int(per_model.get("input_tokens"))
                    slot["output_tokens"] = int(
                        slot.get("output_tokens", 0)
                    ) + _coerce_int(per_model.get("output_tokens"))

            _prune(data)
            _save_unlocked(data)
    except Exception as record_error:
        # Belt-and-suspenders: any unexpected exception (e.g. a monkeypatch
        # that broke _load_unlocked) must never propagate — this is a
        # metrics side-effect and finalize / the pump must stay alive.
        log(f"usage_stats.record_turn failed: {record_error}")


def today_summary() -> Dict[str, Any]:
    """Return today's aggregate bucket (or an empty dict if no data)."""
    try:
        with _lock:
            data = _load_unlocked()
        today = datetime.now(TIMEZONE).date().isoformat()
        return dict(data.get("days", {}).get(today, {}))
    except Exception as summary_error:
        log(f"usage_stats.today_summary failed: {summary_error}")
        return {}


def format_status_line() -> str:
    """One line for /status. Empty string when there's no data today.

    Wording is deliberately conservative: the operator is on flat-rate Max so the
    dollar figure is labelled 'notional' — a signal of intensity, not a
    bill. Never leaks per-model or per-message content, only aggregates.
    """
    bucket = today_summary()
    if not bucket:
        return ""
    dispatched = int(bucket.get("turns_dispatched", 0))
    unsolicited = int(bucket.get("turns_unsolicited", 0))
    total_turns = dispatched + unsolicited
    if total_turns <= 0:
        return ""
    input_tokens = int(bucket.get("input_tokens", 0))
    output_tokens = int(bucket.get("output_tokens", 0))
    cost = float(bucket.get("total_cost_usd_notional", 0.0))
    return (
        f"Today: {total_turns} turns, "
        f"{input_tokens} in / {output_tokens} out tokens "
        f"(~${cost:.4f} notional)"
    )
