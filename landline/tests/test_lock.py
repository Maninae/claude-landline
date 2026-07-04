"""Tests for landline.lock — LockManager state machine.

Critical security invariant: reset() must preserve lockout counters.
"""

import copy
import hashlib
import re
import time
from unittest.mock import patch

import pytest

from landline.config import (
    LOCKED,
    UNLOCK_DURATION_SECONDS,
    UNLOCK_LOCKOUT_MAX_SECONDS,
    UNLOCK_LOCKOUT_SECONDS,
    UNLOCK_MAX_ATTEMPTS,
    UNLOCKED,
)
from landline.lock import LockManager, _normalize_passphrase


# B1/B2/B5 helpers — used by the new regression-tests classes below.
FAKE_UNLOCK_PASSPHRASE = "coconut pudding"
FAKE_UNLOCK_HASH = hashlib.sha256(
    FAKE_UNLOCK_PASSPHRASE.encode("utf-8")
).hexdigest()


class TestNormalizePassphrase:
    def test_lowercases(self):
        assert _normalize_passphrase("HELLO") == "hello"

    def test_strips_whitespace(self):
        assert _normalize_passphrase("  hello  ") == "hello"

    def test_lowercases_and_strips_combined(self):
        assert _normalize_passphrase("  COCONUT Pudding  ") == "coconut pudding"

    def test_no_word_specific_rewrites(self):
        """Normalization must not silently rewrite specific words.  Any
        word-level transformation defeats the purpose of storing only a
        hash and risks corrupting input if the stored passphrase changes."""
        # "puddings" with a trailing 's' is NOT silently rewritten to "pudding".
        assert _normalize_passphrase("coconut puddings") == "coconut puddings"
        # Other plurals are likewise preserved verbatim.
        assert _normalize_passphrase("dogs and cats") == "dogs and cats"

    def test_collapses_internal_whitespace_runs(self):
        """Multiple spaces / tabs between words collapse to a single space."""
        assert _normalize_passphrase("coconut   pudding") == "coconut pudding"
        assert _normalize_passphrase("a\tb") == "a b"
        assert _normalize_passphrase("a  b  c") == "a b c"

    def test_unrelated_text_unchanged(self):
        assert _normalize_passphrase("plain text") == "plain text"

    def test_empty_string(self):
        assert _normalize_passphrase("") == ""

    def test_only_whitespace(self):
        assert _normalize_passphrase("   ") == ""


class TestLockManagerInit:
    def test_starts_locked(self, persist_state_fn):
        lm = LockManager(persist_state_fn)
        assert lm.is_locked is True
        assert lm.lock_state_label == LOCKED

    def test_restore_locked_from_state(self, persist_state_fn, default_state):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.is_locked is True

    def test_restore_unlocked_from_recent_timestamp(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time() - 60
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.is_locked is False

    def test_restore_locked_from_expired_timestamp(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time() - UNLOCK_DURATION_SECONDS - 1
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.is_locked is True

    def test_restore_preserves_lockout_counters(self, persist_state_fn, default_state):
        default_state["failed_unlock_attempts"] = 3
        default_state["unlock_lockout_until"] = time.time() + 100
        default_state["consecutive_lockouts"] = 3
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm._failed_unlock_attempts == 3
        assert lm._unlock_lockout_until > time.time()
        # B1: consecutive_lockouts must round-trip from persisted state.
        assert lm._consecutive_lockouts == 3


class TestCheckExpiry:
    def test_no_op_when_locked(self, persist_state_fn, default_state):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.check_expiry() is False

    def test_relocks_when_expired(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time() - UNLOCK_DURATION_SECONDS - 1
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        lm._lock_state = UNLOCKED
        assert lm.check_expiry() is True
        assert lm.is_locked is True

    def test_no_relock_when_still_valid(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time() - 60
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.check_expiry() is False
        assert lm.is_locked is False


class TestReset:
    def test_relocks(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time()
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm.is_locked is False
        lm.reset()
        assert lm.is_locked is True

    def test_preserves_lockout_counters(self, persist_state_fn, default_state):
        """SECURITY INVARIANT: reset() must NOT clear lockout state."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        lm._failed_unlock_attempts = 4
        lm._unlock_lockout_until = time.time() + 200
        lm._consecutive_lockouts = 4
        lm.reset()
        assert lm._failed_unlock_attempts == 4
        assert lm._unlock_lockout_until > time.time()
        # B1: escalation counter must survive /new the same way the other
        # lockout counters do — otherwise /new becomes a side-door reset.
        assert lm._consecutive_lockouts == 4

    def test_clears_unlock_timestamp(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time()
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        lm.reset()
        assert lm._state["unlock_timestamp"] == 0.0

    def test_persists_state(self, persist_state_fn, default_state):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        persist_state_fn.reset_mock()
        lm.reset()
        assert persist_state_fn.called


class TestUnlockStatusLine:
    def test_locked_status(self, persist_state_fn, default_state):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        line = lm.unlock_status_line()
        assert "locked" in line.lower()

    def test_unlocked_with_expiry(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time()
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        line = lm.unlock_status_line()
        assert "unlocked" in line.lower()
        assert "expires" in line.lower()

    def test_unlocked_expiry_imminent(self, persist_state_fn, default_state):
        default_state["unlock_timestamp"] = time.time() - UNLOCK_DURATION_SECONDS - 1
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        lm._lock_state = UNLOCKED
        line = lm.unlock_status_line()
        assert "imminent" in line.lower()


# ---------------------------------------------------------------------------
# B1 — lockout escalation regression tests
# ---------------------------------------------------------------------------


def _drive_lockout_cycle(lm):
    """Trigger one full lockout cycle (UNLOCK_MAX_ATTEMPTS failures).

    Patches keychain_get_status to return a hash for FAKE_UNLOCK_PASSPHRASE so
    the failing attempts pass the keychain gate and reach the failure counter.
    """
    with patch(
        "landline.lock.keychain_get_status",
        return_value=(FAKE_UNLOCK_HASH, "ok"),
    ):
        for _ in range(UNLOCK_MAX_ATTEMPTS):
            lm.try_silent_unlock("wrong-passphrase")


class TestLockoutEscalation:
    """B1: escalating consecutive_lockouts ramp with hard cap + self-rescue."""

    def test_lockout_escalates(self, persist_state_fn, default_state):
        """Cycle k=1 lockout duration ~= UNLOCK_LOCKOUT_SECONDS * 2."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        # Cycle 0: 5 fails -> lockout = UNLOCK_LOCKOUT_SECONDS.
        t0 = time.time()
        _drive_lockout_cycle(lm)
        cycle0_remaining = lm._unlock_lockout_until - t0
        assert abs(cycle0_remaining - UNLOCK_LOCKOUT_SECONDS) < 2.0
        assert lm._consecutive_lockouts == 1

        # Advance wall+monotonic past the cycle-0 deadline, drive cycle 1.
        # Monotonic moves naturally; we fast-forward wall AND clear the
        # in-memory monotonic floor so the next cycle isn't gated.
        lm._unlock_lockout_until = 0.0
        lm._lockout_monotonic_until = 0.0
        t1 = time.time()
        _drive_lockout_cycle(lm)
        cycle1_remaining = lm._unlock_lockout_until - t1
        assert abs(cycle1_remaining - UNLOCK_LOCKOUT_SECONDS * 2) < 2.0
        assert lm._consecutive_lockouts == 2

    def test_lockout_caps_at_max(self, persist_state_fn, default_state):
        """30 lockout cycles must not exceed UNLOCK_LOCKOUT_MAX_SECONDS, but
        must also exceed baseline (catches both no-escalation and no-cap)."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        last_duration = 0.0
        cycle5_duration = 0.0
        for cycle in range(30):
            lm._unlock_lockout_until = 0.0
            lm._lockout_monotonic_until = 0.0
            t = time.time()
            _drive_lockout_cycle(lm)
            last_duration = lm._unlock_lockout_until - t
            if cycle == 4:
                cycle5_duration = last_duration

        # Cap is enforced.
        assert last_duration <= UNLOCK_LOCKOUT_MAX_SECONDS + 2.0
        # Growth happened — by cycle 5 we must have ramped past baseline.
        assert cycle5_duration > UNLOCK_LOCKOUT_SECONDS
        assert cycle5_duration <= UNLOCK_LOCKOUT_MAX_SECONDS + 2.0
        assert lm._consecutive_lockouts == 30

    def test_successful_unlock_resets_escalation(
        self, persist_state_fn, default_state,
    ):
        """Drive 3 cycles, then successful unlock zeroes the escalation counter."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        for _ in range(3):
            lm._unlock_lockout_until = 0.0
            lm._lockout_monotonic_until = 0.0
            _drive_lockout_cycle(lm)
        assert lm._consecutive_lockouts == 3

        # Clear the lockout window AND the monotonic floor so the next unlock
        # attempt actually reaches the hash comparison branch.
        lm._unlock_lockout_until = 0.0
        lm._lockout_monotonic_until = 0.0

        with patch(
            "landline.lock.keychain_get_status",
            return_value=(FAKE_UNLOCK_HASH, "ok"),
        ):
            assert lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE) is True

        # B1 self-rescue invariant: any successful unlock resets escalation.
        assert lm._consecutive_lockouts == 0
        assert lm._failed_unlock_attempts == 0
        assert lm._unlock_lockout_until == 0.0
        assert lm._lockout_monotonic_until == 0.0

    def test_operator_can_always_recover(self, persist_state_fn, default_state):
        """Even after 50 lockout cycles, the operator waits <= UNLOCK_LOCKOUT_MAX_SECONDS
        before he can retry the correct passphrase. No hand-edit of state ever
        required. This is the audit-checklist regression for the DoS guard."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        for _ in range(50):
            lm._unlock_lockout_until = 0.0
            lm._lockout_monotonic_until = 0.0
            t = time.time()
            _drive_lockout_cycle(lm)
            wait_seconds = lm._unlock_lockout_until - t
            # Always recoverable within the cap. Also catches accidental
            # overflow / cap removal (wait_seconds must stay finite and small).
            assert wait_seconds > 0
            assert wait_seconds < 2 * UNLOCK_LOCKOUT_MAX_SECONDS
            assert wait_seconds <= UNLOCK_LOCKOUT_MAX_SECONDS + 2.0

    def test_consecutive_lockouts_round_trip(
        self, persist_state_fn, default_state,
    ):
        """After a cycle, restoring a fresh LockManager from the persisted
        state dict preserves _consecutive_lockouts."""
        snapshots = []
        persist_state_fn.side_effect = lambda d: snapshots.append(
            copy.deepcopy(d)
        )

        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        _drive_lockout_cycle(lm)
        pre_restart_value = lm._consecutive_lockouts
        assert pre_restart_value == 1

        # Build a fresh manager from the last persisted snapshot.
        last_snapshot = snapshots[-1]
        assert "consecutive_lockouts" in last_snapshot
        lm2 = LockManager(persist_state_fn)
        lm2.restore_from_state(last_snapshot)
        assert lm2._consecutive_lockouts == pre_restart_value

    def test_legacy_state_without_consecutive_lockouts(
        self, persist_state_fn, default_state,
    ):
        """Legacy state files missing the key must restore to 0 without KeyError."""
        # The default_state fixture deliberately does NOT seed this key.
        assert "consecutive_lockouts" not in default_state

        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm._consecutive_lockouts == 0

        # And a lockout cycle still works (no KeyError on persist).
        _drive_lockout_cycle(lm)
        assert lm._consecutive_lockouts == 1


# ---------------------------------------------------------------------------
# B2 — clock-skew lockout regression tests
# ---------------------------------------------------------------------------


class TestClockSkew:
    """B2: persisted wall deadline + in-memory monotonic floor; lockout active
    iff EITHER is in the future. Forward wall-clock jumps cannot retire the
    lockout early; restart falls back to wall-only."""

    def test_lockout_resists_forward_wall_jump(
        self, persist_state_fn, default_state,
    ):
        """PRIMARY revert-sensitive test: a forward wall jump past the
        deadline must NOT retire the lockout — the monotonic floor governs."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        # Arm a lockout via 5 failed attempts.
        _drive_lockout_cycle(lm)
        assert lm._unlock_lockout_until > time.time()
        assert lm._lockout_monotonic_until > time.monotonic()
        pre_failed = lm._failed_unlock_attempts

        # Jump the wall clock past the deadline. Monotonic stays put.
        future_wall = lm._unlock_lockout_until + 1.0
        with patch("landline.lock.time.time", return_value=future_wall), \
             patch(
                 "landline.lock.keychain_get_status",
                 return_value=(FAKE_UNLOCK_HASH, "ok"),
             ):
            # Lockout must still be considered active by the gate.
            assert lm._lockout_remaining() > 0.0
            result = lm.try_silent_unlock("wrong")
        assert result is False
        # Failure counter must NOT have been bumped — we returned at the gate.
        assert lm._failed_unlock_attempts == pre_failed

    def test_wall_only_sufficient_on_backward_wall_jump(
        self, persist_state_fn, default_state,
    ):
        """Sanity: a backward wall jump GROWS wall_remaining; the lockout is
        strictly extended under either gate. Not revert-sensitive — documents
        directionality of the threat model."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        _drive_lockout_cycle(lm)

        past_wall = time.time() - 3600.0
        with patch("landline.lock.time.time", return_value=past_wall), \
             patch(
                 "landline.lock.keychain_get_status",
                 return_value=(FAKE_UNLOCK_HASH, "ok"),
             ):
            assert lm._lockout_remaining() > 0.0
            assert lm.try_silent_unlock("wrong") is False

    def test_lockout_falls_back_to_wall_after_restart(
        self, persist_state_fn, default_state,
    ):
        """Restart loses the monotonic floor; wall-only path still gates."""
        snapshots = []
        persist_state_fn.side_effect = lambda d: snapshots.append(
            copy.deepcopy(d)
        )

        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        _drive_lockout_cycle(lm)
        snapshot = snapshots[-1]

        # Fresh manager — simulates daemon restart. Monotonic floor must NOT
        # be restored from state.
        lm2 = LockManager(persist_state_fn)
        lm2.restore_from_state(snapshot)
        assert lm2._lockout_monotonic_until == 0.0
        # Wall-only path still gates the lockout.
        assert lm2._lockout_remaining() > 0.0
        # Snapshot must not carry a persisted monotonic value (B2 invariant).
        assert "lockout_monotonic_until" not in snapshot

    def test_register_lockout_arms_both_clocks(
        self, persist_state_fn, default_state,
    ):
        """Direct unit test: _register_lockout arms wall + monotonic."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        wall_before = time.time()
        mono_before = time.monotonic()
        lm._register_lockout(120.0)
        assert abs((lm._unlock_lockout_until - wall_before) - 120.0) < 1.0
        assert abs((lm._lockout_monotonic_until - mono_before) - 120.0) < 1.0

    def test_successful_unlock_clears_monotonic_floor(
        self, persist_state_fn, default_state,
    ):
        """Successful direct passphrase must zero the in-memory monotonic floor.

        Revert-sensitivity: the floor must be ACTIVE at call-time so the
        success branch is the only thing that can clear it. We use monkeypatch
        on time.monotonic so _lockout_remaining()'s monotonic comparison sees
        the planted floor as in the past (gate open), while the floor value
        itself is non-zero going into the success branch.
        """
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)

        # Plant a real, non-zero monotonic floor. The wall deadline stays 0.0.
        # We choose a floor value at the *current* monotonic instant so:
        #   - _lockout_remaining() = max(wall_remaining<=0, mono_remaining<=0) = 0
        #     → the lockout gate is open and the success branch runs.
        #   - The floor going INTO the success branch is non-zero, so the
        #     assertion `lm._lockout_monotonic_until == 0.0` is genuinely
        #     proving the success branch cleared it (not that it was 0 to start).
        planted_floor = time.monotonic()
        lm._lockout_monotonic_until = planted_floor
        lm._unlock_lockout_until = 0.0
        # Sanity: floor is non-zero (the precondition this test exercises) AND
        # the gate is open (so the success branch actually runs).
        assert lm._lockout_monotonic_until > 0.0
        assert lm._lockout_remaining() == 0.0

        with patch(
            "landline.lock.keychain_get_status",
            return_value=(FAKE_UNLOCK_HASH, "ok"),
        ):
            assert lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE) is True
        # Revert-sensitive: deleting `self._lockout_monotonic_until = 0.0` from
        # the success branch leaves the planted non-zero floor in place and
        # this assertion fails.
        assert lm._lockout_monotonic_until == 0.0
        assert lm._unlock_lockout_until == 0.0

    def test_clock_skew_logged_when_clocks_diverge(
        self, persist_state_fn, default_state,
    ):
        """A wall jump beyond the 60s tolerance during an active lockout logs
        a skew warning."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        _drive_lockout_cycle(lm)

        # Drift the wall clock so wall_remaining shrinks by > 60s relative to
        # monotonic_remaining. The lockout is still active (monotonic governs).
        future_wall = time.time() + 120.0
        with patch("landline.lock.time.time", return_value=future_wall), \
             patch("landline.lock.log") as mock_log, \
             patch(
                 "landline.lock.keychain_get_status",
                 return_value=(FAKE_UNLOCK_HASH, "ok"),
             ):
            lm.try_silent_unlock("wrong")
        messages = " | ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "clock skew" in messages.lower()

    def test_clock_skew_log_carries_no_pii(
        self, persist_state_fn, default_state,
    ):
        """The skew log line carries floats only — no chat_id, passphrase, or
        hash. Positively assert the expected float-only shape via regex; also
        scan for incidental PII patterns (long digit runs that could be a
        chat_id, hex tokens that could be a hash fragment)."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        _drive_lockout_cycle(lm)

        future_wall = time.time() + 120.0
        with patch("landline.lock.time.time", return_value=future_wall), \
             patch("landline.lock.log") as mock_log, \
             patch(
                 "landline.lock.keychain_get_status",
                 return_value=(FAKE_UNLOCK_HASH, "ok"),
             ):
            lm.try_silent_unlock("wrong-passphrase-do-not-log")
        captured = " | ".join(str(c.args[0]) for c in mock_log.call_args_list)

        # Negative: passphrase and hash must not appear verbatim.
        assert "wrong-passphrase-do-not-log" not in captured
        assert FAKE_UNLOCK_HASH not in captured

        # Positive: the skew log must match the spec's float-only shape
        # `wall_remaining=<int>s monotonic_remaining=<int>s` (signed ints; the
        # impl uses %.0f which can emit negative values when the wall jumped
        # past the deadline).
        skew_line_pattern = re.compile(
            r"wall_remaining=-?\d+s monotonic_remaining=-?\d+s"
        )
        assert skew_line_pattern.search(captured), (
            "skew log missing the expected float-only shape; "
            "captured={!r}".format(captured)
        )

        # Find the skew line specifically and audit only its payload (other
        # log lines from this batch — e.g. "Failed silent unlock attempt #N" —
        # legitimately carry attempt counters and would false-positive a
        # blanket digit-run scan).
        skew_lines = [
            str(c.args[0]) for c in mock_log.call_args_list
            if "clock skew" in str(c.args[0]).lower()
        ]
        assert skew_lines, "no clock-skew log line was emitted"
        for line in skew_lines:
            # No chat_id-shaped digit runs (Telegram chat_ids are 6+ digits).
            # The spec's float-only shape uses small remaining-seconds ints, so
            # any 6+ digit run is suspicious. Strip the known float fields
            # first to avoid false positives when remaining values happen to be
            # large.
            stripped = re.sub(
                r"(wall_remaining|monotonic_remaining)=-?\d+s", "", line
            )
            assert not re.search(r"\d{6,}", stripped), (
                "skew line contains a 6+ digit run that could be a chat_id: "
                "{!r}".format(line)
            )
            # No hex tokens (hash fragments). SHA-256 hex is 64 chars; even a
            # 16-char hex run would be a partial hash leak.
            assert not re.search(r"[0-9a-f]{16,}", line), (
                "skew line contains a hex-token-shaped run that could be a "
                "hash fragment: {!r}".format(line)
            )


# ---------------------------------------------------------------------------
# B5 — Keychain locked vs absent regression tests
# ---------------------------------------------------------------------------


class TestKeychainLocked:
    """B5: a locked login keychain must NOT count as a failed attempt; the
    daemon must log a distinct, actionable warning. Logging-only — never block."""

    def test_silent_unlock_returns_false_when_keychain_locked(
        self, persist_state_fn, default_state,
    ):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        # default_state.unlock_lockout_until == 0.0 — gate is open, so the
        # function actually reaches the keychain read.
        assert lm._unlock_lockout_until == 0.0
        with patch(
            "landline.lock.keychain_get_status",
            return_value=(None, "locked"),
        ):
            result = lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE)
        assert result is False

    def test_silent_unlock_does_not_count_locked_as_failed_attempt(
        self, persist_state_fn, default_state,
    ):
        """Critical security invariant: a locked keychain rejection must NOT
        bump _failed_unlock_attempts, otherwise it would feed the lockout
        escalation and the operator could be locked out by Keychain state alone."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm._unlock_lockout_until == 0.0
        with patch(
            "landline.lock.keychain_get_status",
            return_value=(None, "locked"),
        ):
            lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE)
        assert lm._failed_unlock_attempts == 0
        assert lm._consecutive_lockouts == 0

    def test_silent_unlock_logs_on_locked(
        self, persist_state_fn, default_state,
    ):
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm._unlock_lockout_until == 0.0
        with patch(
            "landline.lock.keychain_get_status",
            return_value=(None, "locked"),
        ), patch("landline.lock.log") as mock_log:
            lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE)
        messages = " | ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "Keychain locked" in messages

    def test_silent_unlock_does_not_log_keychain_warning_on_absent(
        self, persist_state_fn, default_state,
    ):
        """An absent hash is a config problem, not a runtime problem — silent
        return is fine, no Keychain-locked warning should fire."""
        lm = LockManager(persist_state_fn)
        lm.restore_from_state(default_state)
        assert lm._unlock_lockout_until == 0.0
        with patch(
            "landline.lock.keychain_get_status",
            return_value=(None, "absent"),
        ), patch("landline.lock.log") as mock_log:
            lm.try_silent_unlock(FAKE_UNLOCK_PASSPHRASE)
        messages = " | ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "Keychain locked" not in messages
