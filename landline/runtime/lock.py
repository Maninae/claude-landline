"""Session lock/unlock management for the Telegram daemon.

Encapsulates passphrase verification, lockout tracking, unlock expiry,
and state persistence.  The orchestrator holds a LockManager instance
and delegates all lock-related decisions to it.
"""

import hashlib
import hmac
import re
import time
from typing import Any, Callable, Dict, Optional

from landline.config import (
    LOCKED,
    UNLOCK_DURATION_SECONDS,
    UNLOCK_LOCKOUT_MAX_SECONDS,
    UNLOCK_LOCKOUT_SECONDS,
    UNLOCK_MAX_ATTEMPTS,
    UNLOCKED,
)
from landline.runtime.logging import log
# `keychain_get` is re-imported here (unused in lock logic post-B5) for
# backwards compatibility with any out-of-tree test that still does
# `patch("landline.runtime.lock.keychain_get", ...)` — the patch site stays alive so the
# AttributeError described in the Wave-2 review can't reappear. Mirrors the
# same shim in guard.py:20. Safe to remove once all such patches are migrated
# to `landline.runtime.lock.keychain_get_status`.
from landline.runtime.security import keychain_get, keychain_get_status  # noqa: F401


_WHITESPACE_RUN = re.compile(r"\s+")


def _normalize_passphrase(raw_input: str) -> str:
    """Canonicalize a passphrase for hashing.

    Generic normalizations only — no word-specific rewrites.  Baking
    knowledge of a specific passphrase into source would defeat the
    purpose of storing only a hash, and any change to the stored
    passphrase could silently corrupt input via stale rewrites.

    Normalizations applied (in order):
      1. lowercase
      2. strip leading/trailing whitespace
      3. collapse runs of internal whitespace to a single space

    If plural-tolerance is desired in the future, the proper approach
    is storing multiple hashes in Keychain, not mutating user input.
    """
    normalized = raw_input.lower().strip()
    return _WHITESPACE_RUN.sub(" ", normalized)


class LockManager:
    """Manages the locked/unlocked session state with passphrase verification.

    Thread-safety: all mutations happen on the main thread (message-processing
    loop), so no internal locking is needed.  The poller thread only reads
    is_locked indirectly via the orchestrator's gating logic.
    """

    def __init__(self, persist_state_fn: Callable[[Dict[str, Any]], None]) -> None:
        """The persist_state_fn callback is invoked whenever lock-related
        fields in the state dict change and need to be flushed to disk.
        This avoids coupling LockManager to the state module directly.
        """
        self._persist_state = persist_state_fn
        self._lock_state: str = LOCKED
        self._failed_unlock_attempts: int = 0
        self._unlock_lockout_until: float = 0.0
        # B1: escalation counter — persisted; reset on successful unlock.
        self._consecutive_lockouts: int = 0
        # B2: in-memory monotonic floor alongside the persisted wall deadline.
        # Process-local (PEP 418); never persisted across restarts.
        self._lockout_monotonic_until: float = 0.0
        self._state: Dict[str, Any] = {}

    @property
    def is_locked(self) -> bool:
        return self._lock_state == LOCKED

    @property
    def lock_state_label(self) -> str:
        return self._lock_state

    def restore_from_state(self, state: Dict[str, Any]) -> None:
        """Initialize lock fields from the persisted state dict loaded at startup."""
        self._state = state
        self._failed_unlock_attempts = int(
            state.get("failed_unlock_attempts", 0) or 0
        )
        self._unlock_lockout_until = float(
            state.get("unlock_lockout_until", 0.0) or 0.0
        )
        # B1: load escalation counter with legacy-state fallback to 0.
        # B2: do NOT restore any persisted monotonic value — process-local only.
        self._consecutive_lockouts = int(
            state.get("consecutive_lockouts", 0) or 0
        )

        stored_unlock_timestamp = float(
            state.get("unlock_timestamp", 0.0) or 0.0
        )
        unlock_age_seconds = time.time() - stored_unlock_timestamp
        if 0 < stored_unlock_timestamp <= time.time() and unlock_age_seconds < UNLOCK_DURATION_SECONDS:
            self._lock_state = UNLOCKED
            log(
                f"Restored UNLOCKED state from {unlock_age_seconds:.0f}s ago "
                f"(expires in {UNLOCK_DURATION_SECONDS - unlock_age_seconds:.0f}s)"
            )
        else:
            self._lock_state = LOCKED

    def check_expiry(self) -> bool:
        """Check whether the unlock has expired.  If so, re-lock and persist.

        Returns True if the session was re-locked (caller should gate on this).
        """
        if self._lock_state != UNLOCKED:
            return False

        stored_unlock_timestamp = float(
            self._state.get("unlock_timestamp", 0.0) or 0.0
        )
        unlock_age = time.time() - stored_unlock_timestamp
        if stored_unlock_timestamp <= 0 or unlock_age >= UNLOCK_DURATION_SECONDS:
            self._lock_state = LOCKED
            self._state["unlock_timestamp"] = 0.0
            self._persist_state(self._state)
            log("Unlock expired during runtime — re-locking")
            return True
        return False

    def reset(self) -> None:
        """Reset lock state for /new command — re-lock and clear unlock timestamp.

        Does NOT clear failed_unlock_attempts or unlock_lockout_until — a
        locked-out user must wait out the lockout even after /new.
        """
        self._lock_state = LOCKED
        self._state["unlock_timestamp"] = 0.0
        self._persist_lockout()

    def try_silent_unlock(self, text: str) -> bool:
        """Try text as a direct passphrase without counting failures.

        Used when a plain message arrives while locked — lets the user type
        just the passphrase after /new instead of /unlock <passphrase>.
        Returns True if the passphrase matched and session is now unlocked.
        """
        # B2: gate on max(wall_remaining, monotonic_remaining) so a forward
        # wall-clock jump past the deadline cannot retire an active lockout.
        if self._lockout_remaining() > 0.0:
            self._check_clock_skew()
            return False

        # B5: classify keychain failure so a locked login keychain produces a
        # distinct, actionable log line. Logging-only — never block or retry.
        expected_hash, kc_status = keychain_get_status("telegram-unlock-hash")
        if not expected_hash:
            if kc_status == "locked":
                log("Keychain locked during unlock attempt - unlock login keychain and retry")
            return False

        normalized = _normalize_passphrase(text)
        candidate_hash = hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()

        if hmac.compare_digest(candidate_hash, expected_hash.strip().lower()):
            self._failed_unlock_attempts = 0
            self._unlock_lockout_until = 0.0
            # B2: clear in-memory monotonic floor on self-rescue.
            self._lockout_monotonic_until = 0.0
            # B1: any successful unlock resets the escalation counter —
            # the operator's self-rescue path.
            self._consecutive_lockouts = 0
            self._persist_lockout()
            self._lock_state = UNLOCKED
            self._state["unlock_timestamp"] = time.time()
            self._persist_state(self._state)
            log("Session unlocked via direct passphrase")
            return True

        self._failed_unlock_attempts += 1
        log(f"Failed silent unlock attempt #{self._failed_unlock_attempts}")
        if self._failed_unlock_attempts >= UNLOCK_MAX_ATTEMPTS:
            # B1: exponential ramp with hard cap. Cap removes the DoS where an
            # attacker (or stressed the operator) escalates the next window past the operator's
            # ability to wait it out. the operator's successful unlock resets to 0.
            lockout_seconds = min(
                UNLOCK_LOCKOUT_SECONDS * (2 ** self._consecutive_lockouts),
                UNLOCK_LOCKOUT_MAX_SECONDS,
            )
            # B2: arm wall + monotonic deadlines via shared helper.
            self._register_lockout(lockout_seconds)
            self._failed_unlock_attempts = 0
            self._consecutive_lockouts += 1
            log(
                f"Unlock locked out for {lockout_seconds}s "
                f"(consecutive_lockouts={self._consecutive_lockouts})"
            )
        self._persist_lockout()
        return False

    def unlock_status_line(self) -> str:
        """Format the lock status line for /status output."""
        if self._lock_state != UNLOCKED:
            return f"Lock: {self._lock_state}"

        stored_unlock_timestamp = float(
            self._state.get("unlock_timestamp", 0.0) or 0.0
        )
        if stored_unlock_timestamp > 0:
            unlock_expires_in_seconds = (
                UNLOCK_DURATION_SECONDS - (time.time() - stored_unlock_timestamp)
            )
            if unlock_expires_in_seconds > 0:
                unlock_expires_in_hours = unlock_expires_in_seconds / 3600
                return f"Lock: unlocked (expires in {unlock_expires_in_hours:.1f}h)"
            return "Lock: unlocked (expiry imminent)"
        return f"Lock: {self._lock_state}"

    def _lockout_remaining(self) -> float:
        """Seconds left on the active lockout (0.0 if inactive).

        B2: a lockout is active iff EITHER the persisted wall deadline OR the
        in-memory monotonic floor is in the future.  We take the max so the
        later of the two governs.  On restart the monotonic floor is gone, so
        we fall back to wall-only (bounded by B1's escalation cap).
        """
        wall_remaining = self._unlock_lockout_until - time.time()
        mono_remaining = self._lockout_monotonic_until - time.monotonic()
        remaining = max(wall_remaining, mono_remaining)
        return remaining if remaining > 0 else 0.0

    def _register_lockout(self, duration_seconds: float) -> None:
        """Arm both the wall deadline (persisted) and the monotonic floor
        (in-memory).  Single source of truth for arming a lockout (B1+B2)."""
        self._unlock_lockout_until = time.time() + duration_seconds
        self._lockout_monotonic_until = time.monotonic() + duration_seconds

    def _check_clock_skew(self) -> None:
        """Log if wall and monotonic clocks have diverged beyond tolerance.

        Logging-only: a skew is observable but never blocks the user (a wall
        jump must not be a DoS vector against the operator).
        """
        wall_remaining = self._unlock_lockout_until - time.time()
        mono_remaining = self._lockout_monotonic_until - time.monotonic()
        if (
            self._lockout_monotonic_until > 0.0
            and self._unlock_lockout_until > 0.0
            and abs(wall_remaining - mono_remaining) > 60.0
        ):
            log(
                f"Lockout clock skew detected: wall_remaining={wall_remaining:.0f}s "
                f"monotonic_remaining={mono_remaining:.0f}s (wall-clock jump?)"
            )

    def _persist_lockout(self) -> None:
        """Write lockout counters to state and flush to disk."""
        self._state["failed_unlock_attempts"] = self._failed_unlock_attempts
        self._state["unlock_lockout_until"] = self._unlock_lockout_until
        # B1: persist escalation counter; legacy state without this key
        # round-trips cleanly via .get(..., 0) fallback in restore_from_state.
        self._state["consecutive_lockouts"] = self._consecutive_lockouts
        self._persist_state(self._state)
