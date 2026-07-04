"""macOS Keychain access for the daemon."""

import subprocess
from typing import Optional, Tuple

from landline.config import KEYCHAIN_ACCOUNT


# B5 - stable across macOS releases - classification key for `keychain_get_status`.
# Apple has rotated rc codes (errSecAuthFailed etc.) across macOS versions, but
# the stderr phrases have stayed stable. Classify on phrase first, rc second.
_KEYCHAIN_LOCKED_PHRASES = (
    "User interaction is not allowed",  # canonical locked-keychain stderr
    "interaction is not allowed",       # substring guard for variants that omit "User "
)
_KEYCHAIN_ABSENT_PHRASES = (
    "could not be found",               # canonical absent-item stderr
    "The specified item could not be found",
)


def keychain_get(service: str, account: str = KEYCHAIN_ACCOUNT) -> Optional[str]:
    """Read a secret from macOS Keychain.

    Signature is load-bearing - multiple callers do `keychain_get(...) or ""`.
    Do NOT change. For classified failure (absent vs locked vs error),
    use `keychain_get_status` below.
    """
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-a", account, "-s", service, "-w"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None


def keychain_get_status(
    service: str, account: str = KEYCHAIN_ACCOUNT
) -> Tuple[Optional[str], str]:
    """Read a secret from macOS Keychain, classifying failure mode.

    Returns (value, status) where status is one of:
      - "ok":      successful read, value is the stripped secret
      - "absent":  item does not exist (rc=44 / "could not be found")
      - "locked":  login keychain is locked / user interaction disabled
      - "error":   timeout, FileNotFoundError, or any other failure

    Classification prefers stderr phrases over rc codes because Apple has
    historically rotated rc values across macOS releases while the stderr
    phrasing has stayed stable. Logging-only - never block or retry.
    """
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-a", account, "-s", service, "-w"],
            capture_output=True, text=True, timeout=5,
        )
    except Exception:
        return None, "error"

    if result.returncode == 0:
        return result.stdout.strip(), "ok"

    stderr = (result.stderr or "")
    for phrase in _KEYCHAIN_LOCKED_PHRASES:
        if phrase in stderr:
            return None, "locked"
    for phrase in _KEYCHAIN_ABSENT_PHRASES:
        if phrase in stderr:
            return None, "absent"
    # rc=44 is the canonical absent code; fall back to it when stderr is silent.
    if result.returncode == 44:
        return None, "absent"
    return None, "error"
