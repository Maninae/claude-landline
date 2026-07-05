"""macOS Keychain access for the daemon."""

import subprocess
from typing import Optional, Tuple

from landline.config import KEYCHAIN_ACCOUNT


# Classify on stderr phrase first, rc second: Apple rotates rc codes across
# macOS versions but the stderr phrasing has stayed stable.
_KEYCHAIN_LOCKED_PHRASES = (
    "User interaction is not allowed",
    "interaction is not allowed",       # variants that omit "User "
)
_KEYCHAIN_ABSENT_PHRASES = (
    "could not be found",
    "The specified item could not be found",
)


def keychain_get(service: str, account: str = KEYCHAIN_ACCOUNT) -> Optional[str]:
    """Read a secret from macOS Keychain.

    - Signature is load-bearing: callers do `keychain_get(...) or ""` — do NOT change.
    - For classified failure (absent vs locked vs error) use `keychain_get_status`.
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

    Returns:
        `(value, status)` where status is one of:
          - `"ok"`     — successful read; value is the stripped secret.
          - `"absent"` — item does not exist (rc=44 / "could not be found").
          - `"locked"` — login keychain is locked / user interaction disabled.
          - `"error"`  — timeout, FileNotFoundError, or any other failure.

    - Logging-only: never block or retry.
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
    # rc=44 = canonical absent; fall back when stderr is silent.
    if result.returncode == 44:
        return None, "absent"
    return None, "error"
