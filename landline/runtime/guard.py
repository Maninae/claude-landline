"""Telegram sender allowlist gate. Fail-closed: empty allowlist blocks everyone.

Allowed chat IDs are stored in macOS Keychain:
  service: telegram-allowed-chat-ids
  account: <KEYCHAIN_ACCOUNT>   (default "landline"; see landline.json)
  value:   comma-separated chat IDs (e.g. "111111111,222222222")
"""

import json
import sys
import time
import urllib.request
from typing import Optional, Set

from landline.config import REJECTION_MODE
from landline.runtime.security import keychain_get_status

_cached_allowed: Optional[Set[str]] = None
_cached_at: float = 0.0
_CACHE_TTL = 60.0


def allowed_chat_ids() -> Set[str]:
    """Load the allowlist from Keychain with 60s TTL cache.

    - On Keychain read failure (locked after sleep/wake, `security` timeout):
      keep the previous cache. Blanking to empty would lock the operator out
      for 60s. Only successful non-None reads replace the cache.
    - Cold start with no cache still fails closed (empty set) — no safe alternative.
    """
    global _cached_allowed, _cached_at
    now = time.time()
    if _cached_allowed is not None and (now - _cached_at) < _CACHE_TTL:
        return _cached_allowed

    raw, status = keychain_get_status("telegram-allowed-chat-ids")
    if raw is None:
        # Keychain unavailable. Preserve the previous cache if we have one;
        # only fall through to empty on cold start.
        if _cached_allowed is not None:
            # Distinguish locked (transient, actionable) from absent/error
            # (misconfiguration) so the log points at the right fix.
            if status == "locked":
                print(
                    "telegram_guard: keychain locked — keeping cached allowlist "
                    "(unlock login keychain to refresh)",
                    file=sys.stderr,
                )
            else:
                print(
                    "telegram_guard: keychain read failed ({}) — keeping cached "
                    "allowlist".format(status),
                    file=sys.stderr,
                )
            # Refresh timestamp so we don't hammer Keychain per-call while it's
            # broken; retry after the next TTL window.
            _cached_at = now
            return _cached_allowed
        # Cold start with no cache: fail closed.
        _cached_allowed = set()
        _cached_at = now
        return _cached_allowed

    _cached_allowed = {cid.strip() for cid in raw.split(",") if cid.strip()}
    _cached_at = now
    return _cached_allowed


def is_allowed(chat_id) -> bool:
    """Check if a chat_id is in the allowlist. Fail-closed."""
    allowed = allowed_chat_ids()
    if not allowed:
        print("telegram_guard: no allowlist found in Keychain — blocking all", file=sys.stderr)
        return False
    return str(chat_id) in allowed


def reject_message(token: str, chat_id, text: str = "This bot is private.") -> None:
    """Send a rejection notice to an unauthorized sender.

    - Default `REJECTION_MODE == "silent"` sends nothing (no enumeration oracle);
      the rejected chat_id is still logged at the batch_classifier call site so
      abuse/replay signal is preserved. Set `"reply"` for the legacy loud reply.
    """
    if REJECTION_MODE == "silent":
        return
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass
