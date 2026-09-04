"""Telegram sender allowlist gate. Fail-closed: empty allowlist blocks everyone.

Authorization identity is the **Telegram user id** (``message["from"]["id"]``),
NOT the chat id. In a 1:1 private-chat bot the two are numerically equal (which
is why the legacy chat-id gate never mis-authorized in practice), but the user
id is the identity a group/guest chat would present — the chat id in that shape
is the group's, not the sender's. Authorizing on ``from.id`` closes that door.

Allowed user ids are stored in macOS Keychain:
  service: telegram-allowed-chat-ids
  account: <KEYCHAIN_ACCOUNT>   (default "landline"; see landline.json)
  value:   comma-separated Telegram integer user ids
           (e.g. "111111111,222222222")

The service name and comma-string value shape are unchanged from the legacy
chat-id era so an existing Keychain entry keeps working without a migration
step (``chat_id == from_id`` for owner 1:1 chats).
"""

import json
import sys
import time
import urllib.request
from typing import Optional, Set

from landline.config import REJECTION_MODE
from landline.runtime.security import keychain_get_status

_cached_allowed: Optional[Set[int]] = None
_cached_at: float = 0.0
_CACHE_TTL = 60.0


def _parse_int_set(raw: str) -> Set[int]:
    """Parse the Keychain comma-string into a ``Set[int]``.

    - Whitespace tolerant (``" 111 , 222 "`` → ``{111, 222}``).
    - Non-integer tokens are silently skipped rather than crashing the daemon
      on a hand-edited Keychain typo. Effect is fail-closed on a fully-junk
      allowlist (empty set → block everyone), never fail-open.
    - Empty input → empty set (also fail-closed via ``is_allowed``).
    """
    out: Set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            # Log via stderr so launchd captures it; do NOT admit the bad
            # token. A fully-invalid allowlist reduces to empty and is then
            # blocked by is_allowed's fail-closed branch.
            print(
                "telegram_guard: skipping non-integer allowlist token %r"
                % token,
                file=sys.stderr,
            )
    return out


def allowed_chat_ids() -> Set[int]:
    """Load the allowlist from Keychain with 60s TTL cache.

    Function name preserved for backward compat with the pre-migration caller
    surface; semantically these are now Telegram **user ids** (``from.id``),
    NOT chat ids. See module docstring.

    - Return type is ``Set[int]`` (int semantics + set-membership check).
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
            # stderr on purpose, not log(): launchd captures it via
            # StandardErrorPath, and the guard tests assert on captured.err.
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

    _cached_allowed = _parse_int_set(raw)
    _cached_at = now
    return _cached_allowed


def is_allowed(user_id) -> bool:
    """Check if a Telegram user id is in the allowlist. Fail-closed.

    ``user_id`` accepts int-or-str for defense against a caller that hasn't
    coerced yet; anything that fails ``int(...)`` is treated as unauthorized
    (never crash the classifier because of a malformed input).
    """
    allowed = allowed_chat_ids()
    if not allowed:
        print("telegram_guard: no allowlist found in Keychain — blocking all", file=sys.stderr)
        return False
    try:
        return int(user_id) in allowed
    except (TypeError, ValueError):
        return False


def reject_message(token: str, chat_id, text: str = "This bot is private.") -> None:
    """Send a rejection notice to an unauthorized sender.

    - Default `REJECTION_MODE == "silent"` sends nothing (no enumeration oracle);
      the rejected chat_id / user_id is still logged at the batch_classifier
      call site so abuse/replay signal is preserved. Set `"reply"` for the
      legacy loud reply.
    - ``chat_id`` here is the chat to reply INTO — the surface where the
      unauthorized sender messaged from. The AUTH check upstream keys on the
      sender's ``from.id`` (see module docstring); this parameter only picks
      the destination for the (optional) loud-mode reply.
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
