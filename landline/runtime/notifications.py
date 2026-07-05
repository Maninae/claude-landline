"""iMessage alert delivery for the daemon.

- Fire-and-forget contract: `osascript` runs on a short-lived daemon thread so
  a hung AppleScript event never blocks the poller / dispatcher / StreamSender
  worker that fired the alert.
- Transport: `osascript -e 'tell application "Messages" to send "<body>" to
  participant "<handle>"'` — mirrors `deploy/watchdog.sh`. Replaces an earlier
  per-user CLI dep that silently no-op'd on any machine without it.
"""

import subprocess
import threading
from typing import List

from landline.config import AGENT_NAME, IMESSAGE_SEND_SUBPROCESS_TIMEOUT_SECONDS
from landline.runtime.logging import log
from landline.runtime.security import keychain_get


# Live osascript worker threads — populated on spawn, drained by the
# test-only `_wait_for_pending_alerts` (production never joins). Lock guards
# multi-thread producers (poller / dispatcher / StreamSender workers).
_pending_alerts_lock = threading.Lock()
_pending_alert_threads: List[threading.Thread] = []


def _escape_applescript_literal(s: str) -> str:
    """Escape a Python string for embedding in an AppleScript "…" literal.

    - Backslashes escaped first so the "-pass doesn't re-escape them.
    - Both operands escaped defensively: body is agent-generated free text;
      a stray `"` would unbalance the literal and silently drop the send.
    """
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _do_osascript(handle: str, body: str) -> None:
    """Run `osascript` on the worker thread. Logs and swallows every failure —
    the caller has already returned, so a raise would only kill this thread."""
    esc_body = _escape_applescript_literal(body)
    esc_handle = _escape_applescript_literal(handle)
    script = (
        'tell application "Messages" to send "'
        + esc_body
        + '" to participant "'
        + esc_handle
        + '"'
    )
    try:
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=IMESSAGE_SEND_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except Exception as osascript_error:
        log(f"Failed to send iMessage alert: {osascript_error}")


def _run_osascript_in_thread(handle: str, body: str) -> threading.Thread:
    """Fire-and-forget: spawn a daemon thread running `_do_osascript`, return it.

    - Returned Thread is for tests to join; production callers discard it.
    - `daemon=True` so it never blocks process exit.
    """
    thread = threading.Thread(
        target=_do_osascript,
        args=(handle, body),
        daemon=True,
        name="landline-imessage-alert",
    )
    with _pending_alerts_lock:
        # Reap finished threads so the list can't grow unbounded.
        _pending_alert_threads[:] = [t for t in _pending_alert_threads if t.is_alive()]
        _pending_alert_threads.append(thread)
    thread.start()
    return thread


def send_network_alert(outage_seconds: float) -> None:
    """Send a one-shot iMessage alert when the network has been down.

    - Returns immediately (osascript runs on a background thread).
    - Missing `owner-imsg-handle` is a silent no-op + log line; the poller must
      never crash because Keychain is empty (test envs).
    """
    try:
        owner_handle = keychain_get("owner-imsg-handle")
        if not owner_handle:
            log("No owner-imsg-handle in Keychain, skipping network alert")
            return
        msg = (
            f"[{AGENT_NAME}] Telegram daemon cannot reach network "
            f"({int(outage_seconds)}s). Polling with backoff. "
            f"Messages may be delayed."
        )
        log(f"Sent network-down iMessage alert (outage {int(outage_seconds)}s)")
        _run_osascript_in_thread(owner_handle, msg)
    except Exception as alert_error:
        # Never propagate to the poller thread — swallow pre-spawn failures.
        log(f"send_network_alert error: {alert_error}")


def send_health_alert(subject: str, body: str) -> bool:
    """General-purpose async health alert.

    Args:
        subject: single-line title (prefixed with `[<AGENT_NAME>]`).
        body: message body, joined with `\\n`.
    Returns:
        True iff the alert thread was started (owner handle present).
        Callers gate one-shot latches on this: a False means nothing went out.

    - Same fire-and-forget contract as `send_network_alert`.
    """
    try:
        owner_handle = keychain_get("owner-imsg-handle")
        if not owner_handle:
            log(
                f"No owner-imsg-handle in Keychain, skipping health alert "
                f"(subject={subject!r})"
            )
            return False
        msg = f"[{AGENT_NAME}] {subject}\n{body}"
        log(f"Sent health iMessage alert (subject={subject!r})")
        _run_osascript_in_thread(owner_handle, msg)
        return True
    except Exception as alert_error:
        # Same contract as send_network_alert — never propagate.
        log(f"send_health_alert error: {alert_error}")
        return False


def _wait_for_pending_alerts(timeout: float = 2.0) -> None:
    """Test-only: block until every spawned alert thread finishes.

    Production never calls this — the whole point of the async path is that
    the poller doesn't wait on osascript. Tests use it to observe the
    subprocess call site after `send_*_alert` returned.
    """
    with _pending_alerts_lock:
        pending = list(_pending_alert_threads)
        _pending_alert_threads.clear()
    for t in pending:
        t.join(timeout=timeout)
