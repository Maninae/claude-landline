"""iMessage alert delivery for the daemon.

Extracted from BackgroundPoller._send_network_alert so notification
logic is reusable by other modules (e.g. crash alerts, health checks).

Cluster 1 (M13): the actual ``osascript`` subprocess is dispatched on a
short-lived daemon thread so a hung AppleScript event or slow Messages
handoff never blocks the poller / dispatcher / StreamSender worker that
fired the alert. The caller sees a fire-and-forget contract: build the
message, log the intent, return.

Transport: ``osascript -e 'tell application "Messages" to send "<body>"
to participant "<handle>"'``. This mirrors ``deploy/watchdog.sh`` and
removes the previous dependency on a private per-user CLI, which
silently no-op'd on any machine without that personal tool and made
the auth-expiry / network alerts inert.
"""

import subprocess
import threading
from typing import List

from landline.config import AGENT_NAME, IMESSAGE_SEND_SUBPROCESS_TIMEOUT_SECONDS
from landline.runtime.logging import log
from landline.runtime.security import keychain_get


# Threads we've spawned that are still (or may still be) running the
# ``osascript`` subprocess. Populated by ``_run_osascript_in_thread`` and
# drained by ``_wait_for_pending_alerts`` — the latter is a test-only
# helper so the sync-vs-async alert contract stays observable in unit
# tests. Guarded by ``_pending_alerts_lock`` because threads are
# started from the poller, dispatcher, and StreamSender workers.
_pending_alerts_lock = threading.Lock()
_pending_alert_threads: List[threading.Thread] = []


def _escape_applescript_literal(s: str) -> str:
    """Escape a Python string so it can be embedded inside an AppleScript
    string literal (between double quotes) without breaking parsing.

    Backslashes go first so the double-quote pass doesn't re-escape the
    escape character we just inserted. Even though the Keychain-sourced
    handle is trusted, we escape both operands defensively — the body is
    agent-generated free text and a stray `"` in either operand would
    unbalance the AppleScript literal and either silently drop the send
    or produce a compile error we'd only see in the log.
    """
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _do_osascript(handle: str, body: str) -> None:
    """Run the ``osascript`` subprocess on the worker thread.

    Swallows every exception (subprocess timeout, missing binary,
    unicode-encode issues) and logs it — the caller has already
    returned, so a raise here would only kill this daemon thread.
    """
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
    """Fire-and-forget: spawn a daemon thread to run ``_do_osascript`` and return.

    Returns the ``Thread`` so tests can join it. Production callers ignore
    the return value; the thread is ``daemon=True`` so it never blocks
    process exit.
    """
    thread = threading.Thread(
        target=_do_osascript,
        args=(handle, body),
        daemon=True,
        name="landline-imessage-alert",
    )
    with _pending_alerts_lock:
        # Drop any threads that have already finished so the list doesn't
        # grow without bound over the daemon's lifetime.
        _pending_alert_threads[:] = [t for t in _pending_alert_threads if t.is_alive()]
        _pending_alert_threads.append(thread)
    thread.start()
    return thread


def send_network_alert(outage_seconds: float) -> None:
    """Send a one-shot iMessage alert when the network has been down.

    Returns immediately — the osascript subprocess runs on a background
    thread (M13). A missing owner-imsg-handle (Keychain service name
    kept for production compatibility) is a silent no-op except for a
    log line; the poller must never crash because Keychain is empty in
    tests.
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
        # Any failure BEFORE the thread starts (Keychain lookup, log
        # handler, etc.) must not propagate to the poller thread.
        log(f"send_network_alert error: {alert_error}")


def send_health_alert(subject: str, body: str) -> bool:
    """General-purpose async health alert.

    Builds ``[<AGENT_NAME>] <subject>\\n<body>`` and dispatches it on a background
    thread (same fire-and-forget contract as ``send_network_alert``).

    Returns True iff the alert thread was started (owner handle present in
    Keychain). Cluster 3 uses this return value to gate its auth-alert
    latch — a False result means the alert never went out, so the latch
    should NOT be set.
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
        # Same contract as send_network_alert — the caller must never see
        # an exception, even if Keychain / logging misbehave.
        log(f"send_health_alert error: {alert_error}")
        return False


def _wait_for_pending_alerts(timeout: float = 2.0) -> None:
    """Test-only helper: block until every spawned alert thread finishes.

    Production code never calls this — the whole point of the async path
    is that the poller thread doesn't wait on osascript. Tests use it to
    observe the subprocess call site after ``send_*_alert`` returned.
    """
    with _pending_alerts_lock:
        pending = list(_pending_alert_threads)
        _pending_alert_threads.clear()
    for t in pending:
        t.join(timeout=timeout)
