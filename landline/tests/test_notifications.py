"""Tests for landline.notifications — iMessage alert delivery.

Cluster 1 (M13): sends are async — ``send_network_alert`` and
``send_health_alert`` spawn a daemon thread and return immediately.
Tests that observe the ``imsg send`` subprocess must call
``_wait_for_pending_alerts()`` before asserting on ``no_subprocess``.
"""

import threading
import time
from unittest.mock import patch, MagicMock

from landline.notifications import (
    _wait_for_pending_alerts,
    send_health_alert,
    send_network_alert,
)


def _imsg_calls(calls):
    """Filter out non-imsg subprocess calls (defensive against unrelated
    subprocess.run patches leaking calls into the same mock)."""
    return [
        c for c in calls
        if c[0] and c[0][0] and c[0][0][0] == "imsg"
    ]


class TestSendNetworkAlert:
    def test_sends_imsg_with_outage_duration(self, no_subprocess):
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(120.0)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        assert len(imsg_calls) == 1

    def test_message_contains_outage_seconds(self, no_subprocess):
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(300.5)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        cmd = imsg_calls[0][0][0]
        text_idx = cmd.index("--text") + 1
        assert "300" in cmd[text_idx]

    def test_skips_when_no_owner_handle(self, no_subprocess):
        with patch("landline.notifications.keychain_get", return_value=None):
            send_network_alert(60.0)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        assert len(imsg_calls) == 0

    def test_survives_subprocess_exception(self):
        """A subprocess failure must not propagate — alert is best-effort."""
        with patch("landline.notifications.keychain_get", return_value="test-handle"), \
             patch("subprocess.run", side_effect=Exception("imsg not found")):
            # The contract: send_network_alert never raises. If this changes,
            # the daemon's poller would crash on a transient imsg failure.
            send_network_alert(60.0)
            _wait_for_pending_alerts()
            # Reached here without re-raising → contract held.
            assert True

    def test_uses_owner_handle_from_keychain(self, no_subprocess):
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(100.0)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        cmd = imsg_calls[0][0][0]
        to_idx = cmd.index("--to") + 1
        assert cmd[to_idx] == "test-handle"

    def test_subprocess_uses_timeout(self, no_subprocess):
        """imsg call must use a timeout — a hung send process would block the
        alert worker thread indefinitely, and pile up threads on repeated
        alerts."""
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(50.0)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        kwargs = imsg_calls[0][1]
        assert "timeout" in kwargs
        assert kwargs["timeout"] > 0

    def test_outage_truncated_to_int_in_message(self, no_subprocess):
        """The alert body uses int(outage_seconds); fractional seconds must
        not appear as '300.5'."""
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(300.99)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        cmd = imsg_calls[0][0][0]
        text_idx = cmd.index("--text") + 1
        body = cmd[text_idx]
        assert "300s" in body
        assert "300.99" not in body
        assert "300.0" not in body

    def test_message_identifies_agent(self, no_subprocess):
        """Alert should be attributable so the operator knows what's pinging them."""
        from landline.config import AGENT_NAME
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(60.0)
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        cmd = imsg_calls[0][0][0]
        text_idx = cmd.index("--text") + 1
        body = cmd[text_idx]
        assert f"[{AGENT_NAME}]" in body


# ---------------------------------------------------------------------------
# Cluster 1 M13 — async fire-and-forget contract
# ---------------------------------------------------------------------------


class TestSendNetworkAlertAsync:
    """M13 regression: the imsg subprocess must run on a background thread
    so the poller thread returns immediately, no matter how slow imsg is."""

    def test_send_network_alert_returns_before_subprocess_completes(self):
        """subprocess.run sleeps 5s; send_network_alert must return in <500ms.

        Reverting the M13 refactor (in-line subprocess.run inside
        send_network_alert) makes the caller block the full 5s and this
        assertion fails.
        """
        released = threading.Event()
        subprocess_started = threading.Event()

        def slow_run(cmd, *args, **kwargs):
            subprocess_started.set()
            # Block on the event so the worker thread is genuinely slow —
            # we time the caller, not this thread.
            released.wait(timeout=5.0)
            return MagicMock(returncode=0, stdout="", stderr="")

        try:
            with patch("landline.notifications.keychain_get",
                       return_value="test-handle"), \
                 patch("subprocess.run", side_effect=slow_run):
                start = time.monotonic()
                send_network_alert(60.0)
                elapsed = time.monotonic() - start
                # Poller thread must return promptly regardless of imsg
                # latency. 0.5s is very generous — the actual return is
                # sub-millisecond.
                assert elapsed < 0.5, (
                    "send_network_alert blocked for %.3fs — the poller "
                    "thread is supposed to be fire-and-forget (M13)" % elapsed
                )
                # Sanity: the worker thread actually did start the
                # subprocess.run call — we're not returning before
                # anything happens.
                assert subprocess_started.wait(timeout=1.0), (
                    "worker thread never reached subprocess.run"
                )
        finally:
            # Let the worker finish so it doesn't leak into other tests.
            released.set()
            _wait_for_pending_alerts()

    def test_send_network_alert_thread_error_swallowed(self):
        """An exception INSIDE the alert thread must not propagate to the
        caller and must be logged."""
        with patch("landline.notifications.keychain_get",
                   return_value="test-handle"), \
             patch("subprocess.run", side_effect=RuntimeError("boom")), \
             patch("landline.notifications.log") as mock_log:
            # Caller must return normally.
            send_network_alert(42.0)
            _wait_for_pending_alerts()
        # Something in the caller or worker logged — at minimum the
        # worker's `_do_imsg` except-branch logs the exception.
        assert mock_log.called, (
            "expected some log line — either the pre-thread intent line "
            "or the worker's Failed to send iMessage alert branch"
        )
        logged = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "iMessage" in logged or "imsg" in logged.lower(), (
            "expected the imsg-failure log line; got: %s" % logged
        )


# ---------------------------------------------------------------------------
# Cluster 1 — send_health_alert (general-purpose async alert)
# ---------------------------------------------------------------------------


class TestSendHealthAlert:
    def test_send_health_alert_uses_subject_and_body_from_call(self, no_subprocess):
        """The imsg body must contain both the subject and body strings so an
        operator can identify the source at a glance."""
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            result = send_health_alert(
                subject="claude-auth-expired",
                body="the operator's CC token returned 401; all headless jobs failing.",
            )
            _wait_for_pending_alerts()
        assert result is True
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        assert len(imsg_calls) == 1
        cmd = imsg_calls[0][0][0]
        text_idx = cmd.index("--text") + 1
        body = cmd[text_idx]
        assert "claude-auth-expired" in body
        assert "the operator's CC token" in body

    def test_send_health_alert_no_handle_returns_false(self, no_subprocess):
        """When the Keychain lookup returns nothing, no thread is spawned
        AND the caller sees ``False`` — Cluster 3 uses this to gate its
        auth-alert latch (don't set the latch if the alert didn't go out)."""
        with patch("landline.notifications.keychain_get", return_value=None):
            result = send_health_alert(
                subject="unused-subject",
                body="unused-body",
            )
            _wait_for_pending_alerts()
        assert result is False
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        assert len(imsg_calls) == 0

    def test_send_health_alert_returns_true_without_blocking(self):
        """Health alert also honours the fire-and-forget contract."""
        released = threading.Event()

        def slow_run(cmd, *args, **kwargs):
            released.wait(timeout=5.0)
            return MagicMock(returncode=0, stdout="", stderr="")

        try:
            with patch("landline.notifications.keychain_get",
                       return_value="test-handle"), \
                 patch("subprocess.run", side_effect=slow_run):
                start = time.monotonic()
                result = send_health_alert(subject="s", body="b")
                elapsed = time.monotonic() - start
                assert result is True
                assert elapsed < 0.5
        finally:
            released.set()
            _wait_for_pending_alerts()

    def test_send_health_alert_identifies_agent(self, no_subprocess):
        """Alerts should be attributable to the agent so the operator knows
        what's pinging them — mirrors send_network_alert."""
        from landline.config import AGENT_NAME
        with patch("landline.notifications.keychain_get", return_value="test-handle"):
            send_health_alert(subject="a-subject", body="a-body")
            _wait_for_pending_alerts()
        imsg_calls = _imsg_calls(no_subprocess["run"].call_args_list)
        cmd = imsg_calls[0][0][0]
        text_idx = cmd.index("--text") + 1
        body = cmd[text_idx]
        assert f"[{AGENT_NAME}]" in body
