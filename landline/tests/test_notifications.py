"""Tests for landline.runtime.notifications — iMessage alert delivery.

Sends are async — ``send_network_alert`` and ``send_health_alert``
spawn a daemon thread and return immediately. Tests that observe the
``osascript`` subprocess must call ``_wait_for_pending_alerts()``
before asserting on ``no_subprocess``.

Transport: ``osascript -e 'tell application "Messages" to send "<body>"
to participant "<handle>"'``. Argv shape is ``["osascript", "-e", script]``;
the body and handle are extracted from the script literal for content
assertions.
"""

import threading
import time
from unittest.mock import patch, MagicMock

from landline.runtime.notifications import (
    _escape_applescript_literal,
    _wait_for_pending_alerts,
    send_health_alert,
    send_network_alert,
)


def _osascript_calls(calls):
    """Filter out non-osascript subprocess calls (defensive against unrelated
    subprocess.run patches leaking calls into the same mock)."""
    return [
        c for c in calls
        if c[0] and c[0][0] and c[0][0][0] == "osascript"
    ]


def _script_from_call(call):
    """Extract the AppleScript string passed after ``-e`` in an argv tuple.

    ``osascript`` is invoked as ``["osascript", "-e", <script>]``; return
    the raw script so tests can assert on its shape or contents.
    """
    argv = call[0][0]
    assert argv[0] == "osascript"
    assert argv[1] == "-e"
    return argv[2]


def _extract_between_quotes(script, marker_prefix):
    """Pull the literal that follows a marker like `send "` in the script,
    respecting the AppleScript backslash-escape convention we produce.

    Returns the raw (still-escaped) literal — callers that want the
    logical string should unescape ``\\\\`` → ``\\`` and ``\\"`` → ``"``.
    """
    start = script.index(marker_prefix) + len(marker_prefix)
    i = start
    while i < len(script):
        if script[i] == "\\":
            i += 2
            continue
        if script[i] == '"':
            return script[start:i]
        i += 1
    raise AssertionError("no closing quote after %r in %r" % (marker_prefix, script))


def _body_from_call(call):
    """Extract the (escaped) body literal that was substituted into the
    ``send "<body>"`` slot of the AppleScript."""
    return _extract_between_quotes(_script_from_call(call), 'send "')


def _handle_from_call(call):
    """Extract the (escaped) handle literal from the ``to participant
    "<handle>"`` slot of the AppleScript."""
    return _extract_between_quotes(
        _script_from_call(call), 'to participant "'
    )


class TestSendNetworkAlert:
    def test_sends_osascript_with_outage_duration(self, no_subprocess):
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(120.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        assert len(osa_calls) == 1

    def test_message_contains_outage_seconds(self, no_subprocess):
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(300.5)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        body = _body_from_call(osa_calls[0])
        assert "300" in body

    def test_skips_when_no_owner_handle(self, no_subprocess):
        with patch("landline.runtime.notifications.keychain_get", return_value=None):
            send_network_alert(60.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        assert len(osa_calls) == 0

    def test_survives_subprocess_exception(self):
        """A subprocess failure must not propagate — alert is best-effort."""
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"), \
             patch("subprocess.run", side_effect=Exception("osascript not found")):
            # The contract: send_network_alert never raises. If this changes,
            # the daemon's poller would crash on a transient osascript failure.
            send_network_alert(60.0)
            _wait_for_pending_alerts()
            # Reached here without re-raising → contract held.
            assert True

    def test_uses_owner_handle_from_keychain(self, no_subprocess):
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(100.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        assert _handle_from_call(osa_calls[0]) == "test-handle"

    def test_subprocess_uses_timeout(self, no_subprocess):
        """osascript call must use a timeout — a hung Messages handoff would
        block the alert worker thread indefinitely, and pile up threads on
        repeated alerts."""
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(50.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        kwargs = osa_calls[0][1]
        assert "timeout" in kwargs
        assert kwargs["timeout"] > 0

    def test_outage_truncated_to_int_in_message(self, no_subprocess):
        """The alert body uses int(outage_seconds); fractional seconds must
        not appear as '300.5'."""
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(300.99)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        body = _body_from_call(osa_calls[0])
        assert "300s" in body
        assert "300.99" not in body
        assert "300.0" not in body

    def test_message_identifies_agent(self, no_subprocess):
        """Alert should be attributable so the operator knows what's pinging them."""
        from landline.config import AGENT_NAME
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(60.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        body = _body_from_call(osa_calls[0])
        assert f"[{AGENT_NAME}]" in body

    def test_argv_shape_is_osascript_dash_e_script(self, no_subprocess):
        """Regression: argv[0] must be ``osascript``, argv[1] ``-e``, and
        argv[2] the AppleScript. Reverting to a bespoke CLI (imsg send
        --to ... --text ...) would silently no-op on any deploy without
        that private tool — this test locks the transport contract."""
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_network_alert(60.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        argv = osa_calls[0][0][0]
        assert argv[0] == "osascript"
        assert argv[1] == "-e"
        script = argv[2]
        assert 'tell application "Messages"' in script
        assert "to send " in script
        assert "to participant " in script


# ---------------------------------------------------------------------------
# Async fire-and-forget contract
# ---------------------------------------------------------------------------


class TestSendNetworkAlertAsync:
    """Regression: the osascript subprocess must run on a background thread
    so the poller thread returns immediately, no matter how slow Messages is."""

    def test_send_network_alert_returns_before_subprocess_completes(self):
        """subprocess.run sleeps 5s; send_network_alert must return in <500ms.

        Reverting to an in-line ``subprocess.run`` inside send_network_alert
        makes the caller block the full 5s and this assertion fails.
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
            with patch("landline.runtime.notifications.keychain_get",
                       return_value="test-handle"), \
                 patch("subprocess.run", side_effect=slow_run):
                start = time.monotonic()
                send_network_alert(60.0)
                elapsed = time.monotonic() - start
                # Poller thread must return promptly regardless of
                # osascript latency. 0.5s is very generous — the actual
                # return is sub-millisecond.
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
        with patch("landline.runtime.notifications.keychain_get",
                   return_value="test-handle"), \
             patch("subprocess.run", side_effect=RuntimeError("boom")), \
             patch("landline.runtime.notifications.log") as mock_log:
            # Caller must return normally.
            send_network_alert(42.0)
            _wait_for_pending_alerts()
        # Something in the caller or worker logged — at minimum the
        # worker's `_do_osascript` except-branch logs the exception.
        assert mock_log.called, (
            "expected some log line — either the pre-thread intent line "
            "or the worker's Failed to send iMessage alert branch"
        )
        logged = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
        assert "iMessage" in logged, (
            "expected the iMessage-failure log line; got: %s" % logged
        )


# ---------------------------------------------------------------------------
# send_health_alert (general-purpose async alert)
# ---------------------------------------------------------------------------


class TestSendHealthAlert:
    def test_send_health_alert_uses_subject_and_body_from_call(self, no_subprocess):
        """The osascript body must contain both the subject and body strings
        so an operator can identify the source at a glance."""
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            result = send_health_alert(
                subject="claude-auth-expired",
                body="the operator's CC token returned 401; all headless jobs failing.",
            )
            _wait_for_pending_alerts()
        assert result is True
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        assert len(osa_calls) == 1
        body = _body_from_call(osa_calls[0])
        assert "claude-auth-expired" in body
        assert "the operator's CC token" in body

    def test_send_health_alert_no_handle_returns_false(self, no_subprocess):
        """When the Keychain lookup returns nothing, no thread is spawned
        AND the caller sees ``False`` — the auth-alert latch uses this to
        gate itself (don't set the latch if the alert didn't go out)."""
        with patch("landline.runtime.notifications.keychain_get", return_value=None):
            result = send_health_alert(
                subject="unused-subject",
                body="unused-body",
            )
            _wait_for_pending_alerts()
        assert result is False
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        assert len(osa_calls) == 0

    def test_send_health_alert_returns_true_without_blocking(self):
        """Health alert also honours the fire-and-forget contract."""
        released = threading.Event()

        def slow_run(cmd, *args, **kwargs):
            released.wait(timeout=5.0)
            return MagicMock(returncode=0, stdout="", stderr="")

        try:
            with patch("landline.runtime.notifications.keychain_get",
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
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"):
            send_health_alert(subject="a-subject", body="a-body")
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        body = _body_from_call(osa_calls[0])
        assert f"[{AGENT_NAME}]" in body


# ---------------------------------------------------------------------------
# AppleScript escaping — untrusted body content must not break the literal
# ---------------------------------------------------------------------------


class TestAppleScriptEscaping:
    """The AppleScript literal is composed by string concatenation, so a
    stray ``"`` or ``\\`` in the body (or handle) would either unbalance
    the literal or inject an unintended escape. Both operands must be
    escaped: backslashes first (so the double-quote pass doesn't re-escape
    the escape character), then double quotes."""

    def test_helper_escapes_backslash_then_double_quote(self):
        # Order matters: backslashes MUST be doubled before quotes are
        # backslash-escaped, or the quote-escape's backslash gets doubled.
        assert _escape_applescript_literal('a"b') == 'a\\"b'
        assert _escape_applescript_literal("a\\b") == "a\\\\b"
        # A literal that contains BOTH — the composed output must have
        # each backslash doubled and each quote backslash-prefixed, with
        # no cross-escape corruption.
        assert _escape_applescript_literal('a"\\b') == 'a\\"\\\\b'

    def test_body_with_quote_and_backslash_stays_balanced(self, no_subprocess):
        """A body containing ``"`` and ``\\`` must be escaped so the
        AppleScript string literal stays balanced (each unescaped ``"``
        in the emitted script must be a literal delimiter, not a payload
        character). Without escaping, the send would silently corrupt or
        error out."""
        tricky_body = 'she said "hi" then wrote c:\\path\\to\\file'
        with patch("landline.runtime.notifications.keychain_get", return_value="test-handle"), \
             patch("landline.runtime.notifications.AGENT_NAME", "Assistant"):
            # send_network_alert composes its own body; use send_health_alert
            # so we can pass the tricky text through.
            send_health_alert(subject="s", body=tricky_body)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        script = _script_from_call(osa_calls[0])

        # 1. The raw payload characters MUST NOT appear unescaped —
        #    otherwise the AppleScript parser would see them as literal
        #    delimiters / escape starts.
        assert '"hi"' not in script, (
            "raw double-quotes leaked into the script literal; "
            "unbalances the AppleScript string"
        )
        # 2. The escaped forms MUST appear — this is what keeps the
        #    literal balanced.
        assert '\\"hi\\"' in script
        assert "c:\\\\path\\\\to\\\\file" in script

        # 3. The composed script itself is well-formed: count unescaped
        #    double-quotes. There should be exactly six — open+close of
        #    the static "Messages" literal, open+close of the body
        #    literal, open+close of the handle literal. A missed escape
        #    shows up as an odd count.
        unescaped_quotes = 0
        i = 0
        while i < len(script):
            if script[i] == "\\":
                i += 2
                continue
            if script[i] == '"':
                unescaped_quotes += 1
            i += 1
        assert unescaped_quotes == 6, (
            "expected 6 unescaped `\"` (Messages + body + handle "
            "delimiters); got %d in %r" % (unescaped_quotes, script)
        )

    def test_handle_with_quote_is_escaped(self, no_subprocess):
        """The Keychain-sourced handle is defensively escaped too — a
        stray ``"`` in the handle would break the ``participant`` clause."""
        with patch(
            "landline.runtime.notifications.keychain_get",
            return_value='weird"handle',
        ):
            send_network_alert(10.0)
            _wait_for_pending_alerts()
        osa_calls = _osascript_calls(no_subprocess["run"].call_args_list)
        script = _script_from_call(osa_calls[0])
        assert 'participant "weird\\"handle"' in script
