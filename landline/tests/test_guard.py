"""Tests for landline.guard — Telegram sender allowlist gate.

Critical behavior: fail-closed (empty allowlist blocks everyone).
"""

import time
from unittest.mock import patch, MagicMock

import pytest

import landline.guard as guard_module
from landline.guard import allowed_chat_ids, is_allowed, reject_message


class TestAllowedChatIds:
    def test_parses_comma_separated_ids(self):
        with patch("landline.guard.keychain_get_status", return_value=("111,222,333", "ok")):
            ids = allowed_chat_ids()
        assert ids == {"111", "222", "333"}

    def test_strips_whitespace(self):
        with patch("landline.guard.keychain_get_status", return_value=(" 111 , 222 ", "ok")):
            ids = allowed_chat_ids()
        assert ids == {"111", "222"}

    def test_empty_keychain_returns_empty_set(self):
        with patch("landline.guard.keychain_get_status", return_value=(None, "absent")):
            ids = allowed_chat_ids()
        assert ids == set()

    def test_empty_string_returns_empty_set(self):
        with patch("landline.guard.keychain_get_status", return_value=("", "ok")):
            ids = allowed_chat_ids()
        assert ids == set()

    def test_caching_within_ttl(self):
        with patch("landline.guard.keychain_get_status", return_value=("111", "ok")) as mock_kc:
            allowed_chat_ids()
            allowed_chat_ids()
        assert mock_kc.call_count == 1

    def test_cache_expires_after_ttl(self):
        with patch("landline.guard.keychain_get_status", return_value=("111", "ok")) as mock_kc:
            allowed_chat_ids()
            guard_module._cached_at = time.time() - 120
            allowed_chat_ids()
        assert mock_kc.call_count == 2

    def test_single_id(self):
        with patch("landline.guard.keychain_get_status", return_value=("12345", "ok")):
            ids = allowed_chat_ids()
        assert ids == {"12345"}


class TestIsAllowed:
    def test_allowed_chat_id(self):
        with patch("landline.guard.keychain_get_status", return_value=("123,456", "ok")):
            assert is_allowed(123) is True
            assert is_allowed("456") is True

    def test_denied_chat_id(self):
        with patch("landline.guard.keychain_get_status", return_value=("123", "ok")):
            assert is_allowed(999) is False

    def test_fail_closed_empty_allowlist(self):
        """CRITICAL: empty allowlist must block everyone."""
        with patch("landline.guard.keychain_get_status", return_value=(None, "absent")):
            assert is_allowed(123) is False

    def test_fail_closed_empty_string(self):
        with patch("landline.guard.keychain_get_status", return_value=("", "ok")):
            assert is_allowed(123) is False

    def test_int_and_string_chat_id(self):
        with patch("landline.guard.keychain_get_status", return_value=("123456789", "ok")):
            assert is_allowed(123456789) is True
            assert is_allowed("123456789") is True


class TestRejectMessageLoudMode:
    """Legacy loud-reply behavior under REJECTION_MODE='reply' (escape hatch
    for incident response — flip config to verify the bot is live from an
    unauthorized chat). Default mode is "silent"; these tests pin the
    explicit-reply path so it stays functional."""

    def setup_method(self, method):
        self._mode_patcher = patch("landline.guard.REJECTION_MODE", "reply")
        self._mode_patcher.start()

    def teardown_method(self, method):
        self._mode_patcher.stop()

    def test_sends_rejection_via_api(self, no_network):
        import json
        reject_message("fake-token", "12345", "Go away")
        assert no_network.called
        req = no_network.call_args[0][0]
        assert "fake-token" in req.full_url
        assert req.full_url.endswith("/sendMessage")
        body = json.loads(req.data.decode())
        assert body["chat_id"] == "12345"
        assert body["text"] == "Go away"

    def test_survives_network_error(self):
        """Network errors must be swallowed silently — the daemon must never
        crash because a rejection notice failed to deliver."""
        with patch("urllib.request.urlopen", side_effect=Exception("network")):
            # No assertion needed: the absence of a raised exception IS the test.
            reject_message("token", "12345")

    def test_default_message(self, no_network):
        import json
        reject_message("token", "12345")
        body = json.loads(no_network.call_args[0][0].data.decode())
        # Default rejection text — short and unambiguous.
        assert body["text"] == "This bot is private."

    def test_custom_message(self, no_network):
        import json
        reject_message("token", "12345", "Custom rejection")
        body = json.loads(no_network.call_args[0][0].data.decode())
        assert body["text"] == "Custom rejection"


class TestRejectMessageSilentMode:
    """B3 - default silent mode removes the enumeration oracle. Unauthorized
    senders get no outbound reply; rejected chat_id is still logged at the
    classifier (batch_classifier.log) so detection is preserved."""

    def test_silent_is_default_mode(self):
        """Pins the Wave 0 default. Fails if a future commit flips the default
        to 'reply' without updating the security review."""
        from landline.config import REJECTION_MODE as cfg_mode
        assert cfg_mode == "silent"

    def test_silent_rejection_sends_no_reply(self):
        """The core silent-mode contract: no outbound HTTP call."""
        with patch("landline.guard.REJECTION_MODE", "silent"), \
             patch("urllib.request.urlopen") as mock_open:
            reject_message("token", "12345")
        assert mock_open.called is False

    def test_silent_rejection_with_custom_text_still_silent(self):
        """The text= arg must NOT bypass silent mode."""
        with patch("landline.guard.REJECTION_MODE", "silent"), \
             patch("urllib.request.urlopen") as mock_open:
            reject_message("token", "12345", "Custom rejection")
        assert mock_open.called is False


class TestCacheBehavior:
    def test_cache_is_per_value(self):
        """A keychain change WITHIN the TTL is invisible — that's the cache's
        purpose. Documents the operator-visible behavior (60s lag for changes)."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), ("222", "ok")],
        ) as mock_kc:
            first = allowed_chat_ids()
            second = allowed_chat_ids()
        assert first == {"111"}
        assert second == {"111"}  # cached, not the new "222"
        assert mock_kc.call_count == 1

    def test_cache_refresh_picks_up_new_value(self):
        """After the TTL expires, the next call sees the updated keychain value."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), ("222", "ok")],
        ):
            first = allowed_chat_ids()
            guard_module._cached_at = time.time() - 120
            second = allowed_chat_ids()
        assert first == {"111"}
        assert second == {"222"}

    def test_keychain_failure_preserves_cached_allowlist(self):
        """Resilience contract: when keychain_get_status returns (None, ...) after
        TTL expiry, the previously cached allowlist must be preserved — NOT
        replaced with an empty set. Otherwise a transient Keychain failure
        (locked after sleep/wake) would drop every message for the next 60s.
        Uses 'locked' status to also exercise the B5 locked-branch log path."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "locked")],
        ):
            assert is_allowed("111") is True  # populates cache from "111"
            guard_module._cached_at = time.time() - 120  # force TTL expiry
            # Refresh attempt fails (locked) — old cache must survive.
            assert is_allowed("111") is True

    def test_keychain_failure_on_cold_start_fails_closed(self):
        """If keychain_get_status fails BEFORE any successful read, there is no
        prior cache to preserve — fail closed (empty allowlist, deny everyone)."""
        with patch(
            "landline.guard.keychain_get_status",
            return_value=(None, "absent"),
        ):
            assert is_allowed("111") is False
            assert is_allowed("anything") is False

    def test_keychain_failure_does_not_thrash_keychain(self):
        """After a failed refresh, the cache timestamp should advance so we
        don't call `security` on every single message until the next TTL."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "absent")],
        ) as mock_kc:
            allowed_chat_ids()  # populates cache
            guard_module._cached_at = time.time() - 120  # force refresh
            allowed_chat_ids()  # fails, preserves cache, advances timestamp
            allowed_chat_ids()  # should NOT call keychain again — within TTL
        assert mock_kc.call_count == 2

    def test_successful_refresh_replaces_cache(self):
        """A successful keychain read after a failure must update the cache."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "absent"), ("999", "ok")],
        ):
            assert is_allowed("111") is True
            guard_module._cached_at = time.time() - 120  # expire
            assert is_allowed("111") is True  # keychain failed, kept "111"
            guard_module._cached_at = time.time() - 120  # expire again
            # Keychain recovers and returns "999" — cache replaced.
            assert is_allowed("999") is True
            assert is_allowed("111") is False

    def test_guard_logs_locked_distinctly_after_cache_expiry(self, capsys):
        """B5 - when keychain is locked, the guard must log a DISTINCT message
        mentioning 'locked' and 'unlock login keychain' so the operator sees an
        actionable signal. Cache must still be preserved."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "locked")],
        ):
            assert is_allowed("111") is True  # populate cache
            guard_module._cached_at = time.time() - 120  # force TTL expiry
            assert is_allowed("111") is True  # locked branch, cache preserved
        captured = capsys.readouterr()
        assert "keychain locked" in captured.err
        assert "unlock login keychain" in captured.err

    def test_guard_logs_generic_message_on_absent(self, capsys):
        """B5 - the 'absent' branch must NOT say 'locked' (that's a different
        actionable signal). Catches a builder that hardcodes 'locked' for
        every failure mode."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "absent")],
        ):
            assert is_allowed("111") is True
            guard_module._cached_at = time.time() - 120
            assert is_allowed("111") is True
        captured = capsys.readouterr()
        assert "(absent)" in captured.err
        assert "keychain locked" not in captured.err

    def test_guard_locked_does_not_thrash_keychain(self):
        """B5 - after a locked failure, the cache timestamp advances so we
        don't hammer the keychain. Same throttle as the existing error path."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("111", "ok"), (None, "locked")],
        ) as mock_kc:
            allowed_chat_ids()  # populates cache
            guard_module._cached_at = time.time() - 120  # force refresh
            allowed_chat_ids()  # locked, preserves cache, advances timestamp
            allowed_chat_ids()  # should NOT call keychain again — within TTL
        assert mock_kc.call_count == 2

    def test_guard_preserves_cache_on_locked(self):
        """B5 variant of the resilience contract - locked path specifically
        must preserve the prior cache (not invert to empty)."""
        with patch(
            "landline.guard.keychain_get_status",
            side_effect=[("123,456", "ok"), (None, "locked")],
        ):
            assert is_allowed("123") is True
            assert is_allowed("456") is True
            guard_module._cached_at = time.time() - 120  # force TTL expiry
            assert is_allowed("123") is True  # cache preserved across locked
            assert is_allowed("456") is True
            assert is_allowed("999") is False  # still not allowed
