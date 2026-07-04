"""Tests for landline.security — macOS Keychain access."""

import subprocess
from unittest.mock import MagicMock, patch

from landline.security import keychain_get as _real_keychain_get
from landline.security import keychain_get_status as _real_keychain_get_status


class TestKeychainGetRaw:
    """Test the real keychain_get function with subprocess.run mocked.

    _real_keychain_get is captured at module load time (before conftest's
    autouse fixture activates), so it points to the original function.
    """

    def test_returns_stripped_stdout_on_success(self):
        mock_result = MagicMock(returncode=0, stdout="  secret-value  \n")
        with patch("subprocess.run", return_value=mock_result):
            result = _real_keychain_get("test-service")
        assert result == "secret-value"

    def test_returns_none_on_nonzero_exit(self):
        mock_result = MagicMock(returncode=44, stdout="")
        with patch("subprocess.run", return_value=mock_result):
            result = _real_keychain_get("missing-service")
        assert result is None

    def test_returns_none_on_subprocess_exception(self):
        """Any subprocess error (timeout, FileNotFoundError, etc.) returns None."""
        with patch("subprocess.run", side_effect=Exception("boom")):
            result = _real_keychain_get("slow-service")
        assert result is None

    def test_returns_none_on_subprocess_timeout(self):
        """A real TimeoutExpired must be swallowed."""
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="security", timeout=5),
        ):
            assert _real_keychain_get("slow") is None

    def test_passes_correct_args_to_subprocess(self):
        mock_result = MagicMock(returncode=0, stdout="val")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            _real_keychain_get("my-service", "my-account")
        cmd = mock_run.call_args[0][0]
        # Exact command shape — flags must stay in their canonical positions
        # because the `security` CLI is positional-flag-sensitive.
        assert cmd == [
            "security", "find-generic-password",
            "-a", "my-account",
            "-s", "my-service",
            "-w",
        ]

    def test_default_account_matches_config(self):
        """When no account is passed, the keychain item is read under
        the ``KEYCHAIN_ACCOUNT`` from landline.config (default ``landline``,
        deployer-tunable via landline.json)."""
        from landline.config import KEYCHAIN_ACCOUNT
        mock_result = MagicMock(returncode=0, stdout="val")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            _real_keychain_get("svc")
        cmd = mock_run.call_args[0][0]
        a_idx = cmd.index("-a")
        assert cmd[a_idx + 1] == KEYCHAIN_ACCOUNT

    def test_uses_capture_output_and_text(self):
        """Without text=True, .stdout would be bytes and .strip() would fail
        the caller's assumption — guard the kwargs."""
        mock_result = MagicMock(returncode=0, stdout="val")
        with patch("subprocess.run", return_value=mock_result) as mock_run:
            _real_keychain_get("svc")
        kwargs = mock_run.call_args[1]
        assert kwargs.get("text") is True
        assert kwargs.get("capture_output") is True
        # Timeout protects daemon from a hung Keychain prompt.
        assert kwargs.get("timeout") is not None and kwargs["timeout"] > 0

    def test_empty_stdout_returns_empty_string_on_success(self):
        """returncode=0 but blank stdout is unusual — current contract returns ''."""
        mock_result = MagicMock(returncode=0, stdout="\n")
        with patch("subprocess.run", return_value=mock_result):
            result = _real_keychain_get("svc")
        # Documents current behavior: success + blank → empty string, not None.
        assert result == ""


class TestKeychainGetMocked:
    """Test via the conftest autouse mock — verifies fixture wiring."""

    def test_known_services(self):
        from landline.security import keychain_get
        assert keychain_get("telegram-bot-token") is not None
        assert keychain_get("telegram-chat-id") == "123456789"

    def test_unknown_service_returns_none(self):
        from landline.security import keychain_get
        assert keychain_get("nonexistent") is None


class TestKeychainGetStatus:
    """B5 - classification of Keychain failure modes via rc + stderr phrase.

    Mocks subprocess.run directly (bypasses conftest's autouse fixture which
    patches `keychain_get` and `keychain_get_status` symbols, not subprocess).
    """

    def test_status_ok_returns_value_and_ok(self):
        mock_result = MagicMock(returncode=0, stdout="  secret-value  \n", stderr="")
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("svc")
        assert value == "secret-value"
        assert status == "ok"

    def test_status_absent_by_rc_44(self):
        """rc=44 with no stderr phrase falls through to the rc-based absent path."""
        mock_result = MagicMock(returncode=44, stdout="", stderr="")
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("missing")
        assert value is None
        assert status == "absent"

    def test_status_absent_by_stderr_phrase(self):
        """rc!=44 but stderr matches the absent phrase still classifies as absent."""
        mock_result = MagicMock(
            returncode=51, stdout="",
            stderr="security: The specified item could not be found in the keychain.\n",
        )
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("missing")
        assert value is None
        assert status == "absent"

    def test_status_locked_by_stderr_phrase(self):
        """CORE regression guard for B5 - the locked-vs-absent distinction.

        Reverting to 'rc!=0 -> error' loses the locked classification and
        the operator has no signal that his login keychain needs unlocking."""
        mock_result = MagicMock(
            returncode=51, stdout="",
            stderr="security: User interaction is not allowed.\n",
        )
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("locked-svc")
        assert value is None
        assert status == "locked"

    def test_status_locked_phrase_case_variants(self):
        """The shorter 'interaction is not allowed' phrase (a substring of the
        canonical 'User interaction is not allowed') catches stderr variants
        Apple may emit across macOS releases that omit the 'User ' prefix.
        Note: matching is case-SENSITIVE substring containment (`phrase in
        stderr`) - both tuple entries are stored lowercase-after-prefix to
        match Apple's stable lowercase phrasing."""
        mock_result = MagicMock(
            returncode=25, stdout="",
            stderr="error: interaction is not allowed at this time.\n",
        )
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("locked-svc")
        assert value is None
        assert status == "locked"

    def test_status_error_on_subprocess_exception(self):
        """A generic subprocess Exception must be classified as error,
        NOT as absent (which would hide a real failure)."""
        with patch("subprocess.run", side_effect=Exception("boom")):
            value, status = _real_keychain_get_status("svc")
        assert value is None
        assert status == "error"

    def test_status_error_on_timeout(self):
        """TimeoutExpired must NOT be misclassified as locked/absent.
        A hung `security` CLI is a 'something is broken' signal."""
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="security", timeout=5),
        ):
            value, status = _real_keychain_get_status("slow")
        assert value is None
        assert status == "error"

    def test_status_error_on_unknown_rc_no_phrase(self):
        """Unknown rc with no recognizable stderr phrase falls through to
        'error' so a new failure mode doesn't poison the cache or trigger
        a wrong-actionable log line."""
        mock_result = MagicMock(returncode=99, stdout="", stderr="weird new error\n")
        with patch("subprocess.run", return_value=mock_result):
            value, status = _real_keychain_get_status("svc")
        assert value is None
        assert status == "error"

    def test_keychain_get_signature_unchanged(self):
        """INVARIANT 7 guard - keychain_get must keep returning Optional[str],
        NOT a tuple. Callers do `keychain_get(...) or ""` and a tuple is
        always truthy. Fails if a builder accidentally changes the signature."""
        mock_result = MagicMock(returncode=0, stdout="value\n", stderr="")
        with patch("subprocess.run", return_value=mock_result):
            result = _real_keychain_get("svc")
        assert isinstance(result, str)  # not a tuple
        assert result == "value"
