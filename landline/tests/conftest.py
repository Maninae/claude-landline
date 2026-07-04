"""Shared fixtures and mocks for the Telegram daemon test suite."""

import hashlib
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Workspace isolation (must run BEFORE the first ``landline.*`` import)
# ---------------------------------------------------------------------------
# ``landline.config`` resolves ``WORKSPACE`` from ``LANDLINE_WORKSPACE`` at
# import time and loads ``<WORKSPACE>/landline.json`` if present. Point the
# env var at a fresh empty tmp dir so a real config file in the checkout's
# cwd can never leak into the test run. Pytest imports conftest.py before
# any test module and any ``from landline...`` import, so setting the env
# var here wins the race even for module-level imports in test files.
#
# UNCONDITIONAL assignment, never ``setdefault``: the deploy gate
# (deploy/restart.sh) runs this suite with LANDLINE_WORKSPACE exported and
# pointing at the LIVE workspace. Inheriting that value would bind
# import-time WORKSPACE-derived constants (inject queue, image cache,
# project dir, ...) to production paths — the exact "tests touch
# production" incident class this suite exists to prevent. The suite never
# legitimately needs the shell's workspace; per-test paths use tmp_path.
_TEST_WORKSPACE = tempfile.mkdtemp(prefix="landline-test-workspace-")
os.environ["LANDLINE_WORKSPACE"] = _TEST_WORKSPACE


# ---------------------------------------------------------------------------
# Keychain mock values — no real secrets
# ---------------------------------------------------------------------------

FAKE_BOT_TOKEN = "000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
FAKE_CHAT_ID = "123456789"
FAKE_UNLOCK_PASSPHRASE = "coconut pudding"
FAKE_UNLOCK_HASH = hashlib.sha256("coconut pudding".encode("utf-8")).hexdigest()
FAKE_ALLOWED_CHAT_IDS = FAKE_CHAT_ID
FAKE_OWNER_HANDLE = "test-handle"

_KEYCHAIN_MAP: Dict[str, str] = {
    "telegram-bot-token": FAKE_BOT_TOKEN,
    "telegram-chat-id": FAKE_CHAT_ID,
    "telegram-unlock-hash": FAKE_UNLOCK_HASH,
    "telegram-allowed-chat-ids": FAKE_ALLOWED_CHAT_IDS,
    "owner-imsg-handle": FAKE_OWNER_HANDLE,
}


def _fake_keychain_get(service: str, account: str = "landline") -> Optional[str]:
    return _KEYCHAIN_MAP.get(service)


def _fake_keychain_get_status(service: str, account: str = "landline"):
    """Mirror of _fake_keychain_get for the new B5 status helper.
    All mocked services return (value, "ok"); unknown returns (None, "absent")."""
    value = _KEYCHAIN_MAP.get(service)
    if value is None:
        return None, "absent"
    return value, "ok"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_keychain():
    """Globally replace keychain_get and keychain_get_status at every import site."""
    with patch("landline.runtime.security.keychain_get", side_effect=_fake_keychain_get), \
         patch("landline.runtime.security.keychain_get_status", side_effect=_fake_keychain_get_status), \
         patch("landline.runtime.guard.keychain_get_status", side_effect=_fake_keychain_get_status), \
         patch("landline.runtime.lock.keychain_get_status", side_effect=_fake_keychain_get_status), \
         patch("landline.runtime.notifications.keychain_get", side_effect=_fake_keychain_get), \
         patch("landline.orchestrator.keychain_get", side_effect=_fake_keychain_get):
        yield _fake_keychain_get


@pytest.fixture(autouse=True)
def reset_guard_cache():
    """Reset guard module cache between tests to prevent leakage."""
    import landline.runtime.guard as g
    g._cached_allowed = None
    g._cached_at = 0.0
    yield
    g._cached_allowed = None
    g._cached_at = 0.0


@pytest.fixture(autouse=True)
def reset_persistent_claude_singleton():
    """Reset the PersistentClaude module-level singleton between tests.

    The dispatcher's _seed_pc_session_from_state_once (E1) reads and writes
    the singleton. Without this reset, the sid set by one test leaks into
    the seed-once defensive branch (`if pc.get_session_id() is not None:
    return`) of the next test, blocking the new state's seed and causing
    the always-mirror line in _finalize_response to write the stale sid
    back into the new test's state dict. That breaks every existing test
    that asserts on state['session_id'].

    Drop the reference on both entry and exit so any test that constructs
    a dispatcher (and thus touches the singleton via the lazy import)
    leaves a clean slate for the next test.
    """
    import landline.claude.persistent as pc_mod
    pc_mod._persistent_claude = None
    yield
    pc_mod._persistent_claude = None


@pytest.fixture(autouse=True)
def isolate_daemon_log(tmp_path, monkeypatch):
    """Redirect landline.runtime.logging file output to tmp_path for every test.

    Without this, any test that calls log() (directly or through code under
    test) attaches a RotatingFileHandler to the real production daemon.log.
    """
    from landline.runtime import logging as _dlog
    monkeypatch.setenv("LANDLINE_DAEMON_LOG", str(tmp_path / "daemon.log"))
    _dlog._reset_logger_for_tests()
    yield
    _dlog._reset_logger_for_tests()


@pytest.fixture(autouse=True)
def isolate_conversation_log(tmp_path, monkeypatch):
    """Redirect landline.runtime.state's WORKSPACE so log_conversation writes daily
    conversation logs under tmp_path, never the real memory/daily/.

    Without this, any test that drives an orchestrator/handler path calling
    log_conversation() appends synthetic fixture messages (e.g. "/pause"
    floods, "[photo] check this out") into the REAL
    memory/daily/YYYY-MM-DD_telegram.md — the same file the live daemon
    writes and the nightly consolidation reads as the operator's actual
    conversation. Observed polluting the build worktree on 2026-07-03.
    state.py binds WORKSPACE into its own namespace at import, so patching
    landline.runtime.state.WORKSPACE is surgical (config.WORKSPACE and other modules
    are unaffected).
    """
    monkeypatch.setattr("landline.runtime.state.WORKSPACE", tmp_path)
    yield


@pytest.fixture(autouse=True)
def isolate_usage_stats_file(tmp_path, monkeypatch):
    """Cluster 4: redirect usage_stats.USAGE_STATS_FILE to tmp_path.

    Any test that exercises the dispatcher's finalize path or the pump's
    unsolicited-turn branch would otherwise persist real files into
    ``cache/usage-stats.json``. Redirect at both the config and
    usage_stats module reference so patches through either seam land in
    the tmp file.
    """
    stats_file = tmp_path / "usage-stats.json"
    monkeypatch.setattr("landline.config.USAGE_STATS_FILE", stats_file)
    monkeypatch.setattr("landline.runtime.usage_stats.USAGE_STATS_FILE", stats_file)
    yield stats_file


@pytest.fixture(autouse=True)
def isolate_outbound_spool_dir(tmp_path, monkeypatch):
    """Cluster 5: redirect outbound_spool.SPOOL_DIR to tmp_path for every test.

    Without this, any test that exercises _send_with_retry (many do, via
    send_response) persists real files into the workspace's real
    cache/telegram-outbound-spool/. Beyond leaking test artifacts, subsequent
    integration tests would then find those files at startup and try to
    replay them (real network calls, or spurious 30–60s stalls per test).
    """
    spool_dir = tmp_path / "telegram-outbound-spool"
    monkeypatch.setattr("landline.config.SPOOL_DIR", spool_dir)
    monkeypatch.setattr("landline.telegram.spool.SPOOL_DIR", spool_dir)
    yield


def pytest_configure(config):
    """Register custom markers used by the daemon test suite."""
    config.addinivalue_line(
        "markers",
        "reactions_network: opt in to landline.telegram.reactions HTTP behaviour "
        "(default autouse fixture disables REACTION_ACKS_ENABLED for the "
        "whole suite to prevent real setMessageReaction POSTs to Telegram)."
    )


@pytest.fixture(autouse=True)
def disable_reactions_network(request, monkeypatch):
    """Disable REACTION_ACKS_ENABLED globally in tests to prevent leaking
    real ``setMessageReaction`` POSTs to api.telegram.org.

    The classifier's ``_ack_and_record`` and dispatcher's finalize call
    ``reactions.set_reaction_async`` / ``set_reactions_batch_async`` — both
    early-return when the flag is False, so no daemon thread and no
    ``urllib.request.urlopen`` call is made.

    Tests that exercise the reactions HTTP path (test_reactions.py) opt back
    in with ``pytestmark = pytest.mark.reactions_network``.
    """
    if "reactions_network" in request.keywords:
        return
    monkeypatch.setattr("landline.config.REACTION_ACKS_ENABLED", False)


@pytest.fixture()
def tmp_workspace(tmp_path):
    """Create a temporary workspace directory tree mirroring the agent workspace layout."""
    (tmp_path / "cache").mkdir()
    (tmp_path / "cache" / "inject-queue").mkdir()
    (tmp_path / "logs" / "telegram-daemon").mkdir(parents=True)
    (tmp_path / "memory" / "daily").mkdir(parents=True)
    (tmp_path / "briefs_morning").mkdir()
    return tmp_path


@pytest.fixture()
def tmp_state_file(tmp_workspace):
    """Return a Path to a temporary state file inside the tmp workspace."""
    return tmp_workspace / "cache" / "telegram-daemon-state.json"


@pytest.fixture()
def default_state():
    """A clean default state dict matching load_state() defaults."""
    return {
        "session_id": None,
        "last_update_id": 0,
        "turn_count": 0,
        "failed_unlock_attempts": 0,
        "unlock_lockout_until": 0.0,
        "unlock_timestamp": 0.0,
    }


@pytest.fixture()
def persist_state_fn():
    """A mock persist-state callback that records calls."""
    return MagicMock()


@pytest.fixture()
def mock_send_response():
    """Mock for send_response(token, chat_id, text)."""
    return MagicMock()


@pytest.fixture()
def mock_send_typing():
    """Mock for send_typing(token, chat_id)."""
    return MagicMock()


@pytest.fixture()
def mock_run_claude():
    """Mock for run_claude_streaming that returns a default successful result."""
    from landline.claude.dispatch import ClaudeStreamResult

    def _default_run_claude(**kwargs):
        result = ClaudeStreamResult()
        result.session_id = "test-session-id-0000"
        result.streamed_text = "Hello from Claude."
        result.final_result = "Hello from Claude."
        return result

    return MagicMock(side_effect=_default_run_claude)


@pytest.fixture()
def mock_guard_allow_all():
    """Guard function that allows every chat_id."""
    return MagicMock(return_value=True)


@pytest.fixture()
def mock_guard_deny_all():
    """Guard function that denies every chat_id."""
    return MagicMock(return_value=False)


@pytest.fixture()
def mock_reject():
    """Mock for reject_message(token, chat_id)."""
    return MagicMock()


@pytest.fixture()
def no_subprocess():
    """Prevent any real subprocess.run / subprocess.Popen calls."""
    with patch("subprocess.run") as mock_run, \
         patch("subprocess.Popen") as mock_popen:
        mock_run.return_value = MagicMock(
            returncode=0, stdout="", stderr=""
        )
        mock_popen.return_value = MagicMock()
        yield {"run": mock_run, "popen": mock_popen}


@pytest.fixture()
def no_network():
    """Prevent any real urllib network calls."""
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"ok":true,"result":[]}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response
        yield mock_urlopen


def make_telegram_update(
    update_id: int,
    text: str,
    chat_id: str = FAKE_CHAT_ID,
    date: Optional[int] = None,
    has_photo: bool = False,
    is_edit: bool = False,
) -> Dict[str, Any]:
    """Build a synthetic Telegram update dict."""
    message: Dict[str, Any] = {
        "message_id": update_id * 10,
        "chat": {"id": int(chat_id)},
        "date": date or int(time.time()),
    }
    if text is not None:
        message["text"] = text
    if has_photo:
        message["photo"] = [{"file_id": "fake", "width": 100, "height": 100}]
    if is_edit:
        message["edit_date"] = int(time.time())
    return {"update_id": update_id, "message": message}
