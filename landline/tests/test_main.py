"""Tests for daemon/__main__.py — wiring of secure_daily_logs().

These tests guard the one-line wire-up that activates the daily-log
permissions backfill at daemon startup. Reverting either the import
or the call inside main() must fail these tests.
"""

import importlib
from typing import List
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Shim: the suite-wide conftest patches `daemon.{guard,lock,security,
# notifications,orchestrator}.keychain_get`. Some of those modules were
# restructured to use `keychain_get_status` instead, removing the
# `keychain_get` attribute at the call sites. We don't exercise the keychain
# here, so we install a no-op `keychain_get` attribute on any module that's
# missing it, so the conftest's `patch.object` succeeds and the wire-up
# tests below can run on their own.
# ---------------------------------------------------------------------------
def _noop_keychain_get(service, account="landline"):  # type: ignore[no-untyped-def]
    return None


for _modname in (
    "landline.runtime.guard",
    "landline.runtime.lock",
    "landline.runtime.security",
    "landline.runtime.notifications",
    "landline.orchestrator",
):
    _mod = importlib.import_module(_modname)
    if not hasattr(_mod, "keychain_get"):
        _mod.keychain_get = _noop_keychain_get  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Import-time wire-up
# ---------------------------------------------------------------------------


def test_main_module_imports_secure_daily_logs():
    """`landline.__main__` must expose secure_daily_logs at module scope.

    Reverting the `from landline.runtime.state import secure_daily_logs` line
    removes this attribute and fails the test.
    """
    import landline.__main__ as main_module
    importlib.reload(main_module)
    assert hasattr(main_module, "secure_daily_logs")
    # Must be the real function from landline.state, not a stray name.
    from landline.runtime.state import secure_daily_logs as state_secure_daily_logs
    assert main_module.secure_daily_logs is state_secure_daily_logs


# ---------------------------------------------------------------------------
# Runtime wire-up
# ---------------------------------------------------------------------------


def test_main_calls_secure_daily_logs_once_at_startup():
    """`main()` must call secure_daily_logs() exactly once at startup.

    Reverting the `secure_daily_logs()` call inside main() drops the call
    count to 0 and fails the test.
    """
    import landline.__main__ as main_module

    # Sentinel TelegramDaemon: short-circuits run() so main()'s try-block
    # exits cleanly without spinning up real threads / sockets.
    fake_daemon_instance = MagicMock()
    fake_daemon_instance.run.return_value = None
    fake_daemon_cls = MagicMock(return_value=fake_daemon_instance)

    fake_lock_fd = MagicMock()

    with patch.object(main_module, "_acquire_singleton_lock",
                      return_value=fake_lock_fd), \
         patch.object(main_module, "TelegramDaemon", fake_daemon_cls), \
         patch.object(main_module, "ClaudeStreamShutdownHook",
                      return_value=MagicMock()), \
         patch.object(main_module, "ClaudeFailureTracker",
                      return_value=MagicMock()), \
         patch.object(main_module, "secure_daily_logs") as mock_secure:
        main_module.main()

    assert mock_secure.call_count == 1, (
        "main() must call secure_daily_logs() exactly once at startup"
    )
    mock_secure.assert_called_once_with()


def test_secure_daily_logs_called_before_telegram_daemon_constructed():
    """secure_daily_logs() must run BEFORE TelegramDaemon is instantiated.

    The backfill must complete before any code path that might write a
    new daily log line can run. Reverting the wire-up to land after
    TelegramDaemon construction (or removing it) flips the ordering
    and fails this test.
    """
    import landline.__main__ as main_module

    call_order: List[str] = []

    def _record_secure(*args, **kwargs):
        call_order.append("secure_daily_logs")

    def _record_daemon_ctor(*args, **kwargs):
        call_order.append("TelegramDaemon")
        instance = MagicMock()
        instance.run.return_value = None
        return instance

    fake_lock_fd = MagicMock()

    with patch.object(main_module, "_acquire_singleton_lock",
                      return_value=fake_lock_fd), \
         patch.object(main_module, "TelegramDaemon",
                      side_effect=_record_daemon_ctor), \
         patch.object(main_module, "ClaudeStreamShutdownHook",
                      return_value=MagicMock()), \
         patch.object(main_module, "ClaudeFailureTracker",
                      return_value=MagicMock()), \
         patch.object(main_module, "secure_daily_logs",
                      side_effect=_record_secure):
        main_module.main()

    assert "secure_daily_logs" in call_order, (
        "secure_daily_logs() was never called from main()"
    )
    assert "TelegramDaemon" in call_order, (
        "TelegramDaemon constructor was never reached"
    )
    assert call_order.index("secure_daily_logs") < call_order.index(
        "TelegramDaemon"
    ), (
        "secure_daily_logs() must be called BEFORE TelegramDaemon is "
        "instantiated, so the backfill completes before any new daily-log "
        "line can be written"
    )


# ---------------------------------------------------------------------------
# Outbound-spool startup wiring
# ---------------------------------------------------------------------------


def test_main_calls_startup_reclaim_before_daemon_run_and_no_sync_replay():
    """main() must call outbound_spool.startup_reclaim_orphaned_inflight
    BEFORE TelegramDaemon.run() and AFTER secure_daily_logs (the workspace
    backfill). The synchronous ``replay_all`` pass that used to run here is
    intentionally REMOVED: it could block daemon startup for tens of
    minutes on network failure (~200 spool files × 10s urlopen timeout
    each), during which the poller wasn't running and the launchd
    watchdog saw the daemon as "starting". The background
    ``OutboundSpoolReplayer`` (started inside ``TelegramDaemon.run``
    after restart-continuation and before the poller) provides identical
    at-least-once coverage without the availability hole. This test pins
    both invariants: reclaim still runs before ``daemon.run``, and
    ``replay_all`` is NOT invoked synchronously from ``main()``.
    """
    import landline.__main__ as main_module

    call_order: List[str] = []

    def _record_secure(*args, **kwargs):
        call_order.append("secure_daily_logs")

    def _record_reclaim(*args, **kwargs):
        call_order.append("startup_reclaim")
        return 0

    def _record_replay(*args, **kwargs):
        call_order.append("replay_all")

    def _record_run(*args, **kwargs):
        call_order.append("landline.run")

    fake_lock_fd = MagicMock()
    fake_daemon_instance = MagicMock()
    fake_daemon_instance.run.side_effect = _record_run
    fake_daemon_cls = MagicMock(return_value=fake_daemon_instance)

    with patch.object(main_module, "_acquire_singleton_lock",
                      return_value=fake_lock_fd), \
         patch.object(main_module, "TelegramDaemon", fake_daemon_cls), \
         patch.object(main_module, "ClaudeStreamShutdownHook",
                      return_value=MagicMock()), \
         patch.object(main_module, "ClaudeFailureTracker",
                      return_value=MagicMock()), \
         patch.object(main_module, "secure_daily_logs",
                      side_effect=_record_secure), \
         patch.object(
             main_module.outbound_spool,
             "startup_reclaim_orphaned_inflight",
             side_effect=_record_reclaim,
         ) as mock_reclaim, \
         patch.object(
             main_module.outbound_spool, "replay_all",
             side_effect=_record_replay,
         ) as mock_replay, \
         patch.object(main_module.outbound_spool, "ensure_spool_dir"):
        main_module.main()

    assert mock_reclaim.called, "startup_reclaim_orphaned_inflight was never called"
    # The sync replay_all pass has been deliberately removed to avoid a
    # ~tens-of-minutes startup-blocking hole on network failure. The
    # background OutboundSpoolReplayer picks up reclaimed files once
    # TelegramDaemon.run() starts it.
    assert not mock_replay.called, (
        "replay_all must NOT be invoked synchronously from main() — that "
        "was removed to avoid a startup-blocking availability hole; the "
        "background OutboundSpoolReplayer covers it instead."
    )
    # Ordering: secure_daily_logs → startup_reclaim → daemon.run
    assert call_order.index("secure_daily_logs") < call_order.index(
        "startup_reclaim"
    ), "startup_reclaim must run AFTER secure_daily_logs"
    assert call_order.index("startup_reclaim") < call_order.index(
        "landline.run"
    ), "startup_reclaim must complete before TelegramDaemon.run() begins"
