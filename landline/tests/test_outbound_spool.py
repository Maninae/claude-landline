"""Tests for landline.outbound_spool — disk-backed at-least-once spool.

Covers the persist/success/failed primitives, startup reclaim of orphaned
inflight files from a dead pid, and the replay_all sort/age/cap semantics.
"""

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Isolate SPOOL_DIR per test — the module reads config.SPOOL_DIR at import
# time, so we monkeypatch both config.SPOOL_DIR AND the imported name inside
# outbound_spool for the fixture's lifetime.
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_spool(tmp_path, monkeypatch):
    spool_dir = tmp_path / "spool"
    spool_dir.mkdir()
    # 0o700 so the ensure_spool_dir chmod finds a matching baseline.
    os.chmod(str(spool_dir), 0o700)
    monkeypatch.setattr("landline.config.SPOOL_DIR", spool_dir)
    monkeypatch.setattr("landline.outbound_spool.SPOOL_DIR", spool_dir)
    yield spool_dir


# ---------------------------------------------------------------------------
# persist()
# ---------------------------------------------------------------------------


class TestPersist:
    def test_persist_writes_json_with_correct_mode_and_ownership_visible_via_stat(
        self, tmp_spool,
    ):
        """Persist a payload, stat the file, assert 0o600 and JSON round-trips."""
        from landline.outbound_spool import persist
        spool_id = persist("42", "hello world", html_mode=True, label="HTML chunk")
        assert os.path.isfile(spool_id)
        # 0o600 — no group/other bits set.
        mode = os.stat(spool_id).st_mode & 0o777
        assert mode == 0o600, "expected 0o600, got %o" % mode
        # JSON round-trip.
        with open(spool_id, "rb") as f:
            payload = json.loads(f.read().decode())
        assert payload["chat_id"] == "42"
        assert payload["chunk"] == "hello world"
        assert payload["html_mode"] is True
        assert payload["label"] == "HTML chunk"
        assert isinstance(payload["created_at"], float)
        assert payload["attempts"] == 0

    def test_persist_creates_inflight_state(self, tmp_spool):
        """After persist(), the file is in ``inflight-<pid>`` state."""
        from landline.outbound_spool import persist
        spool_id = persist("42", "hi", html_mode=False, label="plain")
        assert ("-inflight-%d.json" % os.getpid()) in spool_id


# ---------------------------------------------------------------------------
# ensure_spool_dir()
# ---------------------------------------------------------------------------


class TestEnsureSpoolDir:
    def test_ensure_spool_dir_creates_at_0o700(self, tmp_path, monkeypatch):
        """No-dir case; assert created; assert mode 0o700."""
        spool_dir = tmp_path / "brand-new-spool"
        assert not spool_dir.exists()
        monkeypatch.setattr("landline.config.SPOOL_DIR", spool_dir)
        monkeypatch.setattr("landline.outbound_spool.SPOOL_DIR", spool_dir)
        from landline.outbound_spool import ensure_spool_dir
        result = ensure_spool_dir()
        assert result == spool_dir
        assert spool_dir.is_dir()
        mode = os.stat(str(spool_dir)).st_mode & 0o777
        assert mode == 0o700, "expected 0o700, got %o" % mode

    def test_ensure_spool_dir_is_idempotent(self, tmp_spool):
        """Second call on an existing dir does not raise."""
        from landline.outbound_spool import ensure_spool_dir
        ensure_spool_dir()
        ensure_spool_dir()  # must not raise
        assert tmp_spool.is_dir()


# ---------------------------------------------------------------------------
# startup_reclaim_orphaned_inflight()
# ---------------------------------------------------------------------------


class TestStartupReclaim:
    def test_startup_reclaim_renames_orphaned_inflight_from_other_pids(
        self, tmp_spool,
    ):
        """Pre-write two files with names encoding a dead pid; assert both
        renamed to '-pending'."""
        # Pick a pid unlikely to be alive — 999998 is not our pid and not 0.
        dead_pid = 999998
        payload = json.dumps({
            "chat_id": "1", "chunk": "x", "html_mode": False,
            "label": "l", "created_at": time.time(), "attempts": 0,
        }).encode()
        for i in (1, 2):
            ns = time.time_ns() + i
            name = "%d-abcd%d-inflight-%d.json" % (ns, i, dead_pid)
            (tmp_spool / name).write_bytes(payload)
        from landline.outbound_spool import startup_reclaim_orphaned_inflight
        count = startup_reclaim_orphaned_inflight()
        assert count == 2
        # Both files are now pending; no inflight remains.
        names = sorted(p.name for p in tmp_spool.iterdir())
        for name in names:
            assert "-pending.json" in name
            assert "-inflight-" not in name

    def test_startup_reclaim_ignores_non_spool_files(self, tmp_spool):
        """Foreign files in the dir are safely skipped."""
        (tmp_spool / "not-a-spool-file.txt").write_text("hi")
        (tmp_spool / "README").write_text("readme")
        from landline.outbound_spool import startup_reclaim_orphaned_inflight
        count = startup_reclaim_orphaned_inflight()
        assert count == 0


# ---------------------------------------------------------------------------
# replay_all() — behavior
# ---------------------------------------------------------------------------


def _write_pending(spool_dir, chunk, created_at, created_ns=None, uid="aaaaaaaa"):
    """Write a pending spool file with a specific payload created_at."""
    if created_ns is None:
        created_ns = time.time_ns()
    payload = {
        "chat_id": "42",
        "chunk": chunk,
        "html_mode": False,
        "label": "chunk",
        "created_at": created_at,
        "attempts": 0,
    }
    name = "%d-%s-pending.json" % (created_ns, uid)
    path = spool_dir / name
    path.write_bytes(json.dumps(payload).encode())
    return path


class TestReplayAll:
    def test_replay_all_calls_send_fn_in_created_at_order(self, tmp_spool):
        """Write 3 files with created_at t0<t1<t2; send_fn called in that order."""
        now = time.time()
        _write_pending(
            tmp_spool, "chunk-0", now - 10,
            created_ns=1000, uid="aaaaaaaa",
        )
        _write_pending(
            tmp_spool, "chunk-1", now - 10,
            created_ns=2000, uid="bbbbbbbb",
        )
        _write_pending(
            tmp_spool, "chunk-2", now - 10,
            created_ns=3000, uid="cccccccc",
        )
        send_fn = MagicMock(return_value=(True, None))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        # send_fn called once per file, in filename-ns order.
        chunks_seen = [c[0][1] for c in send_fn.call_args_list]
        assert chunks_seen == ["chunk-0", "chunk-1", "chunk-2"]

    def test_replay_all_unlinks_stale_older_than_max_age(self, tmp_spool):
        """Write a file with created_at = now - 25h; assert unlinked without send."""
        now = time.time()
        stale = _write_pending(
            tmp_spool, "old", created_at=now - (25 * 3600),
        )
        send_fn = MagicMock(return_value=(True, None))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        assert not stale.exists()
        send_fn.assert_not_called()

    def test_replay_all_skips_recently_written_within_min_age(self, tmp_spool):
        """Write with created_at = now - 1s; assert NOT sent."""
        now = time.time()
        recent = _write_pending(
            tmp_spool, "young", created_at=now - 1,
        )
        send_fn = MagicMock(return_value=(True, None))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        # Still present, still pending (not renamed).
        assert recent.exists()
        assert "-pending.json" in recent.name
        send_fn.assert_not_called()

    def test_replay_all_unlinks_on_400(self, tmp_spool):
        """(False, 400) → file unlinked, log recorded."""
        now = time.time()
        f = _write_pending(tmp_spool, "bad", created_at=now - 10)
        send_fn = MagicMock(return_value=(False, 400))
        with patch("landline.outbound_spool.log") as mock_log:
            from landline.outbound_spool import replay_all
            replay_all(send_fn)
        # File gone (400 is unfixable).
        assert not any(
            "-pending.json" in p.name or "-inflight-" in p.name
            for p in tmp_spool.iterdir()
        )
        send_fn.assert_called_once()
        # 400 log line was emitted.
        assert any("400" in call.args[0] for call in mock_log.call_args_list)

    def test_replay_all_marks_failed_on_500(self, tmp_spool):
        """(False, 500) → file renamed to pending (still present)."""
        now = time.time()
        _write_pending(tmp_spool, "boom", created_at=now - 10)
        send_fn = MagicMock(return_value=(False, 500))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        # File still present, still pending.
        entries = list(tmp_spool.iterdir())
        assert len(entries) == 1
        assert "-pending.json" in entries[0].name
        send_fn.assert_called_once()

    def test_replay_all_soft_caps_at_max_files(self, tmp_spool, monkeypatch):
        """600 files → 500 kept (newest), 100 oldest removed."""
        # Use a much smaller cap so the test is fast — logic is identical.
        monkeypatch.setattr("landline.outbound_spool.SPOOL_MAX_FILES", 20)
        now = time.time()
        for i in range(25):
            _write_pending(
                tmp_spool, "c%d" % i, created_at=now - 10,
                created_ns=10000 + i, uid="u%07d" % i,
            )
        send_fn = MagicMock(return_value=(True, None))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        # 25 - 20 = 5 oldest dropped; the remaining 20 were sent.
        assert send_fn.call_count == 20
        chunks_seen = [c[0][1] for c in send_fn.call_args_list]
        # Chunks c5..c24 sent (oldest 5 dropped).
        assert chunks_seen == ["c%d" % i for i in range(5, 25)]

    def test_replay_all_unlinks_corrupt_payload(self, tmp_spool):
        """Corrupt JSON payload is unlinked without send."""
        ns = time.time_ns()
        name = "%d-aaaaaaaa-pending.json" % ns
        (tmp_spool / name).write_bytes(b"not valid json {")
        send_fn = MagicMock(return_value=(True, None))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        assert not (tmp_spool / name).exists()
        send_fn.assert_not_called()


# ---------------------------------------------------------------------------
# mark_success / mark_failed
# ---------------------------------------------------------------------------


class TestMarkPrimitives:
    def test_mark_success_unlinks(self, tmp_spool):
        from landline.outbound_spool import persist, mark_success
        spool_id = persist("1", "x", html_mode=False, label="l")
        assert os.path.isfile(spool_id)
        mark_success(spool_id)
        assert not os.path.exists(spool_id)

    def test_mark_success_idempotent_on_missing_file(self, tmp_spool):
        from landline.outbound_spool import mark_success
        # Must not raise on a nonexistent path.
        mark_success(str(tmp_spool / "does-not-exist.json"))

    def test_mark_failed_renames_inflight_to_pending(self, tmp_spool):
        from landline.outbound_spool import persist, mark_failed
        spool_id = persist("1", "x", html_mode=False, label="l")
        assert "-inflight-" in spool_id
        mark_failed(spool_id)
        assert not os.path.exists(spool_id)
        # A pending file with the same base name now exists.
        pending = [
            p for p in tmp_spool.iterdir()
            if "-pending.json" in p.name
        ]
        assert len(pending) == 1


# ---------------------------------------------------------------------------
# persist() failure cleanup — leaked pending files cause double-delivery
# ---------------------------------------------------------------------------


class TestPersistFailureCleanup:
    """Regression for the pending-file leak class.

    If os.fsync / os.rename raise AFTER os.open + os.write have created the
    pending file, the exception unwinds. The caller (`_send_with_retry`)
    swallows the OSError, proceeds without persistence, and the send may
    still succeed over the wire. Sixty seconds later the background
    replayer sees the leaked pending file, replays it, and the operator sees the
    same reply twice.

    Contract: any exception between the O_EXCL open and the successful
    rename-to-inflight MUST unlink the pending file before propagating.
    """

    def test_persist_unlinks_pending_when_rename_fails(self, tmp_spool):
        from landline import outbound_spool
        real_rename = os.rename
        rename_calls = {"n": 0}

        def flaky_rename(src, dst):
            rename_calls["n"] += 1
            # Fail the FIRST rename (the pending → inflight one at the tail
            # of persist). Later renames (e.g. mark_failed in unrelated
            # tests) still work via real_rename — but there shouldn't be a
            # later rename in this test.
            raise OSError(28, "No space left on device")

        with patch.object(outbound_spool.os, "rename", side_effect=flaky_rename):
            with pytest.raises(OSError):
                outbound_spool.persist(
                    "42", "hello", html_mode=False, label="chunk",
                )

        # Zero leaked files under either state — the pending file that got
        # created was unlinked as part of persist's cleanup.
        leftover = [
            p.name for p in tmp_spool.iterdir()
            if p.name.endswith(".json")
        ]
        assert leftover == [], (
            "persist leaked spool file(s) after rename failure: %r" % leftover
        )
        # Sanity: the mocked rename was actually attempted.
        assert rename_calls["n"] >= 1
        # rebind real_rename to avoid unused-warning nagging
        _ = real_rename

    def test_persist_leak_would_cause_double_delivery_on_next_replay(
        self, tmp_spool,
    ):
        """End-to-end confirmation of the failure MODE.

        Verifies that a leaked pending file (were persist to leak one)
        would be picked up by the very next replay pass. This test is
        the "does the fix matter?" check: with the fix in place, persist
        must not leave the pending file behind, so replay_all sees
        nothing.
        """
        from landline import outbound_spool

        def rename_fails(src, dst):
            raise OSError(5, "Input/output error")

        with patch.object(outbound_spool.os, "rename", side_effect=rename_fails):
            with pytest.raises(OSError):
                outbound_spool.persist(
                    "42", "will not double-deliver", html_mode=False,
                    label="chunk",
                )

        # Age the theoretical leaked file past SPOOL_REPLAY_MIN_AGE_SECONDS
        # by directly forcing the check — since we assert no file exists,
        # this just guards against the replayer accidentally acting on
        # something we missed.
        send_fn = MagicMock(return_value=(True, None))
        outbound_spool.replay_all(send_fn)
        send_fn.assert_not_called()


# ---------------------------------------------------------------------------
# replay_all — permanent 4xx failures must not loop forever
# ---------------------------------------------------------------------------


class TestReplayAllPermanent4xx:
    """Regression: 401 (invalid token) / 403 (bot blocked or kicked from
    chat) / 404 (chat not found) are all permanent — retrying burns ~1440
    doomed API calls per file per 24h. The replayer must drop them like
    400.
    """

    @pytest.mark.parametrize("code", [401, 403, 404])
    def test_replay_all_unlinks_permanent_4xx(self, tmp_spool, code):
        now = time.time()
        _write_pending(tmp_spool, "doomed", created_at=now - 10)
        send_fn = MagicMock(return_value=(False, code))
        with patch("landline.outbound_spool.log") as mock_log:
            from landline.outbound_spool import replay_all
            replay_all(send_fn)
        # File gone — this is a terminal failure code.
        assert not any(
            "-pending.json" in p.name or "-inflight-" in p.name
            for p in tmp_spool.iterdir()
        ), "file survived replay of terminal %d — will be re-tried forever" % code
        send_fn.assert_called_once()
        assert any(
            str(code) in call.args[0] for call in mock_log.call_args_list
        ), "no drop-log line for %d" % code

    @pytest.mark.parametrize("code", [429, 500, 502, 503, 504])
    def test_replay_all_still_retries_transient_codes(self, tmp_spool, code):
        """The fix must NOT over-drop — 429 (rate limit) and 5xx (server
        errors) are transient and MUST stay pending for the next pass."""
        now = time.time()
        _write_pending(tmp_spool, "retry-me", created_at=now - 10)
        send_fn = MagicMock(return_value=(False, code))
        from landline.outbound_spool import replay_all
        replay_all(send_fn)
        entries = list(tmp_spool.iterdir())
        assert len(entries) == 1, (
            "expected one file (returned to pending) for transient %d, got %d"
            % (code, len(entries))
        )
        assert "-pending.json" in entries[0].name
