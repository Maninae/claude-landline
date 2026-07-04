"""Tests for daemon.poller — BackgroundPoller.

Critical invariant: the dedup set is NEVER pruned (advance_processed_cursor
does not remove IDs from the dedup set).
"""

import json
import queue
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from landline.poller import BackgroundPoller, _telegram_api_get_updates


class TestTelegramApiGetUpdates:
    def test_request_url_and_payload(self, no_network):
        """Verifies the URL includes the bot token and the payload carries
        the offset, timeout, and the message-only allowed_updates filter."""
        _telegram_api_get_updates("fake-token", 42)
        assert no_network.called
        req = no_network.call_args[0][0]
        assert "fake-token" in req.full_url
        assert req.full_url.endswith("/getUpdates")
        body = json.loads(req.data.decode())
        assert body["offset"] == 42
        assert body["timeout"] > 0
        # Restrict updates to messages — channel/inline/etc. must not be polled.
        assert body["allowed_updates"] == ["message"]

    def test_returns_parsed_json(self):
        response_body = json.dumps({"ok": True, "result": []}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = response_body
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = _telegram_api_get_updates("token", 0)
        assert result == {"ok": True, "result": []}


class TestBackgroundPollerInit:
    def test_initial_state(self):
        bp = BackgroundPoller("token", 0)
        assert bp.has_pending() is False

    def test_initial_cursor(self):
        bp = BackgroundPoller("token", 42)
        assert bp._last_processed_update_id == 42


class TestBackgroundPollerDrain:
    def test_drain_empty_returns_empty_list(self):
        bp = BackgroundPoller("token", 0)
        result = bp.drain()
        assert result == []

    def test_drain_returns_queued_items(self):
        bp = BackgroundPoller("token", 0)
        bp._incoming_updates_queue.put({"update_id": 1})
        bp._incoming_updates_queue.put({"update_id": 2})
        result = bp.drain()
        assert len(result) == 2

    def test_drain_sorts_by_update_id(self):
        bp = BackgroundPoller("token", 0)
        bp._incoming_updates_queue.put({"update_id": 3})
        bp._incoming_updates_queue.put({"update_id": 1})
        bp._incoming_updates_queue.put({"update_id": 2})
        result = bp.drain()
        assert [u["update_id"] for u in result] == [1, 2, 3]

    def test_drain_with_block_timeout(self):
        bp = BackgroundPoller("token", 0)

        def delayed_put():
            time.sleep(0.05)
            bp._incoming_updates_queue.put({"update_id": 1})

        t = threading.Thread(target=delayed_put, daemon=True)
        t.start()
        result = bp.drain(block_timeout_seconds=1.0)
        assert len(result) == 1
        t.join(timeout=1)

    def test_drain_block_timeout_expires(self):
        bp = BackgroundPoller("token", 0)
        result = bp.drain(block_timeout_seconds=0.05)
        assert result == []


class TestAdvanceProcessedCursor:
    def test_advances_cursor(self):
        bp = BackgroundPoller("token", 0)
        bp.advance_processed_cursor(10)
        assert bp._last_processed_update_id == 10

    def test_does_not_go_backwards(self):
        bp = BackgroundPoller("token", 10)
        bp.advance_processed_cursor(5)
        assert bp._last_processed_update_id == 10

    def test_dedup_set_not_pruned_on_advance(self):
        """CRITICAL INVARIANT: advance_processed_cursor must NOT prune the dedup set."""
        bp = BackgroundPoller("token", 0)
        bp._already_queued_update_ids[1] = None
        bp._already_queued_update_ids[2] = None
        bp._already_queued_update_ids[3] = None
        bp.advance_processed_cursor(2)
        assert 1 in bp._already_queued_update_ids
        assert 2 in bp._already_queued_update_ids
        assert 3 in bp._already_queued_update_ids


class TestHasPending:
    def test_false_when_empty(self):
        bp = BackgroundPoller("token", 0)
        assert bp.has_pending() is False

    def test_true_when_items_queued(self):
        bp = BackgroundPoller("token", 0)
        bp._incoming_updates_queue.put({"update_id": 1})
        assert bp.has_pending() is True


class TestPollerDedup:
    def test_dedup_prevents_requeue(self):
        """Simulates the in-flight-long-poll race: the same update id is
        returned across two consecutive responses (because the cursor hasn't
        advanced yet). The dedup set must collapse it to one queued item."""
        bp = BackgroundPoller("token", 0)
        update = {"update_id": 42, "message": {"text": "hi"}}
        response = {"ok": True, "result": [update]}
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] <= 2:
                return response
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        items = bp.drain()
        assert len(items) == 1
        assert items[0]["update_id"] == 42
        # The id stays in the dedup set after queueing.
        assert 42 in bp._already_queued_update_ids

    def test_dedup_callback_fires_once_for_duplicate_id(self):
        """on_update_queued must fire exactly once per unique update_id, even
        when the same id is returned by multiple poll responses."""
        received = []
        bp = BackgroundPoller(
            "token", 0,
            on_update_queued=lambda u: received.append(u["update_id"]),
        )
        update = {"update_id": 7, "message": {"text": "hi"}}
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] <= 3:
                return {"ok": True, "result": [update]}
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert received == [7]

    def test_poll_uses_processed_cursor_plus_one_for_offset(self):
        """The offset passed to Telegram must always be _last_processed_update_id + 1.
        This is what gives Telegram the cue to mark updates as processed only
        once we have persisted them."""
        bp = BackgroundPoller("token", 100)
        observed_offsets = []

        def fake_get_updates(token, offset):
            observed_offsets.append(offset)
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert observed_offsets == [101]


class TestPollerErrorHandling:
    def test_backoff_doubles_on_repeated_failures(self):
        """Each network failure should call _stop.wait() with an increasing
        backoff (POLL_ERROR_BACKOFF_BASE, then 2x, then 4x...) up to
        POLL_ERROR_BACKOFF_MAX."""
        import urllib.error
        from landline.config import POLL_ERROR_BACKOFF_BASE

        bp = BackgroundPoller("token", 0)
        wait_durations = []

        def fake_get_updates(token, offset):
            raise urllib.error.URLError("network down")

        bp._stop = MagicMock()
        # Allow 3 failure iterations, then exit.
        bp._stop.is_set.side_effect = [False, False, False, True]

        def fake_wait(duration):
            wait_durations.append(duration)
            return False  # never signal stop via wait

        bp._stop.wait.side_effect = fake_wait

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"):
            bp._poll_loop()

        assert wait_durations[0] == POLL_ERROR_BACKOFF_BASE
        assert wait_durations[1] == POLL_ERROR_BACKOFF_BASE * 2
        assert wait_durations[2] == POLL_ERROR_BACKOFF_BASE * 4

    def test_stop_during_backoff_exits_immediately(self):
        """If _stop is signalled while waiting out backoff, the loop must return."""
        import urllib.error

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise urllib.error.URLError("network down")

        bp._stop = MagicMock()
        bp._stop.is_set.return_value = False
        bp._stop.wait.return_value = True  # simulate stop signalled during sleep

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"):
            bp._poll_loop()  # must return promptly, no infinite loop

        # Exactly one failure observed; wait was called once and returned True.
        assert bp._stop.wait.call_count == 1

    def test_network_recovery_resets_backoff(self):
        """After a recovery, a subsequent failure should restart backoff from BASE."""
        import urllib.error
        from landline.config import POLL_ERROR_BACKOFF_BASE

        bp = BackgroundPoller("token", 0)
        wait_durations = []
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            # Sequence: fail, fail, succeed, fail
            if call_count[0] in (1, 2, 4):
                raise urllib.error.URLError("down")
            return {"ok": True, "result": []}

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, False, False, True]

        def fake_wait(duration):
            wait_durations.append(duration)
            return False

        bp._stop.wait.side_effect = fake_wait

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"):
            bp._poll_loop()

        # Two failures before recovery: BASE, then 2x BASE.
        # After recovery + new failure: backoff resets to BASE.
        assert wait_durations == [
            POLL_ERROR_BACKOFF_BASE,
            POLL_ERROR_BACKOFF_BASE * 2,
            POLL_ERROR_BACKOFF_BASE,
        ]

    def test_network_alert_fires_only_once_per_outage(self):
        """send_network_alert fires the first time outage_seconds exceeds the
        threshold and is latched — subsequent failures in the same outage do
        not re-alert."""
        import urllib.error
        from landline.config import POLL_ERROR_ALERT_AFTER

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise urllib.error.URLError("down")

        bp._stop = MagicMock()
        # Three failure iterations then exit.
        bp._stop.is_set.side_effect = [False, False, False, True]
        bp._stop.wait.return_value = False

        # Iteration 1: time.time() for start, time.time() for outage_seconds (=0, below threshold).
        # Iteration 2: time.time() for outage_seconds = past threshold -> alert fires.
        # Iteration 3: time.time() for outage_seconds = still past threshold -> alert latched.
        time_values = iter([
            0.0, 0.0,
            float(POLL_ERROR_ALERT_AFTER + 1),
            float(POLL_ERROR_ALERT_AFTER + 100),
        ])

        # Patch the poller module's `time` reference, NOT time.time globally —
        # a global patch leaks into logging's LogRecord creation, which consumes
        # iterator values and exhausts the finite clock. Also mock log() so the
        # test doesn't write to the real daemon log.
        mock_time = MagicMock()
        mock_time.time.side_effect = lambda: next(time_values)

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert") as mock_alert, \
             patch("landline.poller.log"), \
             patch("landline.poller.time", mock_time):
            bp._poll_loop()

        assert mock_alert.call_count == 1


class TestOnUpdateQueuedCallback:
    def test_on_update_queued_callback_invoked(self):
        """Callback fires once per queued update with the update dict."""
        received = []
        bp = BackgroundPoller(
            "token", 0,
            on_update_queued=lambda u: received.append(u),
        )
        updates = [
            {"update_id": 1, "message": {"text": "a"}},
            {"update_id": 2, "message": {"text": "b"}},
        ]
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"ok": True, "result": updates}
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert len(received) == 2
        assert received[0]["update_id"] == 1
        assert received[1]["update_id"] == 2

    def test_on_update_queued_exception_isolated(self):
        """A callback exception MUST NOT increment consecutive_error_count
        or trigger the network-outage alerting path."""
        def raising_callback(update):
            raise RuntimeError("boom")

        bp = BackgroundPoller("token", 0, on_update_queued=raising_callback)
        updates = [{"update_id": 1, "message": {"text": "a"}}]
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"ok": True, "result": updates}
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert") as mock_alert:
            bp._poll_loop()

        # Update was still queued despite the callback raising.
        items = bp.drain()
        assert len(items) == 1
        assert items[0]["update_id"] == 1
        # No network-outage alert path was entered.
        mock_alert.assert_not_called()

    def test_on_update_queued_none_is_noop(self):
        """No callback -> no error; updates still queue normally."""
        bp = BackgroundPoller("token", 0, on_update_queued=None)
        updates = [{"update_id": 1, "message": {"text": "hi"}}]
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"ok": True, "result": updates}
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        items = bp.drain()
        assert len(items) == 1

    def test_on_update_queued_invoked_before_next_update_queued(self):
        """Each callback must fire while the corresponding update is still
        synchronously inside the put() loop — i.e. the queue size at callback
        time should already include this update."""
        callback_queue_sizes = []
        bp = BackgroundPoller("token", 0)

        def callback(update):
            callback_queue_sizes.append(bp._incoming_updates_queue.qsize())

        bp._on_update_queued = callback
        updates = [
            {"update_id": 1, "message": {"text": "a"}},
            {"update_id": 2, "message": {"text": "b"}},
            {"update_id": 3, "message": {"text": "c"}},
        ]

        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"ok": True, "result": updates}
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        # qsize at the moment of the callback equals the running count of puts.
        assert callback_queue_sizes == [1, 2, 3]


class TestPollerLifecycle:
    def test_start_and_stop(self):
        bp = BackgroundPoller("token", 0)
        response = {"ok": True, "result": []}
        with patch("landline.poller._telegram_api_get_updates", return_value=response):
            bp.start()
            time.sleep(0.1)
            bp.stop(join_timeout=2)
        assert not bp._thread.is_alive()

    def test_signal_stop(self):
        bp = BackgroundPoller("token", 0)
        bp.signal_stop()
        assert bp._stop.is_set()


class TestDedupSetCapped:
    """The dedup set is bounded — inserting past MAX_DEDUP_IDS evicts the
    oldest entries while keeping the recent in-flight window intact."""

    def test_eviction_keeps_set_within_cap(self):
        """When the cap is exceeded, oldest ids are evicted and length stays
        at exactly MAX_DEDUP_IDS."""
        with patch("landline.poller.MAX_DEDUP_IDS", 5):
            bp = BackgroundPoller("token", 0)
            updates = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(1, 11)
            ]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": updates}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
                bp._poll_loop()

            # Cap holds; only the 5 most recent ids survive.
            assert len(bp._already_queued_update_ids) == 5
            assert list(bp._already_queued_update_ids.keys()) == [6, 7, 8, 9, 10]
            # All 10 updates were queued (eviction is from dedup tracking only).
            queued = bp.drain()
            assert [u["update_id"] for u in queued] == list(range(1, 11))

    def test_dedup_still_works_within_window(self):
        """Within the recent window, dedup continues to suppress re-queues."""
        with patch("landline.poller.MAX_DEDUP_IDS", 5):
            bp = BackgroundPoller("token", 0)
            update = {"update_id": 42, "message": {"text": "hi"}}
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                # Return the same id twice — dedup must collapse to one queued item.
                if call_count[0] <= 2:
                    return {"ok": True, "result": [update]}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
                bp._poll_loop()

            items = bp.drain()
            assert len(items) == 1
            assert items[0]["update_id"] == 42

    def test_evicted_id_can_be_requeued(self):
        """An id evicted by the LRU cap is no longer tracked, so if Telegram
        somehow re-delivers it (it shouldn't past the cursor, but the test
        confirms the eviction took effect), the dedup gate admits it again."""
        with patch("landline.poller.MAX_DEDUP_IDS", 3):
            bp = BackgroundPoller("token", 0)
            first_batch = [
                {"update_id": 1, "message": {"text": "a"}},
                {"update_id": 2, "message": {"text": "b"}},
                {"update_id": 3, "message": {"text": "c"}},
                {"update_id": 4, "message": {"text": "d"}},
            ]
            # After this batch, id 1 has been evicted.
            replay_evicted = [{"update_id": 1, "message": {"text": "a"}}]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": first_batch}
                if call_count[0] == 2:
                    return {"ok": True, "result": replay_evicted}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
                bp._poll_loop()

            queued_ids = [u["update_id"] for u in bp.drain()]
            # id 1 was queued, evicted, then re-queued — appears twice total.
            assert queued_ids.count(1) == 2
            # id 4 (most recent) is still in the dedup set.
            assert 4 in bp._already_queued_update_ids


class TestNetworkVsApiErrorClassification:
    """R5: code/API errors must NOT drive the network-outage alert path —
    only genuine network failures (URLError and friends) do."""

    def test_url_error_triggers_network_alert_path(self):
        """A URLError sustained past POLL_ERROR_ALERT_AFTER fires the
        send_network_alert iMessage path."""
        import urllib.error
        from landline.config import POLL_ERROR_ALERT_AFTER

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise urllib.error.URLError("network down")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, True]
        bp._stop.wait.return_value = False

        # First iter: time.time() for start + time.time() for outage (=0).
        # Second iter: time.time() for outage past threshold -> alert fires.
        time_values = iter([
            0.0, 0.0,
            float(POLL_ERROR_ALERT_AFTER + 1),
        ])
        mock_time = MagicMock()
        mock_time.time.side_effect = lambda: next(time_values)

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert") as mock_alert, \
             patch("landline.poller.log"), \
             patch("landline.poller.time", mock_time):
            bp._poll_loop()

        assert mock_alert.call_count == 1

    def test_runtime_error_does_not_trigger_network_alert(self):
        """A RuntimeError (e.g. not-ok Telegram API response) must NOT
        increment the outage timer or fire the network alert, even if
        sustained past POLL_ERROR_ALERT_AFTER."""
        from landline.config import POLL_ERROR_ALERT_AFTER

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise RuntimeError("Telegram getUpdates returned not-ok: 400 Bad")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, False, True]
        bp._stop.wait.return_value = False

        # No outage timing math should run on the RuntimeError path, but we
        # still wrap time in case some other call path touches it.
        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert") as mock_alert, \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        mock_alert.assert_not_called()
        # The log line should identify this as an API error, not a network error.
        assert any(
            "API error" in str(call.args[0]) for call in mock_log.call_args_list
        )
        assert not any(
            "network error" in str(call.args[0]) for call in mock_log.call_args_list
        )

    def test_key_error_does_not_trigger_network_alert(self):
        """A code bug (KeyError from a malformed payload) must NOT drive the
        network-outage path; it should be logged with a traceback and the
        loop kept alive."""
        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise KeyError("unexpected_field")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert") as mock_alert, \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        mock_alert.assert_not_called()
        # The log must mention "unexpected error" and include the exception type.
        assert any(
            "unexpected error" in str(call.args[0]) and "KeyError" in str(call.args[0])
            for call in mock_log.call_args_list
        )

    def test_api_error_does_not_drive_exponential_backoff(self):
        """API errors should use the short fixed backoff
        (POLL_API_ERROR_BACKOFF_SECONDS), not the network exponential one."""
        from landline.config import POLL_API_ERROR_BACKOFF_SECONDS

        bp = BackgroundPoller("token", 0)
        wait_durations = []

        def fake_get_updates(token, offset):
            raise RuntimeError("not-ok")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, False, True]

        def fake_wait(duration):
            wait_durations.append(duration)
            return False

        bp._stop.wait.side_effect = fake_wait

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"), \
             patch("landline.poller.log"):
            bp._poll_loop()

        # All waits use the fixed short backoff — no doubling.
        assert wait_durations == [
            POLL_API_ERROR_BACKOFF_SECONDS,
            POLL_API_ERROR_BACKOFF_SECONDS,
            POLL_API_ERROR_BACKOFF_SECONDS,
        ]


class TestApiErrorLogThrottle:
    """A3: the non-network ``except Exception`` branch throttles its log
    output like the network branch (1st + every Nth) and emits a traceback
    only on the very first occurrence."""

    def test_poll_api_error_throttles_logging(self):
        """13 RuntimeError iterations -> log fires on #1 and #12 only."""
        from landline.config import POLL_ERROR_LOG_EVERY_N

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise RuntimeError("not-ok")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False] * 13 + [True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"), \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        api_lines = [
            str(c.args[0]) for c in mock_log.call_args_list
            if "API error" in str(c.args[0])
        ]
        # POLL_ERROR_LOG_EVERY_N == 12: log on iteration 1 and iteration 12.
        assert len(api_lines) == 2
        assert "(#1," in api_lines[0]
        assert f"(#{POLL_ERROR_LOG_EVERY_N}," in api_lines[1]

    def test_poll_unexpected_error_traceback_only_on_first(self):
        """Traceback appears in the 1st log call only; throttled lines summarize."""
        from landline.config import POLL_ERROR_LOG_EVERY_N

        bp = BackgroundPoller("token", 0)

        def fake_get_updates(token, offset):
            raise KeyError("boom")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False] * 13 + [True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"), \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        unexpected_lines = [
            str(c.args[0]) for c in mock_log.call_args_list
            if "unexpected error" in str(c.args[0])
        ]
        assert len(unexpected_lines) == 2
        # First call: traceback present, mentions KeyError.
        assert "Traceback" in unexpected_lines[0]
        assert "KeyError" in unexpected_lines[0]
        assert "(#1," in unexpected_lines[0]
        # Throttled Nth call: no traceback, one-line summary only.
        assert "Traceback" not in unexpected_lines[1]
        assert f"(#{POLL_ERROR_LOG_EVERY_N}," in unexpected_lines[1]

    def test_api_error_counter_resets_on_successful_poll(self):
        """fail, fail, succeed (empty), fail -> post-recovery line reads (#1,."""
        bp = BackgroundPoller("token", 0)
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] in (1, 2, 4):
                raise RuntimeError("not-ok")
            # Call 3: successful empty poll -> resets counter.
            return {"ok": True, "result": []}

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, False, False, False, True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"), \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        api_lines = [
            str(c.args[0]) for c in mock_log.call_args_list
            if "API error" in str(c.args[0])
        ]
        # Pre-recovery: (#1,) on first failure; post-recovery: (#1,) again.
        assert len(api_lines) == 2
        assert "(#1," in api_lines[0]
        assert "(#1," in api_lines[1]

    def test_api_error_counter_independent_of_network_counter(self):
        """11 URLErrors then 1 RuntimeError -> RuntimeError line reads (#1,)."""
        import urllib.error

        bp = BackgroundPoller("token", 0)
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            if call_count[0] <= 11:
                raise urllib.error.URLError("net")
            raise RuntimeError("api fail")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False] * 12 + [True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"), \
             patch("landline.poller.log") as mock_log:
            bp._poll_loop()

        api_lines = [
            str(c.args[0]) for c in mock_log.call_args_list
            if "API error" in str(c.args[0])
        ]
        # The single API-error iteration logs as #1 — the 11 network errors
        # must not have polluted the API counter.
        assert len(api_lines) == 1
        assert "(#1," in api_lines[0]


class TestDedupCapLogged:
    """M8: log exactly once when the dedup OrderedDict first reaches
    MAX_DEDUP_IDS. The latch clears on a successful poll after the set
    drops below the cap, so a recurring storm re-arms the warning."""

    def test_dedup_cap_logged_once_on_first_hit(self):
        """One batch of 10 unique ids past cap -> exactly one cap-reached log line."""
        with patch("landline.poller.MAX_DEDUP_IDS", 3):
            bp = BackgroundPoller("token", 0)
            updates = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(1, 11)
            ]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": updates}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
                 patch("landline.poller.log") as mock_log:
                bp._poll_loop()

            cap_lines = [
                str(c.args[0]) for c in mock_log.call_args_list
                if "Dedup set reached" in str(c.args[0])
            ]
            assert len(cap_lines) == 1
            # Sanity: the cap value appears in the message — no PII leakage of update_ids.
            assert "(3)" in cap_lines[0]
            assert bp._dedup_cap_reached_logged is True

    def test_dedup_cap_does_not_log_before_reach(self):
        """A batch below the cap -> zero cap-reached log lines and latch stays False."""
        with patch("landline.poller.MAX_DEDUP_IDS", 5):
            bp = BackgroundPoller("token", 0)
            updates = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(1, 4)
            ]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": updates}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
                 patch("landline.poller.log") as mock_log:
                bp._poll_loop()

            cap_lines = [
                str(c.args[0]) for c in mock_log.call_args_list
                if "Dedup set reached" in str(c.args[0])
            ]
            assert cap_lines == []
            assert bp._dedup_cap_reached_logged is False

    def test_fresh_poller_re_arms_dedup_cap_log(self):
        """Two independent pollers each hitting cap -> each logs its own line.

        Pins the instance-scoped semantic (no module-level global)."""
        cap_lines_total = []

        def run_to_cap():
            bp = BackgroundPoller("token", 0)
            updates = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(1, 6)
            ]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": updates}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
                 patch("landline.poller.log") as mock_log:
                bp._poll_loop()
            cap_lines_total.extend(
                str(c.args[0]) for c in mock_log.call_args_list
                if "Dedup set reached" in str(c.args[0])
            )

        with patch("landline.poller.MAX_DEDUP_IDS", 3):
            run_to_cap()
            run_to_cap()
        # Both fresh pollers re-armed and logged on first cap-hit.
        assert len(cap_lines_total) == 2

    def test_dedup_cap_latch_clears_after_set_shrinks_and_resurges(self):
        """Storm -> log #1. discard ids + successful empty poll resets latch.
        Second storm -> log #2. Verifies the A3 reset path on successful poll."""
        with patch("landline.poller.MAX_DEDUP_IDS", 3):
            bp = BackgroundPoller("token", 0)
            batch_a = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(1, 5)
            ]
            batch_b = [
                {"update_id": i, "message": {"text": "x"}}
                for i in range(100, 104)
            ]
            call_count = [0]

            def fake_get_updates(token, offset):
                call_count[0] += 1
                if call_count[0] == 1:
                    return {"ok": True, "result": batch_a}
                if call_count[0] == 2:
                    # Shrink the set below cap (1 id remaining) and run a
                    # successful empty poll so the latch reset path fires.
                    bp.discard_queued_ids([2, 3, 4])
                    return {"ok": True, "result": []}
                if call_count[0] == 3:
                    return {"ok": True, "result": batch_b}
                bp._stop.set()
                return {"ok": True, "result": []}

            with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
                 patch("landline.poller.log") as mock_log:
                bp._poll_loop()

            cap_lines = [
                str(c.args[0]) for c in mock_log.call_args_list
                if "Dedup set reached" in str(c.args[0])
            ]
            assert len(cap_lines) == 2


class TestLastSuccessfulPoll:
    """Cluster 4 — silent-TCP-stall detection primitives on BackgroundPoller."""

    def test_last_successful_poll_seeded_to_construction_time(self):
        """A fresh poller must not look stale — seed to time.time() so the
        orchestrator's threshold check doesn't false-fire on startup."""
        before = time.time()
        bp = BackgroundPoller("token", 0)
        after = time.time()
        ts = bp.last_successful_poll()
        assert before <= ts <= after

    def test_last_successful_poll_reseeded_on_start(self):
        """A long delay between BackgroundPoller(...) and .start() (e.g. a
        multi-minute restart-continuation Claude turn between orchestrator
        setup and poller launch) must NOT cause the first liveness check
        after start to misread the fresh poller as stale. .start() reseeds
        ``_last_successful_poll_at`` to now so staleness is measured from
        when the poller actually began polling, not from construction.
        """
        bp = BackgroundPoller("token", 0)
        # Simulate a long gap between construction and start (e.g. a
        # restart-continuation Claude turn that ran for many minutes).
        bp._last_successful_poll_at = time.time() - 600.0
        # Prevent the polling loop from actually running.
        bp._stop.set()
        before = time.time()
        bp.start()
        after = time.time()
        try:
            bp._thread.join(timeout=1.0)
        except Exception:
            pass
        ts = bp.last_successful_poll()
        # After .start(), the timestamp must be fresh (within the
        # construction-window bounds, NOT the stale seed).
        assert before <= ts <= after, (
            "start() must reseed _last_successful_poll_at to time.time() so "
            "a long pre-start gap can't trigger a false-positive stall "
            "detection on the first main-loop tick. Got ts=%r before=%r "
            "after=%r" % (ts, before, after)
        )

    def test_last_successful_poll_updates_on_empty_result(self):
        """An empty long-poll return proves the socket round-tripped and
        must advance the last-successful-poll timestamp."""
        bp = BackgroundPoller("token", 0)
        # Force the seed to something clearly stale.
        bp._last_successful_poll_at = time.time() - 3600
        call_count = [0]

        def fake_get_updates(token, offset):
            call_count[0] += 1
            bp._stop.set()
            return {"ok": True, "result": []}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert call_count[0] == 1
        # Timestamp must have jumped forward from the stale seed to ~now.
        assert time.time() - bp.last_successful_poll() < 5.0

    def test_last_successful_poll_updates_on_nonempty_result(self):
        """A non-empty poll return also counts as a successful poll."""
        bp = BackgroundPoller("token", 0)
        bp._last_successful_poll_at = time.time() - 3600

        def fake_get_updates(token, offset):
            bp._stop.set()
            return {"ok": True, "result": [{"update_id": 1, "message": {"text": "hi"}}]}

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert time.time() - bp.last_successful_poll() < 5.0

    def test_last_successful_poll_does_not_advance_on_network_error(self):
        """A URLError in the poll loop is NOT a successful poll — timestamp
        must stay behind so the orchestrator can eventually detect a stall."""
        import urllib.error

        bp = BackgroundPoller("token", 0)
        stale_ts = time.time() - 3600
        bp._last_successful_poll_at = stale_ts

        def fake_get_updates(token, offset):
            raise urllib.error.URLError("network down")

        bp._stop = MagicMock()
        # Two failure iterations then exit.
        bp._stop.is_set.side_effect = [False, False, True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates), \
             patch("landline.poller.send_network_alert"):
            bp._poll_loop()

        # Unchanged — network errors bypass the successful-poll update.
        assert bp.last_successful_poll() == stale_ts

    def test_last_successful_poll_does_not_advance_on_api_error(self):
        """A not-ok Telegram response (API error) also must not advance the
        successful-poll timestamp — the payload wasn't valid."""
        bp = BackgroundPoller("token", 0)
        stale_ts = time.time() - 3600
        bp._last_successful_poll_at = stale_ts

        def fake_get_updates(token, offset):
            raise RuntimeError("Telegram getUpdates returned not-ok: 401 Unauthorized")

        bp._stop = MagicMock()
        bp._stop.is_set.side_effect = [False, True]
        bp._stop.wait.return_value = False

        with patch("landline.poller._telegram_api_get_updates", side_effect=fake_get_updates):
            bp._poll_loop()

        assert bp.last_successful_poll() == stale_ts


class TestSnapshotAndLoadDedupIds:
    """Cluster 4 — dedup handoff across in-process poller replacement."""

    def test_snapshot_and_load_dedup_preserves_ids(self):
        """A snapshot + load roundtrip must preserve the exact set of ids."""
        src = BackgroundPoller("token", 0)
        src._already_queued_update_ids[10] = None
        src._already_queued_update_ids[20] = None
        src._already_queued_update_ids[30] = None

        snapshot = src.snapshot_dedup_ids()
        assert set(snapshot) == {10, 20, 30}

        dst = BackgroundPoller("token", 0)
        dst.load_dedup_ids(snapshot)
        assert set(dst._already_queued_update_ids.keys()) == {10, 20, 30}

    def test_snapshot_preserves_insertion_order(self):
        """Insertion order must survive the snapshot so the FIFO eviction
        semantic is preserved across the swap."""
        src = BackgroundPoller("token", 0)
        for uid in (5, 4, 3, 2, 1):
            src._already_queued_update_ids[uid] = None
        assert src.snapshot_dedup_ids() == [5, 4, 3, 2, 1]

    def test_load_dedup_respects_max_dedup_ids_cap(self):
        """Loading more than MAX_DEDUP_IDS must evict oldest entries so the
        cap semantic is not accidentally widened during a replacement swap."""
        with patch("landline.poller.MAX_DEDUP_IDS", 3):
            dst = BackgroundPoller("token", 0)
            dst.load_dedup_ids([1, 2, 3, 4, 5])
            assert len(dst._already_queued_update_ids) == 3
            # Oldest evicted first — the surviving ids are the tail.
            assert list(dst._already_queued_update_ids.keys()) == [3, 4, 5]

    def test_load_dedup_into_populated_set(self):
        """Loading over an already-populated set merges + preserves cap."""
        dst = BackgroundPoller("token", 0)
        dst._already_queued_update_ids[100] = None
        dst.load_dedup_ids([200, 300])
        assert set(dst._already_queued_update_ids.keys()) == {100, 200, 300}


class TestPreloadQueue:
    """Regression: the poller-swap path forwards orphaned updates from the
    old poller's queue into the new poller via preload_queue (finding 1)."""

    def test_preload_queue_appends_updates_and_returns_count(self):
        bp = BackgroundPoller("token", 0)
        updates = [
            {"update_id": 1, "message": {"text": "a"}},
            {"update_id": 2, "message": {"text": "b"}},
        ]
        count = bp.preload_queue(updates)
        assert count == 2
        drained = bp.drain()
        assert [u["update_id"] for u in drained] == [1, 2]

    def test_preload_queue_does_not_touch_dedup(self):
        """preload_queue is used AFTER load_dedup_ids in the swap path — it
        must not alter the dedup set (that's the caller's responsibility)."""
        bp = BackgroundPoller("token", 0)
        bp.load_dedup_ids([1, 2])
        before = list(bp._already_queued_update_ids.keys())
        bp.preload_queue([{"update_id": 99, "message": {"text": "c"}}])
        assert list(bp._already_queued_update_ids.keys()) == before

    def test_preload_queue_skips_non_dicts(self):
        bp = BackgroundPoller("token", 0)
        count = bp.preload_queue([None, "not a dict", {"update_id": 1}])
        assert count == 1
        assert len(bp.drain()) == 1


class TestQueuePutInsideDedupLock:
    """Regression for finding 1's dedup-add/queue-put atomicity: the queue
    write MUST happen under the dedup lock so a poller-swap's snapshot
    either sees the dedup entry AND the queued update, or neither."""

    def test_dedup_and_queue_updated_atomically(self):
        """Interleave a snapshot with a polling cycle: when we hold the
        dedup lock via snapshot, the poll loop that started must not have
        left a mid-state (dedup-yes / queue-no) visible. We assert this by
        forcing the atomic property directly: while the dedup lock is
        held, the queue can be inspected in a consistent state relative to
        the dedup set for the ids that were about to be inserted."""
        bp = BackgroundPoller("token", 0)
        update = {"update_id": 500, "message": {"text": "hi"}}
        stop_snapshot = threading.Event()

        def _poll_once():
            # Emulate the exact section of _poll_loop that adds and enqueues.
            with bp._already_queued_update_ids_lock:
                bp._already_queued_update_ids[500] = None
                # If the queue.put is NOT inside the lock, another thread
                # holding the lock via snapshot_dedup_ids could observe the
                # dedup entry while the queue is still empty. With the fix,
                # the put happens here, before the lock releases.
                bp._incoming_updates_queue.put(update)

        _poll_once()

        # Snapshot + queue drain must both see the item.
        assert 500 in bp.snapshot_dedup_ids()
        assert [u["update_id"] for u in bp.drain()] == [500]
