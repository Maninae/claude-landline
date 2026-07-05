"""Regression tests for landline.telegram.reactions (setMessageReaction).

Contracts covered:
  - Fire-and-forget: the entrypoint enqueues to the single worker and
    returns immediately; it MUST NEVER block on the HTTP request.
  - Payload shape matches Telegram Bot API 7.0 ``setMessageReaction``.
  - ``emoji=None`` clears via an empty ``reaction`` array.
  - Failure → single retry → swallow (log metadata only, never PII).
  - Kill switch: ``REACTION_ACKS_ENABLED=False`` skips both enqueue and
    HTTP.
  - **Ordering across calls on the same (chat_id, message_id) is FIFO**
    at the Telegram edge — the "reaction race" regression pin.
"""

import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from landline import config
from landline.telegram import reactions


# Opt this whole module back in to the reactions HTTP path (the autouse
# ``disable_reactions_network`` fixture in conftest.py flips
# REACTION_ACKS_ENABLED to False for every other test file).
pytestmark = pytest.mark.reactions_network


FAKE_TOKEN = "000000000:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
FAKE_CHAT = "123456789"


class _FakeUrlopenResponse:
    """Minimal object matching the ``urllib.request.urlopen`` context
    manager contract just enough for _do_react's read of .status."""

    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def getcode(self):
        return self.status

    def read(self):
        return b'{"ok":true,"result":true}'


def _wait_for_queue_idle(timeout: float = 3.0) -> None:
    """Wait for the reactions worker to drain the queue. Replaces the
    old per-call-thread wait — the module now uses one persistent
    worker + FIFO queue for ordering discipline."""
    assert reactions._wait_for_queue_idle(timeout=timeout), (
        "reactions queue did not idle within %.1fs — worker may be "
        "stuck or dead" % timeout
    )


class TestFireAndForget:
    """The entrypoint MUST return within a millisecond even if the HTTP
    layer is arbitrarily slow. Callers rely on this to keep classify /
    finalize hot paths unblocked."""

    def test_returns_immediately_when_urlopen_blocks(self):
        """Even if urlopen would block, the caller returns in <100ms.
        Uses a short sleep + Event so the worker unblocks promptly and
        doesn't leak into a subsequent test."""
        gate = threading.Event()

        def blocking_urlopen(*args, **kwargs):
            # Block until the test releases us. Kept short so the
            # worker exits back to the queue inside the patch scope.
            gate.wait(timeout=1.0)
            raise RuntimeError("we never actually wait for this")

        with patch("urllib.request.urlopen", side_effect=blocking_urlopen):
            start = time.time()
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 42, config.REACTION_ACK_EMOJI,
            )
            elapsed = time.time() - start
            # Prove the caller returned immediately.
            assert elapsed < 0.1, (
                "set_reaction_async blocked for %.3fs — must be "
                "fire-and-forget" % elapsed
            )
            # Cleanup: unblock the worker so it dies inside the patch
            # scope and can't call a later test's patched urlopen.
            gate.set()
            _wait_for_queue_idle(timeout=3.0)

    def test_ensures_single_persistent_worker_thread(self):
        """The reactions module MUST run one persistent worker thread
        (daemon, named ``landline-react-worker``), NOT one thread per
        call. The single-worker design is what serializes SET-then-
        CLEAR at the Telegram edge (see 'reaction race' finding).
        """
        # Reset the module-level worker so the test observes the
        # lazy-start path deterministically.
        with reactions._worker_lock:
            reactions._worker_thread = None

        with patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenResponse(status=200),
        ):
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 1, config.REACTION_ACK_EMOJI,
            )
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 2, config.REACTION_ACK_EMOJI,
            )
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 3, config.REACTION_ACK_EMOJI,
            )
            _wait_for_queue_idle(timeout=3.0)

        # Exactly one worker exists (not three), it's daemonized, and
        # its name is the observable identity.
        worker = reactions._worker_thread
        assert worker is not None, "no worker thread was started"
        assert worker.daemon is True, (
            "worker must be a daemon thread so it can't hold interpreter "
            "shutdown"
        )
        assert worker.name == "landline-react-worker"
        # And no per-call ``landline-react`` threads were spawned (the
        # old buggy model).
        alive_per_call = [
            t for t in threading.enumerate()
            if t.name == "landline-react" and t.is_alive()
        ]
        assert alive_per_call == [], (
            "found %d legacy per-call threads — the module regressed to "
            "the racey per-call model" % len(alive_per_call)
        )


class TestPayloadShape:
    """The Bot API payload shape is load-bearing — a shape drift on the
    server side would silently return 400 and we'd log a metadata line
    with no clue why. Pin the shape here."""

    def _run_and_capture(self, emoji, message_id=7777):
        """Collect every urlopen call the worker makes, then filter to
        the one for OUR specific message_id. Filtering (instead of
        assuming only one call) guards against contamination from
        lingering items enqueued by prior orchestrator tests — the
        worker may still be draining a leftover queue when this test
        starts.
        """
        # Drain any lingering items before we begin so contamination
        # is minimized in the first place.
        _wait_for_queue_idle(timeout=2.0)
        seen = []

        def capture_urlopen(req, *args, **kwargs):
            seen.append({
                "url": req.full_url,
                "method": req.get_method(),
                "body": req.data,
                "ctype": req.get_header("Content-type"),
            })
            return _FakeUrlopenResponse(status=200)

        with patch(
            "urllib.request.urlopen",
            side_effect=capture_urlopen,
        ):
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, message_id, emoji,
            )
            _wait_for_queue_idle()
        # Find OUR call: the payload's message_id is the ground truth.
        for call in seen:
            try:
                decoded = json.loads(call["body"].decode("utf-8"))
            except Exception:
                continue
            if decoded.get("message_id") == message_id:
                return call
        raise AssertionError(
            "No urlopen call captured with message_id=%d (seen %d calls "
            "total — possible queue leak from another test)" % (
                message_id, len(seen),
            )
        )

    def test_ack_emoji_produces_expected_payload(self):
        captured = self._run_and_capture(config.REACTION_ACK_EMOJI)
        assert captured["url"] == (
            "https://api.telegram.org/bot%s/setMessageReaction" % FAKE_TOKEN
        )
        assert captured["method"] == "POST"
        assert captured["ctype"] == "application/json"
        decoded = json.loads(captured["body"].decode("utf-8"))
        assert decoded == {
            "chat_id": FAKE_CHAT,
            "message_id": 7777,
            "reaction": [
                {"type": "emoji", "emoji": config.REACTION_ACK_EMOJI},
            ],
        }

    def test_none_emoji_clears_via_empty_array(self):
        """emoji=None → ``reaction: []`` — Telegram's documented clear
        semantics."""
        captured = self._run_and_capture(None)
        decoded = json.loads(captured["body"].decode("utf-8"))
        assert decoded["reaction"] == []
        assert decoded["chat_id"] == FAKE_CHAT
        assert decoded["message_id"] == 7777


class TestRetryAndSwallow:
    """Reactions are UX polish — a lost 👀/👌 is invisible. Retry once
    and then swallow. NEVER log emoji or user content, only metadata."""

    def test_retries_once_on_urlerror_then_swallows(self):
        from urllib.error import URLError

        _wait_for_queue_idle(timeout=2.0)
        call_count = {"n": 0}

        def failing_urlopen(*args, **kwargs):
            call_count["n"] += 1
            raise URLError("connection refused")

        with patch("urllib.request.urlopen", side_effect=failing_urlopen), \
             patch("landline.telegram.reactions.log") as mock_log:
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 99, config.REACTION_ACK_EMOJI,
            )
            _wait_for_queue_idle()
        # Two attempts (initial + REACTION_MAX_ATTEMPTS - 1 retries).
        assert call_count["n"] == config.REACTION_MAX_ATTEMPTS
        # A single failure log with metadata only.
        assert mock_log.call_count == 1
        log_msg = mock_log.call_args[0][0]
        assert "reaction failed" in log_msg
        assert str(FAKE_CHAT) in log_msg
        assert "99" in log_msg
        # PII / content discipline: the log line must never contain the
        # bot token or the emoji itself (defense-in-depth even though
        # config owns the emoji).
        assert FAKE_TOKEN not in log_msg
        assert config.REACTION_ACK_EMOJI not in log_msg

    def test_success_on_first_try_does_not_log(self):
        _wait_for_queue_idle(timeout=2.0)
        with patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenResponse(status=200),
        ), patch("landline.telegram.reactions.log") as mock_log:
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 5, config.REACTION_ACK_EMOJI,
            )
            _wait_for_queue_idle()
        # Success path emits no failure log.
        mock_log.assert_not_called()

    def test_non_2xx_status_triggers_retry(self):
        """A 400 (bad emoji) is not thrown by urlopen but still counts
        as a failure — the retry loop must observe it."""
        _wait_for_queue_idle(timeout=2.0)
        response = _FakeUrlopenResponse(status=400)
        with patch(
            "urllib.request.urlopen",
            return_value=response,
        ) as mock_open, \
             patch("landline.telegram.reactions.log") as mock_log:
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 33, config.REACTION_ACK_EMOJI,
            )
            _wait_for_queue_idle()
        assert mock_open.call_count == config.REACTION_MAX_ATTEMPTS
        # Fell through both attempts → one metadata log line.
        assert mock_log.call_count == 1


class TestKillSwitch:
    """``REACTION_ACKS_ENABLED=False`` must skip BOTH the enqueue AND
    the HTTP request — otherwise the switch is only cosmetic."""

    def test_disabled_flag_skips_enqueue_and_urlopen(self, monkeypatch):
        monkeypatch.setattr(
            "landline.config.REACTION_ACKS_ENABLED", False,
        )
        # Snapshot queue depth so we can verify nothing was enqueued.
        depth_before = reactions._reaction_queue.qsize()
        with patch(
            "urllib.request.urlopen",
        ) as mock_open:
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 1, config.REACTION_ACK_EMOJI,
            )
            reactions.set_reactions_batch_async(
                FAKE_TOKEN, FAKE_CHAT, [1, 2, 3], config.REACTION_DONE_EMOJI,
            )
        # No enqueue, no HTTP.
        assert reactions._reaction_queue.qsize() == depth_before
        mock_open.assert_not_called()

    def test_enabled_flag_processes_enqueue(self, monkeypatch):
        monkeypatch.setattr(
            "landline.config.REACTION_ACKS_ENABLED", True,
        )
        _wait_for_queue_idle(timeout=2.0)
        with patch(
            "urllib.request.urlopen",
            return_value=_FakeUrlopenResponse(status=200),
        ) as mock_open:
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 1, config.REACTION_ACK_EMOJI,
            )
            _wait_for_queue_idle()
        assert mock_open.call_count >= 1


class TestBatchApi:
    """set_reactions_batch_async enqueues one item per message_id,
    preserving iteration order across the queue."""

    def test_batch_enqueues_one_item_per_id(self):
        _wait_for_queue_idle(timeout=2.0)
        seen_ids = []

        def capture_urlopen(req, *args, **kwargs):
            try:
                decoded = json.loads(req.data.decode("utf-8"))
                seen_ids.append(decoded.get("message_id"))
            except Exception:
                pass
            return _FakeUrlopenResponse(status=200)

        with patch(
            "urllib.request.urlopen", side_effect=capture_urlopen,
        ):
            reactions.set_reactions_batch_async(
                FAKE_TOKEN, FAKE_CHAT, [10, 20, 30],
                config.REACTION_DONE_EMOJI,
            )
            _wait_for_queue_idle()
        # Order preserved: FIFO across the iterable.
        our_ids = [m for m in seen_ids if m in (10, 20, 30)]
        assert our_ids == [10, 20, 30], (
            "batch enqueue did not preserve order — got %r" % our_ids
        )

    def test_batch_empty_iterable_is_noop(self):
        """Empty ids: no enqueue, no urlopen — trivial guard."""
        depth_before = reactions._reaction_queue.qsize()
        with patch(
            "urllib.request.urlopen",
        ) as mock_open:
            reactions.set_reactions_batch_async(
                FAKE_TOKEN, FAKE_CHAT, [], config.REACTION_DONE_EMOJI,
            )
        assert reactions._reaction_queue.qsize() == depth_before
        mock_open.assert_not_called()


class TestOrderingRaceRegression:
    """The reaction-race finding: a fire-and-forget SET followed by a
    fire-and-forget CLEAR on the same ``(chat_id, message_id)`` used to
    race at the Telegram edge because each POST ran on its own thread
    with an independent TCP connection. The single-worker queue removes
    the race: FIFO enqueue-order is preserved through to the HTTP layer.

    This test PINS that guarantee against future refactors — a return
    to the per-call-thread model would break it.
    """

    def test_set_then_clear_hits_telegram_in_order(self):
        """Enqueue SET-👀 then CLEAR ([]) on the same message_id.
        Both HTTP calls MUST hit the mocked urlopen in that order.
        """
        _wait_for_queue_idle(timeout=2.0)
        # Introduce jitter into the SET's HTTP round-trip so a naive
        # per-call-thread model (where the CLEAR thread might race
        # ahead) would visibly fail — the SET call takes longer than
        # the CLEAR call.
        observed_order = []

        def jittered_urlopen(req, *args, **kwargs):
            try:
                decoded = json.loads(req.data.decode("utf-8"))
            except Exception:
                decoded = {}
            if decoded.get("message_id") == 9999:
                observed_order.append(
                    "clear" if decoded.get("reaction") == [] else "set"
                )
                if decoded.get("reaction") != []:
                    # SET is slower than CLEAR; a per-call-thread
                    # implementation would let CLEAR overtake this.
                    time.sleep(0.05)
            return _FakeUrlopenResponse(status=200)

        with patch(
            "urllib.request.urlopen", side_effect=jittered_urlopen,
        ):
            # Program order: SET, then CLEAR. Dispatch is single-
            # threaded so both go on the FIFO queue in this order.
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 9999, config.REACTION_ACK_EMOJI,
            )
            reactions.set_reaction_async(
                FAKE_TOKEN, FAKE_CHAT, 9999, None,
            )
            _wait_for_queue_idle(timeout=5.0)

        # The SET must have hit Telegram BEFORE the CLEAR. If a future
        # refactor re-introduces per-call threads, the CLEAR could
        # complete first because SET's jittered 50ms sleep gives it
        # a head start — that's the bug we're pinning against.
        assert observed_order == ["set", "clear"], (
            "reaction ordering regressed to the racey per-call-thread "
            "model — got %r (expected ['set', 'clear'])" % observed_order
        )

    def test_batch_clear_after_set_preserves_order(self):
        """The photo-all-fail path: classifier fires SET-👀 on each
        photo id via ``set_reaction_async``, then photo_handler fires
        CLEAR on the whole group via ``set_reactions_batch_async``.
        All SETs MUST land at Telegram before any CLEAR on the same ids.
        """
        _wait_for_queue_idle(timeout=2.0)
        events = []

        def track_urlopen(req, *args, **kwargs):
            try:
                decoded = json.loads(req.data.decode("utf-8"))
            except Exception:
                return _FakeUrlopenResponse(status=200)
            mid = decoded.get("message_id")
            if mid in (8001, 8002, 8003):
                op = "clear" if decoded.get("reaction") == [] else "set"
                events.append((op, mid))
            return _FakeUrlopenResponse(status=200)

        with patch(
            "urllib.request.urlopen", side_effect=track_urlopen,
        ):
            # Simulate classifier: SET on each id, one at a time.
            for mid in (8001, 8002, 8003):
                reactions.set_reaction_async(
                    FAKE_TOKEN, FAKE_CHAT, mid, config.REACTION_ACK_EMOJI,
                )
            # Simulate photo_handler's all-fail CLEAR.
            reactions.set_reactions_batch_async(
                FAKE_TOKEN, FAKE_CHAT, [8001, 8002, 8003], None,
            )
            _wait_for_queue_idle(timeout=5.0)

        # Every SET on a given mid must precede its CLEAR.
        for mid in (8001, 8002, 8003):
            set_idx = events.index(("set", mid))
            clear_idx = events.index(("clear", mid))
            assert set_idx < clear_idx, (
                "CLEAR raced past SET for message_id=%d (events=%r)"
                % (mid, events)
            )
