"""Tests for landline.stream_pump — the per-process persistent stdout reader.

The scenarios here encode the 2026-06/07 "desync" root cause and its fix:
harness-initiated turns (background subagents / run_in_background Bash
completing while no turn is in flight) emit init..result blocks on stdout
with no stdin write. The old per-turn reader consumed the first stale
`result` as its own and shifted every later turn's response by one. The
pump must (a) deliver unsolicited turns immediately, and (b) never let a
dispatched turn consume another turn's block.
"""

import json
import queue
import threading
import time
import unittest
from unittest.mock import patch

from landline.stream_pump import StreamPump, TurnHandle, get_or_create_pump


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class _QueueStdout:
    """A blocking stdout fake: the test feeds lines; close() causes EOF."""

    _EOF = object()

    def __init__(self):
        self._q = queue.Queue()
        self.closed = False

    def feed(self, line):
        self._q.put(line)

    def feed_event(self, event):
        self._q.put(json.dumps(event) + "\n")

    def feed_error(self, exc):
        self._q.put(exc)

    def close(self):
        self.closed = True
        self._q.put(self._EOF)

    def __iter__(self):
        return self

    def __next__(self):
        item = self._q.get()
        if item is self._EOF:
            raise StopIteration
        if isinstance(item, Exception):
            raise item
        return item


class _FakeProc:
    def __init__(self):
        self.stdout = _QueueStdout()
        self._returncode = None

    def poll(self):
        return self._returncode


class _FakeSender:
    """Records the ordered stream of sender operations."""

    def __init__(self):
        self.ops = []
        self._lock = threading.Lock()

    def text(self, delta):
        with self._lock:
            self.ops.append(("text", delta))

    def status(self, line):
        with self._lock:
            self.ops.append(("status", line))

    def flush(self):
        with self._lock:
            self.ops.append(("flush", None))

    def snapshot(self):
        with self._lock:
            return list(self.ops)

    def wait_for(self, op, timeout=2.0):
        """Wait until an op tuple appears; returns True if seen in time."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if op in self.snapshot():
                return True
            time.sleep(0.01)
        return False


def _init_event(session_id="sess-1"):
    return {"type": "system", "subtype": "init", "session_id": session_id}


def _assistant_text(msg_id, text):
    return {
        "type": "assistant",
        "parent_tool_use_id": None,
        "message": {"id": msg_id, "content": [{"type": "text", "text": text}]},
    }


def _result_event(text, session_id="sess-1"):
    return {"type": "result", "subtype": "success", "result": text,
            "session_id": session_id}


def _notification_event():
    return {"type": "system", "subtype": "task_notification",
            "task_id": "t1", "status": "completed"}


class _PumpTestCase(unittest.TestCase):
    def setUp(self):
        self.proc = _FakeProc()
        self.pump = StreamPump(self.proc)
        self.addCleanup(self._teardown_pump)

    def _teardown_pump(self):
        self.proc.stdout.close()
        self.pump._thread.join(timeout=2)

    def _settle(self, timeout=2.0):
        """Wait until the pump has drained everything fed so far."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.stdout._q.empty():
                time.sleep(0.05)
                return
            time.sleep(0.01)
        self.fail("pump did not drain fed events in time")


# ---------------------------------------------------------------------------
# Dispatched turns
# ---------------------------------------------------------------------------

class TestDispatchedTurn(_PumpTestCase):
    def test_turn_events_routed_and_handle_completed(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event("sess-abc"))
        self.proc.stdout.feed_event(_assistant_text("m1", "hello"))
        self.proc.stdout.feed_event(_result_event("hello", "sess-abc"))

        self.assertTrue(handle.done.wait(2))
        self.assertTrue(handle.saw_result)
        self.assertEqual(handle.final_result, "hello")
        self.assertEqual("".join(handle.streamed_parts), "hello")
        self.assertEqual(handle.init_session_id, "sess-abc")
        self.assertEqual(handle.result_session_id, "sess-abc")
        self.assertIn(("text", "hello"), sender.snapshot())
        self.assertIsNone(handle.error)

    def test_delta_dedup_against_cumulative_text(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "part one"))
        self.proc.stdout.feed_event(_assistant_text("m1", "part one and two"))
        self.proc.stdout.feed_event(_result_event("part one and two"))

        self.assertTrue(handle.done.wait(2))
        self.assertEqual("".join(handle.streamed_parts), "part one and two")
        texts = [d for (op, d) in sender.snapshot() if op == "text"]
        self.assertEqual(texts, ["part one", " and two"])

    def test_flush_between_assistant_message_ids(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "first bubble"))
        self.proc.stdout.feed_event(_assistant_text("m2", "second bubble"))
        self.proc.stdout.feed_event(_result_event("second bubble"))

        self.assertTrue(handle.done.wait(2))
        ops = sender.snapshot()
        # Trailing flush is the pump-side end-of-turn boundary (H1 fix).
        self.assertEqual(
            ops,
            [("text", "first bubble"), ("flush", None),
             ("text", "second bubble"), ("flush", None)],
        )

    def test_final_result_tail_appended_on_pump_thread_before_done(self):
        """The result payload extending the streamed deltas must be sent as
        a tail INSIDE the turn's bubble, before the end-of-turn flush, and
        streamed_parts must join to the full final text (old-reader parity;
        the append moved pump-side for the H1 race fix)."""
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "hello"))
        self.proc.stdout.feed_event(_result_event("hello, world!"))

        self.assertTrue(handle.done.wait(2))
        self.assertEqual(
            sender.snapshot(),
            [("text", "hello"), ("text", ", world!"), ("flush", None)],
        )
        self.assertEqual("".join(handle.streamed_parts), "hello, world!")

    def test_final_result_only_turn_sends_whole_result_as_text(self):
        """No streamed deltas at all — the whole result payload is sent."""
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_result_event("full reply"))

        self.assertTrue(handle.done.wait(2))
        self.assertEqual(
            sender.snapshot(),
            [("text", "full reply"), ("flush", None)],
        )
        self.assertEqual("".join(handle.streamed_parts), "full reply")

    def test_no_tail_when_interrupted(self):
        """Interrupted turns must not append the final tail (old-reader
        parity with the interrupt_sent gate)."""
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "partial"))
        self.assertTrue(sender.wait_for(("text", "partial")))
        handle.interrupt_suppress.set()
        self.proc.stdout.feed_event(_result_event("partial plus more"))

        self.assertTrue(handle.done.wait(2))
        texts = [d for (op, d) in sender.snapshot() if op == "text"]
        self.assertEqual(texts, ["partial"])

    def test_tool_use_status_routed_once_per_tool_id(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        tool_event = {
            "type": "assistant",
            "parent_tool_use_id": None,
            "message": {"id": "m1", "content": [
                {"type": "tool_use", "id": "tu1", "name": "Bash",
                 "input": {"command": "ls"}},
            ]},
        }
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(tool_event)
        self.proc.stdout.feed_event(tool_event)  # duplicate id — dedup
        self.proc.stdout.feed_event(_result_event("done"))

        self.assertTrue(handle.done.wait(2))
        statuses = [op for op in sender.snapshot() if op[0] == "status"]
        self.assertEqual(len(statuses), 1)

    def test_sidechain_assistant_events_skipped(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        sidechain = _assistant_text("m1", "subagent inner text")
        sidechain["parent_tool_use_id"] = "tu-parent"
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(sidechain)
        self.proc.stdout.feed_event(_result_event("outer"))

        self.assertTrue(handle.done.wait(2))
        # The sidechain delta is skipped; the result payload "outer" is then
        # delivered whole via the final-tail path (old-reader parity).
        self.assertEqual(handle.streamed_parts, ["outer"])
        self.assertNotIn(("text", "subagent inner text"), sender.snapshot())
        self.assertIn(("text", "outer"), sender.snapshot())

    def test_interrupt_suppress_stops_routing(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "before"))
        self.assertTrue(sender.wait_for(("text", "before")))
        handle.interrupt_suppress.set()
        self.proc.stdout.feed_event(_assistant_text("m2", "after interrupt"))
        self.proc.stdout.feed_event(_result_event("after interrupt"))

        self.assertTrue(handle.done.wait(2))
        self.assertNotIn(("text", "after interrupt"), sender.snapshot())
        self.assertEqual("".join(handle.streamed_parts), "before")


# ---------------------------------------------------------------------------
# Unsolicited (harness-initiated) turns — the desync seed
# ---------------------------------------------------------------------------

class TestUnsolicitedTurn(_PumpTestCase):
    def test_unsolicited_block_routed_to_idle_route_immediately(self):
        idle_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=idle_sender):
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)
            self.proc.stdout.feed_event(_notification_event())
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(
                _assistant_text("m1", "background task finished"))
            self.proc.stdout.feed_event(_result_event("background task finished"))

            self.assertTrue(
                idle_sender.wait_for(("text", "background task finished")))
            # Block close marks a bubble boundary.
            self.assertTrue(idle_sender.wait_for(("flush", None)))

    def test_unsolicited_result_only_text_delivered(self):
        idle_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=idle_sender):
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_result_event("only in result"))

            self.assertTrue(idle_sender.wait_for(("text", "only in result")))

    def test_unsolicited_block_with_no_idle_route_does_not_crash(self):
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "dropped"))
        self.proc.stdout.feed_event(_result_event("dropped"))
        # Let the pump finish the unsolicited block before dispatching the
        # next turn (mirrors production, where dispatches are minutes apart;
        # registering DURING an in-flight block is the documented cosmetic
        # attribution race, not what this test targets).
        self._settle()
        # Pump must survive and still serve a later dispatched turn.
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m2", "next turn"))
        self.proc.stdout.feed_event(_result_event("next turn"))
        self.assertTrue(handle.done.wait(2))
        self.assertEqual("".join(handle.streamed_parts), "next turn")


class TestDesyncRegression(_PumpTestCase):
    """The 2026-06-30 off-by-one, reproduced end-to-end at the pump level.

    Old behavior: after an unsolicited block, dispatched turn N delivered
    turn N-1's response forever. New behavior: every turn delivers its own.
    """

    def test_off_by_one_is_dead(self):
        idle_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=idle_sender):
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)

            # Turn A (dispatched, e.g. "Subagent go do it")
            sender_a = _FakeSender()
            handle_a = TurnHandle()
            self.pump.register_turn(handle_a, sender_a)
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("mA", "dispatched. A'"))
            self.proc.stdout.feed_event(_result_event("dispatched. A'"))
            self.assertTrue(handle_a.done.wait(2))
            self.assertEqual("".join(handle_a.streamed_parts), "dispatched. A'")

            # Background subagent completes while idle — harness runs an
            # unsolicited turn (task_notification -> init -> ... -> result).
            self.proc.stdout.feed_event(_notification_event())
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(
                _assistant_text("mBG", "subagent came back: findings"))
            self.proc.stdout.feed_event(
                _result_event("subagent came back: findings"))
            # Delivered NOW — not one message later.
            self.assertTrue(
                idle_sender.wait_for(("text", "subagent came back: findings")))

            # Turn B (dispatched, e.g. "Hey you're back!") — must get B's
            # response, not the background turn's leftovers.
            sender_b = _FakeSender()
            handle_b = TurnHandle()
            self.pump.register_turn(handle_b, sender_b)
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("mB", "B' reply"))
            self.proc.stdout.feed_event(_result_event("B' reply"))
            self.assertTrue(handle_b.done.wait(2))
            self.assertEqual("".join(handle_b.streamed_parts), "B' reply")
            self.assertEqual(handle_b.final_result, "B' reply")
            self.assertNotIn(("text", "subagent came back: findings"),
                             sender_b.snapshot())


# ---------------------------------------------------------------------------
# Lifecycle: EOF, read errors, dead pump, cancellation
# ---------------------------------------------------------------------------

class TestPumpLifecycle(_PumpTestCase):
    def test_eof_completes_pending_handle_without_error(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "partial"))
        self.proc.stdout.close()

        self.assertTrue(handle.done.wait(2))
        self.assertFalse(handle.saw_result)
        self.assertIsNone(handle.error)
        self.assertEqual("".join(handle.streamed_parts), "partial")

    def test_read_error_completes_pending_handle_with_error(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_error(ValueError("I/O operation on closed file"))

        self.assertTrue(handle.done.wait(2))
        self.assertIsNotNone(handle.error)
        self.assertIn("closed file", handle.error)
        self.assertFalse(self.pump.alive)

    def test_register_turn_on_dead_pump_completes_immediately(self):
        self.proc.stdout.close()
        self.pump._thread.join(timeout=2)
        self.assertFalse(self.pump.alive)

        handle = TurnHandle()
        self.pump.register_turn(handle, _FakeSender())
        self.assertTrue(handle.done.wait(2))
        self.assertIsNotNone(handle.error)

    def test_cancel_turn_unregisters_and_completes(self):
        handle = TurnHandle()
        self.pump.register_turn(handle, _FakeSender())
        self.pump.cancel_turn(handle)
        self.assertTrue(handle.done.is_set())

        # A later block is unsolicited, not attributed to the cancelled turn.
        idle_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=idle_sender):
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("m1", "later block"))
            self.proc.stdout.feed_event(_result_event("later block"))
            self.assertTrue(idle_sender.wait_for(("text", "later block")))
        self.assertEqual(handle.streamed_parts, [])

    def test_pump_releases_proc_reference_on_exit(self):
        """The pump must drop its strong proc reference when its thread
        exits, or the weak-keyed pump registry can never collect the
        (proc, pump) pair — a slow leak across respawns (/new, interrupts,
        watchdog kills). See the audit finding of 2026-07-02."""
        self.proc.stdout.close()
        self.pump._thread.join(timeout=2)
        self.assertFalse(self.pump.alive)
        self.assertIsNone(self.pump.proc)

    def test_malformed_event_does_not_kill_pump(self):
        self.proc.stdout.feed("this is not json\n")
        self.proc.stdout.feed_event({"type": "assistant", "message": "not-a-dict"})
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)
        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "still works"))
        self.proc.stdout.feed_event(_result_event("still works"))
        self.assertTrue(handle.done.wait(2))
        self.assertEqual("".join(handle.streamed_parts), "still works")


class TestH1BackToBackRaceRegression(_PumpTestCase):
    """2026-07-02 audit finding H1: a dispatched turn whose result extends
    its streamed text, with an unsolicited block back-to-back in the pipe.

    Broken behavior (tail appended by the dispatch thread after done.wait):
        text 'hello' | text BG | flush | text ', world!' | flush
    → Telegram bubbles "helloBG" and ", world!".

    Required behavior (pump is the sole producer of turn content):
        text 'hello' | text ', world!' | flush | text BG | flush
    """

    def test_back_to_back_unsolicited_block_cannot_garble_turn_bubble(self):
        chat_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=chat_sender):
            # Same per-chat sender for the turn and the idle route — exactly
            # like production, where the registry is chat-keyed.
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)
            handle = TurnHandle()
            self.pump.register_turn(handle, chat_sender)

            # Everything lands in the pipe back-to-back, BEFORE the dispatch
            # thread can react to handle.done — maximum pipeline pressure.
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("m1", "hello"))
            self.proc.stdout.feed_event(_result_event("hello, world!"))
            self.proc.stdout.feed_event(_notification_event())
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("mBG", "BG_TEXT"))
            self.proc.stdout.feed_event(_result_event("BG_TEXT"))

            self.assertTrue(handle.done.wait(2))
            self.assertTrue(chat_sender.wait_for(("text", "BG_TEXT")))
            self.assertEqual(
                chat_sender.snapshot(),
                [("text", "hello"), ("text", ", world!"), ("flush", None),
                 ("text", "BG_TEXT"), ("flush", None)],
            )
            self.assertEqual("".join(handle.streamed_parts), "hello, world!")


class TestStreamingIntegrationRegression(unittest.TestCase):
    """The June 30 desync, end-to-end through run_claude_streaming.

    Turn 1 dispatches normally. A background turn then completes while no
    turn is in flight. Turn 2 must return ITS OWN response — with the old
    per-turn reader it returned the background turn's leftovers and left
    its own response in the pipe (the permanent off-by-one).
    """

    def test_dispatched_turn_after_unsolicited_block_gets_own_response(self):
        from unittest.mock import MagicMock
        from landline.streaming import run_claude_streaming

        proc = _FakeProc()
        proc.returncode = None

        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""

        turn_scripts = [
            [_init_event(), _assistant_text("m1", "A' reply"),
             _result_event("A' reply")],
            [_init_event(), _assistant_text("m2", "B' reply"),
             _result_event("B' reply")],
        ]

        def _feed_next_turn(_msg):
            for event in turn_scripts.pop(0):
                proc.stdout.feed_event(event)

        fake_pc.send_message.side_effect = _feed_next_turn

        sender = _FakeSender()
        sender.is_closed = False
        sender.worker_alive = True

        try:
            with patch("landline.claude._get_persistent_claude",
                       return_value=fake_pc), \
                 patch("landline.claude._get_or_create_sender",
                       return_value=sender):
                result_a = run_claude_streaming(
                    token="tok", chat_id="chatA", message="A",
                    session_id="sess-1", is_new=False,
                    send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
                )
                self.assertEqual(result_a.streamed_text, "A' reply")

                # Background turn completes while idle (no dispatch pending).
                proc.stdout.feed_event(_notification_event())
                proc.stdout.feed_event(_init_event())
                proc.stdout.feed_event(
                    _assistant_text("mBG", "background findings"))
                proc.stdout.feed_event(_result_event("background findings"))
                self.assertTrue(
                    sender.wait_for(("text", "background findings")),
                    "unsolicited turn was not delivered immediately",
                )

                result_b = run_claude_streaming(
                    token="tok", chat_id="chatA", message="B",
                    session_id="sess-1", is_new=False,
                    send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
                )
                # THE regression assertion: B gets B', not the background
                # turn's leftovers.
                self.assertEqual(result_b.streamed_text, "B' reply")
                self.assertEqual(result_b.final_result, "B' reply")
        finally:
            proc.stdout.close()


class TestPumpRegistry(unittest.TestCase):
    def test_same_proc_same_pump(self):
        proc = _FakeProc()
        try:
            pump1 = get_or_create_pump(proc)
            pump2 = get_or_create_pump(proc)
            self.assertIs(pump1, pump2)
        finally:
            proc.stdout.close()
            pump1._thread.join(timeout=2)

    def test_different_procs_different_pumps(self):
        proc1, proc2 = _FakeProc(), _FakeProc()
        try:
            pump1 = get_or_create_pump(proc1)
            pump2 = get_or_create_pump(proc2)
            self.assertIsNot(pump1, pump2)
        finally:
            for proc, pump in ((proc1, pump1), (proc2, pump2)):
                proc.stdout.close()
                pump._thread.join(timeout=2)


class TestPrunedResumeSignals(_PumpTestCase):
    """Cluster 2 (stale-resume auto-recovery) — the pump records the terminal
    result event's error flag / subtype and whether an init opened the block.

    ``landline.claude_dispatch.looks_like_pruned_resume`` consumes those fields
    to catch the empirically-verified pruned/nonexistent --resume shape
    without false-positiving on a mid-session API error (which necessarily
    saw an init on this turn).
    """

    def test_pump_records_is_error_and_subtype_on_result(self):
        """Pruned-resume shape: NO init, single result event with
        is_error=true / subtype=error_during_execution. saw_init stays False."""
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        # The pruned shape emits ONLY a result event on this turn.
        self.proc.stdout.feed_event({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": True,
            "session_id": "pruned-sid",
        })

        self.assertTrue(handle.done.wait(2))
        self.assertTrue(handle.result_is_error)
        self.assertEqual(handle.result_subtype, "error_during_execution")
        self.assertFalse(handle.saw_init)

    def test_pump_saw_init_true_when_init_precedes_result(self):
        """Mid-session error shape: init THEN a result with is_error=true.
        saw_init must be True so the dispatcher can tell it apart from the
        pruned-resume shape."""
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event("sess-live"))
        self.proc.stdout.feed_event({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": True,
            "session_id": "sess-live",
        })

        self.assertTrue(handle.done.wait(2))
        self.assertTrue(handle.saw_init)
        self.assertTrue(handle.result_is_error)
        self.assertEqual(handle.result_subtype, "error_during_execution")


class TestUsageCapture(_PumpTestCase):
    """Cluster 4 (usage/cost stats) — the pump captures optional accounting
    fields from the terminal ``result`` event onto the TurnHandle so the
    streaming layer can surface them on the ClaudeStreamResult.
    """

    def test_pump_records_usage_fields_on_dispatched_result(self):
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event("sess-A"))
        self.proc.stdout.feed_event(_assistant_text("m1", "ok"))
        self.proc.stdout.feed_event({
            "type": "result",
            "subtype": "success",
            "result": "ok",
            "session_id": "sess-A",
            "usage": {
                "input_tokens": 111,
                "output_tokens": 222,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 4,
            },
            "modelUsage": {"claude-opus-4-8": {"input_tokens": 111,
                                                "output_tokens": 222}},
            "total_cost_usd": 0.0456,
            "num_turns": 7,
            "duration_ms": 1234,
        })

        self.assertTrue(handle.done.wait(2))
        self.assertEqual(handle.result_usage["input_tokens"], 111)
        self.assertEqual(handle.result_usage["output_tokens"], 222)
        self.assertEqual(
            handle.result_model_usage["claude-opus-4-8"]["input_tokens"], 111,
        )
        self.assertAlmostEqual(handle.result_total_cost_usd, 0.0456, places=6)
        self.assertEqual(handle.result_num_turns, 7)
        self.assertEqual(handle.result_duration_ms, 1234)

    def test_missing_usage_fields_leave_handle_defaults(self):
        # Regression: existing tests that assert on TurnHandle should keep
        # passing — all new fields default to None when a result event
        # doesn't carry them.
        sender = _FakeSender()
        handle = TurnHandle()
        self.pump.register_turn(handle, sender)

        self.proc.stdout.feed_event(_init_event())
        self.proc.stdout.feed_event(_assistant_text("m1", "hi"))
        self.proc.stdout.feed_event(_result_event("hi"))

        self.assertTrue(handle.done.wait(2))
        self.assertIsNone(handle.result_usage)
        self.assertIsNone(handle.result_model_usage)
        self.assertIsNone(handle.result_total_cost_usd)
        self.assertIsNone(handle.result_num_turns)
        self.assertIsNone(handle.result_duration_ms)

    def test_unsolicited_result_calls_usage_stats_record_turn(self):
        """Background-task completions consume tokens on the operator's Max plan.
        They must land in the daily aggregate tagged as unsolicited so the operator
        can distinguish 'my messages' cost from 'background stuff' cost."""
        chat_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=chat_sender), \
             patch("landline.usage_stats.record_turn") as mock_record:
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)

            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("mBG", "background"))
            self.proc.stdout.feed_event({
                "type": "result",
                "subtype": "success",
                "result": "background",
                "usage": {"input_tokens": 50, "output_tokens": 60},
                "modelUsage": {"claude-opus-4-8": {"input_tokens": 50,
                                                    "output_tokens": 60}},
                "total_cost_usd": 0.02,
                "duration_ms": 500,
            })
            self.assertTrue(chat_sender.wait_for(("text", "background")))

            # Wait briefly for the pump thread to finish _close_block.
            deadline = time.time() + 2
            while time.time() < deadline and not mock_record.called:
                time.sleep(0.01)

            self.assertTrue(mock_record.called)
            call_kwargs = mock_record.call_args.kwargs
            self.assertEqual(call_kwargs["dispatched"], False)
            self.assertEqual(call_kwargs["result_usage"]["input_tokens"], 50)
            self.assertAlmostEqual(
                call_kwargs["total_cost_usd"], 0.02, places=6,
            )
            self.assertEqual(call_kwargs["duration_ms"], 500)

    def test_unsolicited_usage_record_never_crashes_pump(self):
        """A broken usage_stats.record_turn must not kill the pump thread —
        the pump's routing job is load-bearing and stats are polish."""
        chat_sender = _FakeSender()
        with patch("landline.claude._get_or_create_sender",
                   return_value=chat_sender), \
             patch("landline.usage_stats.record_turn",
                   side_effect=RuntimeError("io simulated")):
            self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                     lambda *a: None)

            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("mBG", "background"))
            self.proc.stdout.feed_event(_result_event("background"))
            self.assertTrue(chat_sender.wait_for(("text", "background")))

            # Now dispatch a real turn — proves the pump thread is still alive.
            sender = _FakeSender()
            handle = TurnHandle()
            self.pump.register_turn(handle, sender)
            self.proc.stdout.feed_event(_init_event())
            self.proc.stdout.feed_event(_assistant_text("m1", "next"))
            self.proc.stdout.feed_event(_result_event("next"))
            self.assertTrue(handle.done.wait(2))

    def test_unsolicited_usage_record_is_async_from_pump(self):
        """PIN (Cluster 4 fsync-under-lock hardening): the pump thread must
        NOT block on ``usage_stats.record_turn`` — that call performs
        synchronous fsync inside a module-level lock, and a stall there
        would freeze the pump (violating the "stdout pipe has exactly one
        continuously-reading reader" invariant in ``CLAUDE.md``).

        Simulate an SSD stall by making record_turn block, then verify the
        pump processes a follow-up turn while record_turn is still stuck.
        """
        chat_sender = _FakeSender()
        record_started = threading.Event()
        release_record = threading.Event()

        def stuck_record(**kwargs):
            record_started.set()
            # Block as if fsync is wedged on a busy SSD.
            release_record.wait(timeout=5.0)

        try:
            with patch("landline.claude._get_or_create_sender",
                       return_value=chat_sender), \
                 patch("landline.usage_stats.record_turn",
                       side_effect=stuck_record):
                self.pump.set_idle_route("chat1", "tok", lambda *a: None,
                                         lambda *a: None)

                # 1) Feed an unsolicited turn — that triggers the stuck
                # record_turn call. If record_turn is called on the pump
                # thread, the pump wedges here.
                self.proc.stdout.feed_event(_init_event())
                self.proc.stdout.feed_event(_assistant_text("mBG", "bg"))
                self.proc.stdout.feed_event(_result_event("bg"))
                self.assertTrue(chat_sender.wait_for(("text", "bg")))

                # 2) Prove the pump can still deliver a real dispatched
                # turn WHILE record_turn is stuck. If the pump were blocked
                # on record_turn's lock, this would time out.
                self.assertTrue(
                    record_started.wait(timeout=2.0),
                    "record_turn was never invoked — pump routing broken",
                )
                sender = _FakeSender()
                handle = TurnHandle()
                self.pump.register_turn(handle, sender)
                self.proc.stdout.feed_event(_init_event())
                self.proc.stdout.feed_event(_assistant_text("m2", "next"))
                self.proc.stdout.feed_event(_result_event("next"))
                self.assertTrue(
                    handle.done.wait(3),
                    "pump did NOT process a follow-up turn while "
                    "record_turn was stuck — pump wedged on the usage "
                    "stats lock; async offload regressed",
                )
        finally:
            # Always release the background thread so it exits cleanly.
            release_record.set()


if __name__ == "__main__":
    unittest.main()
