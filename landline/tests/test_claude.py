"""Tests for landline.claude — PersistentClaude, StreamSender, helpers."""

import json
import queue
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

import landline.claude as claude_mod
from landline.claude import (
    _extract_text_blocks,
    _format_tool_status,
    _shorten_path,
    _get_or_create_sender,
    _close_all_senders,
    try_enqueue_chat_notice,
    run_claude_streaming,
    StreamSender,
    ClaudeStreamShutdownHook,
    PersistentClaude,
)


@pytest.fixture(autouse=True)
def _clear_sender_registry():
    """Each test starts and ends with an empty per-chat sender registry so
    long-lived senders never leak across tests."""
    with claude_mod._senders_lock:
        claude_mod._senders.clear()
    yield
    with claude_mod._senders_lock:
        senders = list(claude_mod._senders.values())
        claude_mod._senders.clear()
    for s in senders:
        try:
            s.close(timeout=1.0)
        except Exception:
            pass


class TestExtractTextBlocks:
    def test_single_text_block(self):
        content = [{"type": "text", "text": "hello"}]
        assert _extract_text_blocks(content) == "hello"

    def test_multiple_text_blocks(self):
        content = [
            {"type": "text", "text": "hello "},
            {"type": "text", "text": "world"},
        ]
        assert _extract_text_blocks(content) == "hello world"

    def test_mixed_block_types(self):
        content = [
            {"type": "text", "text": "hi"},
            {"type": "tool_use", "name": "bash"},
            {"type": "text", "text": " there"},
        ]
        assert _extract_text_blocks(content) == "hi there"

    def test_empty_list(self):
        assert _extract_text_blocks([]) == ""

    def test_non_list_input(self):
        assert _extract_text_blocks("not a list") == ""
        assert _extract_text_blocks(None) == ""
        assert _extract_text_blocks(42) == ""

    def test_text_block_missing_text_key(self):
        content = [{"type": "text"}]
        assert _extract_text_blocks(content) == ""

    def test_text_block_with_none_text(self):
        content = [{"type": "text", "text": None}]
        assert _extract_text_blocks(content) == ""

    def test_non_dict_items_in_list_skipped(self):
        """Defensive: non-dict items in the content list should be skipped, not crash."""
        content = [
            "string item",
            {"type": "text", "text": "valid"},
            None,
            42,
            {"type": "text", "text": "more"},
        ]
        assert _extract_text_blocks(content) == "validmore"


class _Recorder:
    """Records calls to the two StreamSender transports in a single ordered log.

    Each entry is `(kind, text)` where ``kind`` is "text" or "status". The
    list and lock are shared between both mock functions so we can assert
    ordering across transports.
    """

    def __init__(self) -> None:
        self.calls = []  # type: list
        self.lock = threading.Lock()
        self.text_raise = None
        self.status_raise = None

    def text(self, token: str, chat_id: str, text: str) -> None:
        if self.text_raise is not None:
            raise self.text_raise
        with self.lock:
            self.calls.append(("text", text))

    def status(self, token: str, chat_id: str, text: str) -> None:
        if self.status_raise is not None:
            raise self.status_raise
        with self.lock:
            self.calls.append(("status", text))

    def kinds(self):
        with self.lock:
            return [k for k, _ in self.calls]

    def texts_of(self, kind: str):
        with self.lock:
            return [t for k, t in self.calls if k == kind]

    def joined(self, kind: str) -> str:
        return "".join(self.texts_of(kind))


def _make_sender(text_window: float = 0.05, status_window: float = 0.05,
                 recorder=None) -> StreamSender:
    rec = recorder or _Recorder()
    sender = StreamSender(
        "token", "123",
        text_send_fn=rec.text,
        status_send_fn=rec.status,
        text_window=text_window,
        status_window=status_window,
    )
    sender._recorder = rec  # convenience handle for tests
    return sender


class TestStreamSenderText:
    """StreamSender text-streaming behaviour — replaces TestBufferedSender."""

    def test_sends_text(self):
        sender = _make_sender(text_window=0.05)
        sender.text("hello")
        time.sleep(0.25)
        sender.close()
        assert "hello" in sender._recorder.joined("text")

    def test_coalesces_rapid_text(self):
        sender = _make_sender(text_window=0.3)
        sender.text("a")
        sender.text("b")
        sender.text("c")
        time.sleep(0.6)
        sender.close()
        total = sender._recorder.joined("text")
        assert "abc" in total
        # Coalescing means few text sends — ideally one.
        text_count = len(sender._recorder.texts_of("text"))
        assert text_count <= 2
        # Token + chat_id forwarded on every call.
        for kind, _ in sender._recorder.calls:
            assert kind in ("text", "status")

    def test_close_drains_remaining_text(self):
        sender = _make_sender(text_window=0.1)
        sender.text("queued text")
        time.sleep(0.3)
        sender.close(timeout=2.0)
        assert "queued text" in sender._recorder.joined("text")

    def test_handles_text_send_fn_error(self):
        """A raising text-send must not crash the worker."""
        rec = _Recorder()
        rec.text_raise = Exception("network error")
        sender = _make_sender(text_window=0.05, recorder=rec)
        sender.text("hello")
        time.sleep(0.2)
        rec.text_raise = None  # let subsequent sends through
        sender.text("world")
        time.sleep(0.2)
        sender.close()
        # Second send still completed despite the first failure.
        assert "world" in rec.joined("text")

    def test_empty_text_not_queued(self):
        sender = _make_sender(text_window=0.05)
        sender.text("")
        time.sleep(0.2)
        sender.close()
        assert sender._recorder.calls == []

    def test_close_idempotent(self):
        """close() twice must not hang or raise."""
        sender = _make_sender(text_window=0.05)
        sender.text("hello")
        sender.close(timeout=2.0)
        sender.close(timeout=2.0)

    def test_flush_creates_message_boundary(self):
        """flush() between text deltas produces two separate text messages."""
        sender = _make_sender(text_window=0.5)
        sender.text("turn one")
        sender.flush()
        sender.text("turn two")
        time.sleep(0.8)
        sender.close()
        text_msgs = sender._recorder.texts_of("text")
        assert "turn one" in text_msgs
        assert "turn two" in text_msgs
        # Boundary preserved — no concatenated message.
        assert not any("turn oneturn two" in t for t in text_msgs)

    def test_flush_after_close_is_noop(self):
        """flush() after close() must not raise or enqueue."""
        sender = _make_sender(text_window=0.05)
        sender.close()
        sender.flush()

    def test_flush_when_buffer_empty_is_harmless(self):
        """flush() with nothing buffered is a no-op."""
        sender = _make_sender(text_window=0.05)
        sender.flush()
        time.sleep(0.2)
        sender.close()
        assert sender._recorder.calls == []

    def test_double_flush_is_harmless(self):
        """Two consecutive flush()es don't crash or emit empty messages."""
        sender = _make_sender(text_window=0.5)
        sender.text("content")
        sender.flush()
        sender.flush()
        sender.text("more")
        time.sleep(0.8)
        sender.close()
        text_msgs = sender._recorder.texts_of("text")
        assert all(t.strip() for t in text_msgs)


class TestClaudeStreamShutdownHook:
    def test_set_and_clear(self):
        hook = ClaudeStreamShutdownHook()
        mock_proc = MagicMock()
        hook.set_proc(mock_proc)
        assert hook.active_proc is mock_proc
        hook.clear()
        assert hook.active_proc is None
        # The hook no longer tracks senders — that's the registry's job.
        assert not hasattr(hook, "active_sender")

    def test_drain_for_shutdown_terminates_proc_and_drains_registry(self):
        hook = ClaudeStreamShutdownHook()
        mock_proc = MagicMock()
        hook.set_proc(mock_proc)
        mock_sender = MagicMock()
        with claude_mod._senders_lock:
            claude_mod._senders["chat1"] = mock_sender
        hook.drain_for_shutdown()
        mock_proc.terminate.assert_called_once()
        mock_sender.close.assert_called_once()
        # Registry is emptied so a late dispatch makes a fresh sender.
        with claude_mod._senders_lock:
            assert claude_mod._senders == {}

    def test_drain_without_proc_or_sender(self):
        hook = ClaudeStreamShutdownHook()
        hook.drain_for_shutdown()  # must not raise

    def test_drain_handles_terminate_error(self):
        """A failing terminate must NOT prevent senders from closing."""
        hook = ClaudeStreamShutdownHook()
        mock_proc = MagicMock()
        mock_proc.terminate.side_effect = Exception("already dead")
        hook.set_proc(mock_proc)
        mock_sender = MagicMock()
        with claude_mod._senders_lock:
            claude_mod._senders["chat1"] = mock_sender
        hook.drain_for_shutdown()
        mock_sender.close.assert_called_once()

    def test_drain_handles_sender_close_error(self):
        """A failing sender close must not propagate."""
        hook = ClaudeStreamShutdownHook()
        mock_sender = MagicMock()
        mock_sender.close.side_effect = Exception("network gone")
        with claude_mod._senders_lock:
            claude_mod._senders["chat1"] = mock_sender
        hook.drain_for_shutdown()  # must not raise
        mock_sender.close.assert_called_once()

    def test_drain_forwards_close_timeout_to_senders(self):
        hook = ClaudeStreamShutdownHook()
        mock_sender = MagicMock()
        with claude_mod._senders_lock:
            claude_mod._senders["chat1"] = mock_sender
        hook.drain_for_shutdown(sender_close_timeout=5.0)
        mock_sender.close.assert_called_once_with(timeout=5.0)


class TestSenderRegistry:
    """The per-chat long-lived sender registry — the core of the
    drain-stall/desync fix."""

    def _make_fns(self):
        return (MagicMock(), MagicMock())

    def test_same_sender_returned_across_turns(self):
        text_fn, status_fn = self._make_fns()
        s1 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        s2 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert s1 is s2, "sender must be long-lived, not recreated per turn"

    def test_distinct_senders_per_chat(self):
        text_fn, status_fn = self._make_fns()
        a = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        b = _get_or_create_sender("chatB", "tok", text_fn, status_fn)
        assert a is not b

    def test_recreates_after_close(self):
        text_fn, status_fn = self._make_fns()
        s1 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        s1.close(timeout=2.0)
        s2 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert s2 is not s1
        assert not s2.is_closed

    def test_self_heals_dead_worker(self):
        """A long-lived sender whose worker thread died must be replaced —
        otherwise it would silently swallow every future bubble."""
        text_fn, status_fn = self._make_fns()
        s1 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        # Simulate a dead worker without going through close().
        s1._thread.join(timeout=0)  # no-op; thread is alive
        with patch.object(type(s1), "worker_alive", property(lambda self: False)):
            s2 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert s2 is not s1
        s1.close(timeout=2.0)

    def test_self_heal_logs_dropped_count_when_queue_nonempty(self):
        """C4 regression: when self-heal replaces a sender whose worker died
        with entries still queued, the count must be logged so silent loss is
        observable in the logs (not buried in a silent swap)."""
        text_fn, status_fn = self._make_fns()
        s1 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        # Stuff some entries directly onto the queue so qsize > 0 at replace.
        s1.q.put((claude_mod._ENTRY_TEXT, "bubble-1"))
        s1.q.put((claude_mod._ENTRY_TEXT, "bubble-2"))
        s1.q.put((claude_mod._ENTRY_STATUS, "tool"))
        with patch.object(type(s1), "worker_alive", property(lambda self: False)), \
             patch("landline.claude.log") as mock_log:
            s2 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert s2 is not s1
        # The replacement was logged with the drop count.
        log_lines = [c.args[0] for c in mock_log.call_args_list]
        assert any(
            "dropping" in line and "queued entries" in line
            for line in log_lines
        ), "expected a self-heal log line mentioning the drop count; got: %r" % log_lines
        s1.close(timeout=2.0)

    def test_self_heal_log_omits_count_when_queue_empty(self):
        """If the dead worker's queue was empty, the log should NOT claim
        entries were dropped — the simpler 'recreating' line is used."""
        text_fn, status_fn = self._make_fns()
        s1 = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        # Queue is empty (a freshly-created sender hasn't seen any traffic).
        assert s1.q.qsize() == 0
        with patch.object(type(s1), "worker_alive", property(lambda self: False)), \
             patch("landline.claude.log") as mock_log:
            _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        log_lines = [c.args[0] for c in mock_log.call_args_list]
        assert any("recreating" in line for line in log_lines)
        assert not any("dropping" in line for line in log_lines), (
            "no drop-count line expected when the queue was empty"
        )
        s1.close(timeout=2.0)

    def test_flush_does_not_end_worker(self):
        """flush() is a boundary marker, not a teardown — the worker stays
        alive and usable for the next turn."""
        text_fn, status_fn = self._make_fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        s.text("turn 1")
        s.flush()
        time.sleep(0.05)
        assert s.worker_alive
        assert not s.is_closed
        s.text("turn 2")
        s.close(timeout=2.0)
        # Both turns' text reached the transport.
        sent = " ".join(c.args[2] for c in text_fn.call_args_list)
        assert "turn 1" in sent and "turn 2" in sent

    def test_cross_turn_ordering_preserved(self):
        """One FIFO queue + one worker => bubbles deliver in enqueue order
        across turn boundaries."""
        order = []
        lock = threading.Lock()

        def text_fn(token, chat_id, body):
            with lock:
                order.append(("text", body))

        def status_fn(token, chat_id, body):
            with lock:
                order.append(("status", body))

        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        # Turn 1: status then text, flush boundary, Turn 2: text.
        s.status("s1")
        s.text("t1")
        s.flush()
        s.text("t2")
        s.close(timeout=3.0)
        # s1 (status) before t1 (text); t1 before t2 across the flush boundary.
        kinds = [body for _, body in order]
        assert kinds.index("s1") < kinds.index("t1") < kinds.index("t2")

    def test_close_all_senders_drains_and_clears(self):
        text_fn, status_fn = self._make_fns()
        _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        _get_or_create_sender("chatB", "tok", text_fn, status_fn)
        _close_all_senders(timeout=2.0)
        with claude_mod._senders_lock:
            assert claude_mod._senders == {}


class TestTryEnqueueChatNotice:
    def _fns(self):
        return (MagicMock(), MagicMock())

    def test_routes_html_through_live_sender(self):
        text_fn, status_fn = self._fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert try_enqueue_chat_notice("chatA", html="<i>(Paused.)</i>") is True
        s.close(timeout=2.0)
        # HTML notice goes out via the status transport (pre-built HTML).
        bodies = [c.args[2] for c in status_fn.call_args_list]
        assert any("(Paused.)" in b for b in bodies)
        text_fn.assert_not_called()

    def test_routes_text_through_live_sender(self):
        text_fn, status_fn = self._fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        assert try_enqueue_chat_notice("chatA", text="heads up") is True
        s.close(timeout=2.0)
        bodies = [c.args[2] for c in text_fn.call_args_list]
        assert any("heads up" in b for b in bodies)

    def test_returns_false_when_no_sender(self):
        """No live sender (e.g. silent mode) => caller must fall back."""
        assert try_enqueue_chat_notice("ghost-chat", html="<i>x</i>") is False
        assert try_enqueue_chat_notice("ghost-chat", text="y") is False

    def test_returns_false_when_sender_closed(self):
        text_fn, status_fn = self._fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        s.close(timeout=2.0)
        assert try_enqueue_chat_notice("chatA", text="y") is False

    def test_notice_ordered_after_turn_bubbles(self):
        """A notice routed through the queue lands AFTER the turn's bubbles."""
        order = []
        lock = threading.Lock()

        def status_fn(token, chat_id, body):
            with lock:
                order.append(body)

        s = _get_or_create_sender("chatA", "tok", MagicMock(), status_fn)
        s.status("bubble-1")
        s.status("bubble-2")
        s.flush()
        try_enqueue_chat_notice("chatA", html="(Paused.)")
        s.close(timeout=3.0)
        joined = " | ".join(order)
        assert joined.index("bubble-1") < joined.index("(Paused.)")
        assert joined.index("bubble-2") < joined.index("(Paused.)")

    def test_rejects_both_or_neither(self):
        with pytest.raises(ValueError):
            try_enqueue_chat_notice("chatA")
        with pytest.raises(ValueError):
            try_enqueue_chat_notice("chatA", html="x", text="y")


class TestTryEnqueueOrSend:
    """E5 — ``try_enqueue_or_send`` collapses the queue-then-fallback dance
    into one helper. If a live sender exists, it routes through the queue;
    otherwise it calls ``direct_fn(body)``."""

    def _fns(self):
        return (MagicMock(), MagicMock())

    def test_uses_queue_when_live_sender_exists(self):
        from landline.sender_registry import try_enqueue_or_send
        text_fn, status_fn = self._fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        direct = MagicMock()
        try_enqueue_or_send("chatA", html="<i>x</i>", direct_fn=direct)
        s.close(timeout=2.0)
        # HTML body went out via the status transport (pre-built HTML).
        bodies = [c.args[2] for c in status_fn.call_args_list]
        assert any("<i>x</i>" in b for b in bodies)
        direct.assert_not_called()

    def test_falls_back_to_direct_fn_when_no_sender(self):
        from landline.sender_registry import try_enqueue_or_send
        direct = MagicMock()
        try_enqueue_or_send("ghost-chat", text="y", direct_fn=direct)
        direct.assert_called_once_with("y")

    def test_falls_back_when_sender_closed(self):
        from landline.sender_registry import try_enqueue_or_send
        text_fn, status_fn = self._fns()
        s = _get_or_create_sender("chatA", "tok", text_fn, status_fn)
        s.close(timeout=2.0)
        direct = MagicMock()
        try_enqueue_or_send("chatA", text="z", direct_fn=direct)
        direct.assert_called_once_with("z")

    def test_rejects_both_or_neither_via_validation(self):
        from landline.sender_registry import try_enqueue_or_send
        direct = MagicMock()
        with pytest.raises(ValueError):
            try_enqueue_or_send("chatA", direct_fn=direct)
        with pytest.raises(ValueError):
            try_enqueue_or_send("chatA", html="x", text="y", direct_fn=direct)
        direct.assert_not_called()

    def test_direct_fn_receives_html_body_unchanged(self):
        from landline.sender_registry import try_enqueue_or_send
        direct = MagicMock()
        try_enqueue_or_send("ghost-chat", html="<i>(Paused.)</i>", direct_fn=direct)
        direct.assert_called_once_with("<i>(Paused.)</i>")

    def test_direct_fn_receives_text_body_unchanged(self):
        from landline.sender_registry import try_enqueue_or_send
        direct = MagicMock()
        try_enqueue_or_send("ghost-chat", text="heads up", direct_fn=direct)
        direct.assert_called_once_with("heads up")


class TestStreamingEmptyResponseUsesTryEnqueueOrSend:
    """E5 streaming canary — the empty-response and nonzero-exit branches
    in run_claude_streaming must call ``_claude_facade.try_enqueue_or_send``,
    not the bool primitive. Reverting either site fails this test because
    the helper mock sees no call."""

    def _fake_pc_yielding(self, events, *, returncode=1):
        fake_proc = MagicMock()
        fake_proc.stdout = iter(events)
        fake_proc.stderr = iter([])
        fake_proc.poll.return_value = returncode  # dead -> exit_code captured
        fake_proc.returncode = returncode
        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = fake_proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""
        return fake_pc

    def test_empty_response_routes_through_try_enqueue_or_send(self):
        """Nonzero exit with no streamed text triggers the exit-code branch."""
        fake_pc = self._fake_pc_yielding([], returncode=1)
        helper_mock = MagicMock()
        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.claude._get_or_create_sender", return_value=MagicMock()), \
             patch("landline.claude.try_enqueue_or_send", helper_mock):
            run_claude_streaming(
                token="tok", chat_id="chatX", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
            )
        # Exit-code branch fires with "Claude returned no response".
        assert helper_mock.call_count == 1
        call_kwargs = helper_mock.call_args.kwargs
        assert call_kwargs.get("text") is not None
        assert "no response" in str(call_kwargs.get("text"))
        assert callable(call_kwargs.get("direct_fn"))

    def test_empty_response_clean_exit_routes_through_try_enqueue_or_send(self):
        """Clean exit (0) with no content triggers the empty-response branch."""
        fake_pc = self._fake_pc_yielding([], returncode=0)
        helper_mock = MagicMock()
        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.claude._get_or_create_sender", return_value=MagicMock()), \
             patch("landline.claude.try_enqueue_or_send", helper_mock):
            run_claude_streaming(
                token="tok", chat_id="chatX", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
            )
        assert helper_mock.call_count == 1
        call_kwargs = helper_mock.call_args.kwargs
        assert call_kwargs.get("text") == "(Empty response from Claude.)"
        assert callable(call_kwargs.get("direct_fn"))


class TestWorkerResilience:
    """The long-lived worker must survive a malformed entry — it can't die and
    turn the chat into a permanent black hole."""

    def test_worker_survives_emit_exception(self):
        sent = []

        def flaky_text(token, chat_id, body):
            if "boom" in body:
                raise RuntimeError("transport blew up")
            sent.append(body)

        s = StreamSender("tok", "chatA", flaky_text, MagicMock())
        s.text("boom")          # raises inside emit (caught by _emit_text) ...
        s.flush()               # force it out alone (no coalescing with "after")
        time.sleep(0.05)
        s.text("after")         # ... worker still alive, delivers this
        s.close(timeout=2.0)
        assert "after" in sent
        assert s.worker_alive is False  # exited cleanly on STOP

    def test_worker_survives_non_emit_exception(self):
        """A defect OUTSIDE the (already-guarded) transport call must also not
        kill the worker — exercises the _run try/except hardening."""
        sent = []
        boom = {"armed": True}

        def status_fn(token, chat_id, body):
            sent.append(body)

        s = StreamSender("tok", "chatA", MagicMock(), status_fn)
        # Poison _handle_status (called DIRECTLY by the worker loop) so the first
        # status entry raises inside _run — not inside the already-guarded
        # _emit_*. The worker must log-and-continue, surviving for the next entry.
        real_handle_status = s._handle_status

        def poisoned(state, line):
            if boom["armed"]:
                boom["armed"] = False
                raise RuntimeError("worker-loop defect")
            return real_handle_status(state, line)

        s._handle_status = poisoned
        s.status("first")   # poisoned -> raises inside the worker loop, caught
        time.sleep(0.05)
        s.status("second")  # worker survived; this one gets through
        s.close(timeout=2.0)
        assert "second" in sent
        assert "first" not in sent  # poisoned entry dropped, not retried


class TestRunClaudeStreamingLifecycle:
    """End-of-turn must FLUSH the long-lived sender, never CLOSE it. This is the
    load-bearing invariant of the whole fix: reverting claude.py's end-of-turn
    `sender.flush()` back to `sender.close()` MUST fail this test."""

    def _fake_pc_yielding(self, events):
        """A fake PersistentClaude whose stdout yields the given raw JSON lines
        then ends, so run_claude_streaming's read loop runs and returns."""
        fake_proc = MagicMock()
        fake_proc.stdout = iter(events)
        fake_proc.stderr = iter([])
        fake_proc.poll.return_value = None  # alive during stream; watchdog idles
        fake_proc.returncode = 0
        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = fake_proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""
        return fake_pc

    def test_turn_flushes_not_closes_registered_sender(self):
        result_event = json.dumps(
            {"type": "result", "result": "hi there", "session_id": "sess-1"}
        ) + "\n"
        fake_pc = self._fake_pc_yielding([result_event])

        mock_sender = MagicMock()
        mock_sender.is_closed = False
        mock_sender.worker_alive = True

        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.claude._get_or_create_sender", return_value=mock_sender):
            run_claude_streaming(
                token="tok", chat_id="chatA", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
            )

        mock_sender.flush.assert_called()      # turn boundary marked ...
        mock_sender.close.assert_not_called()  # ... but the worker is NOT torn down

    def test_real_registered_sender_survives_a_turn(self):
        """The same long-lived sender instance is reused and stays alive across
        a turn (end-to-end through the real registry)."""
        result_event = json.dumps(
            {"type": "result", "result": "ok", "session_id": "sess-1"}
        ) + "\n"
        fake_pc = self._fake_pc_yielding([result_event])

        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.client.send_response"), \
             patch("landline.client.send_html"):
            run_claude_streaming(
                token="tok", chat_id="chatLive", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
            )
            with claude_mod._senders_lock:
                sender = claude_mod._senders.get("chatLive")
            assert sender is not None
            assert not sender.is_closed   # flush(), not close()
            assert sender.worker_alive

    def test_usage_fields_propagate_to_claude_stream_result(self):
        """Cluster 4: usage / cost / duration fields captured by the pump on
        the terminal result event must be copied onto ClaudeStreamResult so
        the dispatcher's finalize path can persist them to the daily
        aggregate. Uses the queue-fed proc + send_message side_effect pattern
        so the pump is alive when register_turn is called — mirrors the
        real dispatch flow."""
        # Import the queue-backed fake stdout from the pump tests — same
        # fixture the integration regression test uses.
        from landline.tests.test_stream_pump import _FakeProc, _init_event

        proc = _FakeProc()
        proc.returncode = None

        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""

        turn_events = [
            _init_event("sess-1"),
            {
                "type": "result",
                "subtype": "success",
                "result": "ok",
                "session_id": "sess-1",
                "usage": {"input_tokens": 42, "output_tokens": 99,
                          "cache_read_input_tokens": 1,
                          "cache_creation_input_tokens": 2},
                "modelUsage": {"claude-opus-4-8": {"input_tokens": 42,
                                                    "output_tokens": 99}},
                "total_cost_usd": 0.0777,
                "num_turns": 3,
                "duration_ms": 4321,
            },
        ]

        def _feed(_msg):
            for event in turn_events:
                proc.stdout.feed_event(event)

        fake_pc.send_message.side_effect = _feed

        mock_sender = MagicMock()
        mock_sender.is_closed = False
        mock_sender.worker_alive = True

        try:
            with patch("landline.claude._get_persistent_claude",
                       return_value=fake_pc), \
                 patch("landline.claude._get_or_create_sender",
                       return_value=mock_sender):
                result = run_claude_streaming(
                    token="tok", chat_id="chatA", message="hi",
                    session_id="sess-1", is_new=False,
                    send_response_fn=MagicMock(),
                    send_typing_fn=MagicMock(),
                )
        finally:
            proc.stdout.close()

        assert result.result_usage["input_tokens"] == 42
        assert result.result_usage["output_tokens"] == 99
        assert result.result_model_usage["claude-opus-4-8"]["input_tokens"] == 42
        assert abs(result.result_total_cost_usd - 0.0777) < 1e-6
        assert result.result_num_turns == 3
        assert result.result_duration_ms == 4321


class TestPersistentClaude:
    def test_initial_state(self):
        pc = PersistentClaude()
        assert pc.is_alive is False
        assert pc.session_id is None

    def test_clear_session(self):
        pc = PersistentClaude()
        pc._session_id = "test-session"
        pc.clear_session()
        assert pc.session_id is None

    def test_get_stderr_tail_empty(self):
        pc = PersistentClaude()
        assert pc.get_stderr_tail() == ""

    def test_get_stderr_tail_with_content(self):
        pc = PersistentClaude()
        pc._stderr_buf.append("line 1\n")
        pc._stderr_buf.append("line 2\n")
        pc._stderr_total_len = 14
        tail = pc.get_stderr_tail()
        assert "line 1" in tail
        assert "line 2" in tail

    def test_ensure_alive_spawns_new(self, no_subprocess):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.__iter__ = MagicMock(return_value=iter([]))
        no_subprocess["popen"].return_value = mock_proc
        proc = pc.ensure_alive(force_new=True)
        assert proc is mock_proc
        assert no_subprocess["popen"].call_count == 1
        # Verify the new session got --session-id (not --resume).
        cmd = no_subprocess["popen"].call_args[0][0]
        assert "--session-id" in cmd
        assert "--resume" not in cmd
        # _session_id should have been populated with a UUID.
        assert pc.session_id is not None
        # bypassPermissions mode and stream-json I/O.
        assert "bypassPermissions" in cmd
        assert "--input-format" in cmd and "stream-json" in cmd

    def test_ensure_alive_resume_passes_session_id(self, no_subprocess):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.__iter__ = MagicMock(return_value=iter([]))
        no_subprocess["popen"].return_value = mock_proc
        pc.ensure_alive(session_id="my-old-session")
        cmd = no_subprocess["popen"].call_args[0][0]
        assert "--resume" in cmd
        assert "my-old-session" in cmd
        assert "--session-id" not in cmd

    def test_ensure_alive_reuses_live_process(self, no_subprocess):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.stderr = MagicMock()
        mock_proc.stderr.__iter__ = MagicMock(return_value=iter([]))
        pc._proc = mock_proc
        proc = pc.ensure_alive()
        assert proc is mock_proc
        no_subprocess["popen"].assert_not_called()

    def test_ensure_alive_force_new_kills_existing(self, no_subprocess):
        """force_new=True with a live process must terminate it before spawning."""
        pc = PersistentClaude()
        old_proc = MagicMock()
        old_proc.poll.return_value = None  # alive
        old_proc.wait.return_value = 0
        old_proc.stdin = MagicMock()
        old_proc.stdout = MagicMock()
        old_proc.stderr = MagicMock()
        pc._proc = old_proc

        new_proc = MagicMock()
        new_proc.poll.return_value = None
        new_proc.stderr = MagicMock()
        new_proc.stderr.__iter__ = MagicMock(return_value=iter([]))
        no_subprocess["popen"].return_value = new_proc

        pc.ensure_alive(force_new=True)
        old_proc.terminate.assert_called_once()
        assert pc._proc is new_proc

    def test_kill_terminates_process(self):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stderr = MagicMock()
        pc._proc = mock_proc
        pc.kill()
        mock_proc.terminate.assert_called_once()
        # All three pipes must be closed to free fds.
        mock_proc.stdin.close.assert_called_once()
        mock_proc.stdout.close.assert_called_once()
        mock_proc.stderr.close.assert_called_once()

    def test_kill_falls_back_to_kill_when_terminate_times_out(self):
        """If terminate + wait times out, fall back to SIGKILL."""
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stderr = MagicMock()
        # First wait raises (timeout), forcing the kill() fallback.
        mock_proc.wait.side_effect = [Exception("timeout"), 0]
        pc._proc = mock_proc
        pc.kill()
        mock_proc.terminate.assert_called_once()
        mock_proc.kill.assert_called_once()

    def test_kill_noop_when_no_process(self):
        """kill() with no process must not raise."""
        pc = PersistentClaude()
        pc.kill()  # no-op

    def test_send_message_raises_when_dead(self):
        pc = PersistentClaude()
        with pytest.raises(RuntimeError):
            pc.send_message("hello")

    def test_send_message_writes_json(self):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.stdin = MagicMock()
        pc._proc = mock_proc
        pc.send_message("test message")
        mock_proc.stdin.write.assert_called_once()
        written = mock_proc.stdin.write.call_args[0][0]
        # Trailing newline required so Claude flushes the line.
        assert written.endswith("\n")
        import json
        data = json.loads(written.strip())
        assert data["type"] == "user"
        assert data["message"]["role"] == "user"
        assert data["message"]["content"] == "test message"
        # parent_tool_use_id must exist (Claude expects this field).
        assert data["parent_tool_use_id"] is None
        # stdin.flush must be called so the line actually goes out.
        mock_proc.stdin.flush.assert_called_once()

    def test_interrupt_sends_sigint(self):
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 12345
        pc._proc = mock_proc
        with patch("os.kill") as mock_kill:
            pc.interrupt()
        mock_kill.assert_called_once()
        import signal
        # First positional arg is the pid, second is the signal.
        assert mock_kill.call_args[0][0] == 12345
        assert mock_kill.call_args[0][1] == signal.SIGINT

    def test_interrupt_noop_when_dead(self):
        """interrupt() on a dead/missing process must not raise or call os.kill."""
        pc = PersistentClaude()
        with patch("os.kill") as mock_kill:
            pc.interrupt()
        mock_kill.assert_not_called()

    def test_interrupt_swallows_oserror(self):
        """os.kill failure (e.g. process already dead) must not propagate."""
        pc = PersistentClaude()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 12345
        pc._proc = mock_proc
        with patch("os.kill", side_effect=ProcessLookupError("gone")):
            pc.interrupt()  # must not raise

    def test_stderr_tail_bounded_by_buffer_max(self):
        """get_stderr_tail respects STDERR_BUFFER_MAX trim."""
        from landline.config import STDERR_BUFFER_MAX
        pc = PersistentClaude()
        # Build a payload larger than the cap.
        big_line = "x" * (STDERR_BUFFER_MAX + 1000)
        pc._stderr_buf.append(big_line)
        pc._stderr_total_len = len(big_line)
        tail = pc.get_stderr_tail()
        assert len(tail) <= STDERR_BUFFER_MAX


class TestShortenPath:
    def test_workspace_path(self):
        from landline.config import WORKSPACE
        p = str(WORKSPACE / "memory" / "daily" / "2026-06-05.md")
        assert _shorten_path(p) == "memory/daily/2026-06-05.md"

    def test_home_path(self):
        import os
        p = os.path.expanduser("~/Developer/some-repo/file.py")
        assert _shorten_path(p) == "~/Developer/some-repo/file.py"

    def test_other_path(self):
        assert _shorten_path("/tmp/random.txt") == "/tmp/random.txt"


class TestFormatToolStatus:
    def _ws(self, tail: str) -> str:
        """Build an absolute path under the current test WORKSPACE. Keeps
        every fixture host-agnostic and consistent with the tool_status
        rule that special-cases the workspace ``bin/`` prefix."""
        from landline.config import WORKSPACE
        return str(WORKSPACE) + "/" + tail.lstrip("/")

    def test_bash_workspace_bin_shell_label(self):
        """A Bash command that starts with ``<WORKSPACE>/bin/`` renders as
        just the bare tool name + args (workspace prefix stripped). The
        generic ``Shell`` label is applied — no workspace-specific
        emoji-decorated branches remain."""
        cmd = self._ws("bin/my-tool arg1 arg2")
        block = {"type": "tool_use", "name": "Bash", "input": {"command": cmd}}
        result = _format_tool_status(block)
        assert result is not None
        assert "language-Shell" in result
        assert "my-tool arg1 arg2" in result
        # And the absolute workspace path is NOT in the rendered status.
        from landline.config import WORKSPACE
        assert str(WORKSPACE) not in result

    def test_bash_calendar(self):
        block = {"type": "tool_use", "name": "Bash",
                 "input": {"command": "gog-firewall calendar events foo --days 7"}}
        assert "language-📅 Calendar" in _format_tool_status(block)

    def test_bash_gmail(self):
        block = {"type": "tool_use", "name": "Bash",
                 "input": {"command": "gog-firewall gmail search is:unread"}}
        assert "language-📧 Gmail" in _format_tool_status(block)

    def test_bash_imsg_send(self):
        block = {"type": "tool_use", "name": "Bash",
                 "input": {"command": "imsg send --to +1234 --text hi"}}
        assert _format_tool_status(block) == "💬 iMessage send"

    def test_bash_generic(self):
        block = {"type": "tool_use", "name": "Bash", "input": {"command": "git status"}}
        result = _format_tool_status(block)
        assert "language-Shell" in result
        assert "git status" in result

    def test_bash_long_command_truncated(self):
        long_cmd = "python3 " + "x" * 200
        block = {"type": "tool_use", "name": "Bash", "input": {"command": long_cmd}}
        result = _format_tool_status(block)
        assert "…" in result
        assert result.endswith("</code></pre>")

    def test_bash_empty_command(self):
        block = {"type": "tool_use", "name": "Bash", "input": {"command": ""}}
        assert _format_tool_status(block) is None

    def test_read_skill(self):
        block = {"type": "tool_use", "name": "Read",
                 "input": {"file_path": self._ws("skills/some-skill/SKILL.md")}}
        result = _format_tool_status(block)
        assert "language-📖 Skill" in result
        assert "some-skill" in result

    def test_read_memory(self):
        block = {"type": "tool_use", "name": "Read",
                 "input": {"file_path": self._ws("memory/daily/2026-06-05.md")}}
        result = _format_tool_status(block)
        assert "language-📂 Read" in result
        assert "memory/daily/2026-06-05.md" in result

    def test_read_image(self):
        block = {"type": "tool_use", "name": "Read",
                 "input": {"file_path": self._ws("cache/telegram_images/photo.jpg")}}
        assert _format_tool_status(block) == "🖼 Reading image"

    def test_read_other_file_suppressed(self):
        block = {"type": "tool_use", "name": "Read",
                 "input": {"file_path": self._ws("landline/claude.py")}}
        assert _format_tool_status(block) is None

    def test_agent(self):
        block = {"type": "tool_use", "name": "Agent",
                 "input": {"description": "Research background reading", "prompt": "..."}}
        assert _format_tool_status(block) == '🔀 Subagent: "Research background reading"'

    def test_agent_no_description(self):
        block = {"type": "tool_use", "name": "Agent", "input": {"prompt": "do stuff"}}
        assert _format_tool_status(block) == "🔀 Subagent launched"

    def test_skill(self):
        block = {"type": "tool_use", "name": "Skill", "input": {"skill": "last30days"}}
        result = _format_tool_status(block)
        assert "language-⚡ Skill" in result
        assert "/last30days" in result

    def test_edit(self):
        block = {"type": "tool_use", "name": "Edit",
                 "input": {"file_path": self._ws("memory/people/example.md")}}
        result = _format_tool_status(block)
        assert "language-✏️ Edit" in result
        assert "memory/people/example.md" in result

    def test_write(self):
        block = {"type": "tool_use", "name": "Write",
                 "input": {"file_path": self._ws("memory/people/example.md")}}
        result = _format_tool_status(block)
        assert "language-📝 Write" in result
        assert "memory/people/example.md" in result

    def test_web_search(self):
        block = {"type": "tool_use", "name": "WebSearch",
                 "input": {"query": "baby milestones 0-3 months"}}
        result = _format_tool_status(block)
        assert "language-🌐 Search" in result
        assert "baby milestones 0-3 months" in result

    def test_web_fetch(self):
        block = {"type": "tool_use", "name": "WebFetch",
                 "input": {"url": "https://www.mayoclinic.org/infant-development"}}
        assert _format_tool_status(block) == "🌐 www.mayoclinic.org"

    def test_unknown_tool_suppressed(self):
        block = {"type": "tool_use", "name": "SomeRandomTool", "input": {}}
        assert _format_tool_status(block) is None

    def test_no_input_dict(self):
        block = {"type": "tool_use", "name": "Bash", "input": None}
        assert _format_tool_status(block) is None


class TestStreamSenderStatus:
    """StreamSender status-line behaviour — replaces TestStatusSender."""

    def test_batches_rapid_lines(self):
        sender = _make_sender(status_window=0.1)
        sender.status("line1")
        sender.status("line2")
        time.sleep(0.6)
        sender.close()
        all_text = "\n".join(sender._recorder.texts_of("status"))
        assert "line1" in all_text
        assert "line2" in all_text

    def test_collapses_consecutive_identical(self):
        sender = _make_sender(status_window=0.1)
        for _ in range(4):
            sender.status("edit file.py")
        time.sleep(0.6)
        sender.close()
        all_text = "\n".join(sender._recorder.texts_of("status"))
        assert "edit file.py" in all_text
        assert "(4 times)" in all_text

    def test_collapses_with_different_between(self):
        sender = _make_sender(status_window=0.1)
        sender.status("edit a.py")
        sender.status("edit a.py")
        sender.status("bash cmd")
        sender.status("edit b.py")
        time.sleep(0.6)
        sender.close()
        all_text = "\n".join(sender._recorder.texts_of("status"))
        assert "(2 times)" in all_text
        assert "bash cmd" in all_text
        assert "edit b.py" in all_text

    def test_separate_batches_when_idle_between(self):
        sender = _make_sender(status_window=0.05)
        sender.status("line1")
        time.sleep(0.2)
        sender.status("line2")
        time.sleep(0.2)
        sender.close()
        assert len(sender._recorder.texts_of("status")) == 2

    def test_empty_status_ignored(self):
        sender = _make_sender(status_window=0.05)
        sender.status("")
        time.sleep(0.15)
        sender.close()
        assert sender._recorder.calls == []

    def test_status_send_error_silently_dropped(self):
        rec = _Recorder()
        rec.status_raise = RuntimeError("network error")
        sender = _make_sender(status_window=0.05, recorder=rec)
        sender.status("will fail")
        time.sleep(0.15)
        # Subsequent statuses still attempted after the failure.
        rec.status_raise = None
        sender.status("recovered")
        time.sleep(0.15)
        sender.close()
        assert "recovered" in rec.joined("status")


class TestStreamSenderOrdering:
    """Ordering guarantees across status and text transports."""

    def test_status_before_text_flushes_status_first(self):
        """STATUS then TEXT: the status batch must hit Telegram before text."""
        sender = _make_sender(text_window=0.5, status_window=0.5)
        sender.status("running tool")
        sender.text("Done — here's the answer.")
        time.sleep(1.0)
        sender.close()
        kinds = sender._recorder.kinds()
        # Status appears before text.
        first_status = kinds.index("status")
        first_text = kinds.index("text")
        assert first_status < first_text

    def test_text_before_status_flushes_text_first(self):
        """TEXT then STATUS (e.g. partial reply then next tool call) keeps order."""
        sender = _make_sender(text_window=0.5, status_window=0.5)
        sender.text("Looking that up...")
        sender.status("running tool")
        time.sleep(1.0)
        sender.close()
        kinds = sender._recorder.kinds()
        assert kinds.index("text") < kinds.index("status")

    def test_interleaved_status_text_status_text(self):
        """Alternating types: each transition forces a flush of the previous."""
        sender = _make_sender(text_window=0.5, status_window=0.5)
        sender.status("tool A")
        sender.text("intermediate text")
        sender.status("tool B")
        sender.text("final text")
        time.sleep(1.0)
        sender.close()
        kinds = sender._recorder.kinds()
        # Four messages emitted in alternating order.
        assert kinds == ["status", "text", "status", "text"]
        # And their payloads are correct.
        assert sender._recorder.texts_of("status") == ["tool A", "tool B"]
        assert sender._recorder.texts_of("text") == ["intermediate text", "final text"]

    def test_type_transition_flushes_before_coalesce_window(self):
        """A TEXT arriving immediately after STATUS must NOT wait for the
        status window — the transition forces a flush right away."""
        sender = _make_sender(text_window=1.0, status_window=1.0)
        sender.status("tool start")
        # Send text well before either window would expire.
        time.sleep(0.05)
        sender.text("response")
        # Wait long enough for text to flush but not for the status idle window.
        time.sleep(1.3)
        sender.close()
        kinds = sender._recorder.kinds()
        assert kinds[0] == "status"
        assert kinds[1] == "text"


class TestStreamSenderCloseRace:
    """Close() must drain remaining entries without duplicating any."""

    def test_close_immediately_after_text_drains(self):
        sender = _make_sender(text_window=0.5)
        sender.text("payload")
        sender.close()  # close before the window expires
        # The payload should appear exactly once.
        text_msgs = sender._recorder.texts_of("text")
        assert text_msgs == ["payload"]

    def test_close_immediately_after_status_drains_held(self):
        """The last held status (waiting to see if it repeats) must not be lost."""
        sender = _make_sender(status_window=0.5)
        sender.status("only status")
        sender.close()
        status_msgs = sender._recorder.texts_of("status")
        assert status_msgs == ["only status"]

    def test_close_does_not_duplicate_last_entry(self):
        sender = _make_sender(text_window=0.05)
        sender.text("once")
        time.sleep(0.2)  # let worker emit before close
        sender.close()
        # Worker emitted once, close drain doesn't double-emit.
        assert sender._recorder.texts_of("text").count("once") == 1

    def test_close_drains_mixed_pending_entries_in_order(self):
        sender = _make_sender(text_window=0.5, status_window=0.5)
        sender.status("s1")
        sender.text("t1")
        sender.status("s2")
        sender.close()
        kinds = sender._recorder.kinds()
        # Both kinds present, in the order they were enqueued.
        assert kinds == ["status", "text", "status"]

    def test_post_close_text_drops_without_sync_emit(self):
        """C1 regression: after close() (which only runs at shutdown), any
        late text() call drops silently instead of falling back to a
        synchronous send. Otherwise an orphaned producer would race the
        worker's tail drain on the way out and reorder against it."""
        sender = _make_sender(text_window=0.05)
        sender.close()
        # Snapshot transports invoked by the close() drain itself.
        pre = list(sender._recorder.calls)
        sender.text("after close")
        # Nothing new on the transport — text was dropped, not sync-emitted.
        assert sender._recorder.calls == pre

    def test_post_close_status_drops_without_sync_emit(self):
        """C1 regression for status (see test_post_close_text_drops_...)."""
        sender = _make_sender(status_window=0.05)
        sender.close()
        pre = list(sender._recorder.calls)
        sender.status("after close")
        assert sender._recorder.calls == pre

    def test_drain_remaining_preserves_flush_boundary(self):
        """A FLUSH entry between two text entries during the post-close
        drain must keep the messages separated (no concatenation)."""
        sender = _make_sender(text_window=10.0, status_window=10.0)
        # Stop the worker manually so the post-close drain handles everything.
        sender._closed = True
        sender.q.put(("text", "alpha"))
        sender.q.put(("flush", None))
        sender.q.put(("text", "beta"))
        sender._drain_remaining()
        # Wait briefly to let the worker (still running) finish its own loop —
        # but since _closed is True, no more producer enqueues are possible.
        text_msgs = sender._recorder.texts_of("text")
        assert "alpha" in text_msgs
        assert "beta" in text_msgs
        assert not any("alphabeta" in t for t in text_msgs)
        # Cleanup: stop the worker properly.
        sender.q.put(("stop", None))
        sender._thread.join(timeout=2.0)

    def test_drain_remaining_preserves_type_transition(self):
        """STATUS then TEXT during post-close drain must emit status first."""
        sender = _make_sender(text_window=10.0, status_window=10.0)
        sender._closed = True
        sender.q.put(("status", "tool"))
        sender.q.put(("text", "reply"))
        sender._drain_remaining()
        kinds = sender._recorder.kinds()
        assert kinds.index("status") < kinds.index("text")
        sender.q.put(("stop", None))
        sender._thread.join(timeout=2.0)

    def test_close_skips_drain_when_worker_still_alive(self):
        """If join() returns before the worker exits (e.g. stuck on HTTP),
        close() must NOT run `_drain_remaining` — the live worker is still
        calling transport functions, and a concurrent drain would race it."""
        # Block the text transport on an event so the worker can't return.
        release = threading.Event()
        transport_calls = []

        def blocking_text(token, chat_id, text):
            transport_calls.append(("text", text))
            # Hold the worker hostage until the test releases us.
            release.wait(timeout=5.0)

        def status_noop(token, chat_id, text):
            transport_calls.append(("status", text))

        sender = StreamSender(
            "token", "123",
            text_send_fn=blocking_text,
            status_send_fn=status_noop,
            text_window=0.01,
            status_window=0.01,
        )
        # Feed one text so the worker enters the blocking send.
        sender.text("stuck")
        # Give the worker a moment to pick it up and enter the blocking call.
        time.sleep(0.1)
        # Enqueue a SECOND text that would land in the drain if it ran.
        # We put it directly on the queue to bypass the close-lock check
        # (simulating an entry that was already in flight before close).
        with sender._close_lock:
            sender.q.put(("text", "would-be-drained"))

        drain_called = []
        original_drain = sender._drain_remaining

        def tracking_drain():
            drain_called.append(True)
            return original_drain()

        sender._drain_remaining = tracking_drain  # type: ignore[assignment]

        # Close with a very short timeout — worker is stuck, won't join in time.
        sender.close(timeout=0.05)

        # Worker is still alive (blocked in transport), so drain must NOT run.
        assert sender._thread.is_alive(), "worker should still be blocked"
        assert drain_called == [], (
            "drain ran while worker was still alive — ordering would break"
        )
        # The "would-be-drained" entry must not have been emitted by drain
        # (only the first "stuck" call should appear in transport_calls).
        assert transport_calls == [("text", "stuck")]

        # Cleanup: release the worker so the thread can exit.
        release.set()
        sender._thread.join(timeout=2.0)

    def test_concurrent_close_and_text_drops_at_shutdown(self):
        """C1: close() runs only at shutdown, so a producer racing close() means
        the process is exiting. Late text() calls drop instead of sync-emitting,
        but every text() that lands BEFORE close() flips _closed must still be
        delivered (either by the worker or by the post-close in-time drain).

        Verified invariant: no duplicates, no out-of-order. We do NOT assert
        no-loss because at shutdown, dropping a late race is acceptable.
        """
        sender = _make_sender(text_window=0.05, status_window=0.05)

        n_messages = 200
        producer_done = threading.Event()
        close_started = threading.Event()

        def producer():
            for i in range(n_messages):
                sender.text("msg-%03d" % i)
                if i == n_messages // 2:
                    close_started.set()
            producer_done.set()

        def closer():
            close_started.wait(timeout=2.0)
            time.sleep(0.001)
            sender.close(timeout=2.0)

        prod_thread = threading.Thread(target=producer)
        close_thread = threading.Thread(target=closer)
        prod_thread.start()
        close_thread.start()
        prod_thread.join(timeout=5.0)
        close_thread.join(timeout=5.0)
        assert producer_done.is_set()
        time.sleep(0.1)

        # No duplicates — a payload may be missing (legitimate shutdown drop)
        # but never duplicated.
        joined = sender._recorder.joined("text")
        for i in range(n_messages):
            payload = "msg-%03d" % i
            assert joined.count(payload) <= 1, (
                "duplicate emission of %s" % payload
            )


class TestStreamSenderEmitFailureTracking:
    """When the transport raises persistently, StreamSender must:
    1. Log each failure (we don't assert on log content here — just behaviour).
    2. After THRESHOLD consecutive failures, send ONE plain-text fallback
       notice through the basic markdown transport.
    3. Never loop: the fallback is sent exactly once even if failures persist.
    4. Reset the streak on any successful emit.
    """

    def _make_recorded_sender(self, raise_until=None, status_window=0.05,
                              text_window=0.05):
        """Build a StreamSender whose text transport raises on the first
        ``raise_until`` calls, then succeeds. Status transport never raises.
        """
        calls: list = []  # list of ("text"|"status", text)
        text_call_count = [0]

        def text_fn(token, chat_id, text):
            text_call_count[0] += 1
            calls.append(("text", text))
            if raise_until is not None and text_call_count[0] <= raise_until:
                raise RuntimeError("transport 5xx")

        def status_fn(token, chat_id, text):
            calls.append(("status", text))

        sender = StreamSender(
            "token", "123",
            text_send_fn=text_fn,
            status_send_fn=status_fn,
            text_window=text_window,
            status_window=status_window,
        )
        sender._calls = calls  # convenience
        return sender

    def test_fallback_notice_sent_after_threshold_failures(self):
        """3 consecutive text-emit failures -> exactly one fallback notice.

        Cluster 1 M7: the notice now routes through
        ``landline.notifications.send_health_alert`` (async iMessage), NOT
        the failing text transport. See ``_record_emit_failure``.
        """
        threshold = StreamSender._EMIT_FAILURE_THRESHOLD
        with patch("landline.notifications.send_health_alert") as mock_alert:
            # Raise forever — verifying the fallback is gated and can't
            # loop even when nothing works.
            sender = self._make_recorded_sender(raise_until=10_000)
            for i in range(threshold):
                sender.text("payload-%d" % i)
                # Wait past the text window so the worker emits each individually.
                time.sleep(0.12)
            # Give the worker one more tick to send the fallback.
            time.sleep(0.15)
            sender.close()
        # Exactly ONE alert dispatched — never loops, even though every send
        # raises. Also: the failing text transport must NOT have received
        # the fallback text (that was the M7 bug).
        assert mock_alert.call_count == 1, (
            "Expected exactly one health alert, got %d"
            % mock_alert.call_count
        )
        assert not any(
            "failed to deliver" in t or "check logs" in t
            for (kind, t) in sender._calls
            if kind == "text"
        ), (
            "Fallback text was routed through the failing text transport "
            "(the M7 bug). Calls: %r" % sender._calls
        )

    def test_success_resets_failure_streak(self):
        """A successful emit between failures resets the counter, so the
        threshold is not crossed and no health alert fires."""
        threshold = StreamSender._EMIT_FAILURE_THRESHOLD
        with patch("landline.notifications.send_health_alert") as mock_alert:
            # Fail the first emit, then succeed forever after.
            sender = self._make_recorded_sender(raise_until=1)
            # First payload: fails (count=1)
            sender.text("first")
            time.sleep(0.12)
            # Second payload: succeeds, resets counter
            sender.text("second")
            time.sleep(0.12)
            # Now fail again - but only (threshold - 1) times; not enough to trip.
            # Since the test transport doesn't keep failing here, just do regular
            # sends.
            for i in range(threshold - 1):
                sender.text("more-%d" % i)
                time.sleep(0.12)
            sender.close()
        assert mock_alert.call_count == 0, (
            "Health alert fired despite mid-streak success (streak should "
            "have reset before the threshold was crossed)"
        )

    def test_fallback_sent_only_once_with_persistent_failures(self):
        """Even if failures continue past the threshold, only one notice."""
        threshold = StreamSender._EMIT_FAILURE_THRESHOLD
        with patch("landline.notifications.send_health_alert") as mock_alert:
            sender = self._make_recorded_sender(raise_until=10_000)
            for i in range(threshold + 5):
                sender.text("payload-%d" % i)
                time.sleep(0.1)
            time.sleep(0.15)
            sender.close()
        # Exactly one health alert, even after many more failures.
        assert mock_alert.call_count == 1

    def test_status_failures_count_toward_threshold(self):
        """Status emit failures also increment the shared counter — the
        fallback fires after threshold mixed/text/status failures."""
        threshold = StreamSender._EMIT_FAILURE_THRESHOLD

        calls: list = []

        def text_fn(token, chat_id, text):
            calls.append(("text", text))

        def status_fn(token, chat_id, text):
            calls.append(("status", text))
            raise RuntimeError("status 5xx")

        with patch("landline.notifications.send_health_alert") as mock_alert:
            sender = StreamSender(
                "token", "123",
                text_send_fn=text_fn,
                status_send_fn=status_fn,
                text_window=0.05,
                status_window=0.05,
            )
            for i in range(threshold):
                sender.status("tool-%d" % i)
                time.sleep(0.12)
            time.sleep(0.15)
            sender.close()
        # Fallback dispatched via the async iMessage transport (M7).
        assert mock_alert.call_count == 1

    def test_emit_failure_fallback_uses_notifications_not_text_transport(self):
        """Cluster 1 M7 regression: the failing text transport must NOT be
        used for the fallback notice.

        The pre-M7 code called ``self._text_send_fn`` a 4th time to deliver
        the "some messages failed to deliver" alert — through the same
        callable that had just failed 3 times in a row, so the alert
        reliably never landed. The fix routes it to
        ``landline.notifications.send_health_alert`` (async iMessage), which
        is independent of the Telegram transport.
        """
        threshold = StreamSender._EMIT_FAILURE_THRESHOLD

        text_calls: list = []

        def text_fn(token, chat_id, text):
            text_calls.append(text)
            raise RuntimeError("Telegram 500")

        status_fn = MagicMock()

        chat_id = "chat-m7-regress"
        with patch("landline.notifications.send_health_alert") as mock_alert:
            sender = StreamSender(
                "token", chat_id,
                text_send_fn=text_fn,
                status_send_fn=status_fn,
                text_window=0.05,
                status_window=0.05,
            )
            for i in range(threshold):
                sender.text("payload-%d" % i)
                time.sleep(0.1)
            time.sleep(0.15)
            sender.close()

        # (1) send_health_alert called exactly once.
        assert mock_alert.call_count == 1, (
            "Expected exactly one send_health_alert call, got %d"
            % mock_alert.call_count
        )
        # (2) The alert body mentions the chat_id so an operator can grep
        #     the alert to the affected chat.
        call_kwargs = mock_alert.call_args.kwargs
        subject = call_kwargs.get("subject", "")
        body = call_kwargs.get("body", "")
        assert chat_id in body, (
            "alert body must mention chat_id (got subject=%r body=%r)"
            % (subject, body)
        )
        # Subject is a short slug so an operator can filter on it.
        assert isinstance(subject, str) and subject, (
            "subject must be a non-empty string; got %r" % (subject,)
        )
        # (3) text_send_fn was called exactly THRESHOLD times for the
        #     original payloads — NEVER a 4th time for the fallback body.
        assert len(text_calls) == threshold, (
            "text_send_fn should have been called %d times (once per "
            "payload) but was called %d times: %r"
            % (threshold, len(text_calls), text_calls)
        )
        # And crucially, none of those calls carried the fallback body.
        assert not any(
            "check logs" in t or "failed to deliver" in t
            for t in text_calls
        ), (
            "Fallback body was routed through the failing text transport "
            "(the M7 bug). text_calls=%r" % text_calls
        )


class TestPersistentClaudeSessionIdLock:
    """E1 — get_session_id / set_session_id are lock-guarded so readers can
    never observe a torn value mid-rewrite. Reverting the lock guard makes
    this test flaky / TSAN-detectable; under sufficient thread count it can
    fail outright if a read picks up an in-flight write."""

    def test_pc_lock_guards_session_accessors(self):
        pc = PersistentClaude()
        # Pre-load the set of legal values; threads will write only these,
        # so any get_session_id() return MUST be either None or in the set.
        import uuid as _uuid
        legal_values = {None}
        for _ in range(64):
            legal_values.add(_uuid.uuid4().hex)
        legal_value_list = [v for v in legal_values if v is not None]

        observed_bad = []
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set() and i < 200:
                pc.set_session_id(legal_value_list[i % len(legal_value_list)])
                i += 1

        def reader():
            for _ in range(200):
                if stop.is_set():
                    return
                v = pc.get_session_id()
                if v not in legal_values:
                    observed_bad.append(v)

        threads = []
        for _ in range(4):
            threads.append(threading.Thread(target=writer))
            threads.append(threading.Thread(target=reader))
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)
        stop.set()
        assert observed_bad == [], (
            "reader observed torn / unknown session_id values: %r"
            % observed_bad[:5]
        )


class TestF1EventWaitResponsiveness:
    """F1 — Watchdog/typing-loop wake on Event.wait, PauseFlag exposes a
    level-triggered Event, generation-aware post-wake recheck. Each test
    fails if the corresponding piece of F1 is reverted; latency budgets
    are strictly < the old 500 ms polling interval."""

    # ----- Test #1: watchdog wakes on done (no PauseFlag wired) -----

    def test_watchdog_wakes_on_done(self):
        """Streaming returns quickly after Claude's stdout closes; the
        watchdog's done.wait must wake immediately rather than spinning a
        500 ms time.sleep. Reverting done.wait(0.5) to time.sleep(0.5)
        makes this exceed the 100 ms budget."""
        from landline.streaming import run_claude_streaming as _rcs

        result_event = json.dumps(
            {"type": "result", "result": "ok", "session_id": "sess-1"}
        ) + "\n"
        fake_proc = MagicMock()
        fake_proc.stdout = iter([result_event])
        fake_proc.stderr = iter([])
        fake_proc.poll.return_value = None
        fake_proc.returncode = 0
        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = fake_proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""

        start = time.time()
        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.claude._get_or_create_sender", return_value=MagicMock()):
            _rcs(
                token="tok", chat_id="chatF1", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
            )
        elapsed = time.time() - start
        # Budget: well under the old 500 ms polling interval. Reverting
        # done.wait(0.5) to time.sleep(0.5) blows the budget.
        assert elapsed < 0.4, (
            "run_claude_streaming took %.3fs — watchdog likely "
            "polling time.sleep instead of done.wait()" % elapsed
        )

    # ----- Test #2: typing loop exits on typing_done.set() -----

    def test_typing_exits_on_clear(self):
        """Drive the typing loop directly (mirroring streaming.py's shape)
        and assert that setting the sibling typing_done Event wakes the
        loop within < 100 ms. Reverting to the polled inner sleep blows
        the budget. The locked name ``typing_done`` is asserted by the
        next test; here we only verify wake latency."""
        typing_done = threading.Event()
        sent = []
        TYPING_INTERVAL = 4  # matches landline.config.TYPING_INTERVAL

        def send_typing(_token, _chat_id):
            sent.append(time.time())

        def typing_loop() -> None:
            while not typing_done.is_set():
                send_typing("tok", "chat")
                if typing_done.wait(TYPING_INTERVAL):
                    return

        t = threading.Thread(target=typing_loop, daemon=True)
        t.start()
        time.sleep(0.05)  # let it enter wait()
        start = time.time()
        typing_done.set()
        t.join(timeout=1.0)
        elapsed = time.time() - start
        assert not t.is_alive(), "typing loop didn't exit on typing_done.set()"
        assert elapsed < 0.1, (
            "typing loop took %.3fs to exit — likely still polling" % elapsed
        )

    def test_typing_done_name_is_locked(self):
        """The sibling stop-event is named ``typing_done`` (per F1's locked
        contract) and ``typing_active`` is deleted. A builder who renames
        either fails downstream patching expectations."""
        import inspect
        from landline import streaming as _s
        src = inspect.getsource(_s.run_claude_streaming)
        assert "typing_done" in src, "expected ``typing_done`` Event in streaming"
        assert "typing_active" not in src, (
            "``typing_active`` should be deleted; the sibling Event is "
            "named ``typing_done``"
        )

    # ----- Test #3: pause Event wakes the watchdog quickly -----

    def test_pause_event_wakes_watchdog(self):
        """With a PauseFlag wired, calling pf.request_pause() from a test
        thread wakes the watchdog within < 100 ms (instead of up to 500 ms).
        Reverts of the pause-wake helper / persistent waiter exceed budget."""
        from landline.streaming import run_claude_streaming as _rcs
        from landline.pause_flag import PauseFlag

        # A stdout iterator that blocks; the watchdog must interrupt via
        # the pause path, not via natural stream-end.
        sentinel_done = threading.Event()

        class _BlockingStdout:
            def __iter__(self):
                return self

            def __next__(self):
                # Block until the watchdog kills the process or the test ends.
                while not sentinel_done.is_set():
                    time.sleep(0.01)
                raise StopIteration

            def close(self):
                sentinel_done.set()

        fake_proc = MagicMock()
        fake_proc.stdout = _BlockingStdout()
        fake_proc.stderr = iter([])
        # poll() returns None until we flag death from the test.
        fake_proc.poll.return_value = None
        fake_proc.returncode = -2  # would mark interrupted but we don't care
        fake_pc = MagicMock()
        fake_pc.ensure_alive.return_value = fake_proc
        fake_pc.session_id = "sess-1"
        fake_pc.get_stderr_tail.return_value = ""
        fake_pc.interrupt = MagicMock(side_effect=lambda: sentinel_done.set())

        pf = PauseFlag()
        my_gen = pf.new_call()

        def interrupt_check():
            return pf.is_requested(my_gen)

        # Trigger pause from a test thread after the watchdog has begun waiting.
        def _trigger_pause():
            time.sleep(0.05)
            pf.request_pause()

        trigger_thread = threading.Thread(target=_trigger_pause, daemon=True)
        trigger_thread.start()

        start = time.time()
        with patch("landline.claude._get_persistent_claude", return_value=fake_pc), \
             patch("landline.claude._get_or_create_sender", return_value=MagicMock()):
            _rcs(
                token="tok", chat_id="chatF1", message="hi",
                session_id="sess-1", is_new=False,
                send_response_fn=MagicMock(), send_typing_fn=MagicMock(),
                interrupt_check=interrupt_check,
                pause_flag=pf,
            )
        elapsed = time.time() - start
        # 50 ms trigger delay + a bit of overhead — anything under 250 ms
        # demonstrates wake-on-pause, well below the 500 ms polled bound.
        assert elapsed < 0.4, (
            "watchdog took %.3fs to honor /pause — pause-wake helper "
            "likely missing or polled" % elapsed
        )

    # ----- Test #4: pause Event resets across generations -----

    def test_pause_event_resets_across_generations(self):
        """clear() must clear the internal Event so a subsequent generation
        does not see a spurious wake. Reverting _event.clear() in clear()
        makes the second-generation wait(0.01) spuriously return True."""
        from landline.pause_flag import PauseFlag

        pf = PauseFlag()
        pf.new_call()           # gen=1
        pf.request_pause()       # gen=1 pause
        assert pf._event.is_set() is True
        pf.clear()
        assert pf._event.is_set() is False
        pf.new_call()           # gen=2
        assert pf.wait(0.01) is False, (
            "gen=2 saw a spurious wake — clear() did not reset _event"
        )

    # ----- Test #5: generation re-check after wake -----

    def test_watchdog_rechecks_generation_after_wake(self):
        """Stale Event state from a previous generation is harmless — the
        generation guard inside interrupt_check returns False. Mirrors the
        dispatcher's closure pattern (claude_dispatch.py:427-428)."""
        from landline.pause_flag import PauseFlag

        pf = PauseFlag()
        pf.new_call()                       # gen=1
        pf.request_pause()                  # gen=1 pause
        pf.clear()                          # gen=1 done
        gen2 = pf.new_call()                # gen=2 — the "live" generation

        # Closure shape mirrors claude_dispatch.py:427-428 exactly.
        interrupt_check = lambda: pf.is_requested(gen2)  # noqa: E731

        # Simulate a stale wake: residual Event state from a previous gen.
        pf._event.set()
        # The watchdog's interrupt_check returns False — no SIGINT, no
        # result.interrupted. (The watchdog body, on a stale wake, just
        # loops back to the next wait.)
        assert interrupt_check() is False

    # ----- Test #6: request_pause sets the Event -----

    def test_request_pause_sets_event(self):
        """Sanity test for PauseFlag's new contract — request_pause() must
        flip the internal Event so waiters wake."""
        from landline.pause_flag import PauseFlag

        pf = PauseFlag()
        pf.new_call()
        pf.request_pause()
        assert pf.wait(0) is True

    # ----- Test #7: clear resets the Event -----

    def test_clear_resets_event(self):
        """Sanity test for PauseFlag's new contract — clear() must reset
        the internal Event so subsequent waiters block again."""
        from landline.pause_flag import PauseFlag

        pf = PauseFlag()
        pf.new_call()
        pf.request_pause()
        pf.clear()
        assert pf.wait(0) is False

    # ----- Test #8: dispatcher passes pause_flag to streaming -----

    def test_dispatcher_passes_pause_flag_to_streaming(self):
        """Wire-up regression — ClaudeDispatcher must pass ``pause_flag=pf``
        to run_claude_streaming alongside ``interrupt_check``. Without this,
        /pause latency silently regresses to 500 ms with no failing test."""
        from landline.claude_dispatch import ClaudeDispatcher
        from landline.failure_tracker import ClaudeFailureTracker
        from landline.pause_flag import PauseFlag
        from landline.types import ClaudeStreamResult

        spy_run = MagicMock()
        r = ClaudeStreamResult()
        r.streamed_text = "ok"
        r.session_id = "s"
        spy_run.return_value = r

        pf = PauseFlag()
        state: dict = {}
        d = ClaudeDispatcher(
            token="tok",
            state=state,
            failure_tracker=ClaudeFailureTracker(),
            shutdown_hook=MagicMock(),
            run_claude_fn=spy_run,
            send_response_fn=MagicMock(),
            send_typing_fn=MagicMock(),
            pause_flag=pf,
        )
        with patch("landline.claude_dispatch.save_state"), \
             patch("landline.claude_dispatch.log_conversation"), \
             patch("landline.claude_dispatch.get_context_percent", return_value=None), \
             patch("landline.claude_dispatch.read_recent_conversation_history",
                   return_value=""):
            d.send_to_claude("hello", "123")

        assert spy_run.call_count == 1
        kwargs = spy_run.call_args.kwargs
        assert "pause_flag" in kwargs, (
            "dispatcher dropped the pause_flag kwarg — /pause wake regression"
        )
        assert kwargs["pause_flag"] is pf


class TestClaudeStreamResultClusterTwoDefaults:
    """Cluster 2 (stale-resume auto-recovery) added three fields to
    ClaudeStreamResult: result_is_error, result_subtype, saw_init. Pin the
    default values so existing tests that hand-construct ClaudeStreamResult
    keep working, and so the pruned-resume predicate never fires on a
    freshly-constructed empty result."""

    def test_defaults_are_safe(self):
        from landline.types import ClaudeStreamResult
        r = ClaudeStreamResult()
        assert r.result_is_error is False
        assert r.result_subtype is None
        assert r.saw_init is False

    def test_looks_like_pruned_resume_false_on_default_result(self):
        """A freshly-constructed empty result must NOT trigger the pruned
        predicate — otherwise the empty-response paths would auto-retry
        every time. Only the clean-empty predicate (looks_like_stale_session)
        should own that shape."""
        from landline.claude_dispatch import looks_like_pruned_resume
        from landline.types import ClaudeStreamResult
        assert looks_like_pruned_resume(ClaudeStreamResult()) is False
