"""Claude streaming engine — drives one assistant turn end-to-end.

Sends a message to the persistent Claude process, waits while the
long-lived ``StreamPump`` (see ``landline.claude.pump``) routes tool-status
and text deltas into the per-chat ``StreamSender``, finalizes the turn with
a ``flush()`` boundary.

- The PUMP reads stdout; this module registers a ``TurnHandle`` before the
  stdin write, blocks on ``handle.done``, and lifts the handle's bookkeeping
  into a ``ClaudeStreamResult``. Continuous reading means unsolicited turns
  (background subagents / ``run_in_background`` completing between dispatched
  turns) deliver immediately instead of shifting later turns by one (the
  2026-06/07 desync — see pump docstring).
- Sole-producer contract to the sender: after ``handle.done.wait()`` returns,
  NOTHING in this module may touch the sender. The pump already appended
  the final-result tail and marked the turn boundary with ``sender.flush()``,
  both on the pump thread, before completing the handle. A late
  ``text()``/``flush()`` here would interleave into an unsolicited block
  that may already be streaming through the sender.
"""

import subprocess
import threading
import time
from typing import Any, Callable, List, Optional

from landline.claude.types import ClaudeStreamResult
from landline.config import CLAUDE_TIMEOUT, TYPING_INTERVAL
from landline.runtime.logging import log
from landline.claude.registry import _SHUTDOWN_DRAIN_TIMEOUT, _close_all_senders
from landline.claude.pump import TurnHandle, get_or_create_pump


class ClaudeStreamShutdownHook:
    """Exposes the active subprocess to the SIGTERM handler for clean shutdown.

    Senders are long-lived (owned by the module-level registry) — shutdown
    drains them ALL via ``_close_all_senders()``, never per turn.
    """

    def __init__(self) -> None:
        self.active_proc: Optional[subprocess.Popen] = None

    def set_proc(self, proc: subprocess.Popen) -> None:
        self.active_proc = proc

    def clear(self) -> None:
        self.active_proc = None

    def drain_for_shutdown(self, sender_close_timeout: float = _SHUTDOWN_DRAIN_TIMEOUT) -> None:
        active_proc = self.active_proc
        if active_proc is not None:
            try:
                active_proc.terminate()
            except Exception:
                pass
        # Drain every long-lived sender's tail (bounded → stays inside
        # launchd's grace window).
        _close_all_senders(timeout=sender_close_timeout)


def _wait_for_done_or_pause(
    done: threading.Event,
    pause_flag: Any,
    pause_waker: Optional[threading.Event],
    timeout: float,
) -> bool:
    """True iff ``done`` was set or a pause was requested before ``timeout``.

    Both signals set ``pause_waker`` (pause via a persistent waiter spawned
    in ``run_claude_streaming``). We block on ``pause_waker`` once per tick
    — no per-tick threads. Caller re-checks ``done``/``interrupt_check`` at
    the top of the loop.
    """
    if pause_flag is None or pause_waker is None:
        return done.wait(timeout)
    if done.is_set():
        return True
    if pause_flag.wait(0):
        return True
    pause_waker.wait(timeout)
    return done.is_set() or pause_flag.wait(0)


def run_claude_streaming(
    token: str,
    chat_id: str,
    message: str,
    session_id: Optional[str],
    is_new: bool,
    suppress_empty_response_notice: bool = False,
    shutdown_hook: Optional[ClaudeStreamShutdownHook] = None,
    interrupt_check: Optional[Callable[[], bool]] = None,
    send_response_fn: Optional[Callable[[str, str, str], None]] = None,
    send_typing_fn: Optional[Callable[[str, str], None]] = None,
    pause_flag: Optional[Any] = None,
) -> ClaudeStreamResult:
    """Send a message to the persistent Claude process and stream the response."""
    from landline.telegram import send_response as default_send_response, send_typing as default_send_typing
    # Late binding via the facade so tests patching
    # ``landline.claude._get_persistent_claude`` / ``_get_or_create_sender`` /
    # ``try_enqueue_chat_notice`` intercept these calls.
    from landline import claude as _claude_facade

    send_response = send_response_fn or default_send_response
    send_typing = send_typing_fn or default_send_typing

    result = ClaudeStreamResult()
    pc = _claude_facade._get_persistent_claude()

    try:
        proc = pc.ensure_alive(
            session_id=session_id if not is_new else None,
            force_new=is_new,
        )
        pump = get_or_create_pump(proc)
        if not pump.alive:
            # Reader died mid-stream — pipe's read position is unknowable,
            # process is unusable. Kill and respawn (session id survives on pc).
            log("Stream pump dead for live Claude process — respawning")
            pc.kill()
            proc = pc.ensure_alive(
                session_id=session_id if not is_new else None,
                force_new=is_new,
            )
            pump = get_or_create_pump(proc)
    except Exception as e:
        result.error = f"Failed to spawn Claude: {e}"
        log(result.error)
        return result

    if shutdown_hook is not None:
        shutdown_hook.set_proc(proc)

    if is_new and pc.session_id:
        result.session_id = pc.session_id

    from landline.telegram import send_html
    # Long-lived, one per chat — see registry for cross-turn ordering.
    sender = _claude_facade._get_or_create_sender(chat_id, token, send_response, send_html)

    # Keep the pump's idle route fresh so unsolicited turns between
    # dispatched turns reach this chat.
    pump.set_idle_route(chat_id, token, send_response, send_html)

    # Register BEFORE stdin write: the turn's ``system/init`` must never
    # find an empty slot and be mistaken for an unsolicited turn.
    handle = TurnHandle()
    pump.register_turn(handle, sender)

    try:
        pc.send_message(message)
    except Exception as e:
        log(f"stdin write error: {e}")
        result.error = f"stdin write failed: {e}"
        pump.cancel_turn(handle)
        if shutdown_hook is not None:
            shutdown_hook.clear()
        return result

    done = threading.Event()
    last_active = handle.last_active  # single-cell clock bumped by the pump
    interrupt_sent = threading.Event()

    def _unblock_reader() -> None:
        # Close stdout (read end) to unblock the pump's ``for raw in
        # proc.stdout:`` even if a grandchild still holds the write end.
        # Idempotent; pump completes ``handle`` on its way out.
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        try:
            pc._close_pipes()
        except Exception as close_err:
            log("watchdog _close_pipes failed: %s" % close_err)

    pause_waker: Optional[threading.Event] = None
    pause_waker_thread: Optional[threading.Thread] = None
    if pause_flag is not None:
        pause_waker = threading.Event()

        def _pause_to_waker() -> None:
            # One persistent thread for the turn's lifetime. Polls the
            # PauseFlag on a short timeout so it also observes ``done``.
            # Either trigger sets ``pause_waker`` and exits.
            while not done.is_set():
                if pause_flag.wait(0.5):
                    pause_waker.set()
                    return
            pause_waker.set()

        pause_waker_thread = threading.Thread(target=_pause_to_waker, daemon=True)
        pause_waker_thread.start()

    def watchdog() -> None:
        while not done.is_set():
            if proc.poll() is not None:
                log(f"Claude process died (exit {proc.returncode}), closing pipes")
                _unblock_reader()
                break
            if (time.time() - last_active[0]) >= CLAUDE_TIMEOUT:
                log(f"Claude silent for {CLAUDE_TIMEOUT}s, killing process")
                try:
                    proc.kill()
                except Exception:
                    pass
                _unblock_reader()
                break
            if interrupt_check is not None and interrupt_check():
                log("Interrupted by new incoming message")
                result.interrupted = True
                interrupt_sent.set()
                handle.interrupt_suppress.set()
                pc.interrupt()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    log("Claude did not exit 5s after SIGINT — sending SIGTERM")
                    try:
                        proc.terminate()
                    except Exception as term_err:
                        log(f"SIGTERM failed: {term_err}")
                _unblock_reader()
                break
            # Wake on EITHER done (shutdown/turn-end) OR pause. The
            # generation re-check inside interrupt_check at the top of
            # next iteration makes stale-generation wakes no-ops.
            if _wait_for_done_or_pause(done, pause_flag, pause_waker, 0.5):
                if done.is_set():
                    break
                continue

    wd_thread = threading.Thread(target=watchdog, daemon=True)
    wd_thread.start()

    typing_done = threading.Event()

    def typing_loop() -> None:
        while not typing_done.is_set():
            send_typing(token, chat_id)
            if typing_done.wait(TYPING_INTERVAL):
                return

    typing_thread = threading.Thread(target=typing_loop, daemon=True)
    typing_thread.start()

    try:
        # Pump routes events to the sender and completes ``handle`` on
        # result / EOF / read error — never abandoned. Watchdog kill
        # paths all end in EOF, so this wait always terminates.
        handle.done.wait()
    finally:
        typing_done.set()
        done.set()
        if pause_waker is not None:
            pause_waker.set()

    if handle.error is not None:
        result.error = handle.error

    sid = handle.result_session_id or handle.init_session_id
    if sid:
        result.session_id = sid

    if proc.poll() is not None:
        result.exit_code = proc.returncode
        pc._close_pipes()

    # Sole-producer contract — see module docstring. ``streamed_parts``
    # already includes the pump-appended final-result tail.
    result.streamed_text = "".join(handle.streamed_parts)
    if handle.saw_result:
        result.final_result = handle.final_result

    # Mirror the pump's terminal-event observations for the dispatcher's
    # ``looks_like_pruned_resume`` predicate (must be set BEFORE the
    # empty-response-notice block below).
    result.result_is_error = handle.result_is_error
    result.result_subtype = handle.result_subtype
    result.saw_init = handle.saw_init

    # Usage/cost — the dispatcher's ``_finalize_response`` persists a daily
    # aggregate on success. None-safe (absent → recorded as zero).
    result.result_usage = handle.result_usage
    result.result_model_usage = handle.result_model_usage
    result.result_total_cost_usd = handle.result_total_cost_usd
    result.result_num_turns = handle.result_num_turns
    result.result_duration_ms = handle.result_duration_ms

    result.stderr_tail = pc.get_stderr_tail()

    if not result.streamed_text.strip() and not (result.final_result or "").strip():
        if not result.interrupted:
            if result.exit_code and result.exit_code != 0:
                tail = result.stderr_tail[-500:] if result.stderr_tail else ""
                log(f"Claude exit {result.exit_code}: {tail}")
                if not suppress_empty_response_notice:
                    # Route through the chat queue so the notice lands AFTER
                    # any bubbles still draining. Direct-send fallback when
                    # no live sender exists yet.
                    _claude_facade.try_enqueue_or_send(
                        chat_id,
                        text=f"(Claude returned no response — exit {result.exit_code}.)",
                        direct_fn=lambda body: send_response(token, chat_id, body),
                    )
            else:
                if not suppress_empty_response_notice:
                    _claude_facade.try_enqueue_or_send(
                        chat_id,
                        text="(Empty response from Claude.)",
                        direct_fn=lambda body: send_response(token, chat_id, body),
                    )

    if shutdown_hook is not None:
        shutdown_hook.clear()

    return result
