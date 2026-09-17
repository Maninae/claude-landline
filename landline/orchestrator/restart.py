"""Restart-continuation handling for the ``TelegramDaemon``.

- ``WORKSPACE`` resolves through the daemon module so
  ``patch("landline.orchestrator.WORKSPACE")`` in tests still redirects here;
  see ``batch.py`` for the shared patch-seam rationale.
- ``RESTART_CONTINUATION_MAX_ATTEMPTS`` also resolves through ``_d`` so tests
  that patch it via ``landline.orchestrator`` (or via ``_d`` directly) win.
"""

from landline.runtime.logging import log

from landline.orchestrator import daemon as _d


# State-key for the consecutive-failure counter (F10 fix 2). Kept in
# state.json so a poisoned trigger's failure count survives the crash-restart
# doom-loop it would otherwise ride forever.
_FAILURE_COUNT_KEY = "restart_continuation_failure_count"


def handle_restart_continuation(daemon) -> None:
    """Inject the restart-continuation trigger file (if present) as a synthetic
    message so Claude resumes mid-task.

    - Routed through ``_inject_and_dispatch`` so any cron reports queued in
      ``cache/inject-queue/`` during the restart window get prepended.
    - Two-phase commit: unlink ONLY after ``_inject_and_dispatch`` returns
      without raising. A dispatch-time exception leaves the file in place so
      the next restart retries (no Telegram update_id exists to replay it).
    - Locked-session path LEAVES the file so the payload survives until the
      next unlock/restart.
    - F10 fix 2 (poisoned-trigger doom loop): a persistent failure counter
      in ``state`` breaks out of the loop after
      ``RESTART_CONTINUATION_MAX_ATTEMPTS`` consecutive failures — the
      trigger is unlinked with a LOUD log, the counter reset, and the
      exception still raised so the caller's normal error path runs. Past
      that point the next restart finds no trigger and startup proceeds
      normally.
    """
    trigger = _d.WORKSPACE / "cache" / "restart-continuation.txt"
    if not trigger.exists():
        return
    try:
        msg = trigger.read_text().strip()
    except Exception as e:
        log("Failed to read restart continuation: %s" % e)
        return
    if not msg:
        # Empty payload — safe to remove so we don't retry forever.
        try:
            trigger.unlink()
        except Exception:
            pass
        # Empty payload is not a dispatch failure — clear the counter.
        if daemon.state.get(_FAILURE_COUNT_KEY, 0):
            daemon.state[_FAILURE_COUNT_KEY] = 0
            try:
                _d.save_state(daemon.state)
            except Exception:
                pass
        return
    if daemon._lock_manager.is_locked:
        log(
            "Restart continuation skipped — session locked; "
            "will retry on next unlock/restart"
        )
        return
    log("Restart continuation: injecting message to Claude")
    try:
        daemon._inject_and_dispatch(msg, daemon.chat_id, update_ids=[])
    except Exception as e:
        # F10 fix 2: increment the persisted consecutive-failure counter
        # BEFORE deciding whether to unlink. Persist immediately so a
        # crash between here and the next successful dispatch doesn't
        # reset the count.
        current = int(daemon.state.get(_FAILURE_COUNT_KEY, 0) or 0)
        new_count = current + 1
        daemon.state[_FAILURE_COUNT_KEY] = new_count
        try:
            _d.save_state(daemon.state)
        except Exception as save_error:
            log(
                "Restart continuation: failed to persist failure counter "
                "(count=%d): %s" % (new_count, save_error)
            )
        if new_count >= _d.RESTART_CONTINUATION_MAX_ATTEMPTS:
            # Poisoned trigger — break the doom loop. Unlink the trigger
            # (this is the ONLY sanctioned exception to the two-phase-commit
            # invariant; see CLAUDE.md "Restart-continuation is two-phase"),
            # reset the counter, and log LOUDLY so the operator can see
            # what happened.
            try:
                trigger.unlink()
                unlink_note = "unlinked"
            except Exception as unlink_error:
                unlink_note = "unlink failed: %r" % (unlink_error,)
            log(
                "LOUD: Restart continuation FAILED %d/%d attempts — "
                "poisoned trigger; %s to break restart doom-loop. "
                "Last error: %s" % (
                    new_count,
                    _d.RESTART_CONTINUATION_MAX_ATTEMPTS,
                    unlink_note,
                    e,
                )
            )
            daemon.state[_FAILURE_COUNT_KEY] = 0
            try:
                _d.save_state(daemon.state)
            except Exception:
                pass
        else:
            # Two-phase commit: dispatch failed — leave the file in place;
            # the run() loop's existing handlers still see the exception.
            log(
                "Restart continuation dispatch failed (attempt %d/%d, "
                "trigger %s preserved for retry): %s" % (
                    new_count,
                    _d.RESTART_CONTINUATION_MAX_ATTEMPTS,
                    trigger,
                    e,
                )
            )
        raise
    # Dispatch succeeded — reset the failure counter and unlink. Failure to
    # unlink is benign (next restart overwrites-then-unlinks; payload already
    # delivered).
    if daemon.state.get(_FAILURE_COUNT_KEY, 0):
        daemon.state[_FAILURE_COUNT_KEY] = 0
        try:
            _d.save_state(daemon.state)
        except Exception:
            pass
    try:
        trigger.unlink()
    except Exception as e:
        log(
            "Failed to unlink restart continuation trigger after "
            "dispatch: %s" % e
        )
