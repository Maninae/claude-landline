"""Restart-continuation handling for the ``TelegramDaemon``.

- ``WORKSPACE`` resolves through the daemon module so
  ``patch("landline.orchestrator.WORKSPACE")`` in tests still redirects here;
  see ``batch.py`` for the shared patch-seam rationale.
"""

from landline.runtime.logging import log

from landline.orchestrator import daemon as _d


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
        # Two-phase commit: dispatch failed — leave the file in place; the
        # run() loop's existing handlers still see the exception.
        log(
            "Restart continuation dispatch failed (trigger %s preserved "
            "for retry): %s" % (trigger, e)
        )
        raise
    # Dispatch succeeded — unlink. Failure here is benign (next restart
    # overwrites-then-unlinks; payload already delivered).
    try:
        trigger.unlink()
    except Exception as e:
        log(
            "Failed to unlink restart continuation trigger after "
            "dispatch: %s" % e
        )
