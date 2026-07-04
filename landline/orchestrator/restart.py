"""Restart-continuation handling for the ``TelegramDaemon``.

Extracted from ``daemon.py`` in Wave 2 of the restructure. See ``batch.py``
for the shared patch-seam reach-back rationale — ``WORKSPACE`` here is
resolved through the daemon module so tests that patch
``landline.orchestrator.WORKSPACE`` still see the redirected path.
"""

from landline.runtime.logging import log

from landline.orchestrator import daemon as _d


def handle_restart_continuation(daemon) -> None:
    """If a restart-continuation trigger file exists, inject its content
    as a synthetic message to Claude so it can resume mid-task.

    Routed through ``_inject_and_dispatch`` so any cron reports queued in
    ``cache/inject-queue/`` during the restart window are prepended too —
    otherwise they'd sit until the operator's next message.

    Two-phase commit (M1): the trigger file is unlinked ONLY AFTER
    ``_inject_and_dispatch`` returns without raising. A dispatch-time
    exception leaves the file in place so the next restart retries —
    otherwise a transient crash silently drops the operator's cross-restart
    instruction (no Telegram update_id exists to replay it).
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
        # LEAVE the file in place so the payload survives until the next
        # unlock/restart. Unlinking here would permanently lose the operator's
        # continuation message.
        log(
            "Restart continuation skipped — session locked; "
            "will retry on next unlock/restart"
        )
        return
    log("Restart continuation: injecting message to Claude")
    try:
        daemon._inject_and_dispatch(msg, daemon.chat_id, update_ids=[])
    except Exception as e:
        # Two-phase commit: dispatch failed — LEAVE the trigger file in
        # place so the next restart retries. Re-raise so the run() loop's
        # existing handlers (and any startup error path) still see this.
        log(
            "Restart continuation dispatch failed (trigger %s preserved "
            "for retry): %s" % (trigger, e)
        )
        raise
    # Dispatch succeeded — commit by unlinking the trigger. A failure
    # here is benign: the next restart will overwrite-then-unlink, and
    # the payload was already delivered to Claude.
    try:
        trigger.unlink()
    except Exception as e:
        log(
            "Failed to unlink restart continuation trigger after "
            "dispatch: %s" % e
        )
