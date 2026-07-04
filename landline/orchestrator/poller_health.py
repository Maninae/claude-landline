"""Poller staleness detection + in-process replacement.

Extracted from ``daemon.py`` in Wave 2 of the restructure. See ``batch.py``
for the shared patch-seam reach-back rationale — ``BackgroundPoller`` and
``time`` are resolved through the daemon module so tests that patch
``landline.orchestrator.BackgroundPoller`` / ``landline.orchestrator.time``
still target the same object these functions look up.
"""

from typing import List

from landline import config
from landline.runtime.logging import log

from landline.orchestrator import daemon as _d


def check_poller_liveness(daemon) -> None:
    """Called from the main loop (fires only every
    ``POLL_STALE_CHECK_INTERVAL_SECONDS`` to avoid noise).

    If the poller thread hasn't reported a successful poll in
    ``POLL_STALE_ALERT_THRESHOLD_SECONDS``, replace the poller in-process.
    Preserves the dedup set and cursor by handing them to the new instance.

    Rationale for in-process (vs process exit): keeps the persistent
    Claude subprocess and the StreamSender queue backlog alive across
    the swap. See CLAUDE.md "Stuck poller (TCP connection stale)".
    """
    now = _d.time.time()
    if now - daemon._poller_stale_check_last_at < config.POLL_STALE_CHECK_INTERVAL_SECONDS:
        return
    daemon._poller_stale_check_last_at = now
    if daemon._background_poller is None:
        return
    staleness = now - daemon._background_poller.last_successful_poll()
    if staleness < config.POLL_STALE_ALERT_THRESHOLD_SECONDS:
        return
    log(
        f"Poller stall detected (last successful poll {int(staleness)}s "
        f"ago) — replacing in-process"
    )
    replace_poller_in_place(daemon, reason=f"stale for {int(staleness)}s")


def replace_poller_in_place(daemon, reason: str) -> None:
    """Swap the current stuck poller for a fresh one, preserving invariants.

    Ordering: (1) signal-stop old poller, (2) snapshot dedup + cursor,
    (3) construct new poller with cursor, (4) hand snapshot to new
    poller, (5) attach fresh ``_on_update_queued`` callback, (6) start
    new poller, (7) let old poller thread die on its next timeout (it's
    daemonized; the fresh urllib request in it will time out within
    ``POLL_TIMEOUT+10`` or hang forever — but it's not our thread
    anymore).

    We use ``signal_stop()`` rather than ``stop()`` — the latter joins
    for up to ``POLL_TIMEOUT+5`` and would stall the main loop on top
    of an already-stalled poller.
    """
    old = daemon._background_poller
    if old is None:
        return
    old.signal_stop()
    # Snapshot invariants before constructing the replacement so any
    # in-flight thread interaction on ``old`` can't race the handoff.
    with old._last_processed_update_id_lock:
        old_cursor = old._last_processed_update_id
    old_dedup = old.snapshot_dedup_ids()
    # Drain any updates that were already queued on the old poller but
    # not yet consumed by the main loop. Without this, they'd be
    # orphaned: the old poller stops, the main loop only reads the new
    # poller's queue, and Telegram's re-delivery of the same ids would
    # be blocked by the new poller's dedup (loaded from ``old_dedup``).
    # Net result: silently lost updates. Forwarding them below preserves
    # at-least-once across the swap.
    orphaned_updates = old.drain()
    new_poller = _d.BackgroundPoller(
        token=daemon.token,
        initial_last_processed_update_id=old_cursor,
        on_update_queued=daemon._on_update_queued,
    )
    # Seed dedup for BOTH the old snapshot AND the forwarded queue's
    # update_ids in a single call. The old snapshot alone is not
    # sufficient: the poller thread may have atomically added an
    # update to (dedup ∪ queue) AFTER ``snapshot_dedup_ids()`` returned
    # but BEFORE ``old.drain()`` — that update lands in the drained
    # payload without ever appearing in ``old_dedup``. If we skipped
    # it here, the new poller's cursor (snapshotted before the update
    # was processed) would ask Telegram for offset=X on the next
    # poll, get X re-delivered, miss the dedup gate, and re-queue X —
    # so the main loop processes X twice (turn N receives its own
    # message twice / a slash command runs twice). Including the
    # forwarded ids in the dedup seed makes the re-delivery a dedup
    # hit and preserves at-most-once processing across the swap.
    forwarded_ids: List[int] = []
    for update in orphaned_updates:
        if not isinstance(update, dict):
            continue
        uid = update.get("update_id")
        if isinstance(uid, int):
            forwarded_ids.append(uid)
    new_poller.load_dedup_ids(list(old_dedup) + forwarded_ids)
    if orphaned_updates:
        forwarded = new_poller.preload_queue(orphaned_updates)
        log(
            f"Poller swap: forwarded {forwarded} in-flight update(s) "
            f"from old poller's queue"
        )
    daemon._background_poller = new_poller
    new_poller.start()
    daemon._poller_stale_recovery_count += 1
    log(
        f"Poller replaced (reason={reason}, "
        f"recovery #{daemon._poller_stale_recovery_count})"
    )
