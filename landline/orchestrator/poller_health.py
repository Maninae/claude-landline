"""Poller staleness detection + in-process replacement.

- ``BackgroundPoller`` and ``time`` resolve through the daemon module so tests
  that patch ``landline.orchestrator.BackgroundPoller`` / ``.time`` still hit
  the object these functions look up; see ``batch.py`` for the seam rationale.
"""

from typing import List

from landline import config
from landline.runtime.logging import log

from landline.orchestrator import daemon as _d


def check_poller_liveness(daemon) -> None:
    """Called from the main loop; internally rate-limited to
    ``POLL_STALE_CHECK_INTERVAL_SECONDS``.

    - If the poller hasn't reported a successful poll in
      ``POLL_STALE_ALERT_THRESHOLD_SECONDS``, replace it in-process,
      preserving dedup set + cursor across the swap.
    - In-process swap (not process exit) keeps the persistent Claude
      subprocess and StreamSender backlog alive; see CLAUDE.md
      "Stuck poller (TCP connection stale)".
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
    """Swap the stuck poller for a fresh one, preserving invariants.

    - Ordering: signal-stop old → snapshot dedup+cursor → construct new with
      cursor → seed snapshot → start new. Old thread is daemonized and dies
      on its next timeout.
    - ``signal_stop()`` not ``stop()``: ``stop()`` joins up to
      ``POLL_TIMEOUT+5`` and would stall the main loop on top of an
      already-stalled poller.
    - See docs/ARCHITECTURE.md "Poller staleness — in-process replacement"
      for the dedup+queue race handled below.
    """
    old = daemon._background_poller
    if old is None:
        return
    old.signal_stop()
    # Snapshot before constructing the replacement so any in-flight thread
    # interaction on ``old`` can't race the handoff.
    with old._last_processed_update_id_lock:
        old_cursor = old._last_processed_update_id
    old_dedup = old.snapshot_dedup_ids()
    # Forward queued-but-not-consumed updates so they aren't orphaned
    # (old stops, main loop only reads new's queue, and dedup blocks
    # Telegram's re-delivery — silently lost). At-least-once across swap.
    orphaned_updates = old.drain()
    new_poller = _d.BackgroundPoller(
        token=daemon.token,
        initial_last_processed_update_id=old_cursor,
        on_update_queued=daemon._on_update_queued,
    )
    # Seed dedup with BOTH old_dedup AND the forwarded ids: the poller
    # thread may have atomically added an update to (dedup ∪ queue) between
    # snapshot_dedup_ids() and old.drain(), landing it in the drained payload
    # without appearing in old_dedup. Without this seed, Telegram's
    # re-delivery would miss the new poller's dedup gate → double-process.
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
