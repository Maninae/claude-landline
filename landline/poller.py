"""Background Telegram polling — decoupled from message processing.

Runs a long-poll loop in a dedicated thread so incoming messages accumulate
in an in-memory queue while Claude is busy.  The main thread drains at its
own pace and coalesces multi-message bursts.
"""

import json
import queue
import socket
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import OrderedDict
from typing import Callable, Dict, Iterable, List, Optional

from landline.config import (
    MAX_DEDUP_IDS,
    POLL_API_ERROR_BACKOFF_SECONDS,
    POLL_ERROR_ALERT_AFTER,
    POLL_ERROR_BACKOFF_BASE,
    POLL_ERROR_BACKOFF_MAX,
    POLL_ERROR_LOG_EVERY_N,
    POLL_TIMEOUT,
)
from landline.logging import log
from landline.notifications import send_network_alert


def _telegram_api_get_updates(token: str, offset: int) -> Dict:
    """Thin wrapper isolating the HTTP call for testability."""
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    payload = json.dumps({
        "offset": offset,
        "timeout": POLL_TIMEOUT,
        "allowed_updates": ["message"],
    }).encode()
    headers = {"Content-Type": "application/json"}
    request = urllib.request.Request(url, data=payload, headers=headers)
    with urllib.request.urlopen(request, timeout=POLL_TIMEOUT + 10) as response:
        data = json.loads(response.read())
    if not data.get("ok"):
        raise RuntimeError(
            "Telegram getUpdates returned not-ok: "
            "%s %s" % (data.get("error_code"), data.get("description"))
        )
    return data


class BackgroundPoller:
    """Continuously polls Telegram for updates in a background thread.

    Decouples polling from message processing so messages arriving while
    Claude is busy accumulate in an in-memory queue rather than sitting on
    Telegram's servers until the next poll cycle.  The main thread drains
    the queue after each Claude call and coalesces accumulated messages
    into a single batch.

    Crash safety: the polling offset tracks the last *processed* update ID
    (not the last *fetched* ID) so Telegram only considers updates confirmed
    once the main thread has persisted them to disk via save_state.
    Unprocessed updates are therefore re-fetched on every poll cycle while
    in flight; a dedup set prevents them from being re-queued.  On a crash,
    the persisted cursor hasn't advanced past unprocessed updates, so
    Telegram re-delivers them on restart.
    """

    def __init__(
        self,
        token: str,
        initial_last_processed_update_id: int,
        on_update_queued: Optional[Callable[[Dict], None]] = None,
    ) -> None:
        """Construct a BackgroundPoller.

        on_update_queued (optional): a callback invoked synchronously on the
        poller thread immediately after each update is added to the queue.
        Contract: MUST be O(1) and non-blocking — anything heavier stalls the
        polling loop. Exceptions raised by the callback are caught and ignored
        (they MUST NOT affect poll error counting or backoff).
        """
        self.token = token
        self._incoming_updates_queue: "queue.Queue[Dict]" = queue.Queue()
        self._last_processed_update_id = initial_last_processed_update_id
        self._last_processed_update_id_lock = threading.Lock()
        # OrderedDict acts as an insertion-ordered set: keys are update_ids,
        # values are unused (None). Capped at MAX_DEDUP_IDS — when full, the
        # oldest insertion is evicted on each new add. This bounds memory
        # independently of restart cadence while still covering the in-flight
        # dedup window (the cursor handles long-term dedup).
        self._already_queued_update_ids: "OrderedDict[int, None]" = OrderedDict()
        self._already_queued_update_ids_lock = threading.Lock()
        # M8: latched once the dedup OrderedDict first reaches MAX_DEDUP_IDS so
        # the cap event is logged exactly once per replay storm (eviction
        # otherwise emits no signal). Cleared inside _poll_loop on a successful
        # poll once the set has dropped back below the cap, so a fresh storm
        # after the dedup window shrinks re-arms the warning.
        self._dedup_cap_reached_logged = False
        self._on_update_queued = on_update_queued
        self._stop = threading.Event()
        # Cluster 4: silent-TCP-stall detection. Seeded to construction time
        # so a fresh poller is not immediately considered stale. Updated on
        # every successful _telegram_api_get_updates return (including the
        # empty-result case, which proves the socket round-tripped at the
        # long-poll timeout boundary). Read by the orchestrator's main loop
        # via last_successful_poll() to detect a silent TCP stall.
        self._last_successful_poll_at: float = time.time()
        self._last_successful_poll_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="background-poller",
        )

    def start(self) -> None:
        # Reseed the successful-poll timestamp at start() (not construction)
        # so a long gap between BackgroundPoller(...) and .start() — e.g. a
        # multi-minute restart-continuation Claude turn between orchestrator
        # setup and poller launch — does NOT make the first liveness check
        # in the main loop misread the fresh poller as stale and immediately
        # replace it. Handoff (poller-swap) also benefits: the replacement
        # poller starts from now, not the construction timestamp.
        with self._last_successful_poll_lock:
            self._last_successful_poll_at = time.time()
        self._thread.start()

    def signal_stop(self) -> None:
        """Signal-handler safe — only sets a threading.Event."""
        self._stop.set()

    def stop(self, join_timeout: Optional[float] = None) -> None:
        self._stop.set()
        effective_join_timeout = (
            join_timeout if join_timeout is not None else POLL_TIMEOUT + 5
        )
        self._thread.join(timeout=effective_join_timeout)

    def has_pending(self) -> bool:
        """Non-blocking check for queued updates. Used by interrupt architecture."""
        return not self._incoming_updates_queue.empty()

    def last_successful_poll(self) -> float:
        """Monotonic-ish wall-clock timestamp of the most recent successful
        _telegram_api_get_updates return (empty results count). Read by the
        orchestrator's main loop to detect a silent TCP stall.
        """
        with self._last_successful_poll_lock:
            return self._last_successful_poll_at

    def snapshot_dedup_ids(self) -> List[int]:
        """Snapshot the current dedup id set for handoff to a replacement
        poller. Preserves insertion order so the cap-based FIFO eviction
        semantic carries across the swap.
        """
        with self._already_queued_update_ids_lock:
            return list(self._already_queued_update_ids.keys())

    def load_dedup_ids(self, ids: Iterable[int]) -> None:
        """Hydrate the dedup set from a snapshot (used on in-process poller
        replacement). Preserves the MAX_DEDUP_IDS cap by evicting oldest
        entries when the load overshoots.
        """
        with self._already_queued_update_ids_lock:
            for uid in ids:
                self._already_queued_update_ids[uid] = None
                while len(self._already_queued_update_ids) > MAX_DEDUP_IDS:
                    self._already_queued_update_ids.popitem(last=False)

    def advance_processed_cursor(self, update_id: int) -> None:
        """Advance the processed cursor so the polling thread's next offset
        skips past confirmed updates.  Called by the main thread after
        persisting state to disk.

        The dedup set is intentionally NOT pruned here.  There is a race
        between cursor advancement and in-flight long polls: the poller
        may have started a 30s poll with the OLD offset before the cursor
        advanced.  When that poll returns, it can include updates the main
        thread already processed.  If the dedup set was pruned (those IDs
        removed), the poller would re-queue them, causing duplicate
        processing.  The set is bounded at MAX_DEDUP_IDS via insertion-
        ordered FIFO eviction (oldest-first, see __init__), which keeps
        memory finite without losing the recent in-flight dedup window —
        long-term dedup is handled by the persisted cursor.
        """
        with self._last_processed_update_id_lock:
            if update_id > self._last_processed_update_id:
                self._last_processed_update_id = update_id

    def discard_queued_ids(self, update_ids: Iterable[int]) -> None:
        """Remove update_ids from the dedup set so a future poll can re-queue
        them. Used to recover updates that were drained from the queue but not
        successfully processed (e.g. a batch error) — the polling cursor stays
        below them, so Telegram re-delivers, and removing them here lets the
        dedup gate admit them again. Safe because these updates were NOT
        processed; the no-prune rule in advance_processed_cursor only protects
        already-PROCESSED ids from re-queueing.
        """
        with self._already_queued_update_ids_lock:
            for update_id in update_ids:
                self._already_queued_update_ids.pop(update_id, None)

    def preload_queue(self, updates: Iterable[Dict]) -> int:
        """Push already-fetched updates onto the queue without touching dedup.

        Used by the orchestrator's in-process poller-swap path to forward
        updates that were sitting on the OLD poller's queue at the moment of
        the swap. The dedup set is already loaded (via ``load_dedup_ids``)
        so these ids stay marked; if we didn't forward the actual update
        payloads too, they'd be orphaned in the discarded old queue and
        Telegram's re-delivery would be blocked by the new poller's dedup.
        Returns the count actually enqueued (skipping any without an id).
        """
        count = 0
        for update in updates:
            if not isinstance(update, dict):
                continue
            self._incoming_updates_queue.put(update)
            count += 1
        return count

    def drain(self, block_timeout_seconds: float = 0) -> List[Dict]:
        """Drain all queued updates, optionally blocking for the first one."""
        updates: List[Dict] = []
        if block_timeout_seconds > 0:
            try:
                updates.append(
                    self._incoming_updates_queue.get(
                        timeout=block_timeout_seconds,
                    )
                )
            except queue.Empty:
                return []
        while True:
            try:
                updates.append(self._incoming_updates_queue.get_nowait())
            except queue.Empty:
                break
        updates.sort(key=lambda u: u.get("update_id", 0))
        return updates

    def _poll_loop(self) -> None:
        consecutive_error_count = 0
        consecutive_api_error_count = 0  # A3: independent of the network counter.
        current_backoff_seconds = POLL_ERROR_BACKOFF_BASE
        network_failure_start_time: Optional[float] = None
        imsg_alert_sent = False

        while not self._stop.is_set():
            try:
                with self._last_processed_update_id_lock:
                    polling_offset = self._last_processed_update_id + 1

                response = _telegram_api_get_updates(self.token, polling_offset)

                for update in response.get("result", []):
                    update_id = update.get("update_id", 0)
                    with self._already_queued_update_ids_lock:
                        if update_id in self._already_queued_update_ids:
                            continue
                        # M8: log once on the cycle that first reaches the cap,
                        # before the eviction below starts dropping oldest
                        # in-flight ids. Latched so the line fires exactly once
                        # per "storm" — subsequent inserts at-or-above-cap stay
                        # silent; the latch clears on a successful poll after
                        # the set drops below the cap.
                        if (
                            not self._dedup_cap_reached_logged
                            and len(self._already_queued_update_ids) >= MAX_DEDUP_IDS
                        ):
                            log(
                                f"Dedup set reached MAX_DEDUP_IDS cap "
                                f"({MAX_DEDUP_IDS}); oldest in-flight ids will "
                                f"be evicted. Replay storm in progress?"
                            )
                            self._dedup_cap_reached_logged = True
                        self._already_queued_update_ids[update_id] = None
                        # Evict oldest ids to keep the set bounded. The recent
                        # in-flight window is what matters; older ids are
                        # already past the persisted cursor.
                        while len(self._already_queued_update_ids) > MAX_DEDUP_IDS:
                            self._already_queued_update_ids.popitem(last=False)
                        # queue.put INSIDE the dedup lock so a concurrent
                        # snapshot_dedup_ids() from a poller-swap either sees
                        # BOTH the dedup entry AND the queued update, or
                        # neither. Without this atomicity, an update whose id
                        # snuck into the dedup snapshot but not the queue was
                        # loaded into the new poller's dedup while sitting
                        # orphaned in the old queue — Telegram's re-delivery
                        # would then be silently dropped. queue.put on an
                        # unbounded Queue is O(1) so this doesn't stretch the
                        # dedup critical section meaningfully.
                        self._incoming_updates_queue.put(update)
                    # Invoke the optional on_update_queued callback with its own
                    # isolated try/except — exceptions here MUST NOT affect
                    # poll error counting / network-outage alerting.
                    if self._on_update_queued is not None:
                        try:
                            self._on_update_queued(update)
                        except Exception:
                            pass

                # Cluster 4: record the successful poll timestamp. This fires
                # on EVERY successful return, including empty result sets —
                # an empty long-poll returning at the POLL_TIMEOUT boundary
                # proves the socket round-tripped and the poller is live.
                with self._last_successful_poll_lock:
                    self._last_successful_poll_at = time.time()

                if consecutive_error_count > 0:
                    outage_duration = time.time() - (network_failure_start_time or time.time())
                    log(f"Network recovered after {int(outage_duration)}s outage ({consecutive_error_count} errors)")
                    consecutive_error_count = 0
                    current_backoff_seconds = POLL_ERROR_BACKOFF_BASE
                    network_failure_start_time = None
                    imsg_alert_sent = False

                # A3: any successful poll clears the API-error throttle counter
                # so a fresh API/code fault always logs its first traceback.
                # M8: also clear the dedup-cap latch once the set has dropped
                # below the cap — if the storm subsides and recurs, the cap
                # log should re-fire on the next first-hit.
                if consecutive_api_error_count > 0:
                    consecutive_api_error_count = 0
                if (
                    self._dedup_cap_reached_logged
                    and len(self._already_queued_update_ids) < MAX_DEDUP_IDS
                ):
                    self._dedup_cap_reached_logged = False

            except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as poll_error:
                # Genuine network-layer failures: DNS, connection refused,
                # timeouts, socket errors, HTTP errors from the server.
                # These drive the network-outage timer + iMessage alert and
                # exponential backoff. (HTTPError is a URLError subclass, so
                # Telegram 429 lands here too — see Retry-After handling below.)
                consecutive_error_count += 1
                if network_failure_start_time is None:
                    network_failure_start_time = time.time()

                if consecutive_error_count == 1 or consecutive_error_count % POLL_ERROR_LOG_EVERY_N == 0:
                    reason = getattr(poll_error, 'reason', None)
                    if reason is None:
                        reason = poll_error
                    log(f"Poll network error (#{consecutive_error_count}): {reason}")

                outage_seconds = time.time() - network_failure_start_time
                if not imsg_alert_sent and outage_seconds >= POLL_ERROR_ALERT_AFTER:
                    imsg_alert_sent = True
                    send_network_alert(outage_seconds)

                # Honor Telegram 429 Retry-After. urllib.error.HTTPError is a
                # subclass of URLError; .code == 429 means "Too Many Requests"
                # and the server tells us how long to wait via the Retry-After
                # response header (or parameters.retry_after in the JSON body).
                # Sleep for max(current_backoff, retry_after) so we never come
                # back sooner than Telegram asks. Be defensive parsing both
                # sources — fall back to current_backoff_seconds on anything
                # missing/malformed.
                sleep_seconds = current_backoff_seconds
                if (
                    isinstance(poll_error, urllib.error.HTTPError)
                    and poll_error.code == 429
                ):
                    retry_after_seconds: Optional[float] = None
                    try:
                        header_value = poll_error.headers.get("Retry-After")
                        if header_value is not None:
                            retry_after_seconds = float(header_value)
                    except (AttributeError, TypeError, ValueError):
                        retry_after_seconds = None
                    if retry_after_seconds is None:
                        try:
                            body = poll_error.read()
                            parsed = json.loads(body) if body else {}
                            params = parsed.get("parameters") or {}
                            body_retry = params.get("retry_after")
                            if body_retry is not None:
                                retry_after_seconds = float(body_retry)
                        except (AttributeError, ValueError, TypeError, OSError, json.JSONDecodeError):
                            retry_after_seconds = None
                    if retry_after_seconds is not None and retry_after_seconds > 0:
                        sleep_seconds = max(current_backoff_seconds, retry_after_seconds)
                        log(
                            f"Telegram 429 received; honoring Retry-After "
                            f"{retry_after_seconds:.1f}s (sleeping {sleep_seconds:.1f}s)"
                        )

                if self._stop.wait(sleep_seconds):
                    return
                current_backoff_seconds = min(
                    current_backoff_seconds * 2, POLL_ERROR_BACKOFF_MAX,
                )

            except Exception as poll_error:
                # Non-network errors: a RuntimeError from a not-ok Telegram API
                # response (transient API issue, bad token, malformed request)
                # or an unexpected code bug (KeyError/TypeError from a
                # malformed payload). These must NOT drive the network-outage
                # timer or send a "network outage" iMessage alert — those
                # signals are reserved for genuine connectivity loss so they
                # stay meaningful.
                #
                # A3: throttle log volume the same way the network branch does
                # — log on the 1st occurrence and every Nth thereafter. For
                # code bugs (non-RuntimeError) emit the full traceback on the
                # first occurrence only; subsequent throttled lines are
                # one-line summaries. This turns ~30 lines/min into ~2.5
                # lines/min on a persistent fault while keeping the
                # first-failure diagnostic.
                consecutive_api_error_count += 1
                exc_type = type(poll_error).__name__
                should_log = (
                    consecutive_api_error_count == 1
                    or consecutive_api_error_count % POLL_ERROR_LOG_EVERY_N == 0
                )
                if should_log:
                    if isinstance(poll_error, RuntimeError):
                        log(
                            f"Poll API error (#{consecutive_api_error_count}, "
                            f"{exc_type}): {poll_error}"
                        )
                    elif consecutive_api_error_count == 1:
                        log(
                            f"Poll unexpected error (#{consecutive_api_error_count}, "
                            f"{exc_type}): {poll_error}\n"
                            f"{traceback.format_exc()}"
                        )
                    else:
                        log(
                            f"Poll unexpected error (#{consecutive_api_error_count}, "
                            f"{exc_type}): {poll_error}"
                        )
                # Short fixed backoff so we don't tight-loop on a persistent
                # API/code error, but don't escalate the network exponential
                # backoff — that timer is for connectivity loss only.
                if self._stop.wait(POLL_API_ERROR_BACKOFF_SECONDS):
                    return
