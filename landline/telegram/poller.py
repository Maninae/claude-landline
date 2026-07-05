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
from landline.runtime.logging import log
from landline.runtime.notifications import send_network_alert


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
    """Long-poll Telegram updates on a background thread; queue for main.

    - Decouples polling from processing: updates arriving during a Claude
      call accumulate in memory instead of waiting on Telegram's servers.
    - Main thread drains and coalesces into one batch per Claude call.
    - Crash safety: the polling offset tracks the last PROCESSED update id
      (not the last fetched), so unprocessed updates are re-fetched every
      cycle while in flight; a dedup set blocks re-queueing. On crash,
      Telegram re-delivers everything past the persisted cursor.
    """

    def __init__(
        self,
        token: str,
        initial_last_processed_update_id: int,
        on_update_queued: Optional[Callable[[Dict], None]] = None,
    ) -> None:
        """Construct a BackgroundPoller.

        Args:
            on_update_queued: optional callback invoked synchronously on the
                poller thread after each enqueue. MUST be O(1) and non-blocking;
                exceptions are swallowed and do NOT affect poll error counting.
        """
        self.token = token
        self._incoming_updates_queue: "queue.Queue[Dict]" = queue.Queue()
        self._last_processed_update_id = initial_last_processed_update_id
        self._last_processed_update_id_lock = threading.Lock()
        # OrderedDict-as-set: keys=update_ids, capped at MAX_DEDUP_IDS with
        # oldest-first FIFO eviction. Bounds memory; long-term dedup lives
        # on the persisted cursor.
        self._already_queued_update_ids: "OrderedDict[int, None]" = OrderedDict()
        self._already_queued_update_ids_lock = threading.Lock()
        # Latched: log the cap-reached event exactly once per replay storm.
        # Cleared in _poll_loop once the set drops back below the cap.
        self._dedup_cap_reached_logged = False
        self._on_update_queued = on_update_queued
        self._stop = threading.Event()
        # Silent-TCP-stall detector: last successful get_updates timestamp
        # (empty results count — an empty long-poll return proves the socket
        # round-tripped). Seeded to construction time so a fresh poller isn't
        # immediately flagged stale. Read via ``last_successful_poll()``.
        self._last_successful_poll_at: float = time.time()
        self._last_successful_poll_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="background-poller",
        )

    def start(self) -> None:
        # Reseed at start() (not __init__) so a long gap before .start()
        # (e.g. multi-minute restart-continuation turn) can't make the first
        # liveness check misread this fresh poller as stale. Poller-swap
        # replacements also benefit.
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
        """Wall-clock timestamp of the last successful get_updates return
        (empty results included). Read by the main loop for stall detection.
        """
        with self._last_successful_poll_lock:
            return self._last_successful_poll_at

    def snapshot_dedup_ids(self) -> List[int]:
        """Ordered snapshot of the dedup set for handoff to a replacement poller.

        Insertion order is preserved so cap-based FIFO eviction carries across.
        """
        with self._already_queued_update_ids_lock:
            return list(self._already_queued_update_ids.keys())

    def load_dedup_ids(self, ids: Iterable[int]) -> None:
        """Hydrate the dedup set from a snapshot (in-process poller replacement).

        Preserves the ``MAX_DEDUP_IDS`` cap by evicting oldest on overshoot.
        """
        with self._already_queued_update_ids_lock:
            for uid in ids:
                self._already_queued_update_ids[uid] = None
                while len(self._already_queued_update_ids) > MAX_DEDUP_IDS:
                    self._already_queued_update_ids.popitem(last=False)

    def advance_processed_cursor(self, update_id: int) -> None:
        """Advance the processed cursor after the main thread persists state.

        Args:
            update_id: id past which the poller's next offset should skip.

        - Invariant: the dedup set is NEVER pruned here. An in-flight long
          poll started with the OLD offset can return already-processed ids;
          pruning them would let the poller re-queue and double-process.
          Memory stays bounded via the ``MAX_DEDUP_IDS`` FIFO eviction in
          ``__init__`` — long-term dedup lives on the persisted cursor.
        """
        with self._last_processed_update_id_lock:
            if update_id > self._last_processed_update_id:
                self._last_processed_update_id = update_id

    def discard_queued_ids(self, update_ids: Iterable[int]) -> None:
        """Remove ids from the dedup set so a future poll can re-queue them.

        Used to recover updates drained from the queue but NOT processed
        (e.g. batch error). Safe because these ids were never processed —
        the no-prune invariant in ``advance_processed_cursor`` only guards
        already-processed ids.
        """
        with self._already_queued_update_ids_lock:
            for update_id in update_ids:
                self._already_queued_update_ids.pop(update_id, None)

    def preload_queue(self, updates: Iterable[Dict]) -> int:
        """Forward updates from an old poller's queue during in-process swap.

        Dedup is already loaded via ``load_dedup_ids``; if we didn't forward
        the actual payloads too, they'd orphan in the old queue and the new
        poller's dedup would block re-delivery. Returns count enqueued.
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
        """Long-poll loop; drives the network-outage detector and backoff.

        - Network errors (``URLError``/``socket.timeout``/``TimeoutError``/
          ``OSError``, incl. Telegram 429 via ``HTTPError``) drive the outage
          timer, the iMessage alert, and exponential backoff. 429 honors
          ``Retry-After`` (header → body → current backoff).
        - Non-network errors (RuntimeError from a not-ok API response, or
          code bugs like KeyError/TypeError) MUST NOT touch the outage
          timer — those signals stay meaningful. They get a short fixed
          backoff and a throttled log volume; the first bug traceback is
          logged in full, subsequent instances are one-line.
        - Success clears both counters and the dedup-cap latch (so a
          recurring storm re-fires the cap log on its next first-hit).
        """
        consecutive_error_count = 0
        consecutive_api_error_count = 0  # independent of the network counter
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
                        # Log once when the cap is first reached this storm;
                        # latch cleared below after the set drops back below cap.
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
                        # Cap-bounded FIFO eviction (older ids are past the cursor).
                        while len(self._already_queued_update_ids) > MAX_DEDUP_IDS:
                            self._already_queued_update_ids.popitem(last=False)
                        # queue.put INSIDE the dedup lock so a concurrent
                        # snapshot_dedup_ids() sees BOTH dedup entry AND
                        # queued update, or neither. Otherwise a swap could
                        # load the id but orphan the payload → re-delivery
                        # silently dropped.
                        self._incoming_updates_queue.put(update)
                    # Isolated try/except — callback exceptions MUST NOT touch
                    # poll error counting or outage alerting.
                    if self._on_update_queued is not None:
                        try:
                            self._on_update_queued(update)
                        except Exception:
                            pass

                # Record success on EVERY return (empty results count — an
                # empty long-poll return proves the socket round-tripped).
                with self._last_successful_poll_lock:
                    self._last_successful_poll_at = time.time()

                if consecutive_error_count > 0:
                    outage_duration = time.time() - (network_failure_start_time or time.time())
                    log(f"Network recovered after {int(outage_duration)}s outage ({consecutive_error_count} errors)")
                    consecutive_error_count = 0
                    current_backoff_seconds = POLL_ERROR_BACKOFF_BASE
                    network_failure_start_time = None
                    imsg_alert_sent = False

                # Success clears the API-error throttle (so the next fault
                # logs its first traceback) and re-arms the dedup-cap latch.
                if consecutive_api_error_count > 0:
                    consecutive_api_error_count = 0
                if (
                    self._dedup_cap_reached_logged
                    and len(self._already_queued_update_ids) < MAX_DEDUP_IDS
                ):
                    self._dedup_cap_reached_logged = False

            except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as poll_error:
                # Network-layer failure branch. See ``_poll_loop`` docstring
                # for the network-vs-code-error partition.
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

                # Telegram 429 (Too Many Requests) rides through as HTTPError:
                # honor Retry-After (header → JSON body → current backoff)
                # and sleep the max so we never return sooner than asked.
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
                # Non-network / code-bug branch. See ``_poll_loop`` docstring
                # for why this MUST NOT touch the outage timer.
                # Throttled log: 1st + every Nth. Non-RuntimeError code bugs
                # get the full traceback on the FIRST hit only.
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
                # Fixed backoff (no exponential escalation — that's the
                # connectivity-loss lever, not this branch).
                if self._stop.wait(POLL_API_ERROR_BACKOFF_SECONDS):
                    return
