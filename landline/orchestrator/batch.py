"""Per-batch update processing for the ``TelegramDaemon`` main loop.

- Each helper takes the daemon coordinator as its first arg; all state stays
  on the daemon object (helpers read/write daemon attributes directly).
- Patchable module-scope names (``save_state``, ``log_conversation``,
  ``drain_inject_queue``, ``classify_updates``, ``time``, ``INJECT_QUEUE_DIR``)
  resolve through the ``_d`` alias so ``patch("landline.orchestrator.<name>")``
  lands as an attribute write on ``daemon`` and the mock is seen at call time.
  Importing them directly would bypass the seam.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from landline.config import (
    LOCKED_HELP,
    TIMEZONE,
    USER_NAME,
)
from landline.runtime.logging import log
from landline.telegram import reactions

# Late binding to daemon.py's namespace — see module docstring.
from landline.orchestrator import daemon as _d


def process_update_batch(daemon, updates: List[Dict]) -> None:
    """Classify + process one drained batch (commands/text/photo/voice/doc/pause).

    Initializes per-batch trackers (👀→👌 ids, LOCKED_HELP coalescing set,
    /pause deferral state) and clears them in ``finally`` so a mid-batch
    exception can't leak state into the next batch.
    """
    daemon._batch_ack_message_ids = {}
    daemon._batch_locked_help_chats = set()
    daemon._batch_pause_was_deferred = False
    daemon._batch_dispatch_attempted = False
    daemon._batch_pause_notify_chat = None
    try:
        run_batch_classification_and_dispatch(daemon, updates)
    finally:
        daemon._batch_ack_message_ids = None
        daemon._batch_locked_help_chats = None
        daemon._batch_pause_was_deferred = False
        daemon._batch_dispatch_attempted = False
        daemon._batch_pause_notify_chat = None


def run_batch_classification_and_dispatch(
    daemon, updates: List[Dict],
) -> None:
    """Inner body of ``process_update_batch`` — pass 1 classify, pass 2 dispatch.

    Sequencing is verbatim from the pre-decomposition daemon; the split exists
    to keep the batch-tracker try/finally in ``process_update_batch`` tight.
    """
    # Pass 1: trivial-skip side effects (cursor advance, reject, too-long
    # notice) are handled inline by the classifier.
    (
        command_updates,
        text_updates,
        photo_updates,
        pause_updates,
        document_updates,
        voice_updates,
    ) = _d.classify_updates(daemon, updates)

    # Pass 2: /pause first, so we know whether dispatch will run this batch.
    # Dispatch-pending MUST leave the flag set — the watchdog consumes it
    # mid-stream during the upcoming Claude call.
    handle_pause_updates(
        daemon,
        pause_updates,
        dispatch_pending=(
            bool(text_updates)
            or bool(photo_updates)
            or bool(document_updates)
            or bool(voice_updates)
        ),
    )

    process_commands(daemon, command_updates)
    try:
        if photo_updates and daemon.running:
            daemon._process_photo_batch(photo_updates)
        if voice_updates and daemon.running:
            daemon._process_voice_batch(voice_updates)
        if document_updates and daemon.running:
            daemon._process_document_batch(document_updates)
        if text_updates and daemon.running:
            process_text_batch(daemon, text_updates)
    finally:
        daemon._drain_deferred_pause_ids()
        consume_stranded_pause_flag(daemon)


def consume_stranded_pause_flag(daemon) -> None:
    """Clear a /pause flag deferred this batch when no Claude call ever ran.

    Called from ``run_batch_classification_and_dispatch``'s finally. Silent
    no-op unless the batch both deferred a /pause AND never invoked Claude.

    - Deferral leaves ``_pause_requested`` set for the upcoming Claude call
      to consume (watchdog interrupt → cleared in ``_finalize_response``).
    - If no Claude call ran (locked gate, silent unlock, all-fail download,
      whisper timeout, unsupported doc, backoff-gated dispatch, or an
      exception before ``_invoke_claude_call``), the flag would strand and
      the re-anchor in ``ClaudeDispatcher._invoke_claude_call`` would fire
      "(Paused.)" on the NEXT unrelated turn.
    - Locked-session path stays silent — ``check_lock_gate`` already sent a
      batch-coalesced LOCKED_HELP; a "(Nothing to pause.)" on top is noise.
    - Load-bearing no-op when a real Claude call ran (``send_to_claude``
      returned True → ``_batch_dispatch_attempted`` True): dispatcher /
      watchdog / ``_finalize_response`` own the flag's lifecycle there.

    See docs/ARCHITECTURE.md "Deferred /pause — stranded flag".
    """
    if not daemon._batch_pause_was_deferred:
        return
    if daemon._batch_dispatch_attempted:
        return
    if not daemon._pause_requested.is_set():
        return
    daemon._pause_requested.clear()
    if daemon._lock_manager.is_locked:
        return
    notice_chat = daemon._batch_pause_notify_chat or daemon.chat_id
    try:
        daemon._send_response(
            daemon.token, notice_chat, "(Nothing to pause.)",
        )
    except Exception as pause_notify_error:
        log(
            "Failed to send deferred /pause notice: %s"
            % pause_notify_error,
        )


def handle_pause_updates(
    daemon,
    pause_updates: List[Tuple[Dict, int, str]],
    dispatch_pending: bool,
) -> None:
    """Log /pause, advance cursors, and (only when no dispatch is pending)
    notify + clear the flag.

    Args:
        pause_updates: /pause messages this batch.
        dispatch_pending: True if the batch will invoke Claude — leaves the
            flag set so the watchdog can consume it mid-stream.

    - ``notified`` guard: at most one LOCKED_HELP or "Nothing to pause" per
      batch even when multiple /pause updates arrive together.
    - Deferred path records ``_batch_pause_was_deferred`` + a notify chat so
      the batch's finally can clean up a stranded flag; see
      ``consume_stranded_pause_flag``.
    """
    notified = False
    for message, update_id, chat_id in pause_updates:
        _d.log_conversation(USER_NAME, "/pause")
        if dispatch_pending:
            daemon._deferred_pause_ids.append(update_id)
            daemon._batch_pause_was_deferred = True
            if daemon._batch_pause_notify_chat is None:
                daemon._batch_pause_notify_chat = chat_id
        else:
            daemon._advance_update_cursor(update_id)

        if dispatch_pending or notified:
            continue

        if daemon._lock_manager.is_locked:
            daemon._send_response(daemon.token, chat_id, LOCKED_HELP)
            daemon._pause_requested.clear()
            notified = True
        elif daemon._pause_requested.is_set():
            daemon._pause_requested.clear()
            daemon._send_response(daemon.token, chat_id, "(Nothing to pause.)")
            notified = True
        # else: flag already consumed (e.g. by _finalize_response) — silent.


def process_commands(
    daemon, command_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Process slash commands in order."""
    # Local import — avoids a module-level dep on batch_classifier for callers
    # that never see command_updates.
    from landline.runtime.batch_classifier import extract_chat_id

    for message, update_id, text in command_updates:
        if not daemon.running:
            break
        chat_id = extract_chat_id(message)
        response = daemon._command_router.handle(text)
        if response is not None:
            daemon._send_response(daemon.token, chat_id, response)
        daemon._advance_update_cursor(update_id)
        daemon._dispatcher.last_process_time = _d.time.time()


def check_lock_gate(daemon, chat_id: str, update_ids: List[int]) -> bool:
    """Check lock expiry, send LOCKED_HELP if locked, advance cursors.

    Returns:
        True if locked (caller should return early), False if unlocked.

    - Cross-type coalescing via ``daemon._batch_locked_help_chats``: a mixed
      (photo+voice+doc+text) batch delivers one LOCKED_HELP per chat, not
      one per media bucket. Outside a batch the tracker is None and every
      call sends.
    - Send BEFORE advancing cursors so a raised send leaves updates
      un-advanced for Telegram re-delivery (user must not silently lose
      visibility that the session is locked).
    """
    daemon._lock_manager.check_expiry()
    if not daemon._lock_manager.is_locked:
        return False
    already_sent = (
        isinstance(daemon._batch_locked_help_chats, set)
        and chat_id in daemon._batch_locked_help_chats
    )
    if not already_sent:
        try:
            daemon._send_response(daemon.token, chat_id, LOCKED_HELP)
        except Exception as notify_error:
            log(
                f"Failed to send LOCKED_HELP for {len(update_ids)} update(s) — "
                f"leaving un-advanced for Telegram re-delivery: {notify_error}"
            )
            return True
        if isinstance(daemon._batch_locked_help_chats, set):
            daemon._batch_locked_help_chats.add(chat_id)
    for uid in update_ids:
        daemon._advance_update_cursor(uid)
    daemon._dispatcher.last_process_time = _d.time.time()
    return True


def inject_and_dispatch(
    daemon,
    text: str,
    chat_id: str,
    update_ids: List[int],
    ack_message_ids: Optional[List[int]] = None,
) -> None:
    """Drain inject queue, prepend context, dispatch to Claude, advance cursors.

    Args:
        ack_message_ids: ONLY the message_ids being dispatched in THIS turn
            (caller's dispatch, not the per-batch tracker). ``ClaudeDispatcher``
            fires 👌 on exactly these on successful finalize. Callers with
            nothing to ack (e.g. restart continuation) pass None/[].

    - Inject files travel WITH the text through ``send_to_claude`` and are
      committed only after the message reaches a real Claude call; on the
      backoff-queue path the paths ride the queue tuple (crash mid-backoff
      doesn't drop morning briefs).
    - Ack partitioning is load-bearing: mixed batches (voice+text, photo+text,
      multi-doc) each get their own dispatch — a failed dispatch never
      cross-pollinates 👌 onto messages that were never processed. See
      docs/ARCHITECTURE.md "Reactions — 👀 → 👌 invariant".
    - ``_batch_dispatch_attempted`` flips True ONLY when ``send_to_claude``
      confirms it reached ``_invoke_claude_call``. Backoff-gated / pre-Claude
      exception paths return False so ``consume_stranded_pause_flag`` can
      clean up a stranded /pause (setting True too early stranded the flag
      across turns — the bug this ordering exists to prevent).
    """
    inject_prefix, consumed_paths = _d.drain_inject_queue(_d.INJECT_QUEUE_DIR)
    if inject_prefix:
        text = inject_prefix + "\n\n" + text
    ack_ids: List[int] = list(ack_message_ids or [])
    # Pop dispatched ids off the per-batch tracker so a subsequent dispatch
    # in the same batch (text after voice) can't re-👌 them.
    if isinstance(daemon._batch_ack_message_ids, dict) and ack_ids:
        remaining = [
            m for m in daemon._batch_ack_message_ids.get(chat_id, [])
            if m not in ack_ids
        ]
        if remaining:
            daemon._batch_ack_message_ids[chat_id] = remaining
        else:
            daemon._batch_ack_message_ids.pop(chat_id, None)
    claude_invoked = daemon._dispatcher.send_to_claude(
        text, chat_id,
        consumed_paths=consumed_paths,
        ack_message_ids=ack_ids,
    )
    daemon._batch_dispatch_attempted = bool(claude_invoked)
    for uid in update_ids:
        daemon._advance_update_cursor(uid)


def process_text_batch(
    daemon, text_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Coalesce text messages and dispatch to Claude."""
    from landline.runtime.batch_classifier import extract_chat_id

    representative_chat_id = extract_chat_id(text_updates[0][0])
    update_ids = [uid for _, uid, _ in text_updates]

    # message_ids the classifier 👀'd. Cleared on rejection (lock gate, silent
    # unlock) so 👀 never lingers without a matching 👌.
    text_mids: List[int] = [
        msg.get("message_id")
        for msg, _, _ in text_updates
        if isinstance(msg.get("message_id"), int)
    ]

    if daemon._lock_manager.is_locked and len(text_updates) == 1:
        raw_text = text_updates[0][2]
        if daemon._lock_manager.try_silent_unlock(raw_text.strip()):
            daemon._send_response(
                daemon.token, representative_chat_id,
                "Unlocked. Send a message to begin.",
            )
            # Passphrase-typed-directly isn't dispatched — clear its 👀.
            if text_mids:
                reactions.set_reactions_batch_async(
                    daemon.token, representative_chat_id, text_mids, None,
                )
            for uid in update_ids:
                daemon._advance_update_cursor(uid)
            daemon._dispatcher.last_process_time = _d.time.time()
            return

    if check_lock_gate(daemon, representative_chat_id, update_ids):
        log(f"Sending LOCKED_HELP for batch of {len(text_updates)} text message(s)")
        # Clear 👀 so it doesn't linger as a false "accepted" signal.
        if text_mids:
            reactions.set_reactions_batch_async(
                daemon.token, representative_chat_id, text_mids, None,
            )
        return

    coalesced_text = coalesce_messages(text_updates)

    for _, _, individual_text in text_updates:
        _d.log_conversation(USER_NAME, individual_text)

    # Ack only the ids being dispatched now — prevents 👌 leaking onto
    # messages dispatched in a different pass (photo/voice/doc).
    text_ack_ids: List[int] = [
        msg.get("message_id")
        for msg, _, _ in text_updates
        if isinstance(msg.get("message_id"), int)
    ]
    inject_and_dispatch(
        daemon,
        coalesced_text,
        representative_chat_id,
        update_ids,
        ack_message_ids=text_ack_ids,
    )


def coalesce_messages(
    text_updates: List[Tuple[Dict, int, str]],
) -> str:
    """Format one or more messages into a single Claude input."""
    def _fmt_ts(message: Dict) -> str:
        unix_ts = message.get("date")
        if unix_ts:
            dt = datetime.fromtimestamp(unix_ts, tz=TIMEZONE)
            return dt.strftime("%Y-%m-%d %H:%M %Z")
        return ""

    if len(text_updates) == 1:
        message, _, text = text_updates[0]
        ts = _fmt_ts(message)
        return f"[{ts}]\n{text}" if ts else text

    formatted_parts: List[str] = []
    for message_index, (message, _, text) in enumerate(text_updates, 1):
        ts = _fmt_ts(message)
        header = f"[message {message_index}]" + (f" [{ts}]" if ts else "")
        formatted_parts.append(f"{header}\n{text}")
    log(f"Coalesced {len(text_updates)} messages into one turn")
    return "\n---\n".join(formatted_parts)
