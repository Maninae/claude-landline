"""Per-batch update processing for the ``TelegramDaemon`` main loop.

Extracted from ``daemon.py`` in Wave 2 of the restructure. Every helper here is
a module-level function that takes the daemon coordinator as its first argument
(same first-arg pattern as ``landline.media.photo.process_photo_batch``). All
state stays on the daemon object — these functions read/write daemon attributes
directly and never own state of their own.

Patch-seam reach-back
---------------------
Test suites patch module-scope names on ``landline.orchestrator`` (e.g.
``patch("landline.orchestrator.save_state")``). The facade in
``landline/orchestrator/__init__.py`` delegates those writes to the
``daemon`` module's namespace. So this module resolves patchable names —
``save_state``, ``log_conversation``, ``drain_inject_queue``,
``classify_updates``, ``time``, ``INJECT_QUEUE_DIR`` — through the daemon
module at call time via the ``_d`` alias below. Importing them directly
(``from landline.runtime.state import save_state``) would bypass the seam
and silently break mocks.
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

# Late binding to daemon.py's namespace: patches like
# ``patch("landline.orchestrator.save_state")`` land as attribute writes on
# ``_d``, so ``_d.save_state(...)`` picks up the mock at call time.
from landline.orchestrator import daemon as _d


def process_update_batch(daemon, updates: List[Dict]) -> None:
    """Classify updates into commands, text, photos, voice notes,
    documents, and /pause, then process each group."""
    # Cluster 3: initialize the per-batch ack tracker. Classifier
    # populates chat_id → [message_id, ...] for every accepted
    # content message so dispatch can pass the ids to the dispatcher
    # for a completion 👌 on successful finalize. Cleared in the
    # finally so mid-batch exceptions can't leak state.
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
    """Inner body of ``process_update_batch`` — classification and
    the pass-2 dispatch. Extracted to keep the batch-tracker
    try/finally in ``process_update_batch`` tight and to preserve
    the original sequencing verbatim."""
    # Pass 1: classify all updates (trivial-skip side effects handled
    # inline by the classifier — cursor advance, reject, too-long notice).
    (
        command_updates,
        text_updates,
        photo_updates,
        pause_updates,
        document_updates,
        voice_updates,
    ) = _d.classify_updates(daemon, updates)

    # Pass 2: handle /pause with full knowledge of whether dispatch will
    # occur in this batch. When dispatch is pending we MUST NOT clear the
    # flag here — the watchdog needs to see it during the upcoming Claude
    # call so it can interrupt mid-stream.
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
    """Findings #1 / #2: if /pause was deferred in this batch (i.e.
    ``handle_pause_updates`` saw ``dispatch_pending=True`` and left
    ``_pause_requested`` set for the upcoming Claude call to consume)
    but NO Claude call actually ran this batch — locked-session
    gate, silent unlock, all-fail download, whisper timeout,
    unsupported doc, backoff-gated dispatch (queued for later
    drain), or an exception raised before ``_invoke_claude_call``
    — the flag would strand and the round-8 re-anchor logic in
    ``ClaudeDispatcher._invoke_claude_call`` (``if pf.is_set():
    pf.request_pause()``) would fire "(Paused.)" on the NEXT
    unrelated turn.

    Clear the flag here and, if the session isn't locked, notify the
    user their /pause didn't land on a running turn. When the session
    IS locked, ``check_lock_gate`` already sent a batch-coalesced
    LOCKED_HELP — sending "(Nothing to pause.)" on top of that would
    be double-noise, so stay silent in that path.

    Load-bearing invariant preserved: when a real Claude call
    actually ran (``send_to_claude`` returned True → set
    ``_batch_dispatch_attempted`` True), the dispatcher/watchdog/
    ``_finalize_response`` own the flag's lifecycle. This helper is
    a no-op in that path so the mocked "dispatched but not
    interrupted" test scenarios (e.g.
    ``test_pause_with_text_before_in_same_batch_does_NOT_send_
    nothing_to_pause``) continue to observe the flag still set on
    their MockClaude callback.
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
    """Log /pause, advance cursors, and (only when no dispatch is pending
    in this batch) notify + clear the flag.

    When dispatch IS pending, leave the flag set so the watchdog can
    consume it during the upcoming Claude call.

    Uses a `notified` guard to prevent N copies of LOCKED_HELP or "Nothing
    to pause" when multiple /pause updates arrive in the same batch.
    """
    notified = False
    for message, update_id, chat_id in pause_updates:
        _d.log_conversation(USER_NAME, "/pause")
        if dispatch_pending:
            daemon._deferred_pause_ids.append(update_id)
            # Record that /pause was deferred THIS batch so the
            # finally in ``run_batch_classification_and_dispatch``
            # can consume a stranded flag when dispatch never
            # actually reached Claude (Findings #1 / #2). Also
            # remember a chat_id so the cleanup notice targets the
            # same chat that typed /pause.
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
    # Local import avoids leaking a module-level dependency on batch_classifier
    # for the many callers that never see command_updates.
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
    """Check lock expiry and send LOCKED_HELP if locked.

    Advances cursors and returns True if the session is locked (caller
    should return early).  Returns False if unlocked (caller proceeds).

    Cross-type coalescing: if a LOCKED_HELP has already been sent to
    this ``chat_id`` earlier in the current batch (tracked on
    ``daemon._batch_locked_help_chats``), the send is suppressed but the
    cursor still advances — so a mixed batch (photo + voice + document
    + text) delivers exactly one LOCKED_HELP per chat instead of one
    per media bucket. Outside a batch (e.g. defensive callers that
    skip the tracker), the tracker is ``None`` and every call sends.
    """
    daemon._lock_manager.check_expiry()
    if not daemon._lock_manager.is_locked:
        return False
    already_sent = (
        isinstance(daemon._batch_locked_help_chats, set)
        and chat_id in daemon._batch_locked_help_chats
    )
    # Send LOCKED_HELP BEFORE advancing cursors. If the send raises, leave
    # the updates un-advanced so Telegram re-delivers them — the user must
    # not silently lose visibility that the session is locked.
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

    Inject files travel WITH the text through ``send_to_claude`` and are
    committed only after the message reaches a real Claude call. If
    ``send_to_claude`` stashes the text on the backoff queue (gated by
    failure backoff), the paths ride along on the queue tuple — so a
    daemon death mid-backoff doesn't drop morning briefs on the floor.

    Cluster 3: ``ack_message_ids`` names ONLY the Telegram message_ids
    actually being dispatched in this turn — passed by each media
    handler from the messages it's dispatching right now. On a
    successful finalize, ``ClaudeDispatcher`` fires 👌 on exactly
    those ids. This partitioning is load-bearing: mixed batches
    (voice+text, photo+text, multi-doc) each get their own dispatch,
    and a failed dispatch (e.g., whisper failure) never lets a later
    successful dispatch cross-pollinate 👌 onto messages that were
    never processed. The per-batch tracker
    (``_batch_ack_message_ids``) is populated by the classifier for
    observability/tests but is NOT read here; the caller owns which
    ids to ack for its own dispatch. Callers with no ids to ack
    (e.g. restart continuation) pass ``None`` / an empty list.

    Additionally, the given ids are popped from the per-batch tracker
    so any observability read reflects the remaining un-dispatched
    ids for the chat.
    """
    inject_prefix, consumed_paths = _d.drain_inject_queue(_d.INJECT_QUEUE_DIR)
    if inject_prefix:
        text = inject_prefix + "\n\n" + text
    ack_ids: List[int] = list(ack_message_ids or [])
    # Pop the dispatched ids off the per-batch tracker so a subsequent
    # dispatch in the same batch (e.g. text after voice) cannot see
    # them and re-👌 messages that don't belong to it.
    if isinstance(daemon._batch_ack_message_ids, dict) and ack_ids:
        remaining = [
            m for m in daemon._batch_ack_message_ids.get(chat_id, [])
            if m not in ack_ids
        ]
        if remaining:
            daemon._batch_ack_message_ids[chat_id] = remaining
        else:
            daemon._batch_ack_message_ids.pop(chat_id, None)
    # Ownership of ``_batch_dispatch_attempted``: flip True ONLY
    # when ``send_to_claude`` confirms it actually reached
    # ``_invoke_claude_call`` (return True). The watchdog +
    # ``_finalize_response`` are the ONLY consumers of the pause
    # flag downstream, and both only run inside that path. On the
    # backoff-gated path the dispatcher stashes the text on the
    # backoff queue and returns False WITHOUT invoking Claude — so
    # the pause flag would strand, and the round-8 re-anchor logic
    # in ``ClaudeDispatcher._invoke_claude_call`` (``if pf.is_set():
    # pf.request_pause()``) would fire "(Paused.)" on a FUTURE
    # unrelated turn (e.g. after backoff clears and the queued text
    # is drained into a fresh dispatch). Same story if
    # ``send_to_claude`` raises before Claude runs: the exception
    # propagates without setting the flag True, so
    # ``consume_stranded_pause_flag`` in
    # ``run_batch_classification_and_dispatch``'s finally can
    # clean up. The pre-fix design (set True before the call)
    # blocked both cleanup paths and stranded /pause across turns.
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

    # message_ids that the classifier fired 👀 on. Used to clear the
    # ack on rejection paths (lock gate, silent unlock) so 👀 never
    # lingers without a matching 👌.
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
            # The passphrase-typed-directly message is not dispatched
            # to Claude — clear its 👀 so it doesn't sit as an
            # unmatched ack.
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
        # Clear 👀 acks on all text messages so they don't linger as
        # false "accepted" signals under the locked-session rejection.
        if text_mids:
            reactions.set_reactions_batch_async(
                daemon.token, representative_chat_id, text_mids, None,
            )
        return

    coalesced_text = coalesce_messages(text_updates)

    for _, _, individual_text in text_updates:
        _d.log_conversation(USER_NAME, individual_text)

    # Cluster 3: ack only the message_ids being dispatched now — one
    # per accepted text message in this batch. Prevents 👌 leaking
    # onto messages that were dispatched in a different pass
    # (photo/voice/doc) whose success/failure semantics differ.
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
