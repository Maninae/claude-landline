"""Document handling — download dispatch + Claude prompt assembly.

Documents (PDFs, plain-text logs, JSON/YAML/CSV etc.) arrive as
``message.document`` envelopes. Unlike photos, Telegram does NOT group
documents by ``media_group_id`` — each document is a standalone update, so
there is no album coalescing here.

State lives on the ``TelegramDaemon`` coordinator. These helpers receive the
daemon as their first argument and reuse its lock gate, dispatcher, send
helpers, and cursor tracker.

Downloads route through ``landline.orchestrator.download_file`` so existing
tests that patch ``landline.orchestrator.download_file`` continue to work
without modification (mirrors the photo_handler pattern).
"""

from datetime import datetime
from typing import Dict, List, Tuple, TYPE_CHECKING

from landline.config import (
    DOCUMENT_ALLOWED_EXTENSIONS,
    DOCUMENT_MAX_SIZE_BYTES,
    TIMEZONE,
    TELEGRAM_FILE_DIR,
    USER_NAME,
)
from landline.runtime.logging import log
from landline.telegram import reactions
from landline.runtime.state import log_conversation
from landline.telegram.download import _safe_basename

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


def _clear_ack(daemon: "TelegramDaemon", chat_id: str, message: Dict) -> None:
    """Clear the classifier's 👀 ack on a message when a document bails
    out before dispatch (lock gate, unsafe basename, download failure)."""
    mid = message.get("message_id")
    if isinstance(mid, int):
        reactions.set_reaction_async(daemon.token, chat_id, mid, None)


def process_document_batch(
    daemon: "TelegramDaemon",
    document_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Download each document (standalone, no album coalescing) and dispatch
    to Claude with a ``[document: name, size, saved at: path]`` prompt.

    Batch-level lock gate coalesces LOCKED_HELP to one notice per batch
    (mirrors the text-batch behaviour) instead of one per document, and
    clears the classifier's 👀 acks on all document messages so they
    don't linger as false "accepted" signals.
    """
    if not document_updates:
        return
    chat_id = document_updates[0][2]
    all_update_ids = [uid for _, uid, _ in document_updates]
    if daemon._check_lock_gate(chat_id, all_update_ids):
        for message, _, _ in document_updates:
            _clear_ack(daemon, chat_id, message)
        return
    for message, update_id, chat_id in document_updates:
        dispatch_document(daemon, message, update_id, chat_id)


def _format_size(num_bytes: int) -> str:
    """Human-readable size string. Prefer MB for anything over 1MB, KB
    otherwise. Small enough to stay inline in the prompt."""
    if num_bytes >= 1024 * 1024:
        return "%.1f MB" % (num_bytes / (1024.0 * 1024.0))
    if num_bytes >= 1024:
        return "%.1f KB" % (num_bytes / 1024.0)
    return "%d B" % num_bytes


def dispatch_document(
    daemon: "TelegramDaemon",
    message: Dict,
    update_id: int,
    chat_id: str,
) -> None:
    """Download one document and hand it to Claude as a file path."""
    # Gate lock BEFORE any download — a locked session must never leave a
    # downloaded document sitting in cache/telegram_files/ forever.
    # process_document_batch already clears acks on the batch-level lock
    # gate, but a race can transition the session to locked between that
    # check and this per-item re-check — clear the 👀 here too so it
    # never lingers with no matching 👌.
    if daemon._check_lock_gate(chat_id, [update_id]):
        _clear_ack(daemon, chat_id, message)
        return

    # Late-import via the orchestrator module so test patches against
    # ``landline.orchestrator.download_file`` continue to apply. Mirrors the
    # photo_handler pattern.
    from landline import orchestrator as _orch

    document = message.get("document") or {}
    file_id = document.get("file_id", "")
    raw_name = document.get("file_name") or ""
    file_size = int(document.get("file_size") or 0)

    sanitized = _safe_basename(raw_name, DOCUMENT_ALLOWED_EXTENSIONS)
    if not sanitized:
        # Should be filtered by the classifier's _is_acceptable_document
        # gate; belt-and-suspenders in case somebody wires this in from a
        # different code path in the future.
        # PRIVACY: never log the raw filename — a rejected doc's name
        # (e.g. "private_medical_records.pdf" or an attacker-crafted
        # traversal string) still leaks through the classifier's reject
        # branch here. Metadata-only: chat_id + size are safe.
        log(
            "Document dispatch rejected — unsafe basename "
            "(chat=%s, size=%d bytes)" % (chat_id, file_size)
        )
        daemon._send_response(
            daemon.token, chat_id,
            "(That document type isn't supported.)",
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    # Filename on disk: `<timestamp>_<sanitized>` mirrors the photo pattern.
    ts = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
    filename = f"{ts}_{sanitized}"

    local_path = _orch.download_file(
        daemon.token,
        file_id,
        filename,
        target_dir=TELEGRAM_FILE_DIR,
        size_cap=DOCUMENT_MAX_SIZE_BYTES,
    )
    if not local_path:
        # PRIVACY: never log the sanitized filename — sensitive doc
        # names (e.g. "birth_certificate.pdf") would land in the
        # rotating daemon log and outlive the 0700 cache dir. Log
        # chat_id + size + mime metadata only, matching
        # voice_transcribe's discipline.
        mime_type = document.get("mime_type") or "unknown"
        log(
            "Document download failed (chat=%s, size=%d bytes, mime=%s)"
            % (chat_id, file_size, mime_type)
        )
        daemon._send_response(
            daemon.token, chat_id,
            "(Failed to download the document — please try again.)",
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    caption = message.get("caption")
    size_display = _format_size(file_size) if file_size else "unknown size"
    # PROMPT-INJECTION SAFETY: the filename is UNTRUSTED, attacker-
    # controlled content. ``_safe_basename`` blocks path traversal / NUL /
    # control chars but still allows brackets, commas, quotes, and angle
    # brackets in the stem — enough for an attacker to close the
    # ``[document: ...]`` fragment and inject a fake instruction into
    # Claude's context (e.g. ``invoice], [SYSTEM OVERRIDE: ....pdf``).
    # The on-disk ``local_path`` is derived from the same sanitized name
    # so it carries the same hostile characters.
    #
    # Mirror voice_handler.dispatch_voice: wrap the untrusted filename in
    # ``<document_filename>`` XML delimiters (Anthropic's recommended
    # framing) and escape any pre-existing close-tag inside so a hostile
    # filename cannot break out of the delimiter frame. The on-disk path
    # is DERIVED from the sanitized name (``<ts>_<sanitized>``) so it
    # carries the same attacker-influenced characters — wrap the path in
    # its own delimiter too and keep the outer ``[document: ...]`` line
    # to trusted metadata only (size), so the attacker has no way to
    # break out into Claude's instruction stream.
    #
    # ``_safe_basename`` currently strips any ``/`` from the raw name
    # (``os.path.basename`` split point), which makes ``</document_...>``
    # unreachable through the file_name field today. The escape is
    # defense-in-depth against future sanitizer changes and a stricter
    # mirror of voice_handler discipline.
    safe_name = sanitized.replace(
        "</document_filename>", "</document_filename_escaped>",
    )
    safe_path = local_path.replace(
        "</document_path>", "</document_path_escaped>",
    )
    path_section = (
        f"[document: {size_display}]\n"
        f"<document_filename>{safe_name}</document_filename>\n"
        f"<document_path>{safe_path}</document_path>"
    )
    if caption:
        prompt_text = f"{caption}\n\n{path_section}"
    else:
        prompt_text = f"{USER_NAME} sent a document:\n\n{path_section}"

    # PRIVACY: daemon log is metadata-only — chat_id + size. The
    # sanitized filename (which can be a sensitive user-supplied name
    # like "private_medical_records.pdf") stays out of the rotating
    # daemon.log. log_conversation writes to memory/daily/, which is
    # 0600 and inside the 0700 daily dir — a fundamentally different
    # trust boundary than the daemon log — so the filename is fine
    # there (it's part of the actual conversation transcript). Use the
    # same delimited shape as the prompt so the recent-dialogue replay
    # in a fresh session keeps the injection guard intact.
    log(
        "Document prompt: chat=%s size=%s"
        % (chat_id, size_display)
    )
    log_conversation(
        USER_NAME,
        f"[document] <document_filename>{safe_name}</document_filename>",
    )

    # Cluster 3: ack ONLY this document's message_id — never the batch's
    # union. In a multi-document batch each document dispatches
    # separately, so partitioning here guarantees 👌 lands on the doc
    # that actually finalized, not on later docs still queued.
    mid = message.get("message_id")
    ack_ids: List[int] = [mid] if isinstance(mid, int) else []
    daemon._inject_and_dispatch(
        prompt_text, chat_id, [update_id], ack_message_ids=ack_ids,
    )
