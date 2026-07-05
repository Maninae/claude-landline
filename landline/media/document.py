"""Document handling — download dispatch + Claude prompt assembly.

- Documents arrive as ``message.document`` envelopes; Telegram does NOT
  group them by ``media_group_id``, so no album coalescing here.
- State lives on the ``TelegramDaemon`` coordinator; helpers receive it as
  the first arg and reuse its lock gate, dispatcher, send helpers, cursor.
- Downloads route through ``landline.orchestrator.download_file`` so test
  patches apply (mirrors ``photo.py``).

Prompt-injection safety (load-bearing):

- Filenames are UNTRUSTED, attacker-controlled. ``_safe_basename`` blocks
  path traversal / NUL / control chars but still allows brackets, commas,
  quotes, and angle brackets — enough to close a ``[document: …]`` fragment
  and inject a fake instruction (``invoice], [SYSTEM OVERRIDE: ….pdf``).
- On-disk paths derive from the sanitized name, so they carry the same
  hostile chars. Both filename and path are wrapped in dedicated XML
  delimiters (``<document_filename>``, ``<document_path>``), with any
  pre-existing close-tag inside escaped. The outer ``[document: …]``
  line is kept metadata-only (size).
- ``_safe_basename`` today strips ``/`` via ``os.path.basename`` so
  ``</document_...>`` is unreachable through ``file_name`` — the escape is
  defense-in-depth against future sanitizer changes.
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
    """Clear the 👀 ack when a document bails before dispatch."""
    mid = message.get("message_id")
    if isinstance(mid, int):
        reactions.set_reaction_async(daemon.token, chat_id, mid, None)


def process_document_batch(
    daemon: "TelegramDaemon",
    document_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Download each document and dispatch to Claude with an XML-delimited prompt.

    Batch-level lock gate coalesces LOCKED_HELP to one notice per batch
    (mirrors text-batch), and clears 👀 on all documents when bailing.
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
    """Human-readable size string; inline-safe for the prompt."""
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
    # Per-item lock re-check: batch-level check may have raced with a
    # lock transition. Clear 👀 on rejection.
    if daemon._check_lock_gate(chat_id, [update_id]):
        _clear_ack(daemon, chat_id, message)
        return

    # Late import so ``landline.orchestrator.download_file`` test patches apply.
    from landline import orchestrator as _orch

    document = message.get("document") or {}
    file_id = document.get("file_id", "")
    raw_name = document.get("file_name") or ""
    file_size = int(document.get("file_size") or 0)

    sanitized = _safe_basename(raw_name, DOCUMENT_ALLOWED_EXTENSIONS)
    if not sanitized:
        # Belt-and-suspenders (classifier should filter first).
        # PRIVACY: NEVER log the raw filename — chat_id + size only.
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

    # On-disk filename ``<ts>_<sanitized>`` mirrors the photo pattern.
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
        # PRIVACY: chat_id + size + mime only — sanitized names outlive
        # the 0700 cache in the rotating daemon log.
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
    # Prompt-injection framing — see module docstring for full rationale.
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

    # PRIVACY: daemon.log stays metadata-only (chat_id + size). The
    # sanitized filename is fine in memory/daily/ (0600 file, 0700 dir —
    # a different trust boundary); use the same delimited shape as the
    # prompt so a fresh-session dialogue replay keeps the injection guard.
    log(
        "Document prompt: chat=%s size=%s"
        % (chat_id, size_display)
    )
    log_conversation(
        USER_NAME,
        f"[document] <document_filename>{safe_name}</document_filename>",
    )

    # Ack ONLY this document's message_id — never the batch union.
    # Each doc dispatches separately, so 👌 lands on the finalized doc,
    # not on later docs still queued.
    mid = message.get("message_id")
    ack_ids: List[int] = [mid] if isinstance(mid, int) else []
    daemon._inject_and_dispatch(
        prompt_text, chat_id, [update_id], ack_message_ids=ack_ids,
    )
