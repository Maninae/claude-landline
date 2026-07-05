"""Update-batch classification — split drained Telegram updates into
command / text / photo / pause / document / voice buckets.

- Stateless helper: `classify_updates(daemon, updates)` walks once, handles
  trivial-skip side effects inline (cursor advance for edits, missing chat.id,
  unauthorized chats, non-text/non-photo media, too-long text), and returns the
  buckets for the dispatch passes.
- State lives on the `TelegramDaemon` coordinator; this module mutates only
  the daemon's per-batch tracker via `_advance_update_cursor`.
- `BackgroundPoller` requests `allowed_updates=["message"]` so callback queries
  / edited_channel_post / inline_query never reach here. If that filter is
  loosened, update this module alongside it (test_poller fails first).
"""

from typing import Dict, List, TYPE_CHECKING, Tuple

from landline import config as _config
from landline.config import (
    DOCUMENT_ALLOWED_EXTENSIONS,
    DOCUMENT_ALLOWED_MIME_PREFIXES,
    DOCUMENT_MAX_SIZE_BYTES,
    MAX_MESSAGE_LENGTH,
    VOICE_ACCEPT_TYPES,
)
from landline.runtime.logging import log
from landline.telegram import reactions
from landline.telegram.download import _safe_basename

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


def _is_pause_command(text: str) -> bool:
    """True if `text` (already stripped + lowercased) is the /pause command,
    with or without a trailing argument (e.g. '/pause' or '/pause now')."""
    return text == "/pause" or text.startswith("/pause ")


def extract_chat_id(message: Dict, default: str = "") -> str:
    """Pull `str(chat.id)` from a Telegram message dict, defaulting on miss.

    - Centralizes the defensive two-level `.get("chat", {}).get("id", ...)`
      walk + `str(...)` coercion the rest of the daemon depends on
      (allowlist cache key, lock checks, log fields).
    - Always returns a `str`; `default` on missing chat / chat.id.
    """
    return str(message.get("chat", {}).get("id", default))


def _is_acceptable_document(document_field: Dict) -> bool:
    """True iff the Telegram document envelope passes ingestion gates.

    Rejects on missing filename, unsafe basename, disallowed extension,
    over-cap size, or mismatched mime (when present). Extension is the primary
    gate; mime is belt-and-suspenders and absent mime does NOT block.
    """
    file_name = document_field.get("file_name")
    if not isinstance(file_name, str) or not file_name:
        return False
    if _safe_basename(file_name, DOCUMENT_ALLOWED_EXTENSIONS) is None:
        return False
    file_size = document_field.get("file_size", 0)
    try:
        if file_size and int(file_size) > DOCUMENT_MAX_SIZE_BYTES:
            return False
    except (TypeError, ValueError):
        return False
    mime_type = document_field.get("mime_type")
    if isinstance(mime_type, str) and mime_type:
        mt = mime_type.lower()
        if not any(mt.startswith(p) for p in DOCUMENT_ALLOWED_MIME_PREFIXES):
            return False
    return True


def classify_updates(
    daemon: "TelegramDaemon",
    updates: List[Dict],
) -> Tuple[
    List[Tuple[Dict, int, str]],  # command_updates (message, update_id, text)
    List[Tuple[Dict, int, str]],  # text_updates (message, update_id, text)
    List[Tuple[Dict, int, str]],  # photo_updates (message, update_id, chat_id)
    List[Tuple[Dict, int, str]],  # pause_updates (message, update_id, chat_id)
    List[Tuple[Dict, int, str]],  # document_updates (message, update_id, chat_id)
    List[Tuple[Dict, int, str]],  # voice_updates (message, update_id, chat_id)
]:
    """Pass 1 of `_process_update_batch`: classify each update; trivial-skip
    side effects (cursor advance, reject, too-long notice) run inline.

    - `/pause` is intercepted BEFORE the `/`-prefix branch so it never reaches
      `CommandRouter` (which would reply "Unknown command").
    - Returns the six classified buckets in dispatch-pass order.
    """
    command_updates: List[Tuple[Dict, int, str]] = []
    text_updates: List[Tuple[Dict, int, str]] = []
    photo_updates: List[Tuple[Dict, int, str]] = []
    pause_updates: List[Tuple[Dict, int, str]] = []
    document_updates: List[Tuple[Dict, int, str]] = []
    voice_updates: List[Tuple[Dict, int, str]] = []

    def _ack_and_record(msg: Dict, chat: str) -> None:
        """Fire 👀 for accepted content messages, record the message_id on the
        daemon's per-batch tracker so the dispatcher can fire 👌 at finalize.

        - Only called AFTER the guard passes — a reaction to an unauthorized
          sender would be an enumeration oracle.
        - Never called for /pause (control) or slash commands (text-rendered,
          no receipt semantics).
        """
        mid = msg.get("message_id")
        if not isinstance(mid, int):
            return
        reactions.set_reaction_async(
            daemon.token, chat, mid, _config.REACTION_ACK_EMOJI,
        )
        tracker = getattr(daemon, "_batch_ack_message_ids", None)
        if isinstance(tracker, dict):
            tracker.setdefault(chat, []).append(mid)

    for update in updates:
        if not daemon.running:
            break
        update_id = update.get("update_id", 0)

        message = update.get("message")

        if not message or "edit_date" in message:
            daemon._advance_update_cursor(update_id)
            continue

        chat_id = extract_chat_id(message)
        if not chat_id:
            log(f"Dropping message with missing chat.id: update fields={list(message.keys())}")
            daemon._advance_update_cursor(update_id)
            continue

        if not daemon._guard_fn(chat_id):
            log(f"Rejected message from unauthorized chat: {chat_id}")
            daemon._advance_update_cursor(update_id)
            daemon._reject_fn(daemon.token, chat_id)
            continue

        # Check for photo messages first (they may also have text as caption)
        if "photo" in message:
            _ack_and_record(message, chat_id)
            photo_updates.append((message, update_id, chat_id))
            continue

        # Voice / audio / video_note: classifier only buckets; the handler
        # enforces duration limits, picks a filename, and sanitizes any
        # advertised name via `_voice_filename`.
        voice_key = next(
            (k for k in ("voice", "audio", "video_note") if k in message),
            None,
        )
        if voice_key is not None and voice_key in VOICE_ACCEPT_TYPES:
            _ack_and_record(message, chat_id)
            voice_updates.append((message, update_id, chat_id))
            continue

        # Document ingestion: extension-allow-listed, size-capped,
        # path-traversal-safe. Rejected documents fall through to the
        # generic non-text notice.
        if "document" in message:
            document_field = message.get("document") or {}
            if isinstance(document_field, dict) and _is_acceptable_document(
                document_field
            ):
                _ack_and_record(message, chat_id)
                document_updates.append((message, update_id, chat_id))
                continue
            # PRIVACY: never log the attacker-controlled `file_name` — it
            # would land in the rotating daemon log verbatim. Metadata only.
            log(
                "Rejecting document from chat %s: size=%r mime=%r" % (
                    chat_id,
                    document_field.get("file_size"),
                    document_field.get("mime_type"),
                )
            )
            daemon._handle_non_text_update(message, update_id, chat_id)
            continue

        text = message.get("text")
        if not text:
            daemon._handle_non_text_update(message, update_id, chat_id)
            continue

        stripped_lower = text.strip().lower()

        # /pause intercepted BEFORE the `/`-prefix branch — CommandRouter
        # would else reply "Unknown command". Accepts `/pause` and
        # `/pause <anything>` (first token wins).
        if _is_pause_command(stripped_lower):
            # No reaction on /pause — control, not content.
            pause_updates.append((message, update_id, chat_id))
            continue

        if text.strip().startswith("/"):
            # No reaction on slash commands — they render as text.
            command_updates.append((message, update_id, text))
        elif len(text) > MAX_MESSAGE_LENGTH:
            log(f"Message too long: {len(text)} chars (max {MAX_MESSAGE_LENGTH})")
            daemon._send_response(
                daemon.token, chat_id,
                f"(Message too long — {len(text):,} chars, max {MAX_MESSAGE_LENGTH:,}. "
                f"Please split into smaller messages.)",
            )
            daemon._advance_update_cursor(update_id)
        else:
            _ack_and_record(message, chat_id)
            text_updates.append((message, update_id, text))

    return (
        command_updates,
        text_updates,
        photo_updates,
        pause_updates,
        document_updates,
        voice_updates,
    )
