"""Update-batch classification — split a drained list of Telegram updates
into command / text / photo / pause groups.

The classifier walks each update once, handling the trivial-skip side effects
inline (cursor advance for edited messages, missing chat.id, unauthorized
chats, non-text/non-photo media, too-long text). What remains is bucketed
and returned for the orchestrator's dispatch passes.

State lives on the ``TelegramDaemon`` coordinator — this module is a
stateless helper that receives the daemon and the updates list, mutates the
daemon's per-batch tracker (``_batch_processed_ids``) via
``_advance_update_cursor``, and returns the four buckets.

Note: ``BackgroundPoller`` requests ``allowed_updates=["message"]`` (see
``poller.py``), so callback queries / edited_channel_post / inline_query
never reach this classifier. The ``message`` key lookup below is the
single entry point. If that filter is ever loosened, this module must be
updated alongside it (and ``test_poller.test_request_url_and_payload``
will fail first).
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
from landline.logging import log
from landline import reactions
from landline.telegram_download import _safe_basename

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


def _is_pause_command(text: str) -> bool:
    """True if `text` (already stripped + lowercased) is the /pause command,
    with or without a trailing argument (e.g. '/pause' or '/pause now')."""
    return text == "/pause" or text.startswith("/pause ")


def extract_chat_id(message: Dict, default: str = "") -> str:
    """Pull ``str(chat.id)`` from a Telegram message dict, defaulting on miss.

    The Telegram envelope nests ``chat.id`` two levels deep; this helper
    centralizes the defensive ``.get("chat", {}).get("id", ...)`` walk + the
    ``str(...)`` coercion that the rest of the daemon (allowlist cache key,
    lock checks, log fields) depends on. Returns ``default`` if either
    ``chat`` or ``chat.id`` is missing. Always returns a ``str``.
    """
    return str(message.get("chat", {}).get("id", default))


def _is_acceptable_document(document_field: Dict) -> bool:
    """Return True iff the Telegram document envelope is acceptable for
    ingestion. Rejects on: missing filename, unsafe basename, disallowed
    extension, over-cap size, or mime that (when present) doesn't match one
    of the allowed prefixes. Extension is the primary gate; mime is a
    belt-and-suspenders confirmation and its absence does NOT block."""
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
    """Pass 1 of ``_process_update_batch``: classify each update and perform
    the trivial-skip side effects inline (cursor advance, reject, too-long
    notice). Returns the four classified buckets for the dispatch passes.

    /pause is intercepted BEFORE the ``/``-prefix branch so it never reaches
    ``CommandRouter`` (which would reply with "Unknown command").
    """
    command_updates: List[Tuple[Dict, int, str]] = []
    text_updates: List[Tuple[Dict, int, str]] = []
    photo_updates: List[Tuple[Dict, int, str]] = []
    pause_updates: List[Tuple[Dict, int, str]] = []
    document_updates: List[Tuple[Dict, int, str]] = []
    voice_updates: List[Tuple[Dict, int, str]] = []

    def _ack_and_record(msg: Dict, chat: str) -> None:
        """Cluster 3: fire 👀 for accepted content messages and record the
        server-side message_id on the daemon's per-batch tracker so the
        dispatcher can fire 👌 on the same ids at finalize time.

        Only called AFTER the guard passes — an unauthorized sender must
        never receive a reaction (that would be an enumeration oracle).
        Never called for /pause (control) or slash commands (they render
        as text; no receipt semantics).
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

        # Voice / audio / video_note: transcribed locally with whisper.
        # The classifier only buckets — the handler enforces duration
        # limits and picks a filename. No mime/ext gate here; the
        # handler's ``_voice_filename`` sanitizes any advertised name.
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
            # Rejected — fall through to the non-text notice path.
            # PRIVACY: never log the attacker-controlled file_name here —
            # a rejection ("../../../etc/passwd.evil", or a sensitive
            # legit name that just failed the extension gate) would land
            # in the rotating daemon log verbatim. Metadata-only: size
            # and mime are safe.
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

        # /pause intercepted BEFORE the `/`-prefix branch — never reaches
        # CommandRouter (which would return "Unknown command"). Accept
        # `/pause` and `/pause <anything>` (e.g. `/pause now`) — first
        # whitespace-token is what matters.
        if _is_pause_command(stripped_lower):
            # NO reaction on /pause — it's control, not content.
            pause_updates.append((message, update_id, chat_id))
            continue

        if text.strip().startswith("/"):
            # NO reaction on slash commands — they render as text
            # (CommandRouter reply); no receipt semantics needed.
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
