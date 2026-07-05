"""Photo handling — album grouping, download dispatch, Claude prompt assembly.

- Photos sharing ``media_group_id`` are an album (one Telegram UI message,
  many update deliveries); standalone photos dispatch independently.
- State lives on the ``TelegramDaemon`` coordinator; helpers receive it as
  the first arg and reuse its lock gate, dispatcher, send helpers, cursor.
- Downloads route through ``landline.orchestrator.download_file`` so tests
  patching that symbol keep working.
"""

from datetime import datetime
from typing import Dict, List, TYPE_CHECKING, Tuple

from landline.config import TIMEZONE, TELEGRAM_FILE_SIZE_LIMIT, USER_NAME
from landline.runtime.logging import log
from landline.telegram import reactions
from landline.runtime.state import log_conversation

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


def process_photo_batch(
    daemon: "TelegramDaemon",
    photo_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Download photos, coalesce albums, and dispatch to Claude."""
    # Photos with a shared media_group_id form an album; the rest dispatch alone.
    groups: Dict[str, List[Tuple[Dict, int, str]]] = {}
    standalone: List[Tuple[Dict, int, str]] = []

    for message, update_id, chat_id in photo_updates:
        media_group_id = message.get("media_group_id")
        if media_group_id:
            if media_group_id not in groups:
                groups[media_group_id] = []
            groups[media_group_id].append((message, update_id, chat_id))
        else:
            standalone.append((message, update_id, chat_id))

    for message, update_id, chat_id in standalone:
        dispatch_photo_group(daemon, [message], [update_id], chat_id)

    for media_group_id, group_items in groups.items():
        messages = [item[0] for item in group_items]
        update_ids = [item[1] for item in group_items]
        chat_id = group_items[0][2]
        dispatch_photo_group(daemon, messages, update_ids, chat_id)


def _clear_group_acks(
    daemon: "TelegramDaemon", chat_id: str, messages: List[Dict],
) -> None:
    """Clear the 👀 acks on every message in a group when the batch bails.

    Called on lock-gate rejection or all-downloads-fail so the 👀 never
    lingers without a matching 👌. Mirrors ``voice._clear_ack`` /
    ``document._clear_ack``.
    """
    clear_ids: List[int] = [
        m.get("message_id")
        for m in messages
        if isinstance(m.get("message_id"), int)
    ]
    if clear_ids:
        reactions.set_reactions_batch_async(
            daemon.token, chat_id, clear_ids, None,
        )


def dispatch_photo_group(
    daemon: "TelegramDaemon",
    messages: List[Dict],
    update_ids: List[int],
    chat_id: str,
) -> None:
    """Download one or more photos and send them to Claude as file paths."""
    # Lock BEFORE any downloads — locked session must not leave jpegs in cache.
    if daemon._check_lock_gate(chat_id, update_ids):
        _clear_group_acks(daemon, chat_id, messages)
        return

    # Late import so ``landline.orchestrator.download_file`` test patches apply.
    from landline import orchestrator as _orch

    downloaded_paths: List[str] = []
    caption = None  # first non-empty caption wins

    for message in messages:
        photos = message.get("photo", [])
        if not photos:
            continue

        # Largest photo is last in the array.
        largest = photos[-1]
        file_id = largest.get("file_id", "")
        file_size = largest.get("file_size", 0)

        if file_size and file_size > TELEGRAM_FILE_SIZE_LIMIT:
            log(f"Photo too large ({file_size} bytes), skipping")
            continue

        ts = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
        idx = len(downloaded_paths)
        filename = f"{ts}_{idx}.jpg"

        local_path = _orch.download_file(daemon.token, file_id, filename)
        if local_path:
            downloaded_paths.append(local_path)

        if not caption:
            msg_caption = message.get("caption")
            if msg_caption:
                caption = msg_caption

    if not downloaded_paths:
        log("All photo downloads failed in group")
        _clear_group_acks(daemon, chat_id, messages)
        for uid in update_ids:
            daemon._advance_update_cursor(uid)
        daemon._send_response(
            daemon.token, chat_id,
            "(Failed to download the image. Please try sending it again.)",
        )
        return

    # Build the prompt for Claude.
    if len(downloaded_paths) == 1:
        path_section = f"[Image saved at: {downloaded_paths[0]}]"
        default_caption = f"{USER_NAME} sent this image:"
    else:
        path_lines = "\n".join(
            f"  - {p}" for p in downloaded_paths
        )
        path_section = f"[Images saved at:\n{path_lines}]"
        default_caption = f"{USER_NAME} sent {len(downloaded_paths)} images:"

    prompt_caption = caption if caption else default_caption
    prompt_text = f"{prompt_caption}\n\n{path_section}"

    log(f"Photo prompt: {len(downloaded_paths)} image(s), caption_chars={len(prompt_caption)}")
    log_conversation(USER_NAME, f"[photo] {prompt_caption}")

    # Ack only THIS group's message_ids (album: all photos; standalone: one).
    # Never the union across buckets — see ``_inject_and_dispatch``.
    ack_ids: List[int] = [
        m.get("message_id")
        for m in messages
        if isinstance(m.get("message_id"), int)
    ]
    daemon._inject_and_dispatch(
        prompt_text, chat_id, update_ids, ack_message_ids=ack_ids,
    )
