"""Voice-note handling — download, local whisper transcribe, dispatch.

Voice / audio / video_note → local whisper → dispatched to Claude as if
typed. Mirrors ``photo.py`` and ``document.py``:

- Lock gate BEFORE any download (a locked session must not leave audio in cache).
- Duration guard BEFORE download: exceeding ``VOICE_MAX_DURATION_SECONDS``
  → compact notice, advance cursor.
- Whisper failure (timeout / non-zero / empty) → notice, no dispatch,
  advance cursor. ``transcribe_file`` never raises → dispatch loop can't wedge.

Prompt-injection safety: transcript is UNTRUSTED. Wrap in ``<voice_note>``
XML delimiters; escape any pre-existing ``</voice_note>`` inside so a
hostile transcript can't break out of the frame.

Downloads route through ``landline.orchestrator.download_file`` so test
patches apply (mirrors photo.py / document.py).
"""

from datetime import datetime
from typing import Dict, List, TYPE_CHECKING, Tuple

from landline.config import (
    TIMEZONE,
    TELEGRAM_VOICE_DIR,
    USER_NAME,
    VOICE_ACCEPT_TYPES,
    VOICE_ALLOWED_EXTENSIONS,
    VOICE_MAX_DURATION_SECONDS,
    VOICE_TRANSCRIBE_TIMEOUT_SECONDS,
    WHISPER_LANGUAGE,
    WHISPER_MODEL,
    WHISPER_MODEL_DIR,
)
from landline.runtime.logging import log
from landline.telegram import reactions
from landline.runtime.state import log_conversation
from landline.telegram.download import _safe_basename
from landline.media.transcribe import transcribe_file

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


def _clear_ack(daemon: "TelegramDaemon", chat_id: str, message: Dict) -> None:
    """Clear the 👀 ack when a message bails before dispatch.

    Called on lock-gate / duration cap / download / transcribe failure so
    the 👀 never lingers without a matching 👌.
    """
    mid = message.get("message_id")
    if isinstance(mid, int):
        reactions.set_reaction_async(daemon.token, chat_id, mid, None)


def process_voice_batch(
    daemon: "TelegramDaemon",
    voice_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Transcribe each voice note in order and dispatch its transcript.

    Batch-level lock gate coalesces LOCKED_HELP to one notice per batch
    (mirrors text-batch), and clears 👀 on all voice messages when bailing.
    """
    if not voice_updates:
        return
    chat_id = voice_updates[0][2]
    all_update_ids = [uid for _, uid, _ in voice_updates]
    if daemon._check_lock_gate(chat_id, all_update_ids):
        for message, _, _ in voice_updates:
            _clear_ack(daemon, chat_id, message)
        return
    for message, update_id, chat_id in voice_updates:
        dispatch_voice(daemon, message, update_id, chat_id)


def _get_voice_field(message: Dict) -> Tuple[str, Dict]:
    """Return the (key, field) tuple for the first accepted media type on
    ``message``. Returns ("", {}) if none present.
    """
    for key in ("voice", "audio", "video_note"):
        if key in VOICE_ACCEPT_TYPES and key in message:
            field = message.get(key)
            if isinstance(field, dict):
                return key, field
    return "", {}


def _format_duration(seconds: int) -> str:
    """Format seconds as ``M:SS``. 0-safe."""
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    return "%d:%02d" % (m, s)


def _voice_filename(field: Dict, source_key: str) -> str:
    """Safe local filename for downloaded audio.

    Prefer Telegram's advertised ``file_name`` (sanitized). Voice fields
    typically have no name → synthesize ``voice_<ts>.ogg`` (``.m4a`` for
    audio, ``.mp4`` for video_note).
    """
    raw_name = field.get("file_name")
    if isinstance(raw_name, str) and raw_name:
        sanitized = _safe_basename(raw_name, VOICE_ALLOWED_EXTENSIONS)
        if sanitized:
            return sanitized
    ext = {
        "voice": ".ogg",
        "audio": ".m4a",
        "video_note": ".mp4",
    }.get(source_key, ".ogg")
    ts = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
    return "voice_%s%s" % (ts, ext)


def dispatch_voice(
    daemon: "TelegramDaemon",
    message: Dict,
    update_id: int,
    chat_id: str,
) -> None:
    """Handle one voice/audio/video_note update end-to-end."""
    # Per-item lock re-check: the batch-level check may have raced with a
    # lock transition. Clear 👀 on rejection.
    if daemon._check_lock_gate(chat_id, [update_id]):
        _clear_ack(daemon, chat_id, message)
        return

    source_key, field = _get_voice_field(message)
    if not field:
        # Should never fire — classifier bucketed this. Belt-and-suspenders.
        log(
            f"Voice dispatch: message had no recognized voice field "
            f"(chat={chat_id})"
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    duration_raw = field.get("duration", 0)
    try:
        duration_s = int(duration_raw or 0)
    except (TypeError, ValueError):
        duration_s = 0

    if duration_s > VOICE_MAX_DURATION_SECONDS:
        log(
            f"Voice too long ({duration_s}s > {VOICE_MAX_DURATION_SECONDS}s), "
            f"chat={chat_id}"
        )
        daemon._send_response(
            daemon.token, chat_id,
            "(Voice note too long — %ds, cap %ds. Try under %ds.)" % (
                duration_s,
                VOICE_MAX_DURATION_SECONDS,
                VOICE_MAX_DURATION_SECONDS,
            ),
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    # Late import so ``landline.orchestrator.download_file`` test patches apply.
    from landline import orchestrator as _orch

    file_id = field.get("file_id", "")
    ts_prefix = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
    base_name = _voice_filename(field, source_key)
    filename = f"{ts_prefix}_{base_name}"

    local_path = _orch.download_file(
        daemon.token,
        file_id,
        filename,
        target_dir=TELEGRAM_VOICE_DIR,
    )
    if not local_path:
        log(f"Voice download failed for chat {chat_id}")
        daemon._send_response(
            daemon.token, chat_id,
            "(Failed to download the voice note — please try again.)",
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    from pathlib import Path as _P
    # Give whisper an interruptible pause_flag so /pause during a 30-90s
    # transcription doesn't starve the single-threaded dispatch loop.
    # See ``transcribe._run_whisper_interruptible`` for the polling loop.
    pause_flag = getattr(daemon, "_pause_requested", None)
    # Edge-triggered: /pause queued in the SAME batch as this voice note
    # (flag ALREADY set before whisper starts) MUST NOT abort whisper —
    # that would drop the voice content on the floor. Only a pause DURING
    # whisper aborts; a pre-existing pause routes through the downstream
    # Claude dispatch (transcript's own turn) instead, preserving both.
    already_paused_at_start = False
    if pause_flag is not None:
        try:
            already_paused_at_start = bool(pause_flag.is_set())
        except Exception:
            already_paused_at_start = False
    effective_pause_flag = None if already_paused_at_start else pause_flag
    result = transcribe_file(
        _P(local_path),
        model=WHISPER_MODEL,
        model_dir=WHISPER_MODEL_DIR,
        language=WHISPER_LANGUAGE,
        timeout_seconds=VOICE_TRANSCRIBE_TIMEOUT_SECONDS,
        pause_flag=effective_pause_flag,
    )

    if not result.ok:
        if result.error == "paused":
            # Mirror ClaudeDispatcher._finalize_response: send "(Paused.)"
            # and clear the pause flag so a still-pending /pause update
            # takes the "already consumed" branch (not "(Nothing to
            # pause.)"). Voice content is NOT dispatched.
            daemon._send_response(daemon.token, chat_id, "(Paused.)")
            if pause_flag is not None:
                try:
                    pause_flag.clear()
                except Exception:
                    pass
            _clear_ack(daemon, chat_id, message)
            daemon._advance_update_cursor(update_id)
            return
        if result.error == "timeout":
            notice = (
                "(Couldn't transcribe that voice note — took too long. "
                "Try a shorter one.)"
            )
        else:
            notice = (
                "(Couldn't transcribe that voice note. Try again or send text.)"
            )
        daemon._send_response(daemon.token, chat_id, notice)
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    # Escape any pre-existing close-tag so the XML frame stays intact.
    safe_text = result.text.replace(
        "</voice_note>", "</voice_note_escaped>",
    )
    duration_display = _format_duration(duration_s)
    prompt_text = (
        "[voice note, %s, transcribed locally with whisper]\n"
        "<voice_note>\n%s\n</voice_note>" % (duration_display, safe_text)
    )

    # Metadata-only log — NEVER include transcript text.
    log(
        "Voice prompt: chat=%s duration=%ds chars=%d"
        % (chat_id, duration_s, len(result.text))
    )
    log_conversation(
        USER_NAME, "[voice] (%ds, %d chars)" % (duration_s, len(result.text)),
    )

    # Ack ONLY this voice note's message_id — never the batch union.
    # A voice failure earlier in the batch stays with its own message.
    mid = message.get("message_id")
    ack_ids: List[int] = [mid] if isinstance(mid, int) else []
    daemon._inject_and_dispatch(
        prompt_text, chat_id, [update_id], ack_message_ids=ack_ids,
    )
