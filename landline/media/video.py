"""Video handling — async download + Claude prompt assembly via inject queue.

- Videos arrive as ``message.video`` envelopes; some clients (rare) send
  video as ``message.document`` with a ``video/*`` mime — both route here.
  Telegram does NOT group videos by ``media_group_id``, so no album
  coalescing.
- State lives on the ``TelegramDaemon`` coordinator; helpers receive it as
  the first arg and reuse its lock gate, send helpers, cursor.
- Downloads route through ``landline.orchestrator.download_file`` so test
  patches apply (mirrors ``photo.py`` / ``document.py``).

Async architecture (this is the KEY difference from document.py):

- The main-loop handler (``dispatch_video``) does NO network work. It runs
  lock-gate + policy-cap validation, spawns a daemon thread to download
  in the background, advances the cursor, clears the 👀 ack, and returns.
- The background thread does the actual ``download_file`` and, on success,
  drops the ``<video_path>`` prompt into the inject queue (the same
  mechanism cron reports use). Claude sees the video prepended to the
  next dispatched turn.
- On any failure (Telegram's 20MB bot-download refusal, network error,
  disk error) the background thread sends a compact Telegram notice to
  the user's chat instead. No dispatch, no inject.

Why the inject queue (option b, "notify agent when download completes")
rather than dispatch-from-thread (option a):

- ``ClaudeDispatcher.send_to_claude`` uses the main loop's rate limit,
  backoff queue, pause flag, and persistent Claude subprocess — none of
  those are thread-safe. Calling it from a background thread would race
  the whole coordinator.
- The inject queue is designed for exactly this: an out-of-band producer
  drops a JSON report, and the daemon prepends it to the next Claude
  call. Reusing it here is zero new machinery.
- Trade-off: if the user only sends a video and no follow-up message, the
  video sits in the inject queue until they next message. The immediate
  "receiving video, will process it..." Telegram notice makes this
  behavior visible so it never feels like the video was dropped.

The Telegram 20MB bot-download limit (external constraint):

- Cloud Bot API refuses ``getFile`` for files >20MB. That's the effective
  hard ceiling — no matter how big ``TELEGRAM_VIDEO_SIZE_LIMIT`` is set,
  a 20-100MB video still fails at ``getFile``. The download helper returns
  None on that failure; we map None → "over Telegram's 20MB bot-download
  limit" notice with a suggestion to use Drive / iMessage / email.
- ``TELEGRAM_VIDEO_SIZE_LIMIT`` (100MB default) gates the OBVIOUSLY-too-big
  case up front so we don't waste an API round-trip on a 500MB video.

Prompt-injection safety (mirrors document.py):

- Filenames from bare ``video`` messages usually don't exist (Telegram
  omits ``file_name``); we synthesize ``video_<ts>.mp4`` in that case.
  When present (from a ``document`` envelope) they are attacker-supplied
  and pass through ``_safe_basename`` against ``VIDEO_ALLOWED_EXTENSIONS``.
- The sanitized filename and the on-disk path both wrap in dedicated
  ``<video_filename>`` / ``<video_path>`` XML delimiters; any pre-existing
  close-tag inside is escaped defense-in-depth.
"""

import json
import os
import tempfile
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from landline.config import (
    INJECT_TIMESTAMP_FORMAT,
    MEDIA_CACHE_DIR_MODE,
    TELEGRAM_FILE_SIZE_LIMIT,
    TELEGRAM_VIDEO_DIR,
    TELEGRAM_VIDEO_SIZE_LIMIT,
    TIMEZONE,
    USER_NAME,
    VIDEO_ALLOWED_EXTENSIONS,
    WORKSPACE,
)
from landline.runtime.logging import log
from landline.runtime.state import log_conversation
from landline.telegram import reactions
from landline.telegram.download import _safe_basename

if TYPE_CHECKING:  # pragma: no cover - typing-only
    from landline.orchestrator import TelegramDaemon


# Inject-queue directory — mirrors ``landline.orchestrator.INJECT_QUEUE_DIR``.
# Reproduced here (not imported) to keep this module usable without a live
# orchestrator import cycle. Both point at the same WORKSPACE path.
_INJECT_QUEUE_DIR = WORKSPACE / "cache" / "inject-queue"

# Default synthesized extension when Telegram omits ``file_name`` on a bare
# ``video`` message (typical for camera roll uploads).
_DEFAULT_VIDEO_EXT = ".mp4"


def _clear_ack(daemon: "TelegramDaemon", chat_id: str, message: Dict) -> None:
    """Clear the 👀 ack when a video bails or completes handoff.

    Called after the main-loop handler advances the cursor — the download
    itself is fire-and-forget, so the ack is cleared as soon as the update
    is accepted for background processing (not when the download finishes).
    Mirrors ``document._clear_ack``.
    """
    mid = message.get("message_id")
    if isinstance(mid, int):
        reactions.set_reaction_async(daemon.token, chat_id, mid, None)


def process_video_batch(
    daemon: "TelegramDaemon",
    video_updates: List[Tuple[Dict, int, str]],
) -> None:
    """Dispatch each video via the background-download path.

    Batch-level lock gate coalesces LOCKED_HELP to one notice per batch
    (mirrors text/doc/voice batches), and clears 👀 on all videos when
    bailing.
    """
    if not video_updates:
        return
    chat_id = video_updates[0][2]
    all_update_ids = [uid for _, uid, _ in video_updates]
    if daemon._check_lock_gate(chat_id, all_update_ids):
        for message, _, _ in video_updates:
            _clear_ack(daemon, chat_id, message)
        return
    for message, update_id, chat_id in video_updates:
        dispatch_video(daemon, message, update_id, chat_id)


def _format_size(num_bytes: int) -> str:
    """Human-readable size string; inline-safe for the prompt."""
    if num_bytes >= 1024 * 1024:
        return "%.1f MB" % (num_bytes / (1024.0 * 1024.0))
    if num_bytes >= 1024:
        return "%.1f KB" % (num_bytes / 1024.0)
    return "%d B" % num_bytes


def _extract_video_field(message: Dict) -> Tuple[str, Dict]:
    """Return the (source_key, field) tuple for a video-shaped message.

    - Bare ``video`` message: ("video", message["video"]).
    - ``document`` with a ``video/*`` mime: ("document", message["document"]).
    - Neither: ("", {}) — should never fire (classifier bucketed this).
    """
    video_field = message.get("video")
    if isinstance(video_field, dict):
        return "video", video_field
    document_field = message.get("document")
    if isinstance(document_field, dict):
        mime = document_field.get("mime_type")
        if isinstance(mime, str) and mime.lower().startswith("video/"):
            return "document", document_field
    return "", {}


def _video_filename(field: Dict, source_key: str) -> Optional[str]:
    """Safe local filename for a downloaded video.

    - When Telegram advertises ``file_name`` (typical for ``document``
      envelopes and some ``video`` messages), sanitize against
      ``VIDEO_ALLOWED_EXTENSIONS`` and use that.
    - When absent (bare camera-roll ``video`` uploads), synthesize
      ``video_<ts>.mp4``.
    - Returns None if a supplied name fails sanitization (unsafe extension,
      traversal, control chars) — the handler treats that as a rejection.
    """
    raw_name = field.get("file_name")
    if isinstance(raw_name, str) and raw_name:
        sanitized = _safe_basename(raw_name, VIDEO_ALLOWED_EXTENSIONS)
        if sanitized:
            return sanitized
        return None
    ts = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
    return "video_%s%s" % (ts, _DEFAULT_VIDEO_EXT)


def _neutralize_for_frame(text: str) -> str:
    """Scrub attacker-controlled text before it lands inside an XML frame.

    Mirrors ``document._neutralize_for_frame`` at a smaller scope — video
    frames are just ``<video_filename>`` and ``<video_path>``. Newlines
    become spaces so a one-line-delimited frame stays on one line; the
    ``<`` of a real frame-tag substring is replaced with ``&lt;`` so no
    attacker copy parses as a closer.
    """
    scrubbed = text.replace("\r", " ").replace("\n", " ")
    for tag in (
        "</video_path>", "<video_path>",
        "</video_filename>", "<video_filename>",
    ):
        scrubbed = scrubbed.replace(tag, "&lt;" + tag[1:])
    return scrubbed


def _build_video_prompt(
    safe_name: str, local_path: str, file_size: int, caption: Optional[str],
) -> str:
    """Assemble the Claude prompt for one downloaded video.

    Structure matches document.py's single-doc frame:

        [video: <size>]
        <video_filename>...</video_filename>
        <video_path>...</video_path>

    Caption (when present) rides above the frame so Claude sees the user's
    intent before the metadata block.
    """
    size_display = _format_size(file_size) if file_size else "unknown size"
    neutral_name = _neutralize_for_frame(safe_name)
    neutral_path = _neutralize_for_frame(local_path)
    path_section = (
        "[video: %s]\n"
        "<video_filename>%s</video_filename>\n"
        "<video_path>%s</video_path>"
    ) % (size_display, neutral_name, neutral_path)
    if caption:
        return "%s\n\n%s" % (caption, path_section)
    return "%s sent a video:\n\n%s" % (USER_NAME, path_section)


def _write_inject_file(
    label: str, content: str,
    uid_token: str,
    queue_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Drop one ``<ts>_video_<uid>.json`` inject file with the video prompt.

    Producer contract for ``runtime/inject.py``:
      - filename ``<INJECT_TIMESTAMP_FORMAT>_video_<uid>.json`` (stem[:15]
        parses back to the timestamp).
      - payload = ``{"label": str, "content": str}``.
      - 0o600 file, under the 0o700 queue dir.

    Uniqueness (Bug 1 fix): the stem carries ``uid_token`` (an 8-char
    uuid4 hex generated once per dispatched video in ``dispatch_video``),
    NOT ``os.getpid()``. Every video-download thread in this process
    shares the same pid, so a pid-based tail collided at second
    resolution and the second inject write clobbered the first — video
    silently lost. A per-video uuid makes stem uniqueness independent of
    both second-resolution timing and pid. The same ``uid_token`` also
    lands in the on-disk video filename (see ``dispatch_video``) so the
    inject payload and the file it references stay one-to-one correlated.

    Atomicity (Bug 3 fix): the drain-side consumer globs ``*.json`` and
    unlinks any file that fails to parse (see ``runtime/inject.py``).
    Writing straight to the final ``*.json`` path via
    ``os.open(O_CREAT|O_TRUNC)`` + ``os.write`` exposes an empty or
    half-written file to a concurrently-running drain, which then
    unlinks it — video silently lost, no retry. We instead write to a
    ``*.json.tmp`` sibling (which does NOT match the ``*.json`` drain
    glob) and ``os.replace()`` it to the final path atomically. On
    POSIX ``os.replace`` is a rename syscall, so the drain either sees
    no file yet or the complete file — never a partial. The 0o600 mode
    comes from ``tempfile.mkstemp``'s default and is preserved through
    the rename.

    Returns the written path, or None on write failure (which the caller
    logs but does not surface to the user — a failed inject just means the
    video won't get prepended to the next turn, which we compensate for
    with the Telegram notice).

    ``queue_dir`` defaults to the module-level ``_INJECT_QUEUE_DIR``
    looked up at call time (NOT bound as a default arg) so tests
    monkeypatching the module attribute redirect writes to a tmp dir
    without patch-target churn.
    """
    if queue_dir is None:
        # Re-read from the module namespace so pytest monkeypatch of
        # ``landline.media.video._INJECT_QUEUE_DIR`` is respected.
        import landline.media.video as _self
        queue_dir = _self._INJECT_QUEUE_DIR
    tmp_path: Optional[Path] = None
    try:
        queue_dir.mkdir(parents=True, exist_ok=True, mode=MEDIA_CACHE_DIR_MODE)
        try:
            os.chmod(str(queue_dir), MEDIA_CACHE_DIR_MODE)
        except OSError:
            pass
        ts = datetime.now(tz=TIMEZONE).strftime(INJECT_TIMESTAMP_FORMAT)
        stem = "%s_video_%s" % (ts, uid_token)
        final_path = queue_dir / (stem + ".json")
        payload = json.dumps({"label": label, "content": content}).encode("utf-8")
        # Same-dir tempfile so os.replace stays a same-filesystem rename
        # (never EXDEV). ``.json.tmp`` suffix cannot match the drain's
        # ``*.json`` glob (pathlib.Path.glob is fnmatch-anchored to the
        # full name). mkstemp creates the file with mode 0o600 by
        # default (workspace invariant: never os.umask; see CLAUDE.md).
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=stem + ".", suffix=".json.tmp", dir=str(queue_dir),
        )
        tmp_path = Path(tmp_name)
        try:
            os.write(tmp_fd, payload)
        finally:
            os.close(tmp_fd)
        os.replace(str(tmp_path), str(final_path))
        tmp_path = None  # renamed away; nothing to clean up on exit.
        return final_path
    except Exception as write_error:
        log(
            "Video inject file write failed (exc=%s): %s"
            % (type(write_error).__name__, write_error)
        )
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass
        return None


def _download_and_inject(
    token: str,
    chat_id: str,
    file_id: str,
    filename: str,
    file_size: int,
    caption: Optional[str],
    send_response_fn,
    uid_token: str,
) -> None:
    """Background-thread body: download the video, then inject / notify.

    Runs on a daemon thread spawned by ``dispatch_video``. Never touches
    the main-loop dispatcher, the pause flag, or the reaction pipeline —
    those are single-threaded assumptions on the coordinator. All
    cross-thread interaction is through:
      - ``download_file`` (thread-safe: pure network + FS work).
      - ``_write_inject_file`` (thread-safe: one write per file, distinct
        stems per-video via ``uid_token``, and atomic via tempfile +
        ``os.replace``).
      - ``send_response_fn`` (Telegram transport — already thread-safe via
        its per-chat outbound spool + retry).

    ``uid_token`` is the per-video uuid tail generated in ``dispatch_video``
    and threaded through here so the on-disk video filename and the
    inject payload's stem share the same token — the two artifacts
    remain one-to-one correlated even when several videos race.

    Failure branches (each sends a Telegram notice, no dispatch, no inject):
      - ``getFile too big`` / oversized: the standard 20MB bot-download
        refusal. Notice tells the user to use a different channel.
      - Any other download failure (network, disk, malformed response):
        generic "couldn't download" notice.
    """
    # Late import so ``landline.orchestrator.download_file`` test patches apply.
    from landline import orchestrator as _orch

    local_path = _orch.download_file(
        token,
        file_id,
        filename,
        target_dir=TELEGRAM_VIDEO_DIR,
        size_cap=TELEGRAM_FILE_SIZE_LIMIT,
    )
    if not local_path:
        # PRIVACY: chat_id + size only. Filename never hits the daemon log.
        log(
            "Video background download failed (chat=%s, size=%d bytes) — "
            "assuming Telegram 20MB bot-download limit or network error"
            % (chat_id, file_size)
        )
        try:
            send_response_fn(
                token,
                chat_id,
                (
                    "(Couldn't download that video. Telegram bots can only "
                    "fetch files up to 20MB — for anything larger, send it "
                    "via Google Drive, iMessage, or email instead.)"
                ),
            )
        except Exception as notify_error:
            log(
                "Video failure notice send failed (chat=%s, exc=%s)"
                % (chat_id, type(notify_error).__name__)
            )
        return

    # Success: build the prompt frame and drop it into the inject queue.
    prompt_text = _build_video_prompt(
        filename, local_path, file_size, caption,
    )
    inject_path = _write_inject_file(
        label="video", content=prompt_text, uid_token=uid_token,
    )
    if inject_path is None:
        # Inject write failed: still notify the user so they know we got
        # the video, but flag that Claude may not see it on the next turn
        # automatically. Fire-and-forget — no re-try path.
        try:
            send_response_fn(
                token,
                chat_id,
                (
                    "(Video downloaded, but I couldn't queue it for the next "
                    "turn — mention the video when you next message me and "
                    "I'll open it from cache.)"
                ),
            )
        except Exception as notify_error:
            log(
                "Video partial-success notice send failed (chat=%s, exc=%s)"
                % (chat_id, type(notify_error).__name__)
            )
        return

    # PRIVACY: metadata-only log — chat_id + size only. Filename lives in
    # memory/daily/ (0600) via log_conversation, a different trust boundary,
    # wrapped in the same delimiter so a fresh-session replay keeps the
    # injection guard.
    size_display = _format_size(file_size) if file_size else "unknown size"
    log(
        "Video ready + inject-queued: chat=%s size=%s"
        % (chat_id, size_display)
    )
    try:
        log_conversation(
            USER_NAME,
            "[video] <video_filename>%s</video_filename>" % (
                _neutralize_for_frame(filename),
            ),
        )
    except Exception as conv_log_error:
        # log_conversation is best-effort; don't let a disk hiccup wedge
        # the notify path below.
        log(
            "Video log_conversation failed (exc=%s)"
            % type(conv_log_error).__name__
        )

    # Confirmation notice: makes the async completion visible to the user so
    # a video-only turn doesn't feel like the daemon silently swallowed it.
    try:
        send_response_fn(
            token,
            chat_id,
            (
                "(Video downloaded (%s). I'll open it on our next message — "
                "or send another now with your question about it.)"
            ) % size_display,
        )
    except Exception as notify_error:
        log(
            "Video success notice send failed (chat=%s, exc=%s)"
            % (chat_id, type(notify_error).__name__)
        )


def dispatch_video(
    daemon: "TelegramDaemon",
    message: Dict,
    update_id: int,
    chat_id: str,
) -> None:
    """Kick off the async video download and hand the main loop back.

    Runs on the main loop. Contract:
      - No network work, no long CPU work, no blocking waits.
      - Cursor advances immediately (Telegram must not re-deliver — the
        background thread is now responsible for the video's outcome).
      - 👀 clears immediately (accepted for background processing;
        completion notice lands separately via the download thread).
      - The download thread is a plain daemon thread — it dies with the
        process, so a mid-download shutdown loses the partial file but
        never leaves a hung worker.
    """
    # Per-item lock re-check: batch-level check may have raced with a
    # lock transition. Clear 👀 on rejection.
    if daemon._check_lock_gate(chat_id, [update_id]):
        _clear_ack(daemon, chat_id, message)
        return

    source_key, field = _extract_video_field(message)
    if not field:
        # Belt-and-suspenders — classifier should have bucketed a real
        # video. Fall through with a clear ack + advance so the update
        # doesn't re-deliver forever.
        log(
            "Video dispatch: message had no recognized video field "
            "(chat=%s)" % chat_id
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    file_id = field.get("file_id", "")
    file_size = int(field.get("file_size") or 0)

    # Policy cap — reject obviously-huge videos before we spend a getFile
    # round-trip. See TELEGRAM_VIDEO_SIZE_LIMIT docstring for the rationale
    # (this is ABOVE Telegram's own 20MB bot-download ceiling on purpose).
    if file_size and file_size > TELEGRAM_VIDEO_SIZE_LIMIT:
        log(
            "Video too large (%d bytes > %d cap), chat=%s"
            % (file_size, TELEGRAM_VIDEO_SIZE_LIMIT, chat_id)
        )
        cap_mb = TELEGRAM_VIDEO_SIZE_LIMIT // (1024 * 1024)
        size_mb = file_size / (1024.0 * 1024.0)
        daemon._send_response(
            daemon.token,
            chat_id,
            (
                "(That video is %.1f MB — over the %d MB cap for videos. "
                "Send a shorter clip, or share via Google Drive / iMessage.)"
            ) % (size_mb, cap_mb),
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    filename = _video_filename(field, source_key)
    if filename is None:
        # Only reachable when a document-envelope video advertised a
        # ``file_name`` that failed VIDEO_ALLOWED_EXTENSIONS. PRIVACY: name
        # never hits the daemon log — metadata-only.
        log(
            "Video dispatch rejected — unsafe filename "
            "(chat=%s, size=%d, source=%s)" % (chat_id, file_size, source_key)
        )
        daemon._send_response(
            daemon.token,
            chat_id,
            "(That video type isn't supported.)",
        )
        _clear_ack(daemon, chat_id, message)
        daemon._advance_update_cursor(update_id)
        return

    # Per-video uniqueness token (Bug 1 & 2 fix). Two bare-video uploads
    # in the same second synthesize identical ``video_<ts>.mp4`` base
    # names, and the timestamp prefix below also collides at second
    # resolution. Without a per-video tail, both background threads
    # would ``open(path, "wb")`` the SAME on-disk path concurrently
    # (interleaved / truncated bytes → one corrupt file both injects
    # point at), AND both would emit the same ``<ts>_video_<pid>.json``
    # inject filename (second write clobbers the first → one video's
    # inject payload silently lost).
    #
    # A per-dispatch uuid4 tail makes both the on-disk video filename
    # AND the downstream inject stem globally unique regardless of
    # second-resolution timing or pid. The SAME token flows into both
    # artifacts so the inject payload and the video file it references
    # stay one-to-one correlated.
    uid_token = uuid.uuid4().hex[:8]
    ts = datetime.now(tz=TIMEZONE).strftime("%Y%m%d_%H%M%S")
    local_filename = "%s_%s_%s" % (ts, uid_token, filename)
    caption = message.get("caption")

    # Immediate visible ack so the user sees the daemon received the video —
    # the download itself may take tens of seconds, and without this notice
    # the fire-and-forget path would feel silent.
    try:
        size_hint = (
            " (%s)" % _format_size(file_size) if file_size else ""
        )
        daemon._send_response(
            daemon.token,
            chat_id,
            "(Receiving video%s — downloading in the background...)"
            % size_hint,
        )
    except Exception as notify_error:
        # A failed notice is cosmetic — the download thread will still
        # run and either complete or send its own failure notice. Log
        # and continue.
        log(
            "Video receive-notice send failed (chat=%s, exc=%s)"
            % (chat_id, type(notify_error).__name__)
        )

    # Advance the cursor + clear 👀 BEFORE spawning the thread so a
    # thread-spawn failure can't leave us with a re-delivery loop or a
    # stuck ack. The thread inherits nothing from the batch trackers.
    _clear_ack(daemon, chat_id, message)
    daemon._advance_update_cursor(update_id)

    log(
        "Video dispatch: starting background download "
        "(chat=%s, size=%d bytes, source=%s)"
        % (chat_id, file_size, source_key)
    )

    thread = threading.Thread(
        target=_download_and_inject,
        kwargs={
            "token": daemon.token,
            "chat_id": chat_id,
            "file_id": file_id,
            "filename": local_filename,
            "file_size": file_size,
            "caption": caption,
            "send_response_fn": daemon._send_response,
            "uid_token": uid_token,
        },
        name="video-download-%d" % update_id,
        daemon=True,
    )
    thread.start()
