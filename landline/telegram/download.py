"""Telegram file downloads — ``getFile`` + bounded-stream binary fetch.

Separates photo/document download (byte-cap streaming, FS cleanup) from the
transport's request/retry policy and the chunker. Callers reach this through
``landline.telegram.download_file`` (re-export) so import paths keep working.
"""

import os
import unicodedata
import urllib.request
from pathlib import Path
from typing import FrozenSet, Optional

from landline.config import (
    MEDIA_CACHE_DIR_MODE,
    TELEGRAM_FILE_SIZE_LIMIT,
    TELEGRAM_IMAGE_DIR,
)
from landline.runtime.logging import log
from landline.telegram.transport import telegram_api


# Path-traversal / control-char sanitizer. Module-level for direct test use.
_MAX_BASENAME_LEN = 255


def _safe_basename(raw: str, allowed_exts: FrozenSet[str]) -> Optional[str]:
    """Sanitize an attacker-supplied filename → ``<safe_stem><ext>`` or None.

    Args:
        raw: attacker-supplied filename.
        allowed_exts: lowercased extensions to accept (with dot).

    Returns:
        Sanitized basename, or None on any rule failure.

    - Reject empty, ``.``, ``..``, NUL, or ``/`` in the name.
    - Reject names longer than 255 chars.
    - Require ``ext.lower() in allowed_exts``.
    - Strip anything Python considers non-printable OR in Unicode category
      ``C*`` (controls, format, surrogates, private-use, unassigned) —
      catches BOM, ZWSP, RTL-override U+202E, U+2028/2029, DEL, C1 controls
      that an ``ord >= 0x20`` gate would let through.
    - Empty stem after strip → substitute ``'file'``.
    """
    if raw is None:
        return None
    base = os.path.basename(raw)
    if base in ("", ".", ".."):
        return None
    if "\x00" in base or "/" in base:
        return None
    if len(base) > _MAX_BASENAME_LEN:
        return None
    stem, ext = os.path.splitext(base)
    ext_lower = ext.lower()
    if ext_lower not in allowed_exts:
        return None

    def _stem_safe(ch: str) -> bool:
        # ``isprintable`` catches controls/DEL/separators; the C-category
        # guard closes the gap for format-class chars (BOM, ZWSP, U+202E)
        # that some Python builds treat as printable.
        if not ch.isprintable():
            return False
        cat0 = unicodedata.category(ch)[:1]
        return cat0 != "C"

    safe_stem = "".join(ch for ch in stem if _stem_safe(ch))
    if not safe_stem:
        safe_stem = "file"
    return safe_stem + ext_lower


def download_file(
    token: str,
    file_id: str,
    filename: str,
    target_dir: Optional[Path] = None,
    size_cap: int = TELEGRAM_FILE_SIZE_LIMIT,
) -> Optional[str]:
    """Download a file from Telegram servers to a local cache directory.

    Args:
        token: Bot token for API auth.
        file_id: Telegram file_id from the PhotoSize / Document object.
        filename: Local filename to save as (within ``target_dir``).
        target_dir: Destination directory. Defaults to ``TELEGRAM_IMAGE_DIR``
            (preserves the photo path); documents pass ``TELEGRAM_FILE_DIR``.
        size_cap: Hard byte ceiling for the download. Defaults to the
            Telegram-wide ``TELEGRAM_FILE_SIZE_LIMIT``; documents may pass a
            tighter cap.

    Returns:
        Absolute path to the saved file, or None if download failed.
    """
    dest_dir = target_dir if target_dir is not None else TELEGRAM_IMAGE_DIR
    # mkdir's ``mode=`` is umask-masked; the follow-up chmod is load-bearing
    # (workspace invariant: never call os.umask; see CLAUDE.md).
    try:
        dest_dir.mkdir(parents=True, exist_ok=True, mode=MEDIA_CACHE_DIR_MODE)
        try:
            os.chmod(str(dest_dir), MEDIA_CACHE_DIR_MODE)
        except OSError:
            pass
    except Exception as mkdir_error:
        log(
            f"download_file: failed to prepare target dir {dest_dir} "
            f"(exc={type(mkdir_error).__name__}): {mkdir_error}"
        )
        return None

    # Step 1: getFile → file_path on Telegram's servers.
    try:
        resp = telegram_api(token, "getFile", {"file_id": file_id}, timeout=15)
    except Exception as e:
        log(
            f"getFile API call failed for file_id={file_id[:20]}... "
            f"(exc={type(e).__name__}): {e}"
        )
        return None

    if not resp.get("ok"):
        log(f"getFile returned not-ok: {resp}")
        return None

    file_info = resp.get("result", {})
    file_path_remote = file_info.get("file_path")
    if not file_path_remote:
        log(f"getFile returned no file_path: {file_info}")
        return None

    file_size = file_info.get("file_size", 0)
    if file_size and file_size > size_cap:
        log(f"File too large ({file_size} bytes > {size_cap}), skipping")
        return None

    # Step 2: stream download with hard byte cap so a malformed/oversized
    # response can't balloon daemon RSS.
    download_url = f"https://api.telegram.org/file/bot{token}/{file_path_remote}"
    local_path = dest_dir / filename
    chunk_size = 64 * 1024  # 64 KB per read

    try:
        req = urllib.request.Request(download_url)
        bytes_written = 0
        with urllib.request.urlopen(req, timeout=30) as resp_data, \
                open(local_path, "wb") as f:
            while True:
                buf = resp_data.read(chunk_size)
                if not buf:
                    break
                bytes_written += len(buf)
                if bytes_written > size_cap:
                    raise IOError(
                        "Download exceeded %d bytes (byte cap)"
                        % size_cap
                    )
                f.write(buf)
        # PRIVACY: basename may be a user-supplied doc name; log dir + byte
        # count only, never the filename.
        log(
            "Downloaded file to %s (%d bytes)"
            % (str(dest_dir), bytes_written)
        )
        return str(local_path)
    except Exception as e:
        log(
            f"File download failed for {file_path_remote} "
            f"(exc={type(e).__name__}): {e}"
        )
        # Clean up any partial file.
        if local_path.exists():
            try:
                os.remove(str(local_path))
            except OSError:
                pass
        return None
