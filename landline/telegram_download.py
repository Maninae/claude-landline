"""Telegram file downloads — `getFile` + bounded-stream binary fetch.

Splits the photo/document download path out of the transport so that the
request/retry policy and the chunker don't have to live next to byte-cap
streaming and filesystem cleanup. Callers reach this through
`landline.client.download_file` (a re-export) so the existing import paths
keep working.
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
from landline.logging import log
from landline.telegram_transport import telegram_api


# Path traversal / control-char sanitizer used by document handling. Kept at
# module level so the tests can call it directly.
_MAX_BASENAME_LEN = 255


def _safe_basename(raw: str, allowed_exts: FrozenSet[str]) -> Optional[str]:
    """Sanitize an attacker-supplied filename to a safe basename or None.

    Rules (any failure returns None):
      1. Strip via ``os.path.basename`` (removes any leading path segments).
      2. Reject empty, ``.``, ``..``, or anything containing NUL / ``/``.
      3. Reject names longer than 255 chars.
      4. Split into ``stem + ext``; require ``ext.lower() in allowed_exts``.
      5. Strip non-printable / control-class characters from the stem.
         The prior ``ord(ch) >= 0x20`` gate preserved DEL (0x7F), C1
         controls (0x80–0x9F), Unicode BOM/ZWSP, RTL-override U+202E,
         and U+2028/2029 line separators — an attacker-controlled
         document filename with U+202E could visually disguise the
         extension in the daemon's log tail and in the ``[document:
         <name>, ...]`` fragment of the Claude prompt, and U+2028 could
         inject an apparent new line into that fragment. The tighter
         gate here rejects any character whose Unicode general category
         starts with ``C`` (Cc, Cf, Cs, Co, Cn — controls, formatting,
         surrogates, private-use, unassigned) or that Python's own
         ``str.isprintable()`` considers non-printable. If the stem
         becomes empty, substitute ``'file'``.

    Returns ``<safe_stem><lowercased_ext>`` on success.
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
        # str.isprintable() catches ASCII controls, DEL, and Unicode
        # separators/line breaks. The category guard closes the residual
        # gap for format-class chars (BOM, ZWSP, RTL-override) that some
        # Python builds treat as printable.
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
    # Ensure the destination directory exists with a 0700 mode. mkdir's own
    # ``mode=`` is masked by umask; a follow-up chmod is the load-bearing
    # step (workspace invariant: never call os.umask; see CLAUDE.md).
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

    # Step 1: Call getFile to get the file_path on Telegram's servers
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

    # Check file size if available
    file_size = file_info.get("file_size", 0)
    if file_size and file_size > size_cap:
        log(f"File too large ({file_size} bytes > {size_cap}), skipping")
        return None

    # Step 2: Download the binary content — stream in chunks and enforce a
    # hard byte cap. Reading the whole body into memory at once would let a
    # malformed/oversized response balloon the daemon's RSS; streaming with a
    # ceiling makes the worst case bounded.
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
        # PRIVACY: the file basename can be a user-supplied document
        # name (documents pass through _safe_basename but the caller may
        # still hand us a sensitive stem, e.g. "private_medical_records
        # _<ts>.pdf"). Metadata-only log line — dir + byte count.
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
        # Clean up partial file if it exists
        if local_path.exists():
            try:
                os.remove(str(local_path))
            except OSError:
                pass
        return None
