"""Telegram transport facade — re-exports the public + private surface used by
the rest of the daemon and the test suite.

The actual implementation now lives in three focused sibling modules:

  - `landline.telegram_transport`  — `telegram_api`, `chunk_text`, `_send_chunk`,
    `_send_with_retry`, `_parse_retry_after`, `send_response`, `send_typing`,
    `_TRANSIENT_HTTP_STATUSES` (the HTTP request/retry/429 path).
  - `landline.html_chunker`        — `_chunk_html`, `_scan_open_tags_at`,
    `_index_inside_tag`, `_strip_tags`, `_open_simple_tags_at`, `_utf16_len`,
    `_TAG_RE`, `_REOPENABLE_SIMPLE_TAGS`, `send_html` (the tag-aware chunker
    + its `send_html` driver).
  - `landline.telegram_download`   — `download_file` (getFile + bounded streaming).

This file stays as a thin facade so the daemon-wide imports (`from landline.telegram
import send_response, send_html, …`) and the test suite's patch targets
(`landline.telegram._send_chunk`, `landline.telegram.send_response`, etc.) keep
resolving without churn.
"""

# Logging — re-exported so callers / tests can patch `landline.telegram.log` if
# they only care about messages emitted from this module's own surface area.
from landline.runtime.logging import log

# Markdown → HTML formatter — used by `send_response` and re-exported for
# tests that exercise the formatting layer directly through `landline.client`.
from landline.telegram.fmt import md_to_telegram_html

# Tag-aware HTML chunker internals + `send_html`.
from landline.telegram.chunker import (
    _REOPENABLE_SIMPLE_TAGS,
    _TAG_RE,
    _chunk_html,
    _index_inside_tag,
    _open_simple_tags_at,
    _scan_open_tags_at,
    _strip_tags,
    _utf16_len,
    send_html,
)

# Telegram HTTP transport — API call, retry policy, send entry points,
# markdown chunker.
from landline.telegram.transport import (
    _TRANSIENT_HTTP_STATUSES,
    _parse_retry_after,
    _send_chunk,
    _send_with_retry,
    chunk_text,
    send_response,
    send_typing,
    telegram_api,
)

# File download — getFile + byte-capped streaming.
from landline.telegram.download import download_file


__all__ = [
    # Public API
    "telegram_api",
    "send_response",
    "send_html",
    "send_typing",
    "download_file",
    "chunk_text",
    # Re-exported formatter
    "md_to_telegram_html",
    # Private helpers exposed for tests and (rarely) other modules
    "_TRANSIENT_HTTP_STATUSES",
    "_REOPENABLE_SIMPLE_TAGS",
    "_TAG_RE",
    "_chunk_html",
    "_index_inside_tag",
    "_open_simple_tags_at",
    "_parse_retry_after",
    "_scan_open_tags_at",
    "_send_chunk",
    "_send_with_retry",
    "_strip_tags",
    "_utf16_len",
    "log",
]
