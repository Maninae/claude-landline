"""Telegram transport facade — re-exports the public + private surface used by
the rest of the daemon and the test suite.

The actual implementation now lives in four focused sibling modules:

  - `landline.telegram.transport`  — `telegram_api`, `_send_chunk`,
    `_send_with_retry`, `_parse_retry_after`, `send_response`,
    `_TRANSIENT_HTTP_STATUSES` (the HTTP request/retry/429 path).
  - `landline.telegram.typing`     — `send_typing` + per-thread pooled
    HTTPSConnection for `sendChatAction` (typing indicator only).
  - `landline.telegram.chunker`    — `chunk_text` (markdown chunker) plus
    `_chunk_html`, `_scan_open_tags_at`, `_index_inside_tag`, `_strip_tags`,
    `_open_simple_tags_at`, `_utf16_len`, `_TAG_RE`, `_REOPENABLE_SIMPLE_TAGS`,
    `send_html` (the tag-aware HTML chunker + its `send_html` driver).
  - `landline.telegram.download`   — `download_file` (getFile + bounded streaming).

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

# Markdown + tag-aware HTML chunker internals + `send_html`.
from landline.telegram.chunker import (
    _REOPENABLE_SIMPLE_TAGS,
    _TAG_RE,
    _chunk_html,
    _index_inside_tag,
    _open_simple_tags_at,
    _scan_open_tags_at,
    _strip_tags,
    _utf16_len,
    chunk_text,
    send_html,
)

# Telegram HTTP transport — API call, retry policy, send entry points.
from landline.telegram.transport import (
    _TRANSIENT_HTTP_STATUSES,
    _parse_retry_after,
    _send_chunk,
    _send_with_retry,
    send_response,
    telegram_api,
)

# Scoped HTTPS keep-alive typing pool + `send_typing`.
from landline.telegram.typing import send_typing

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
