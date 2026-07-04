"""Claude streaming engine — facade re-exporting the sub-modules.

The implementation is split across focused sibling modules:

  * `landline.tool_status`       — tool_use → status-line formatters
  * `landline.stream_sender`     — `StreamSender` + worker loop + queue constants
  * `landline.sender_registry`   — per-chat long-lived sender registry + notice routing
  * `landline.persistent_claude` — `PersistentClaude` subprocess manager + singleton
  * `landline.stream_pump`       — per-process persistent stdout reader (turn demux)
  * `landline.streaming`         — `run_claude_streaming` + `ClaudeStreamShutdownHook`

This module remains the canonical public import path: every name any other
module or test currently imports from / patches on `landline.claude` is
re-exported here, including private `_underscore` helpers and shared module
globals (`_senders`, `_senders_lock`, `_persistent_claude`, etc.). The
globals are re-exported as the SAME object so test mutations land on the
real registry. `log` is re-exported so `patch("landline.claude.log")` keeps
intercepting calls from the moved helpers (they look it up via this facade).
"""

# Re-export `log` so `patch("landline.claude.log")` keeps working for tests
# (sender_registry resolves its log call through this facade at runtime).
from landline.runtime.logging import log  # noqa: F401

from landline.claude.tool_status import (  # noqa: F401
    _extract_text_blocks,
    _format_repeated_status,
    _format_tool_status,
    _shorten_path,
)

from landline.claude.sender import (  # noqa: F401
    StreamSender,
    _ENTRY_FLUSH,
    _ENTRY_STATUS,
    _ENTRY_STOP,
    _ENTRY_TEXT,
    _IDLE_POLL_SECONDS,
    _QUEUE_HIGH_WATER,
    _SHUTDOWN_DRAIN_TIMEOUT,
    _StreamEntry,
    _StreamSenderState,
)

from landline.claude.registry import (  # noqa: F401
    _close_all_senders,
    _get_or_create_sender,
    _senders,
    _senders_lock,
    try_enqueue_chat_notice,
    try_enqueue_or_send,
)

from landline.claude.persistent import (  # noqa: F401
    PersistentClaude,
    _get_persistent_claude,
    _persistent_claude,
    _persistent_claude_lock,
)

from landline.claude.pump import (  # noqa: F401
    StreamPump,
    TurnHandle,
    _pumps,
    _pumps_lock,
    get_or_create_pump,
)

from landline.claude.streaming import (  # noqa: F401
    ClaudeStreamShutdownHook,
    run_claude_streaming,
)
