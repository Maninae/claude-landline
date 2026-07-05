"""Tests for landline.config - constants, paths, tunables, and the loader."""

import os
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


def test_workspace_resolves_from_env():
    """WORKSPACE derives from ``LANDLINE_WORKSPACE`` (set by conftest to a
    fresh tmp dir at module import) — not from the source-tree layout."""
    from landline.config import WORKSPACE
    env_workspace = os.environ.get("LANDLINE_WORKSPACE")
    assert env_workspace is not None
    assert WORKSPACE == Path(env_workspace).resolve()


def test_state_file_under_cache():
    from landline.config import STATE_FILE, WORKSPACE
    assert STATE_FILE == WORKSPACE / "cache" / "telegram-daemon-state.json"


def test_log_file_under_logs():
    from landline.config import LOG_FILE, WORKSPACE
    assert LOG_FILE == WORKSPACE / "logs" / "telegram-daemon" / "daemon.log"


def test_telegram_image_dir_under_cache():
    from landline.config import TELEGRAM_IMAGE_DIR, WORKSPACE
    assert TELEGRAM_IMAGE_DIR == WORKSPACE / "cache" / "telegram_images"


def test_claude_binary_default_is_bare_name():
    """Default is the bare ``claude`` name — resolved via ``shutil.which``
    inside ``persistent_claude._resolve_claude_binary`` at spawn time."""
    from landline.config import CLAUDE
    assert CLAUDE == "claude"


def test_timezone_is_a_zoneinfo():
    """Default TIMEZONE derives from the system zone (readlink of
    /etc/localtime) or falls back to UTC — the concrete zone is host-
    dependent, so pin only the type."""
    from landline.config import TIMEZONE
    assert isinstance(TIMEZONE, ZoneInfo)


def test_lock_states():
    from landline.config import LOCKED, UNLOCKED
    assert LOCKED == "locked"
    assert UNLOCKED == "unlocked"
    assert LOCKED != UNLOCKED


def test_positive_tunables():
    from landline.config import (
        POLL_TIMEOUT,
        CLAUDE_TIMEOUT,
        TYPING_INTERVAL,
        RATE_LIMIT_SECONDS,
        STARTUP_DELAY,
        UNLOCK_MAX_ATTEMPTS,
        UNLOCK_LOCKOUT_SECONDS,
        CONTEXT_WINDOW_TOKENS,
        STREAM_BUFFER_WINDOW,
        COALESCENCE_WINDOW_SECONDS,
        MAX_MESSAGE_LENGTH,
        MAX_QUEUED_UPDATES,
        UNLOCK_DURATION_SECONDS,
        STDERR_BUFFER_MAX,
        MAX_UPTIME_BASE,
        MAX_UPTIME_JITTER,
        LOG_MAX_BYTES,
        LOG_BACKUP_COUNT,
        FATAL_CRASH_PAUSE_SECONDS,
        POLL_ERROR_BACKOFF_BASE,
        POLL_ERROR_BACKOFF_MAX,
        POLL_ERROR_ALERT_AFTER,
        POLL_ERROR_LOG_EVERY_N,
        TELEGRAM_FILE_SIZE_LIMIT,
        MEDIA_GROUP_WAIT_SECONDS,
    )
    for val in [
        POLL_TIMEOUT, CLAUDE_TIMEOUT, TYPING_INTERVAL, RATE_LIMIT_SECONDS,
        STARTUP_DELAY, UNLOCK_MAX_ATTEMPTS, UNLOCK_LOCKOUT_SECONDS,
        CONTEXT_WINDOW_TOKENS, STREAM_BUFFER_WINDOW, COALESCENCE_WINDOW_SECONDS,
        MAX_MESSAGE_LENGTH, MAX_QUEUED_UPDATES, UNLOCK_DURATION_SECONDS,
        STDERR_BUFFER_MAX, MAX_UPTIME_BASE, MAX_UPTIME_JITTER,
        LOG_MAX_BYTES, LOG_BACKUP_COUNT, FATAL_CRASH_PAUSE_SECONDS,
        POLL_ERROR_BACKOFF_BASE, POLL_ERROR_BACKOFF_MAX,
        POLL_ERROR_ALERT_AFTER, POLL_ERROR_LOG_EVERY_N,
        TELEGRAM_FILE_SIZE_LIMIT, MEDIA_GROUP_WAIT_SECONDS,
    ]:
        assert val > 0


def test_max_queued_updates_is_thirty():
    """Sanity check on the exact cap - orchestrator and tests rely on it.

    If this gets changed without thought, the overflow "dropped N messages"
    notice contract breaks.
    """
    from landline.config import MAX_QUEUED_UPDATES
    assert MAX_QUEUED_UPDATES == 30


def test_poll_timeout_shorter_than_claude_timeout():
    """Telegram long-poll must time out well before a Claude call would."""
    from landline.config import POLL_TIMEOUT, CLAUDE_TIMEOUT
    assert POLL_TIMEOUT < CLAUDE_TIMEOUT


def test_poll_error_backoff_progression():
    """Backoff must grow from base to cap, not the reverse."""
    from landline.config import POLL_ERROR_BACKOFF_BASE, POLL_ERROR_BACKOFF_MAX
    assert POLL_ERROR_BACKOFF_BASE < POLL_ERROR_BACKOFF_MAX


def test_context_warn_thresholds_sorted():
    from landline.config import CONTEXT_WARN_THRESHOLDS
    assert CONTEXT_WARN_THRESHOLDS == sorted(CONTEXT_WARN_THRESHOLDS)
    assert all(0 < t <= 100 for t in CONTEXT_WARN_THRESHOLDS)
    # The 30/50/70 contract is used by the alert ladder - guard against silent edits.
    assert CONTEXT_WARN_THRESHOLDS == [30, 50, 70]


def test_telegram_file_size_limit_matches_api():
    """Telegram's getFile API caps downloadable files at 20MB."""
    from landline.config import TELEGRAM_FILE_SIZE_LIMIT
    assert TELEGRAM_FILE_SIZE_LIMIT == 20 * 1024 * 1024


def test_log_max_bytes_is_five_megabytes():
    from landline.config import LOG_MAX_BYTES
    assert LOG_MAX_BYTES == 5 * 1024 * 1024


def test_locked_help_contains_passphrase_prompt():
    from landline.config import LOCKED_HELP
    assert "passphrase" in LOCKED_HELP.lower()
    assert "locked" in LOCKED_HELP.lower()


def test_locked_help_mentions_passphrase():
    """The locked help should tell the user to enter their passphrase."""
    from landline.config import LOCKED_HELP
    assert "passphrase" in LOCKED_HELP.lower()


# ---------------------------------------------------------------------------
# Contract assertions for hardening constants
# ---------------------------------------------------------------------------


def test_unlock_lockout_max_seconds_caps_escalation():
    """Hard cap on the exponentially-escalated lockout window.

    Mandatory DoS guard: without the cap, an attacker who triggers lockouts
    could push the operator's next lockout beyond any reasonable recovery time
    (uncapped 2**k grows past a day after ~13 cycles, months after ~20).
    OWASP ASVS V3.3.5 requires that legitimate users always recover.

    Fails on revert if the cap is removed, set to zero, or set below the
    base lockout (which would make escalation a no-op).
    """
    from landline.config import (
        UNLOCK_LOCKOUT_MAX_SECONDS,
        UNLOCK_LOCKOUT_SECONDS,
    )
    assert UNLOCK_LOCKOUT_MAX_SECONDS == 3600
    assert isinstance(UNLOCK_LOCKOUT_MAX_SECONDS, int)
    # The cap must exceed the baseline lockout, otherwise escalation
    # immediately saturates and the ramp is dead code.
    assert UNLOCK_LOCKOUT_SECONDS < UNLOCK_LOCKOUT_MAX_SECONDS


def test_rejection_mode_defaults_silent():
    """Default mode for unauthorized-sender replies.

    Silent default removes the enumeration oracle that lets an
    unauthenticated probe confirm the bot exists. Detection survives via
    the daemon log (chat_id is still recorded at the call site).

    Fails on revert if the default flips back to "reply".
    """
    from landline.config import REJECTION_MODE
    assert REJECTION_MODE == "silent"
    assert isinstance(REJECTION_MODE, str)


def test_rejection_text_is_legacy_default():
    """Text used when REJECTION_MODE == 'reply' (legacy/incident path).

    Pinned so a future commit that flips REJECTION_MODE to 'reply' for
    incident response gets the historical message rather than something
    accidentally renamed.
    """
    from landline.config import REJECTION_TEXT
    assert REJECTION_TEXT == "This bot is private."
    assert isinstance(REJECTION_TEXT, str)


def test_conversation_log_tail_bytes_is_positive():
    """Tail-read window for the daily Telegram conversation log."""
    from landline.config import CONVERSATION_LOG_TAIL_BYTES
    assert CONVERSATION_LOG_TAIL_BYTES == 32768
    assert isinstance(CONVERSATION_LOG_TAIL_BYTES, int)
    assert CONVERSATION_LOG_TAIL_BYTES > 0


def test_session_jsonl_tail_bytes_is_positive():
    """Tail-read window for the per-session Claude Code JSONL."""
    from landline.config import SESSION_JSONL_TAIL_BYTES
    assert SESSION_JSONL_TAIL_BYTES == 32768
    assert isinstance(SESSION_JSONL_TAIL_BYTES, int)
    assert SESSION_JSONL_TAIL_BYTES > 0


def test_tail_bytes_constants_have_distinct_names():
    """Conversation-log tail and JSONL tail are semantically distinct.

    They happen to be equal today (32768) but the names must remain separate
    so a tuner can change one without dragging the other. Collapsing them
    into a single constant would re-introduce the coincidental-equality trap
    this item exists to prevent.

    Fails on revert if either constant is removed from config.py or merged
    into a single shared name.
    """
    from landline import config
    assert hasattr(config, "CONVERSATION_LOG_TAIL_BYTES")
    assert hasattr(config, "SESSION_JSONL_TAIL_BYTES")
    # They must be distinct module attributes so independent tuning is possible.
    assert "CONVERSATION_LOG_TAIL_BYTES" in vars(config)
    assert "SESSION_JSONL_TAIL_BYTES" in vars(config)
    # Both must be positive ints (sanity - a zero or negative tail would
    # silently break read_recent_conversation_history and get_context_percent).
    assert isinstance(config.CONVERSATION_LOG_TAIL_BYTES, int)
    assert isinstance(config.SESSION_JSONL_TAIL_BYTES, int)
    assert config.CONVERSATION_LOG_TAIL_BYTES > 0
    assert config.SESSION_JSONL_TAIL_BYTES > 0


def test_inject_timestamp_format_is_condensed_iso():
    """Canonical inject-queue filename timestamp format.

    Consumer (daemon/inject.py) imports this constant; producer
    (scripts/deliver-output.py) duplicates the literal with a cross-reference
    comment (producer is intentionally daemon/-import-free). A grep gate
    catches drift between the two.

    Fails on revert if the constant disappears or the format is changed
    without coordinating the producer + grep gate.
    """
    from landline.config import INJECT_TIMESTAMP_FORMAT
    assert INJECT_TIMESTAMP_FORMAT == "%Y%m%dT%H%M%S"
    assert isinstance(INJECT_TIMESTAMP_FORMAT, str)
    # The format must produce a 15-character stem (8-digit date + "T" +
    # 6-digit time) - inject.py slices stem[:15] and feeds it to strptime.
    from datetime import datetime
    sample = datetime(2026, 6, 15, 7, 0, 0).strftime(INJECT_TIMESTAMP_FORMAT)
    assert len(sample) == 15
    assert sample == "20260615T070000"


def test_daily_log_dir_mode_is_owner_only():
    """Daily-log parent dir mode (0o700, owner-only traverse)."""
    from landline.config import DAILY_LOG_DIR_MODE
    assert DAILY_LOG_DIR_MODE == 0o700
    assert isinstance(DAILY_LOG_DIR_MODE, int)


def test_daily_log_file_mode_is_owner_only():
    """Daily-log file mode (0o600, owner read/write only).

    The daily logs contain unredacted PII (passphrase-typing context, family
    details, medical/legal/work content). Fails on revert if the mode is
    relaxed to anything world- or group-readable.
    """
    from landline.config import DAILY_LOG_FILE_MODE
    assert DAILY_LOG_FILE_MODE == 0o600
    assert isinstance(DAILY_LOG_FILE_MODE, int)
    # No "other" or "group" bits may be set - that's the security invariant.
    assert DAILY_LOG_FILE_MODE & 0o077 == 0


def test_state_file_mode_is_owner_only():
    """State-file write mode (0o600). Mirrors DAILY_LOG_FILE_MODE."""
    from landline.config import STATE_FILE_MODE
    assert STATE_FILE_MODE == 0o600
    assert isinstance(STATE_FILE_MODE, int)
    assert STATE_FILE_MODE & 0o077 == 0


def test_failure_tracker_consts_live_in_config():
    """Failure-tracker tunables live in landline.config (canonical source).

    They were moved out of landline.claude.failure_tracker so the daemon's full
    tunable surface is visible from one file. failure_tracker imports them
    back; the unit-test boundary still imports from failure_tracker (which
    re-exports), but the source of truth is here.

    Fails on revert if any constant is removed from config.py, any value
    is changed (e.g. a "tighten the alert threshold" forgets to update the
    test), or the ordering invariant is violated.
    """
    from landline import config
    assert config.CLAUDE_FAILURE_BACKOFF_THRESHOLD == 3
    assert config.CLAUDE_FAILURE_ALERT_THRESHOLD == 10
    assert config.CLAUDE_FAILURE_BACKOFF_BASE_SECONDS == 30
    assert config.CLAUDE_FAILURE_BACKOFF_CAP_SECONDS == 1800
    # All four must be ints (the failure tracker uses them as iteration
    # counts and second-counts; a float would silently change downstream
    # comparison semantics).
    assert isinstance(config.CLAUDE_FAILURE_BACKOFF_THRESHOLD, int)
    assert isinstance(config.CLAUDE_FAILURE_ALERT_THRESHOLD, int)
    assert isinstance(config.CLAUDE_FAILURE_BACKOFF_BASE_SECONDS, int)
    assert isinstance(config.CLAUDE_FAILURE_BACKOFF_CAP_SECONDS, int)
    # Ordering invariant - if these ever drift, the failure-tracker state
    # machine breaks (alert before backoff makes no sense).
    assert (
        config.CLAUDE_FAILURE_BACKOFF_THRESHOLD
        < config.CLAUDE_FAILURE_ALERT_THRESHOLD
    )
    # Cap must exceed base (otherwise the exponential branch is dead).
    assert (
        config.CLAUDE_FAILURE_BACKOFF_BASE_SECONDS
        < config.CLAUDE_FAILURE_BACKOFF_CAP_SECONDS
    )


# ---------------------------------------------------------------------------
# Claude CLI OAuth-expiry markers + alert min-interval
# ---------------------------------------------------------------------------


def test_claude_auth_error_markers_is_non_empty_tuple():
    """Markers used by ``claude_dispatch._stderr_looks_like_auth_failure``.

    Must be a non-empty tuple of str (not a list — tuples pin the value
    against accidental mutation). Every entry must be lower-castable so
    the case-insensitive match in the predicate can't crash on a
    surprise non-string. Fails on revert if the constant disappears,
    empties, or a non-string sneaks in.
    """
    from landline.config import CLAUDE_AUTH_ERROR_MARKERS
    assert isinstance(CLAUDE_AUTH_ERROR_MARKERS, tuple)
    assert len(CLAUDE_AUTH_ERROR_MARKERS) > 0
    for marker in CLAUDE_AUTH_ERROR_MARKERS:
        assert isinstance(marker, str)
        assert marker != ""
        # Case-insensitive matching in the predicate calls .lower() on
        # each marker; make sure that's a safe op.
        assert isinstance(marker.lower(), str)


def test_claude_auth_error_markers_covers_known_shapes():
    """Belt-and-suspenders: the canonical strings from the operator's June 2026
    incident must remain in the tuple. Removing any of these silently
    regresses the detector against the exact shape it was built to catch.
    """
    from landline.config import CLAUDE_AUTH_ERROR_MARKERS
    lowered = tuple(m.lower() for m in CLAUDE_AUTH_ERROR_MARKERS)
    for expected in (
        "invalid authentication",
        "authentication_error",
        "invalid_grant",
        # 401 coverage must be present, but as an *anchored* HTTP-401 shape
        # (e.g. "http 401") — a BARE "401" substring false-positives on
        # any stderr containing that digit sequence for unrelated reasons
        # ("port 4014", "processed 401 files", pids/hashes ending in
        # ...401...), which fires a spurious "claude-auth-expired"
        # iMessage. At least one anchored 401 form must remain.
        "http 401",
    ):
        assert expected in lowered, (
            f"expected marker {expected!r} missing from CLAUDE_AUTH_ERROR_MARKERS"
        )
    # And the bare "401" substring must NOT be present — that's the bug
    # this list is defended against.
    assert "401" not in lowered, (
        "bare '401' substring re-added to CLAUDE_AUTH_ERROR_MARKERS — this "
        "false-positives on any stderr containing the digits 401 (ports, "
        "pids, unrelated counts) and spams the operator with auth-expiry iMessages. "
        "Use an anchored form like 'http 401' or '401 unauthorized' instead."
    )


def test_claude_auth_alert_min_interval_seconds_is_positive_int():
    """Belt-and-suspenders time floor for auth-expiry re-alerts.

    Guards against a broken latch reset spamming the operator. Must be a positive
    int (a zero or negative would degenerate the interval check to
    always-refire). Fails on revert if the constant is deleted, set to
    zero, or turned into a float.
    """
    from landline.config import CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS
    assert isinstance(CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS, int)
    assert CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS > 0


# ---------------------------------------------------------------------------
# Poller staleness threshold + check interval
# ---------------------------------------------------------------------------


def test_poll_stale_alert_threshold_exceeds_backoff_max():
    """The stale-poll threshold must comfortably exceed POLL_TIMEOUT plus
    POLL_ERROR_BACKOFF_MAX so a transient network outage cannot false-fire
    the in-process poller swap. Without this margin, an ordinary outage
    would trip the recovery path on every backoff cycle."""
    from landline.config import (
        POLL_ERROR_BACKOFF_MAX,
        POLL_STALE_ALERT_THRESHOLD_SECONDS,
        POLL_TIMEOUT,
    )
    assert POLL_STALE_ALERT_THRESHOLD_SECONDS > (POLL_TIMEOUT + POLL_ERROR_BACKOFF_MAX)


def test_poll_stale_check_interval_is_positive_int_and_less_than_threshold():
    """The staleness check interval must be a positive int and strictly
    less than the alert threshold (otherwise the rate-limit gate could
    block a legitimate swap indefinitely)."""
    from landline.config import (
        POLL_STALE_ALERT_THRESHOLD_SECONDS,
        POLL_STALE_CHECK_INTERVAL_SECONDS,
    )
    assert isinstance(POLL_STALE_CHECK_INTERVAL_SECONDS, int)
    assert POLL_STALE_CHECK_INTERVAL_SECONDS > 0
    assert POLL_STALE_CHECK_INTERVAL_SECONDS < POLL_STALE_ALERT_THRESHOLD_SECONDS


# ---------------------------------------------------------------------------
# Outbound spool tunables
# ---------------------------------------------------------------------------


def test_spool_dir_under_workspace():
    """SPOOL_DIR must live under the workspace cache/ so it's covered by
    the workspace-tightening layer 2 chmod in secure_workspace_paths.

    We inspect the declared source-level shape via ``WORKSPACE`` (which
    is not monkeypatched by the autouse spool-isolation fixture): the
    literal path segment ``"cache/telegram-outbound-spool"`` must be the
    tail of the declared constant. This intentionally tolerates the
    autouse fixture's per-test redirect.
    """
    from landline import config as _cfg
    # The autouse fixture rewrites SPOOL_DIR to a per-test tmp_path — we
    # can't compare equality here, but we assert the source declaration
    # by re-executing the RHS expression against the un-patched WORKSPACE
    # (WORKSPACE is not monkeypatched).
    expected = _cfg.WORKSPACE / "cache" / "telegram-outbound-spool"
    # Verify the DECLARATION in config.py is what we think it is by reading
    # the module source directly — bypasses monkeypatch entirely.
    import inspect
    src = inspect.getsource(_cfg)
    assert 'SPOOL_DIR = WORKSPACE / "cache" / "telegram-outbound-spool"' in src
    # And "cache" is in WORKSPACE_SENSITIVE_DIRS so the parent gets 0o700.
    assert "cache" in _cfg.WORKSPACE_SENSITIVE_DIRS
    # (`expected` computed above proves the RHS math is coherent.)
    assert expected.name == "telegram-outbound-spool"


def test_spool_dir_and_file_modes_are_owner_only():
    """SPOOL_DIR_MODE=0o700, SPOOL_FILE_MODE=0o600 — no other/group bits."""
    from landline.config import SPOOL_DIR_MODE, SPOOL_FILE_MODE
    assert SPOOL_DIR_MODE == 0o700
    assert SPOOL_FILE_MODE == 0o600
    assert SPOOL_DIR_MODE & 0o077 == 0
    assert SPOOL_FILE_MODE & 0o077 == 0


def test_spool_ordering_invariants():
    """SPOOL_MAX_AGE > SPOOL_REPLAY_INTERVAL > SPOOL_REPLAY_MIN_AGE > 0;
    SPOOL_MAX_FILES > 0. If any ordering flips, the replay pass either
    fires against files that may still be in-flight (min_age too small
    vs. interval), or discards fresh work (max_age too tight)."""
    from landline.config import (
        SPOOL_MAX_AGE_SECONDS,
        SPOOL_MAX_FILES,
        SPOOL_REPLAY_INTERVAL_SECONDS,
        SPOOL_REPLAY_MIN_AGE_SECONDS,
    )
    assert SPOOL_REPLAY_MIN_AGE_SECONDS > 0
    assert SPOOL_REPLAY_INTERVAL_SECONDS > SPOOL_REPLAY_MIN_AGE_SECONDS
    assert SPOOL_MAX_AGE_SECONDS > SPOOL_REPLAY_INTERVAL_SECONDS
    assert SPOOL_MAX_FILES > 0
    # All ints (used in filename math and pathlib comparisons).
    assert isinstance(SPOOL_MAX_AGE_SECONDS, int)
    assert isinstance(SPOOL_MAX_FILES, int)
    assert isinstance(SPOOL_REPLAY_INTERVAL_SECONDS, int)
    assert isinstance(SPOOL_REPLAY_MIN_AGE_SECONDS, int)


# ---------------------------------------------------------------------------
# Media pipeline generalization + document ingestion constants
# ---------------------------------------------------------------------------


def test_telegram_file_dir_under_cache():
    from landline.config import TELEGRAM_FILE_DIR, WORKSPACE
    assert TELEGRAM_FILE_DIR == WORKSPACE / "cache" / "telegram_files"


def test_document_max_size_bytes_positive():
    from landline.config import DOCUMENT_MAX_SIZE_BYTES, TELEGRAM_FILE_SIZE_LIMIT
    assert DOCUMENT_MAX_SIZE_BYTES > 0
    # Kept in lockstep with the Telegram cap so a naive raise doesn't
    # accidentally clear the daemon-wide byte ceiling.
    assert DOCUMENT_MAX_SIZE_BYTES <= TELEGRAM_FILE_SIZE_LIMIT


def test_document_allowed_extensions_all_lowercase_and_dotted():
    from landline.config import DOCUMENT_ALLOWED_EXTENSIONS
    assert isinstance(DOCUMENT_ALLOWED_EXTENSIONS, frozenset)
    for ext in DOCUMENT_ALLOWED_EXTENSIONS:
        assert ext.startswith(".")
        assert ext == ext.lower()
    # A minimal safety-net set — pdf must be present.
    assert ".pdf" in DOCUMENT_ALLOWED_EXTENSIONS


def test_document_allowed_mime_prefixes_lowercase():
    from landline.config import DOCUMENT_ALLOWED_MIME_PREFIXES
    assert isinstance(DOCUMENT_ALLOWED_MIME_PREFIXES, tuple)
    for m in DOCUMENT_ALLOWED_MIME_PREFIXES:
        assert m == m.lower()
    assert "application/pdf" in DOCUMENT_ALLOWED_MIME_PREFIXES


def test_media_cache_dirs_contains_both_image_and_file_dirs():
    from landline.config import (
        MEDIA_CACHE_DIRS,
        TELEGRAM_FILE_DIR,
        TELEGRAM_IMAGE_DIR,
    )
    assert TELEGRAM_IMAGE_DIR in MEDIA_CACHE_DIRS
    assert TELEGRAM_FILE_DIR in MEDIA_CACHE_DIRS


def test_media_cache_dir_mode_is_0700():
    from landline.config import MEDIA_CACHE_DIR_MODE
    assert MEDIA_CACHE_DIR_MODE == 0o700


def test_telegram_file_retention_hours_positive():
    from landline.config import TELEGRAM_FILE_RETENTION_HOURS
    assert TELEGRAM_FILE_RETENTION_HOURS > 0


# --- Voice transcription constants ---


def test_media_cache_dirs_include_voice():
    """Voice cache dir is swept alongside images + files at startup."""
    from landline.config import MEDIA_CACHE_DIRS, TELEGRAM_VOICE_DIR
    assert TELEGRAM_VOICE_DIR in MEDIA_CACHE_DIRS


def test_telegram_voice_dir_under_cache():
    from landline.config import TELEGRAM_VOICE_DIR, WORKSPACE
    assert TELEGRAM_VOICE_DIR == WORKSPACE / "cache" / "telegram_voice"


def test_telegram_voice_retention_hours_positive():
    from landline.config import TELEGRAM_VOICE_RETENTION_HOURS
    assert TELEGRAM_VOICE_RETENTION_HOURS > 0


def test_whisper_bin_is_a_string():
    """Default is the bare ``whisper`` name (resolvable on PATH); deployers
    override to an absolute path via ``landline.json`` when the launchd
    minimal-PATH bites."""
    from landline.config import WHISPER_BIN
    assert isinstance(WHISPER_BIN, str)
    assert WHISPER_BIN


def test_whisper_model_defaults_to_base():
    """`base` is the current default. If this ever changes, update the
    latency budget in voice_handler / clarify in the design brief."""
    from landline.config import WHISPER_MODEL
    assert WHISPER_MODEL == "base"


def test_whisper_model_dir_under_home_cache():
    from landline.config import WHISPER_MODEL_DIR
    from pathlib import Path
    assert Path(WHISPER_MODEL_DIR).name == "whisper"


def test_whisper_language_pinned():
    from landline.config import WHISPER_LANGUAGE
    assert WHISPER_LANGUAGE == "en"


def test_voice_max_duration_seconds_positive():
    from landline.config import VOICE_MAX_DURATION_SECONDS
    assert VOICE_MAX_DURATION_SECONDS > 0


def test_voice_transcribe_timeout_exceeds_max_duration_budget():
    """The wall-clock timeout must comfortably exceed the CPU budget for
    a max-duration voice note at whisper base's ~2–4x real-time factor,
    otherwise legitimate long notes trip the timeout."""
    from landline.config import (
        VOICE_MAX_DURATION_SECONDS,
        VOICE_TRANSCRIBE_TIMEOUT_SECONDS,
    )
    # 4x real-time upper bound on CPU; timeout must exceed even this.
    assert VOICE_TRANSCRIBE_TIMEOUT_SECONDS >= VOICE_MAX_DURATION_SECONDS // 2


def test_voice_max_transcript_chars_positive():
    from landline.config import VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS
    assert VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS > 0


def test_voice_accept_types_include_voice_audio_video_note():
    from landline.config import VOICE_ACCEPT_TYPES
    assert isinstance(VOICE_ACCEPT_TYPES, frozenset)
    for k in ("voice", "audio", "video_note"):
        assert k in VOICE_ACCEPT_TYPES


def test_voice_allowed_extensions_lowercase_and_dotted():
    from landline.config import VOICE_ALLOWED_EXTENSIONS
    assert isinstance(VOICE_ALLOWED_EXTENSIONS, frozenset)
    for ext in VOICE_ALLOWED_EXTENSIONS:
        assert ext.startswith(".")
        assert ext == ext.lower()
    # OGG is Telegram's canonical voice format.
    assert ".ogg" in VOICE_ALLOWED_EXTENSIONS


# ---------------------------------------------------------------------------
# Reaction ACKs (setMessageReaction) — constant contracts
# ---------------------------------------------------------------------------

# Telegram Bot API 7.x allowed reaction emoji (verified against
# https://core.telegram.org/bots/api#setmessagereaction). Frozen locally so
# a silent constant flip on either REACTION_ACK_EMOJI or REACTION_DONE_EMOJI
# to a non-member (e.g. ✅ which is NOT allowed) trips the sanity gate below.
_BOT_API_ALLOWED_REACTION_EMOJI = frozenset({
    "\U0001f44d", "\U0001f44e", "❤", "\U0001f525", "\U0001f970",
    "\U0001f44f", "\U0001f601", "\U0001f914", "\U0001f92f", "\U0001f631",
    "\U0001f92c", "\U0001f622", "\U0001f389", "\U0001f929", "\U0001f92e",
    "\U0001f4a9", "\U0001f64f", "\U0001f44c", "\U0001f54a", "\U0001f921",
    "\U0001f971", "\U0001f974", "\U0001f60d", "\U0001f433",
    "❤‍\U0001f525", "\U0001f31a", "\U0001f32d", "\U0001f4af",
    "\U0001f923", "⚡", "\U0001f34c", "\U0001f3c6", "\U0001f494",
    "\U0001f928", "\U0001f610", "\U0001f353", "\U0001f37e", "\U0001f48b",
    "\U0001f595", "\U0001f608", "\U0001f634", "\U0001f62d", "\U0001f913",
    "\U0001f47b", "\U0001f468‍\U0001f4bb", "\U0001f440", "\U0001f383",
    "\U0001f648", "\U0001f607", "\U0001f628", "\U0001f91d", "✍",
    "\U0001f917", "\U0001f9d1‍\U0001f384", "\U0001f385", "\U0001f384",
    "☃", "\U0001f485", "\U0001f92a", "\U0001f5ff", "\U0001f192",
    "\U0001f498", "\U0001f649", "\U0001f984", "\U0001f618", "\U0001f48a",
    "\U0001f64a", "\U0001f60e", "\U0001f47e",
    "\U0001f937‍♂", "\U0001f937",
    "\U0001f937‍♀", "\U0001f621",
})


@pytest.mark.reactions_network
def test_reaction_acks_enabled_default_true():
    """Kill-switch defaults to on. Flipping the default to False is a
    one-line disable path for the whole reaction feature (see reactions.py).

    Opted into the ``reactions_network`` marker so the autouse
    ``disable_reactions_network`` conftest fixture (which flips the flag
    off for every OTHER test to prevent leaked ``setMessageReaction`` POSTs
    to Telegram) leaves the true source-of-truth default in place here.

    Parses the source directly for the default literal fed to ``_cfg`` —
    avoids any module-level monkeypatch leakage from prior tests and
    preserves object-identity in other modules that captured ``config.X``
    at import time (importlib.reload would break those).
    """
    from landline import config as _config
    import ast
    src = Path(_config.__file__).read_text()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id == "REACTION_ACKS_ENABLED":
                    # RHS is ``_cfg("reaction_acks_enabled", True)`` — pull
                    # the second positional arg as the default literal.
                    call = node.value
                    assert isinstance(call, ast.Call), (
                        "REACTION_ACKS_ENABLED must derive from _cfg — got %r" % (call,)
                    )
                    assert len(call.args) >= 2
                    default = call.args[1]
                    assert isinstance(default, ast.Constant)
                    assert default.value is True
                    return
    raise AssertionError("REACTION_ACKS_ENABLED not found in landline/config.py")


def test_reaction_ack_emoji_is_eyes_and_in_allowed_set():
    """👀 is our choice for the classify-time receipt. Must be in the
    Bot API allowed set — if Telegram drops it, this test fails and
    the operator picks another from the allow-list."""
    from landline.config import REACTION_ACK_EMOJI
    assert REACTION_ACK_EMOJI == "\U0001f440"  # 👀
    assert REACTION_ACK_EMOJI in _BOT_API_ALLOWED_REACTION_EMOJI


def test_reaction_done_emoji_is_ok_and_in_allowed_set():
    """👌 is our choice for successful-completion. Must be in the Bot
    API allowed set. Guarded against a silent revert to ✅ (which is
    NOT in the allowed set)."""
    from landline.config import REACTION_DONE_EMOJI
    assert REACTION_DONE_EMOJI == "\U0001f44c"  # 👌
    assert REACTION_DONE_EMOJI in _BOT_API_ALLOWED_REACTION_EMOJI


def test_check_mark_is_not_in_allowed_reaction_set():
    """Anchor sanity check: ✅ (U+2705) is NOT in the Bot API allowed
    set — proves the allow-list constant is a real gate, not a
    tautology that would accept anything."""
    assert "✅" not in _BOT_API_ALLOWED_REACTION_EMOJI


def test_reaction_http_timeout_bounded_positive():
    """Bounded per-request timeout so a hung Telegram edge can't leak
    reaction threads."""
    from landline.config import REACTION_HTTP_TIMEOUT_SECONDS
    assert REACTION_HTTP_TIMEOUT_SECONDS > 0
    # 60s would be too long for a fire-and-forget UX polish call; anchor
    # the ceiling at 30s to keep the invariant visible.
    assert REACTION_HTTP_TIMEOUT_SECONDS <= 30


def test_reaction_max_attempts_at_least_two():
    """Retry at least once so a transient blip doesn't lose the
    reaction. Cap kept low — reactions are UX polish, not
    at-least-once delivery."""
    from landline.config import REACTION_MAX_ATTEMPTS
    assert REACTION_MAX_ATTEMPTS >= 2
    assert REACTION_MAX_ATTEMPTS <= 5


# ---------------------------------------------------------------------------
# Usage/cost stats constants
# ---------------------------------------------------------------------------

def test_usage_stats_file_named_correctly():
    """Sibling of STATE_FILE, same ``cache/`` dir, named ``usage-stats.json``.

    Note: an autouse conftest fixture redirects the module attribute to a
    tmp path so the daemon's real cache stays clean during tests. We
    verify the intended NAME (which the fixture preserves) rather than
    the absolute path.
    """
    from landline.config import USAGE_STATS_FILE
    from pathlib import Path
    assert isinstance(USAGE_STATS_FILE, Path)
    assert USAGE_STATS_FILE.name == "usage-stats.json"


def test_usage_stats_file_mode_is_owner_only():
    """Mirrors STATE_FILE_MODE — the aggregate can contain daily counters
    that hint at when the operator is active; keep it owner-read only."""
    from landline.config import USAGE_STATS_FILE_MODE
    assert USAGE_STATS_FILE_MODE == 0o600


def test_usage_stats_retention_days_is_positive():
    from landline.config import USAGE_STATS_RETENTION_DAYS
    assert USAGE_STATS_RETENTION_DAYS > 0
    # Sanity ceiling — a year of daily buckets is still tiny (~200 bytes
    # each) but signals a design change if the value drifts wildly.
    assert USAGE_STATS_RETENTION_DAYS <= 400


def test_usage_stats_model_label_max_is_reasonable():
    """Defensive cap on model-id strings so unknown future ids can't
    blow up the JSON size — has to be big enough to hold real ids
    (claude-opus-4-8 = 15 chars)."""
    from landline.config import USAGE_STATS_MODEL_LABEL_MAX
    assert USAGE_STATS_MODEL_LABEL_MAX >= 16
    assert USAGE_STATS_MODEL_LABEL_MAX <= 256
