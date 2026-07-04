"""Landline configuration - constants, paths, tunables, + ``landline.json`` overrides.

All values are plain constants with no behavioral side effects on import. Tunables
that govern user-visible daemon behaviour (timeouts, thresholds, retention
windows, backoff curves) live here. Tunables that are pure implementation
details of a worker loop or thread internals intentionally stay in their
owning module to keep this file scannable.

Runtime seam: values that a deployer legitimately wants to change (Keychain
account, Claude binary / model, personas, launchd label prefix, whisper knobs,
rejection posture, reaction ACKs) are pulled from ``<WORKSPACE>/landline.json``
via ``_cfg`` at module import. See ``_ALLOWED_KEYS`` for the full list.

Module-local tunables that intentionally are NOT here:
    - landline.stream_sender._IDLE_POLL_SECONDS  - worker-thread idle poll
    - landline.stream_sender._QUEUE_HIGH_WATER   - log-once backlog threshold
    - landline.image_cache.*                     - retention sweep internals
    - landline.html_chunker.* (regex/limits)     - chunker implementation details
If you find yourself wanting to move one of these here, ask first whether it
is a knob the deployer would tune from ``landline.json`` - if not, leave it
local.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Workspace + landline.json loader
# ---------------------------------------------------------------------------

# WORKSPACE is the agent workspace. Under launchd the plist sets
# WorkingDirectory to the workspace so ``os.getcwd()`` returns it; the
# ``LANDLINE_WORKSPACE`` env var is an explicit override for interactive runs
# and for tests (an autouse conftest fixture points it at a fresh tmp dir).
WORKSPACE = Path(os.environ.get("LANDLINE_WORKSPACE", os.getcwd())).resolve()


# Value-type validators keyed by JSON key. Each returns the coerced value or
# raises ``ValueError`` when the type is wrong. Path-like keys ``expanduser``.
def _v_str(v: Any) -> str:
    if not isinstance(v, str):
        raise ValueError("expected string")
    return v


def _v_str_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str):
        raise ValueError("expected string or null")
    return v


def _v_bool(v: Any) -> bool:
    if not isinstance(v, bool):
        raise ValueError("expected boolean")
    return v


def _v_path_str(v: Any) -> str:
    if not isinstance(v, str):
        raise ValueError("expected string (path)")
    return os.path.expanduser(v)


def _v_path_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    if not isinstance(v, str):
        raise ValueError("expected string (path) or null")
    return os.path.expanduser(v)


# Fixed allowlist of JSON keys -> validator. An unknown key in landline.json
# raises SystemExit at import (fail-fast — never run half-configured).
_ALLOWED_KEYS = {
    "keychain_account": _v_str,
    "claude_binary": _v_path_str,
    "claude_model": _v_str_or_none,
    "claude_permission_mode": _v_str,
    "user_name": _v_str,
    "agent_name": _v_str,
    "timezone": _v_str_or_none,
    "launchd_label_prefix": _v_str,
    "morning_brief_glob": _v_str_or_none,
    "whisper_bin": _v_path_str,
    "whisper_model": _v_str,
    "whisper_model_dir": _v_path_str,
    "whisper_language": _v_str,
    "reaction_acks_enabled": _v_bool,
    "rejection_mode": _v_str,
}


def _die(msg: str) -> None:
    """Fail-fast with a one-line error to stderr. launchd's ThrottleInterval
    (30s) bounds the crash loop so a persistent typo is loud but bounded."""
    sys.stderr.write("landline config error: " + msg + "\n")
    raise SystemExit(2)


def _load_overrides(workspace: Path = None) -> dict:
    """Load ``<workspace>/landline.json`` and validate every key.

    Returns ``{}`` if the file is absent. Raises ``SystemExit`` on: malformed
    JSON, non-object top-level, unknown key, or type mismatch on any value.

    The ``workspace`` parameter defaults to the module-level ``WORKSPACE``;
    tests pass a tmp dir explicitly so the loader logic can be exercised
    without side-effect-reloading the whole config module (which would
    break ``is`` identity for downstream constants).
    """
    if workspace is None:
        workspace = WORKSPACE
    cfg_path = workspace / "landline.json"
    if not cfg_path.exists():
        return {}
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        _die("cannot read %s: %s" % (cfg_path, e))
    if not isinstance(raw, dict):
        _die("%s must be a JSON object at the top level" % cfg_path)
    coerced = {}
    for key, value in raw.items():
        if key not in _ALLOWED_KEYS:
            _die("unknown key %r in %s (allowed: %s)" % (
                key, cfg_path, ", ".join(sorted(_ALLOWED_KEYS))
            ))
        try:
            coerced[key] = _ALLOWED_KEYS[key](value)
        except ValueError as e:
            _die("bad value for %r in %s: %s" % (key, cfg_path, e))
    return coerced


_OVERRIDES = _load_overrides()


def _cfg(key: str, default: Any) -> Any:
    """Fetch an override value or the default. Every configurable constant
    below derives from ``_cfg``; every other name in this module is a plain
    constant."""
    return _OVERRIDES.get(key, default)


def _system_timezone() -> ZoneInfo:
    """Best-effort read of the system timezone by inspecting ``/etc/localtime``.

    ``/etc/localtime`` is a symlink into ``.../zoneinfo/<region>/<city>`` on
    macOS + most Linux distros. If we can't parse it we fall back to UTC.
    """
    try:
        target = os.readlink("/etc/localtime")
    except OSError:
        return ZoneInfo("UTC")
    parts = Path(target).parts
    if "zoneinfo" in parts:
        idx = parts.index("zoneinfo")
        name = "/".join(parts[idx + 1:])
        if name:
            try:
                return ZoneInfo(name)
            except Exception:
                pass
    return ZoneInfo("UTC")


# ---------------------------------------------------------------------------
# Deployer-facing constants (derived from landline.json + defaults)
# ---------------------------------------------------------------------------

# Keychain account under which every ``telegram-*`` service name is stored.
# Service names stay fixed (see security.py); only the account is deployer-tunable.
KEYCHAIN_ACCOUNT = _cfg("keychain_account", "landline")

# Claude Code CLI. Bare name (default ``claude``) is resolved on PATH at spawn
# via ``shutil.which``. An absolute path is passed through as-is (launchd's
# minimal PATH makes bare names risky in production; deployers who need a
# specific fork should set an absolute path).
CLAUDE = _cfg("claude_binary", "claude")

# Model override for the persistent Claude subprocess. ``None`` omits the
# ``--model`` flag entirely (uses the CLI's compiled-in default).
CLAUDE_MODEL = _cfg("claude_model", None)

# Permission mode passed to ``claude -p --permission-mode``. Default is
# ``bypassPermissions`` because the daemon runs unattended and the operator
# is not around to answer tool prompts. Documented in docs/SETUP.md.
CLAUDE_PERMISSION_MODE = _cfg("claude_permission_mode", "bypassPermissions")

# Personas surfaced in prompts, iMessage prefixes, log labels, /status header,
# and daily-log markdown role prefixes. Defaults are neutral ("User" /
# "Assistant"); deployers set them to whatever their daily-log convention is.
USER_NAME = _cfg("user_name", "User")
AGENT_NAME = _cfg("agent_name", "Assistant")

# Timezone applied to all datestamps the operator sees (daily-log names, "since"
# labels, inject-queue time formatting, tz abbreviation in prose). Default is
# the system timezone; ``None`` in landline.json also picks the system tz.
_tz_name = _cfg("timezone", None)
if _tz_name is None:
    TIMEZONE = _system_timezone()
else:
    try:
        TIMEZONE = ZoneInfo(_tz_name)
    except Exception as _tz_err:
        _die("unknown timezone %r: %s" % (_tz_name, _tz_err))

# Prefix used by /status to filter ``launchctl list`` output for jobs the
# deployer considers "own" jobs. Empty-string default would match every job,
# so we require an explicit ``com.landline`` default that a deployer can
# override to their own reverse-DNS prefix.
LAUNCHD_LABEL_PREFIX = _cfg("launchd_label_prefix", "com.landline")

# Glob (relative to WORKSPACE) for "morning brief"-style files surfaced by
# /status. ``None`` skips the briefs line entirely — most deployers don't
# have a briefing pipeline.
MORNING_BRIEF_GLOB = _cfg("morning_brief_glob", None)

# Whisper CLI defaults. When ``whisper`` is on PATH the bare name works;
# absolute paths (Homebrew, ``/Volumes/vega/...``) are passed through as-is.
WHISPER_BIN = _cfg("whisper_bin", "whisper")
WHISPER_MODEL = _cfg("whisper_model", "base")
# Default lives under the invoking user's home; expanduser applied by the
# validator when set from landline.json.
WHISPER_MODEL_DIR = _cfg("whisper_model_dir", os.path.expanduser("~/.cache/whisper"))
WHISPER_LANGUAGE = _cfg("whisper_language", "en")

# Kill switch for the reaction ACK feature (👀 → 👌). See reactions.py.
REACTION_ACKS_ENABLED = _cfg("reaction_acks_enabled", True)

# B3 - rejection mode for unauthorized senders.
# "silent": no outbound reply; daemon still logs the rejected chat_id (default).
# "reply":  legacy behavior - send REJECTION_TEXT to the sender.
# Silent mode removes an enumeration oracle for the bot's privacy gate. To
# restore the loud reply for incident-response signal, flip to "reply" and
# restart the daemon (one-line config change, no code revert needed).
REJECTION_MODE = _cfg("rejection_mode", "silent")
REJECTION_TEXT = "This bot is private."

# ---------------------------------------------------------------------------
# Poll / Claude / typing timing
# ---------------------------------------------------------------------------
POLL_TIMEOUT = 30
CLAUDE_TIMEOUT = 600
TYPING_INTERVAL = 4
RATE_LIMIT_SECONDS = 1
STARTUP_DELAY = 3

# ---------------------------------------------------------------------------
# Unlock / lock state machine
# ---------------------------------------------------------------------------
UNLOCK_MAX_ATTEMPTS = 5
UNLOCK_LOCKOUT_SECONDS = 300
# B1 - hard cap on the exponentially-escalated lockout window. Without this
# cap, an attacker who can trigger repeated lockouts (or a stressed operator
# mistyping) pushes the next lockout past any reasonable ability to wait it
# out; per OWASP ASVS V3.3.5 lockout policies must escalate AND be bounded so
# legitimate users always recover. 3600s = 1h: long enough to make brute-force
# boring (<=120 guesses/day at the cap), short enough that a real recovery is
# a coffee break.
UNLOCK_LOCKOUT_MAX_SECONDS = 3600
UNLOCK_DURATION_SECONDS = 345600

# ---------------------------------------------------------------------------
# Context / window / streaming buffers
# ---------------------------------------------------------------------------
CONTEXT_WARN_THRESHOLDS = [30, 50, 70]
CONTEXT_WINDOW_TOKENS = 1_000_000
STREAM_BUFFER_WINDOW = 0.5
STATUS_BUFFER_WINDOW = 0.3
COALESCENCE_WINDOW_SECONDS = 1.0
MAX_MESSAGE_LENGTH = 32768
MAX_QUEUED_UPDATES = 30
STDERR_BUFFER_MAX = 8192

# ---------------------------------------------------------------------------
# Uptime / logging / fatal-crash pause
# ---------------------------------------------------------------------------
MAX_UPTIME_BASE = 345600
MAX_UPTIME_JITTER = 7200
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 5
FATAL_CRASH_PAUSE_SECONDS = 60

# ---------------------------------------------------------------------------
# Poller backoff & retry
# ---------------------------------------------------------------------------
POLL_ERROR_BACKOFF_BASE = 5
POLL_ERROR_BACKOFF_MAX = 300
POLL_ERROR_ALERT_AFTER = 300
POLL_ERROR_LOG_EVERY_N = 12
POLL_API_ERROR_BACKOFF_SECONDS = 2

# ---------------------------------------------------------------------------
# Outbound Telegram send retry
# ---------------------------------------------------------------------------
# The transport retries 429s (honoring the server-advertised Retry-After) and
# a small set of transient failures (HTTP 5xx, connection/timeout errors) up
# to SEND_MAX_ATTEMPTS times total (initial try + retries). Non-429 retries
# use a short exponential backoff (SEND_RETRY_BACKOFF_SECONDS, applied
# positionally per retry index). 429s honor the advertised delay, clamped
# between SEND_RETRY_AFTER_FALLBACK (used when the server doesn't advertise
# one) and SEND_RETRY_AFTER_CAP. 5 attempts / (1,2,4,8) backoff rides out the
# ~15-35s TLS/handshake stalls that flaky networks show in the daemon log
# (the old 3 / (1,2) gave up ~4s in, silently dropping the reply). A rare
# duplicate send on a timed-out-but-delivered request is an accepted trade
# against silent drops.
SEND_MAX_ATTEMPTS = 5
SEND_RETRY_BACKOFF_SECONDS = (1, 2, 4, 8)
SEND_RETRY_AFTER_FALLBACK = 3
SEND_RETRY_AFTER_CAP = 30

# ---------------------------------------------------------------------------
# Dedup / media / storage
# ---------------------------------------------------------------------------
MAX_DEDUP_IDS = 100_000

# C3 - tail-read window for the daily Telegram conversation log when
# rebuilding "recent dialogue" context for a freshly-reset Claude session.
# 32 KB ~= 20 turns of dialogue. Tunes a function of Telegram dialogue density.
CONVERSATION_LOG_TAIL_BYTES = 32768

# C3 - tail-read window for the per-session Claude Code JSONL when scanning
# backwards for the last assistant message's usage block (used by /status
# context-percent). 32 KB ~= 1-2 recent assistant turns. Tunes a function of
# Anthropic SDK JSONL verbosity (cache_*_input_tokens etc.).
SESSION_JSONL_TAIL_BYTES = 32768

TELEGRAM_IMAGE_DIR = WORKSPACE / "cache" / "telegram_images"
TELEGRAM_FILE_DIR = WORKSPACE / "cache" / "telegram_files"
TELEGRAM_VOICE_DIR = WORKSPACE / "cache" / "telegram_voice"
TELEGRAM_FILE_SIZE_LIMIT = 20 * 1024 * 1024  # 20MB - Telegram getFile limit
TELEGRAM_IMAGE_RETENTION_HOURS = 24  # Sweep cached photos older than this at startup
TELEGRAM_FILE_RETENTION_HOURS = 24  # Sweep cached documents older than this at startup
TELEGRAM_VOICE_RETENTION_HOURS = 24  # Sweep cached voice notes older than this at startup
MEDIA_GROUP_WAIT_SECONDS = 1.5  # Time to wait for album photos to arrive

# ---------------------------------------------------------------------------
# Document ingestion
# ---------------------------------------------------------------------------
# Mirrors TELEGRAM_FILE_SIZE_LIMIT (kept as a separate name so a future
# lower-for-PDFs tightening is a one-liner).
DOCUMENT_MAX_SIZE_BYTES = 20 * 1024 * 1024

# Extensions accepted for inbound document ingestion. Case-insensitive match
# at check time (.lower()); the extension is the primary content-type gate.
DOCUMENT_ALLOWED_EXTENSIONS = frozenset({
    ".pdf", ".txt", ".md", ".csv", ".json", ".log", ".tsv", ".yaml", ".yml",
})

# Belt-and-suspenders mime confirmation when Telegram supplies `mime_type`.
# The extension is the primary gate; a missing mime does NOT block acceptance.
DOCUMENT_ALLOWED_MIME_PREFIXES = (
    "text/",
    "application/pdf",
    "application/json",
    "application/x-yaml",
    "application/x-ndjson",
)

# Cache dirs iterated by the generalized ``sweep_media_caches`` at startup.
MEDIA_CACHE_DIRS = (TELEGRAM_IMAGE_DIR, TELEGRAM_FILE_DIR, TELEGRAM_VOICE_DIR)

# Per-cache retention (hours) consumed by ``sweep_media_caches`` at startup.
# Each dir gets its OWN retention window so voice-note privacy (raw audio +
# transcript) can be tuned independently of PDF or image retention. Without
# this mapping the sweep silently applied ``TELEGRAM_IMAGE_RETENTION_HOURS``
# (24h) to every dir, and the two ``TELEGRAM_FILE_RETENTION_HOURS`` /
# ``TELEGRAM_VOICE_RETENTION_HOURS`` knobs above were dead config.
MEDIA_CACHE_RETENTION_HOURS = {
    TELEGRAM_IMAGE_DIR: TELEGRAM_IMAGE_RETENTION_HOURS,
    TELEGRAM_FILE_DIR: TELEGRAM_FILE_RETENTION_HOURS,
    TELEGRAM_VOICE_DIR: TELEGRAM_VOICE_RETENTION_HOURS,
}

# 0700 dir mode for media cache directories. Matches SPOOL_DIR_MODE — the
# workspace-wide "no umask, chmod each dir" invariant.
MEDIA_CACHE_DIR_MODE = 0o700

# D2 - inject-queue filename timestamp format - canonical authority.
# Consumer: landline/inject.py imports this constant and parses <stem> via
#   datetime.strptime(stem[:15], INJECT_TIMESTAMP_FORMAT).
# Producer: an out-of-tree deliver-output.py-shaped script writes
#   "<stem>-<label>.json" where <stem> = datetime.now().strftime("%Y%m%dT%H%M%S").
#   The producer keeps the literal (not an import) to stay landline/-import-free
#   so cron deliveries survive any landline import-time error.
INJECT_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
STATE_FILE = WORKSPACE / "cache" / "telegram-daemon-state.json"
LOG_FILE = WORKSPACE / "logs" / "telegram-daemon" / "daemon.log"

# B4 - daily-log PII permissions: 0o600 files + 0o700 dir, set via os.fchmod
# (process-wide os.umask is forbidden - it races concurrent file creation in
# poller/sender threads). Used by landline.state.log_conversation and
# landline.state.secure_daily_logs.
DAILY_LOG_DIR_MODE = 0o700
DAILY_LOG_FILE_MODE = 0o600
# B4 - state-file write mode. Mirrors DAILY_LOG_FILE_MODE; cache/ is
# already 700-isolated, but defense-in-depth removes a special case from
# future audits.
STATE_FILE_MODE = 0o600

# ---------------------------------------------------------------------------
# State labels
# ---------------------------------------------------------------------------
LOCKED = "locked"
UNLOCKED = "unlocked"

LOCKED_HELP = "\U0001f512 Session is locked. Enter the passphrase to unlock."

# ---------------------------------------------------------------------------
# Claude failure tracker (constants live here, failure_tracker imports them)
# ---------------------------------------------------------------------------
# Governs when the daemon enters Claude-call backoff and when it fires the
# "Claude unavailable" iMessage alert. See landline.failure_tracker.
# Ordering invariants (enforced by test_config):
#   CLAUDE_FAILURE_BACKOFF_THRESHOLD < CLAUDE_FAILURE_ALERT_THRESHOLD
#   CLAUDE_FAILURE_BACKOFF_BASE_SECONDS < CLAUDE_FAILURE_BACKOFF_CAP_SECONDS
CLAUDE_FAILURE_BACKOFF_THRESHOLD = 3
CLAUDE_FAILURE_ALERT_THRESHOLD = 10
CLAUDE_FAILURE_BACKOFF_BASE_SECONDS = 30
CLAUDE_FAILURE_BACKOFF_CAP_SECONDS = 1800

# ---------------------------------------------------------------------------
# Cluster 1 (foundation): async iMessage notifications + workspace perms
# ---------------------------------------------------------------------------
# Subprocess timeout for the ``osascript`` iMessage-send call spawned by
# landline.notifications._do_osascript. Bounded so a hung AppleScript event
# or slow Messages handoff can't hold onto the alert-worker thread forever;
# matches the historical value from the in-line subprocess.run(..., timeout=30)
# call site (M13 hoisted it out of that in-line block).
IMESSAGE_SEND_SUBPROCESS_TIMEOUT_SECONDS = 30

# Workspace-sensitive top-level dirs receive this mode at startup so a
# fresh checkout / re-mount / new-machine bootstrap never leaves them
# world-readable. Distinct from DAILY_LOG_DIR_MODE so a future 0o750 posture
# for daily-log-vs-workspace can diverge without collateral. See
# landline.state.secure_workspace_paths for the call site.
WORKSPACE_SENSITIVE_DIR_MODE = 0o700
WORKSPACE_SENSITIVE_DIRS = ("memory", "cache", "inbox", "outbox", "logs")

# ---------------------------------------------------------------------------
# Cluster 2: stale --resume auto-recovery
# ---------------------------------------------------------------------------
# Stderr markers used by claude_dispatch.looks_like_pruned_resume to catch
# the pruned/nonexistent --resume shape when the result-event path did not
# populate is_error (defense-in-depth for CLI drift). The canonical path is
# is_error + no init on the failing turn; these markers are the belt-and-
# suspenders fallback for the tail of the process's stderr buffer.
STALE_RESUME_STDERR_MARKERS = (
    "No conversation found with session ID",
    "session not found",
)

# ---------------------------------------------------------------------------
# Cluster 3: Claude CLI OAuth-expiry detection
# ---------------------------------------------------------------------------
# Markers scanned (case-insensitive) in the CC subprocess's stderr_tail by
# claude_dispatch._record_outcome. Match generously: Anthropic CLI strings
# drift and we would rather false-positive on a mid-turn 401 (the operator
# gets one iMessage; the latch resets on the next success) than miss a
# multi-day silent auth outage (see the June 2026 incident narrative in
# claude_dispatch.py).
CLAUDE_AUTH_ERROR_MARKERS = (
    "invalid authentication",
    "authentication_error",
    "invalid_grant",
    # Anchored HTTP-401 shapes — a bare "401" substring false-positives
    # on any stderr containing the digit sequence 401 (e.g. "port 4014",
    # "processed 401 files", pids/hashes ending in ...401...). Anchoring
    # to real 401-response patterns (a leading space + `401 unauthorized`,
    # `http 401`, `status 401`, `status: 401`, `http/1.1 401`) keeps the
    # match generous while excluding numeric-substring collisions.
    "401 unauthorized",
    "http 401",
    "http/1.1 401",
    "status 401",
    "status: 401",
    "please run /login",
    "session has expired",
)
# Belt-and-suspenders lower bound on re-alerts if the latch reset path ever
# fails (defensive; the latch is the primary rate limit). 6h.
CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS = 6 * 3600

# ---------------------------------------------------------------------------
# Cluster 4: Poller staleness detection + in-process replacement
# ---------------------------------------------------------------------------
# Poller staleness threshold — how long we tolerate zero successful polls
# before assuming a silent TCP stall. Must comfortably exceed POLL_TIMEOUT
# (30s) plus POLL_ERROR_BACKOFF_MAX (300s) so that transient network
# outages recover on their own without a poller swap. 7 minutes gives that
# margin with headroom for a bursty backoff. Tuning knob.
POLL_STALE_ALERT_THRESHOLD_SECONDS = 420
# How often the main loop checks the poller's staleness. 30s is dense
# enough to bound recovery latency (~half a minute past the threshold)
# without spamming the check on every drain tick.
POLL_STALE_CHECK_INTERVAL_SECONDS = 30

# ---------------------------------------------------------------------------
# Cluster 5: outbound spool (at-least-once) + scoped keep-alive
# ---------------------------------------------------------------------------
# Disk-backed spool for outbound Telegram chunks. Every chunk handed to
# ``_send_with_retry`` is persisted at entry, marked-success on ok, and
# renamed back to a pending state on final retry-exhaustion. A background
# thread (and a synchronous startup pass) replays pending files so a crash
# mid-send is bounded to at-least-once delivery on the next boot.
#
# Files live under WORKSPACE / cache / telegram-outbound-spool at 0o700; the
# JSON payloads live at 0o600. Filenames encode ``{created_epoch_ns}-{uid8}-
# {state}.json`` where state is either ``pending`` or ``inflight-<pid>``. A
# ``mark_success`` deletes the file; a ``mark_failed`` renames it back to
# ``pending`` so the periodic replay pass picks it up next round.
#
# Age/size caps trade duration and disk cost against staleness: 24h max age
# keeps replayed content actionable (a stale morning brief replayed hours
# later is worse than no brief); 500 max files caps disk to ~2MB of 4KB
# chunks.
SPOOL_DIR = WORKSPACE / "cache" / "telegram-outbound-spool"
SPOOL_DIR_MODE = 0o700
SPOOL_FILE_MODE = 0o600
SPOOL_MAX_AGE_SECONDS = 24 * 3600
SPOOL_MAX_FILES = 500
SPOOL_REPLAY_INTERVAL_SECONDS = 60
# Don't touch a file spooled less than SPOOL_REPLAY_MIN_AGE_SECONDS ago —
# the primary send-path may still be inside ``_send_with_retry`` on it,
# and the ``-inflight-<pid>`` rename guards that case as well.
SPOOL_REPLAY_MIN_AGE_SECONDS = 5

# ---------------------------------------------------------------------------
# Cluster 2 (voice): local whisper transcription of voice notes
# ---------------------------------------------------------------------------
# Reject voice notes longer than this. 3 minutes matches typical use and
# keeps CPU time well below the wall-clock timeout.
VOICE_MAX_DURATION_SECONDS = 180
# Hard wall-clock cap on the whisper subprocess. Even a maxed-out
# 180s-audio-at-4x-real-time transcription (~45s) finishes well under
# this; the cap defends against wedged subprocesses (torch import hang,
# corrupt model file, ffmpeg lockup).
VOICE_TRANSCRIBE_TIMEOUT_SECONDS = 90
# Belt-and-suspenders truncate on the transcript before it becomes a
# Claude prompt. Guards against whisper hallucination loops that emit
# tens of thousands of repeated characters on silence/noise.
VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS = 8000
# Telegram voice/audio/video_note fields we accept for transcription.
VOICE_ACCEPT_TYPES = frozenset({"voice", "audio", "video_note"})

# ---------------------------------------------------------------------------
# Cluster 3: Reaction ACKs (Telegram Bot API 7.0+ setMessageReaction)
# ---------------------------------------------------------------------------
# Emoji fired on classify-time acknowledgement (message accepted, queued for
# Claude). Must be in the Telegram Bot API 7.0 allowed set — 👀 is a member.
REACTION_ACK_EMOJI = "\U0001f440"  # 👀
# Emoji fired on successful turn completion. Must be in the allowed set —
# 👌 is a member. ✅ is NOT (verified against the API docs).
REACTION_DONE_EMOJI = "\U0001f44c"  # 👌
# Per-request HTTP timeout on the setMessageReaction POST. Bounded so a
# hung Telegram edge can't leak reaction threads. 5s comfortably exceeds
# normal round-trip and matches the daemon's existing "fast small POST"
# convention.
REACTION_HTTP_TIMEOUT_SECONDS = 5
# Total attempts per reaction (initial + 1 retry). Reactions are UX polish
# — a lost 👀/👌 is invisible, so we retry once and swallow anything after.
REACTION_MAX_ATTEMPTS = 2
# Filenames Telegram advertises (or that we synthesize) must match one of
# these extensions after sanitization. Whisper handles all of them via its
# internal ffmpeg step.
VOICE_ALLOWED_EXTENSIONS = frozenset({
    ".ogg", ".oga", ".mp3", ".m4a", ".mp4", ".mpeg", ".wav",
})

# ---------------------------------------------------------------------------
# Cluster 4: usage/cost stats (turn count, tokens, notional USD)
# ---------------------------------------------------------------------------
# Persistent aggregate of per-turn usage/cost data reported by the CC
# persistent stream's terminal `result` event. Sibling of STATE_FILE and
# uses the same 0o600 mode; the file lives under cache/ (already 0o700).
# On a flat-rate Claude Max plan the dollar amount is notional — the cost
# label is enforced everywhere the aggregate is surfaced.
USAGE_STATS_FILE = WORKSPACE / "cache" / "usage-stats.json"
USAGE_STATS_FILE_MODE = 0o600
# Prune day-buckets older than this on each save. 30d at ~200 bytes/day
# keeps the aggregate under ~10KB indefinitely.
USAGE_STATS_RETENTION_DAYS = 30
# Defensive cap on per-model label strings written into the aggregate —
# unknown / future model ids never blow up the JSON size.
USAGE_STATS_MODEL_LABEL_MAX = 40
