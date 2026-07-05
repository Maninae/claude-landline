"""Landline configuration — constants, paths, tunables + ``landline.json`` overrides.

- All values are plain constants; no behavioral side effects on import.
- Deployer-facing tunables (Keychain account, Claude binary/model, personas,
  launchd label prefix, whisper knobs, rejection posture, reaction ACKs) come
  from ``<WORKSPACE>/landline.json`` via ``_cfg``; see ``_ALLOWED_KEYS``.
- Worker-loop / thread-internal tunables (e.g.
  ``landline.claude.sender._IDLE_POLL_SECONDS`` / ``_QUEUE_HIGH_WATER``,
  ``landline.media.cache.*``, ``landline.telegram.chunker.*``) intentionally
  stay local to keep this file scannable — move here only if a deployer
  would tune it from ``landline.json``.
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

# Agent workspace. launchd sets WorkingDirectory so os.getcwd() = workspace;
# LANDLINE_WORKSPACE overrides for interactive runs and tests (autouse fixture).
WORKSPACE = Path(os.environ.get("LANDLINE_WORKSPACE", os.getcwd())).resolve()


# Value-type validators keyed by JSON key. Each returns the coerced value or
# raises ValueError. Path-like keys expanduser.
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


# Fixed allowlist of JSON keys → validator. Unknown key raises SystemExit at
# import (fail-fast; never run half-configured).
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
    """Fail-fast with a one-line stderr error. launchd's ThrottleInterval (30s)
    bounds the crash loop so a persistent typo is loud but bounded."""
    sys.stderr.write("landline config error: " + msg + "\n")
    raise SystemExit(2)


def _load_overrides(workspace: Path = None) -> dict:
    """Load ``<workspace>/landline.json`` and validate every key.

    Returns:
        {} if the file is absent.
    Raises:
        SystemExit on malformed JSON, non-object top-level, unknown key,
        or type mismatch.

    - ``workspace`` defaults to the module-level ``WORKSPACE``; tests pass a
      tmp dir explicitly so the loader logic can be exercised without
      side-effect-reloading the whole config module (which would break
      ``is`` identity for downstream constants).
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
    below derives from ``_cfg``; every other name is a plain constant."""
    return _OVERRIDES.get(key, default)


def _system_timezone() -> ZoneInfo:
    """Best-effort read of the system tz by inspecting ``/etc/localtime`` (a
    symlink into ``.../zoneinfo/<region>/<city>`` on macOS + most Linux).
    Falls back to UTC on parse failure.
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

# Keychain account for the ``telegram-*`` services; service names stay fixed
# (see security.py), only the account is deployer-tunable.
KEYCHAIN_ACCOUNT = _cfg("keychain_account", "landline")

# Claude Code CLI. Bare name → resolved on PATH via ``shutil.which``; absolute
# path passes through (launchd's minimal PATH makes bare names risky in prod).
CLAUDE = _cfg("claude_binary", "claude")

# Model override; ``None`` omits ``--model`` (CLI default).
CLAUDE_MODEL = _cfg("claude_model", None)

# ``claude -p --permission-mode``. Default is ``bypassPermissions`` — daemon
# runs unattended, operator can't answer tool prompts. See docs/SETUP.md.
CLAUDE_PERMISSION_MODE = _cfg("claude_permission_mode", "bypassPermissions")

# Personas surfaced in prompts, iMessage prefixes, log labels, /status header,
# daily-log role prefixes. Neutral defaults; deployers set per daily-log convention.
USER_NAME = _cfg("user_name", "User")
AGENT_NAME = _cfg("agent_name", "Assistant")

# Timezone for operator-visible datestamps. Default = system tz.
_tz_name = _cfg("timezone", None)
if _tz_name is None:
    TIMEZONE = _system_timezone()
else:
    try:
        TIMEZONE = ZoneInfo(_tz_name)
    except Exception as _tz_err:
        _die("unknown timezone %r: %s" % (_tz_name, _tz_err))

# /status ``launchctl list`` filter prefix. Explicit ``com.landline`` default
# so an empty-string doesn't match every job; deployers override to their prefix.
LAUNCHD_LABEL_PREFIX = _cfg("launchd_label_prefix", "com.landline")

# Glob (WORKSPACE-relative) for "morning brief" files surfaced by /status.
# ``None`` skips the briefs line entirely.
MORNING_BRIEF_GLOB = _cfg("morning_brief_glob", None)

# Whisper CLI defaults; bare name works when on PATH.
WHISPER_BIN = _cfg("whisper_bin", "whisper")
WHISPER_MODEL = _cfg("whisper_model", "base")
# Validator applies expanduser when set from landline.json.
WHISPER_MODEL_DIR = _cfg("whisper_model_dir", os.path.expanduser("~/.cache/whisper"))
WHISPER_LANGUAGE = _cfg("whisper_language", "en")

# Kill switch for reaction ACKs (👀 → 👌). See reactions.py.
REACTION_ACKS_ENABLED = _cfg("reaction_acks_enabled", True)

# Rejection posture for unauthorized senders. "silent" default removes an
# enumeration oracle; daemon still logs the rejected chat_id. Flip to "reply"
# (one-line config change) to restore the loud reply for incident-response signal.
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
# 1h hard cap on exponentially-escalated lockout (OWASP ASVS V3.3.5 — escalate
# AND bound so legitimate users always recover). See docs/ARCHITECTURE.md.
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
# 5 attempts / (1,2,4,8)s backoff rides out ~15-35s TLS/handshake stalls; 429s
# honor server Retry-After clamped [FALLBACK, CAP]. See docs/ARCHITECTURE.md.
SEND_MAX_ATTEMPTS = 5
SEND_RETRY_BACKOFF_SECONDS = (1, 2, 4, 8)
SEND_RETRY_AFTER_FALLBACK = 3
SEND_RETRY_AFTER_CAP = 30

# ---------------------------------------------------------------------------
# Dedup / media / storage
# ---------------------------------------------------------------------------
MAX_DEDUP_IDS = 100_000

# Tail-read window for the daily Telegram conversation log (rebuilding recent
# dialogue for a fresh Claude session). 32 KB ~= 20 turns.
CONVERSATION_LOG_TAIL_BYTES = 32768

# Tail-read window for the per-session Claude Code JSONL when scanning for the
# last assistant usage block (/status context-percent). 32 KB ~= 1-2 turns.
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
# Mirrors TELEGRAM_FILE_SIZE_LIMIT — separate name so a future PDF-only
# tightening is a one-liner.
DOCUMENT_MAX_SIZE_BYTES = 20 * 1024 * 1024

# Case-insensitive extension check is the PRIMARY content-type gate.
DOCUMENT_ALLOWED_EXTENSIONS = frozenset({
    ".pdf", ".txt", ".md", ".csv", ".json", ".log", ".tsv", ".yaml", ".yml",
})

# Belt-and-suspenders mime check when Telegram supplies mime_type; missing
# mime does NOT block acceptance.
DOCUMENT_ALLOWED_MIME_PREFIXES = (
    "text/",
    "application/pdf",
    "application/json",
    "application/x-yaml",
    "application/x-ndjson",
)

# Cache dirs iterated by ``sweep_media_caches`` at startup.
MEDIA_CACHE_DIRS = (TELEGRAM_IMAGE_DIR, TELEGRAM_FILE_DIR, TELEGRAM_VOICE_DIR)

# Per-cache retention — each dir gets its own window so voice-note privacy
# (raw audio + transcript) tunes independently of PDF/image retention.
MEDIA_CACHE_RETENTION_HOURS = {
    TELEGRAM_IMAGE_DIR: TELEGRAM_IMAGE_RETENTION_HOURS,
    TELEGRAM_FILE_DIR: TELEGRAM_FILE_RETENTION_HOURS,
    TELEGRAM_VOICE_DIR: TELEGRAM_VOICE_RETENTION_HOURS,
}

# 0700 for media cache dirs (workspace-wide "no umask, chmod each dir" invariant).
MEDIA_CACHE_DIR_MODE = 0o700

# Canonical authority for the inject-queue filename timestamp. Consumer:
# landline/inject.py parses via strptime(stem[:15], INJECT_TIMESTAMP_FORMAT).
# Producer (out-of-tree cron): keeps the literal (not an import) to stay
# landline/-import-free so cron deliveries survive any import-time error.
INJECT_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------
STATE_FILE = WORKSPACE / "cache" / "telegram-daemon-state.json"
LOG_FILE = WORKSPACE / "logs" / "telegram-daemon" / "daemon.log"

# Daily-log PII perms via os.fchmod (process-wide os.umask is forbidden —
# races concurrent file creation in poller/sender threads).
DAILY_LOG_DIR_MODE = 0o700
DAILY_LOG_FILE_MODE = 0o600
# State-file mode. Mirrors DAILY_LOG_FILE_MODE (defense-in-depth; cache/ is
# already 700-isolated).
STATE_FILE_MODE = 0o600

# ---------------------------------------------------------------------------
# State labels
# ---------------------------------------------------------------------------
LOCKED = "locked"
UNLOCKED = "unlocked"

LOCKED_HELP = "\U0001f512 Session is locked. Enter the passphrase to unlock."

# ---------------------------------------------------------------------------
# Claude failure tracker (failure_tracker imports these)
# ---------------------------------------------------------------------------
# Backoff threshold + "Claude unavailable" iMessage alert threshold.
# Ordering invariants (enforced by test_config):
#   BACKOFF_THRESHOLD < ALERT_THRESHOLD; BACKOFF_BASE < BACKOFF_CAP.
CLAUDE_FAILURE_BACKOFF_THRESHOLD = 3
CLAUDE_FAILURE_ALERT_THRESHOLD = 10
CLAUDE_FAILURE_BACKOFF_BASE_SECONDS = 30
CLAUDE_FAILURE_BACKOFF_CAP_SECONDS = 1800

# ---------------------------------------------------------------------------
# iMessage notifications + workspace perms
# ---------------------------------------------------------------------------
# osascript iMessage-send timeout; bounds a hung AppleScript event from
# holding the alert-worker thread forever.
IMESSAGE_SEND_SUBPROCESS_TIMEOUT_SECONDS = 30

# 0o700 mode applied at startup to sensitive top-level workspace dirs so a
# fresh checkout / re-mount never leaves them world-readable. Distinct from
# DAILY_LOG_DIR_MODE so a future 0o750 daily-log posture can diverge.
WORKSPACE_SENSITIVE_DIR_MODE = 0o700
WORKSPACE_SENSITIVE_DIRS = ("memory", "cache", "inbox", "outbox", "logs")

# ---------------------------------------------------------------------------
# Stale --resume auto-recovery
# ---------------------------------------------------------------------------
# Stderr fallback markers for the pruned/nonexistent --resume shape when the
# result-event path didn't populate is_error. Canonical: is_error + no init on
# the failing turn (see landline.claude.predicates.looks_like_pruned_resume).
STALE_RESUME_STDERR_MARKERS = (
    "No conversation found with session ID",
    "session not found",
)

# ---------------------------------------------------------------------------
# Claude CLI OAuth-expiry detection
# ---------------------------------------------------------------------------
# Case-insensitive stderr_tail markers. Match generously — Anthropic CLI
# strings drift and a false-positive iMessage beats missing a multi-day
# silent outage. See docs/ARCHITECTURE.md "June 2026 auth-expiry outage".
CLAUDE_AUTH_ERROR_MARKERS = (
    "invalid authentication",
    "authentication_error",
    "invalid_grant",
    # Anchored 401 shapes — bare "401" false-positives on port numbers,
    # pids, "processed 401 files", etc. Anchor to real 401-response
    # patterns to keep the match generous without numeric collisions.
    "401 unauthorized",
    "http 401",
    "http/1.1 401",
    "status 401",
    "status: 401",
    "please run /login",
    "session has expired",
)
# 6h defensive floor if the latch-reset path ever breaks (latch is primary).
CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS = 6 * 3600

# ---------------------------------------------------------------------------
# Poller staleness detection + in-process replacement
# ---------------------------------------------------------------------------
# 7 minutes — must exceed POLL_TIMEOUT (30s) + POLL_ERROR_BACKOFF_MAX (300s)
# so transient outages recover without a swap; headroom for bursty backoff.
POLL_STALE_ALERT_THRESHOLD_SECONDS = 420
# 30s check cadence — bounds recovery latency ~half a minute past threshold
# without spamming the check on every drain tick.
POLL_STALE_CHECK_INTERVAL_SECONDS = 30

# ---------------------------------------------------------------------------
# Outbound spool (at-least-once) + scoped keep-alive
# ---------------------------------------------------------------------------
# Disk-backed spool: persist chunk → send → unlink on 200 (else rename to
# pending for replay). Files 0o600 under 0o700 dir; filename encodes
# {created_epoch_ns}-{uid8}-{pending|inflight-<pid>}.json. Age/size caps trade
# staleness vs duration (24h keeps replays actionable; 500 files caps ~2MB).
# See docs/ARCHITECTURE.md "Outbound spool — persist-first at-least-once".
SPOOL_DIR = WORKSPACE / "cache" / "telegram-outbound-spool"
SPOOL_DIR_MODE = 0o700
SPOOL_FILE_MODE = 0o600
SPOOL_MAX_AGE_SECONDS = 24 * 3600
SPOOL_MAX_FILES = 500
SPOOL_REPLAY_INTERVAL_SECONDS = 60
# Don't touch files spooled less than this ago — primary send-path may still
# be inside _send_with_retry; the -inflight-<pid> rename also guards.
SPOOL_REPLAY_MIN_AGE_SECONDS = 5

# ---------------------------------------------------------------------------
# Voice notes — local whisper transcription
# ---------------------------------------------------------------------------
# 3 minutes matches typical use and keeps CPU well under the wall-clock cap.
VOICE_MAX_DURATION_SECONDS = 180
# 90s hard wall-clock cap defends against wedged subprocesses (torch import
# hang, corrupt model file, ffmpeg lockup) — a maxed-out 180s@4x runs ~45s.
VOICE_TRANSCRIBE_TIMEOUT_SECONDS = 90
# Belt-and-suspenders truncate — guards against whisper hallucination loops
# that emit tens of thousands of repeated chars on silence/noise.
VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS = 8000
VOICE_ACCEPT_TYPES = frozenset({"voice", "audio", "video_note"})

# ---------------------------------------------------------------------------
# Reaction ACKs (Telegram Bot API 7.0+ setMessageReaction)
# ---------------------------------------------------------------------------
# Classify-time ack. 👀 is in the API 7.0 allowed set.
REACTION_ACK_EMOJI = "\U0001f440"  # 👀
# Completion. 👌 is in the allowed set; ✅ is NOT (verified against API docs).
REACTION_DONE_EMOJI = "\U0001f44c"  # 👌
# 5s HTTP timeout — bounds a hung Telegram edge from leaking reaction threads.
REACTION_HTTP_TIMEOUT_SECONDS = 5
# Total attempts (initial + 1 retry). Reactions are UX polish; a lost 👀/👌
# is invisible, so we retry once and swallow anything after.
REACTION_MAX_ATTEMPTS = 2
# Extensions whisper accepts via its internal ffmpeg step.
VOICE_ALLOWED_EXTENSIONS = frozenset({
    ".ogg", ".oga", ".mp3", ".m4a", ".mp4", ".mpeg", ".wav",
})

# ---------------------------------------------------------------------------
# Usage / cost stats (turn count, tokens, notional USD)
# ---------------------------------------------------------------------------
# Persistent aggregate from the CC persistent stream's terminal `result`
# event. Sibling of STATE_FILE, same 0o600 mode, under cache/ (0o700).
# Dollar amount is notional on the flat-rate Claude Max plan.
USAGE_STATS_FILE = WORKSPACE / "cache" / "usage-stats.json"
USAGE_STATS_FILE_MODE = 0o600
# 30d retention — day-buckets ~200 B/day keeps the aggregate under ~10KB.
USAGE_STATS_RETENTION_DAYS = 30
# Cap per-model label length — unknown/future model ids can't blow up JSON size.
USAGE_STATS_MODEL_LABEL_MAX = 40
