# Landline — Developer Guide

This file is for AI agents (and humans) working ON the `landline` package. If you're
here to install and run Landline, read [`README.md`](README.md) and
[`docs/SETUP.md`](docs/SETUP.md) first.

## Runtime Environment

- **Python 3.9** (the macOS system Python at `/usr/bin/python3`).
- **Zero runtime dependencies.** Standard library only. Do not add a
  requirement without a very good reason — the whole point of shipping on
  the system Python is that a fresh Mac can run this with nothing installed.
- Entry point: `python3 -m landline` with the agent workspace as the
  working directory (launchd sets `WorkingDirectory`; interactive runs
  should `cd` into it or set `LANDLINE_WORKSPACE`).
- Managed by launchd via the templates in `deploy/`. `KeepAlive: true`
  + `ThrottleInterval: 30` bounds the crash loop; the watchdog re-bootstraps
  the label if it falls off launchd entirely.

### Python 3.9 syntax cheat sheet

Never use 3.10+ syntax. The daemon starts fine if a file imported before the
edit, then crashes on next restart — sometimes hours later when nobody's
watching. **Always compile-check every `landline/*.py` after editing.**

| Forbidden (3.10+) | Use instead |
|-------------------|-------------|
| `str \| None`     | `Optional[str]` from `typing` |
| `int \| float`    | `Union[int, float]` from `typing` |
| `match x:`        | `if / elif` chains |
| `type Alias = …`  | `Alias = …` (plain assignment) |

Compile-check:

```bash
cd claude-landline && /usr/bin/python3 -c \
  "import py_compile, glob; [py_compile.compile(f, doraise=True) for f in glob.glob('landline/*.py')]; print('OK')"
```

## Architecture

Single-threaded main loop with daemon helper threads. The main loop reads
Telegram updates from a background poller, classifies them, and dispatches to
a **persistent** Claude Code subprocess (`claude -p --input-format stream-json
--output-format stream-json`) whose stdout is drained by a single
long-lived pump thread. Text replies stream back to Telegram through a
per-chat FIFO sender.

`orchestrator.py` is the coordinator — it wires the extracted modules
together AND still hosts batch processing, photo handling, update
classification, `/pause` routing, and the main loop (~700 lines). Further
decomposition (extracting photo handling and batch classification into
their own modules) is a known refactor target.

Secrets — bot token, allowlist, passphrase hash, iMessage alert handle —
live in the macOS Keychain, addressed by fixed service names and the
configurable `KEYCHAIN_ACCOUNT` (default `landline`).

### Modules

| Module | Purpose |
|---|---|
| `__main__.py` | Entry point — PID flock, fatal-crash pause, top-level wiring |
| `orchestrator.py` | `TelegramDaemon` coordinator — main loop, cursor mgmt, dispatch wiring, shutdown |
| `pause_flag.py` | `PauseFlag` — generation-aware `/pause` interrupt flag (re-exported by orchestrator) |
| `batch_classifier.py` | Update-batch classification (Pass 1) + `_is_pause_command` |
| `photo_handler.py` | Photo batch processing + album grouping + download dispatch |
| `image_cache.py` | `cache/telegram_images/` age-based retention sweep |
| `claude.py` | **Facade** re-exporting the Claude-core sub-modules (canonical import path) |
| `types.py` | Leaf module — `ClaudeStreamResult` only; breaks the `claude_dispatch`↔`streaming` import cycle (NOT facade-re-exported) |
| `tool_status.py` | tool_use → status-line formatters |
| `stream_sender.py` | `StreamSender` + worker loop + queue constants (text + status, one ordered queue) |
| `sender_registry.py` | Per-chat long-lived sender registry + `try_enqueue_chat_notice` |
| `persistent_claude.py` | `PersistentClaude` subprocess manager + singleton |
| `stream_pump.py` | `StreamPump` — ONE persistent stdout reader per Claude process; turn demux + unsolicited-turn delivery |
| `streaming.py` | `run_claude_streaming` turn lifecycle (register handle → wait) + watchdog / typing + `ClaudeStreamShutdownHook` |
| `claude_dispatch.py` | `ClaudeDispatcher` — call lifecycle, backoff, finalization |
| `poller.py` | `BackgroundPoller` — long-poll thread, bounded dedup set, cursor advance |
| `client.py` | **Facade** re-exporting the Telegram I/O sub-modules |
| `telegram_transport.py` | HTTP path — `telegram_api`, `send_response` / `send_typing`, 429 / Retry-After + bounded retry |
| `telegram_download.py` | `download_file` — `getFile` + byte-capped streaming |
| `html_chunker.py` | Tag-aware HTML chunker + `send_html` (Telegram 4096 / UTF-16 safe) |
| `commands.py` | `CommandRouter` — `/new`, `/status`, unknown (the unlock path is passphrase-typed-directly, handled in the orchestrator, not a slash command) |
| `lock.py` | `LockManager` — passphrase, lockout, idle expiry |
| `outbound_spool.py` | Disk-backed at-least-once outbound send spool (persist → send → unlink; replay on startup with age / count caps) |
| `reactions.py` | Reaction ACKs (👀 received → 👌 done) — single persistent FIFO worker, fire-and-forget, `REACTION_ACKS_ENABLED` kill switch |
| `usage_stats.py` | Per-day turn / token / notional-cost counters from result events; surfaced in `/status` |
| `voice_transcribe.py` | Local whisper subprocess wrapper (timeout, pause-aware kill, temp-dir hygiene) |
| `voice_handler.py` | Voice / audio message pipeline — download → transcribe → delimited dispatch to Claude |
| `document_handler.py` | Document / PDF pipeline — sanitized basenames, XML-delimited untrusted filenames, allowlisted types |
| `inject.py` | Just-in-time context queue prepended to next Claude turn |
| `state.py` | Atomic state save, conversation log append, JSONL usage parse |
| `config.py` | Constants, paths, tunables + `landline.json` loader — no behavioral side effects on import |
| `failure_tracker.py` | Consecutive-failure tracker + exponential backoff state machine |
| `guard.py` | Allowlist gate — fail-closed, 60s Keychain cache |
| `notifications.py` | iMessage alerts (poller stall, Claude auth expiry) |
| `security.py` | `keychain_get` / `keychain_get_status` wrappers |
| `logging.py` | Rotating file + stdout logger |
| `telegram_fmt.py` | Shared markdown → Telegram HTML formatter |

**Facade pattern:** `claude.py` and `client.py` are thin re-export facades
over their sub-modules — `from landline.claude import …` /
`from landline.client import …` remain the canonical import paths and stay
stable as internals move. Some moved helpers reach back through the facade at
*call time* (e.g. `streaming` → `landline.claude._get_persistent_claude`,
`html_chunker.send_html` → `landline.telegram_transport._send_with_retry`) to
break import cycles and preserve test patch points — keep that indirection if
you edit them.

## Hard-Won Invariants

The rules below are load-bearing. Each one exists because breaking it caused
a real production incident.

### The Claude process's stdout has exactly ONE reader, for the process's life (StreamPump)

This is the repo's crown jewel and the fix for the "off-by-one desync" bug
hunt that lived for months.

The persistent Claude process runs turns the daemon never asked for: when a
background subagent or `run_in_background` Bash task completes while no turn
is in flight, the Claude Code harness starts an UNSOLICITED agent turn
(`system/task_notification` → `system/init` → assistant… → `result`) on
stdout with no stdin write. The old per-turn reader read "until the first
`result`" and detached between turns, so an unsolicited turn's events piled
up unread; the next dispatched turn consumed the stale block, broke at the
stale `result`, and left its OWN response in the pipe. Every turn after that
delivered the PREVIOUS turn's answer (A → nothing → B → A' → C → B'), until
a restart or `/new` killed the process. Symptoms were misattributed for
weeks — first to a Telegram client bug, then to send-retry drops — both
wrong.

The fix (`stream_pump.py`):

- `StreamPump` is created once per subprocess (`get_or_create_pump(proc)`,
  weak-keyed registry) and reads stdout continuously for the process's life.
  NOTHING else may read that pipe.
- Turn blocks are delimited by `system/init` … `result` (every turn opens
  with `init`). A dispatched turn registers a `TurnHandle` BEFORE its stdin
  write; the next `init` is attributed to it; its `result` completes the
  handle. Blocks with no registered handle are unsolicited: their text
  routes to the chat's `StreamSender` IMMEDIATELY — background results reach
  the user when they finish, not one message later.
- A registered handle is ALWAYS completed (result / EOF / read error), so
  dispatch can never hang on an abandoned turn.
- If the pump thread dies while the process lives, the pipe's read position
  is unknowable — `run_claude_streaming` kills and respawns the process
  (session id survives on `PersistentClaude`). NEVER create a second pump
  for a live process.
- Known cosmetic race (documented in `stream_pump.py`): a background turn
  starting in the sub-second window between `register_turn` and the turn's
  `init` can swap attribution. All text is still delivered either way; only
  bookkeeping snapshots the wrong block. Do not "fix" this with
  notification counting — a miscount can orphan a dispatched turn, which is
  strictly worse.

### StreamSender unifies text + status on one ordered queue

Claude's output flows through a single `StreamSender` worker thread that
receives both text deltas (`sender.text(...)`) and tool-status lines
(`sender.status(...)`) on one queue of `(tag, payload)` tuples. The worker
coalesces text with `STREAM_BUFFER_WINDOW` and batches identical-collapsed
status with `STATUS_BUFFER_WINDOW`. Type transitions (TEXT after STATUS, or
vice versa) force a flush of the previous bucket — that's what guarantees
Telegram sees status lines before the text reply that references them, with
no cross-thread synchronization required.

**Senders are LONG-LIVED, one per chat — NOT created per turn.** A
module-level registry (`_senders`, keyed by chat_id; `_get_or_create_sender`)
keeps one sender + worker alive for the daemon's whole life. This is what
kills the drain-stall/desync class:

- One FIFO queue + one worker per chat ⇒ bubbles deliver in enqueue order
  **across turns**. Dispatch is single-threaded (`orchestrator.run()`), so
  turn N fully enqueues (incl. its trailing FLUSH) before turn N+1 begins.
- End-of-turn calls `sender.flush()` (a non-blocking FLUSH boundary),
  **never** `close()`. The dispatch thread never blocks on a drain; the
  worker drains in the background at Telegram's rate. A slow turn delays the
  next turn's bubbles *in order* — it never freezes the daemon or
  drops/reorders them. (The old per-turn blocking `close()` is what stalled
  the dispatch thread up to 30s and then abandoned the worker, leaking an
  in-flight bubble past the turn boundary = desync.)
- `close()` runs **only at shutdown**, via `_close_all_senders()` from
  `drain_for_shutdown`. `_drain_remaining()` is the shutdown-only safety
  net that honours FLUSH / type transitions.
- The long-lived worker is hardened so it can't become a permanent black
  hole: `_run` wraps each entry in catch-log-continue, and
  `_get_or_create_sender` self-heals by recreating a sender whose worker
  thread has died (`worker_alive`).
- Daemon-generated notices ("(Paused.)", context warning, "(Still
  working…)", empty / error) route through the same queue via
  `try_enqueue_chat_notice(chat_id, html=/text=)` so they land **after** any
  draining bubbles; each caller keeps a direct-send fallback (used before
  the first turn, when no live sender exists yet). Out-of-band health
  alerts (backoff-gate, "Claude unavailable") intentionally stay direct —
  immediate delivery beats ordering, and the queue may be the thing that's
  stuck.
- The queue is intentionally unbounded (dropping is the bug we avoid);
  `_note_queue_depth` logs once past `_QUEUE_HIGH_WATER` so a backlog is
  observable.

### Outbound spool is at-least-once, never at-most-once

`telegram_transport` persists the payload before sending and unlinks only on
a confirmed 200. Replay (startup + a 60s background pass) honors age and
count caps so stale briefs don't resurrect. A rare duplicate send is the
accepted trade; do not "optimize" the persist-first ordering away.

### Reactions are fire-and-forget and ordered

One persistent worker thread drains a FIFO queue (SET / CLEAR pairs for the
same message must never race each other on separate threads). A reaction
failure must never delay or fail message processing. Every 👀 must reach 👌
or CLEAR on EVERY exit path (locked, paused, overflow, batch-error,
brush-off) — several review rounds were spent pinning this; check the
reaction tests before touching any bail-out path. Kill switch:
`REACTION_ACKS_ENABLED`.

### Voice + `/pause`

A pause set BEFORE whisper starts lets whisper finish and re-anchors the
pause for the Claude turn (voice content is never silently dropped); a
pause DURING whisper kills the subprocess. Transcripts and document
filenames are UNTRUSTED: they go to Claude inside XML delimiters
(close-tag escaped) and must NEVER appear in daemon log lines (PII rule
— log chat_id / sizes only).

### Stale-resume recovery

A pruned `--resume` (error_during_execution result, no init, exit 1, "No
conversation found" stderr) routes into `_retry_with_fresh_session`.
Mid-session API errors must NOT match this shape — see the discriminator
tests.

### Poller self-heal + auth-expiry alert

The main loop replaces the poller in-process on staleness while preserving
the dedup-set/cursor contract. Auth-expiry alert is once-per-incident
latched, reset on success, delivered via the async iMessage path (never
blocks dispatch).

### Session id — single source of truth

`PersistentClaude` owns the live session id
(`get_session_id()` / `set_session_id()`, guarded by the existing `_lock`).
`state["session_id"]` is a write-on-save serialization slot, **lazy-seeded**
into pc on the dispatcher's first `send_to_claude`. The dispatcher routes
all session-id decisions through pc; `_retry_with_fresh_session` clears pc
**before** state; `_finalize_response` always mirrors pc into state before
save (so an interrupted / exit-143 turn can't clobber the session). Tests
reset the pc singleton via the autouse
`reset_persistent_claude_singleton` fixture and patch the seam at
`landline.claude._get_persistent_claude` (the lazy import inside the
dispatcher), never `landline.claude_dispatch._get_persistent_claude`.

### PID-file flock prevents dual instances

Without this, launchd restart races can spawn two daemons polling the same
bot. The lock is in `cache/telegram-daemon.pid` via
`fcntl.flock(LOCK_EX | LOCK_NB)`.

### SIGTERM during Claude call ≠ stale session

If the shutdown handler kills Claude (exit 143 = 128 + 15), the empty
result looks like a pruned session.
`looks_like_stale_session` excludes exit code 143 and interrupted results.
`_record_outcome` skips interrupted results so they don't trip the failure
counter.

### Dedup set must NOT be pruned on cursor advance

In-flight long polls can return updates the main thread already processed.
Pruning them from the dedup set re-queues and double-processes them. The
set grows by one int per message — negligible over the daemon lifetime.

### Keychain allowlist must be cached

`guard.is_allowed()` is called per-message. Without caching, every message
spawns a `security` subprocess. Cache with 60s TTL via module-level
globals; if you reset globals in tests, use the autouse
`reset_guard_cache` fixture.

### Tests must not write to the real log

Tests that call `log()` write to the real log file unless the logger is
mocked. Rely on the autouse `isolate_daemon_log` conftest fixture — it
points the log at a tmp path. If a test needs to verify logging, mock
`landline.logging.log`.

### The watchdog must close stdout if the process dies

If Claude's process dies but a grandchild holds the stdout pipe, the main
thread blocks forever in `for raw in proc.stdout:`. The watchdog detects
`proc.poll() is not None` and closes stdout to unbreak the reader.

### Interrupts must not trigger failure backoff

When a new message arrives and interrupts Claude (SIGINT), the empty result
is NOT a Claude failure. Without this check, fast typing triggers
exponential backoff lockout.

### Never `os.umask`

Process-wide, races concurrent file creation across the poller / sender /
watchdog threads. Set file modes with `os.open(..., 0o600)` + `os.fchmod`,
dir modes with `os.chmod`. Daily logs `0600`, `memory/daily/` `0700`, state
file `0600` (see `DAILY_LOG_FILE_MODE` / `DAILY_LOG_DIR_MODE` /
`STATE_FILE_MODE` in `config.py`).

### Never log PII or secrets

Chat_id is semi-public and OK. Message text, passphrases, hashes, tokens,
Keychain values, voice transcripts, document filenames — never.

### Restart-continuation is two-phase

The trigger file is unlinked only AFTER a successful dispatch handoff (a
dispatch error no longer drops the operator's cross-restart instruction); a
locked session still preserves the trigger.

## Queueing + `/pause` contract

- Messages received during a Claude call are queued in
  `BackgroundPoller._incoming_updates_queue`, not auto-interrupted.
- `/pause` is the ONLY way to interrupt a running Claude call.
- `/pause` is intercepted BEFORE the `/`-prefix routing in
  `_process_update_batch` — never reaches `CommandRouter`.
- `_pause_requested` is a `PauseFlag` — generation-aware so stale pauses
  can't interrupt the next call.
- `_pause_requested` is set ONLY by the poller's `on_update_queued`
  callback.
- `_pause_requested` is cleared ONLY in (a) `_finalize_response` when
  `result.interrupted`, and (b) `_handle_pause_updates` when no dispatch is
  pending in the same batch.
- NEVER clear `_pause_requested` at the start of `_invoke_claude_call` —
  it races with the watchdog when `/pause` arrives in the same batch as
  text.
- Max `MAX_QUEUED_UPDATES` drained updates per loop iteration; overflow
  gets a "dropped N messages" notice.
- Cap is applied in `run()` AFTER drain, BEFORE classification — covers
  text + photos + commands under one budget.
- Poller `on_update_queued` callback contract: O(1), non-blocking,
  exceptions isolated from the poll loop (must NOT increment
  `consecutive_error_count`).
- Callback queries are discarded — the orchestrator advances the cursor
  and `continue`s without calling `answerCallbackQuery`.

## Telegram Formatting Pipeline

Two sending paths — **never mix them.** The shared formatter
(`landline/telegram_fmt.py`) is used by `landline/client.py` and by any
external out-of-band delivery script.

- **`send_response()`** — for markdown text. Runs through
  `md_to_telegram_html()` which converts `**bold**`, `_italic_`,
  `` `code` ``, etc. to HTML tags.
- **`send_html()`** — for pre-built HTML. Bypasses the markdown converter.
  Use when building HTML with `telegram_fmt` helpers (`bold()`, `italic()`,
  `code()`, `pre()`).

**The bug to avoid:** Using `telegram_fmt` helpers (which return raw `<i>`,
`<pre>` tags) and then sending through `send_response()` — the converter
HTML-escapes the tags, showing literal `<i>text</i>` in Telegram. This has
caused bugs twice.

| Building with...                                     | Send via...          |
|------------------------------------------------------|----------------------|
| Markdown (`_italic_`, `**bold**`)                    | `send_response()`    |
| `telegram_fmt` helpers (`italic()`, `pre()`, `bold()`) | `send_html()`      |
| Plain text                                           | Either works         |

## Config

`landline/config.py` is the single source of truth for constants. It has
one runtime seam:

1. `WORKSPACE = Path(os.environ.get("LANDLINE_WORKSPACE", os.getcwd())).resolve()`.
2. A tolerant loader reads `<WORKSPACE>/landline.json` if present. **Fixed
   allowlist of keys.** An unknown key, malformed JSON, or type mismatch
   raises `SystemExit` with a clear one-line error — fail fast at startup,
   never run half-configured. launchd's `ThrottleInterval` bounds the crash
   loop that a persistent typo would create.
3. Constants derive from `_cfg("key", default)`. Everything else in
   `config.py` stays a plain constant.

The full key table (defaults, meanings, examples) is documented in
[`docs/SETUP.md`](docs/SETUP.md). Additions require:

- A new row in the `_ALLOWED_KEYS` table + `_cfg` call.
- Type-check in the loader (str / bool / None / path).
- A row in the SETUP table.
- Tests: defaults when file absent, override application, type mismatch,
  unknown key still raises.

Keychain **service** names stay fixed constants (`telegram-bot-token`,
`telegram-chat-id`, `telegram-allowed-chat-ids`, `telegram-unlock-hash`,
`owner-imsg-handle`); only the **account** is configurable
(`KEYCHAIN_ACCOUNT`, default `landline`).

## Tests

Before ANY change, run the full suite and verify it passes:

```bash
cd claude-landline && /usr/bin/python3 -m pytest landline/tests/ -q
```

The suite covers:

- Unit tests for pure functions (normalization, markdown, chunking, state,
  config).
- `LockManager` state machine (unlock, lockout, expiry, reset).
- `CommandRouter` dispatch (`/new`, `/status`, unknown).
- `BackgroundPoller` thread safety (dedup, cursor advance, callback
  isolation).
- `ClaudeDispatcher` decomposition (backoff queue, stale detection,
  finalization).
- `StreamPump` turn demux: dispatched-turn routing / bookkeeping,
  unsolicited-turn immediate delivery, the off-by-one desync regression
  (pump-level AND through `run_claude_streaming`), EOF / read-error handle
  completion, dead-pump registration safety.
- `StreamSender` text coalescing, status batching / collapsing, turn
  boundaries via flush, ordering across type transitions, shutdown-only
  drain safety.
- Per-chat long-lived sender registry: same sender reused across turns,
  recreate-after-close, self-heal of a dead worker, cross-turn FIFO
  ordering, `try_enqueue_chat_notice` routing + fallback, and the
  `run_claude_streaming` flush-not-close lifecycle invariant.
- Guard allowlist caching (60s TTL, reset fixture).
- Inject queue commit / drain semantics.
- Config loader (defaults, overrides, unknown key raises, malformed JSON
  raises, type mismatch raises, `expanduser` on path keys, timezone
  fallback).
- E2E mock conversation flow.

### Test isolation is load-bearing

Autouse `conftest.py` fixtures are what let the suite run inside the live
workspace without corrupting it: daemon log path, conversation log
(`landline.state.WORKSPACE` → tmp), usage-stats file, spool dir, reactions
network kill, `LANDLINE_WORKSPACE` set to a fresh temp dir before any
`landline` import. Any new subsystem that writes files or hits the network
needs its own autouse fixture — the restart script runs the suite inside
the live workspace as a deploy gate, so a leaked write goes straight to
production.

## Restart Procedure

Always use `deploy/restart.sh` — never raw `launchctl` commands. Skipping
the compile / import / test gates causes production outages where the
daemon crashes on restart and is unresponsive for hours.

```bash
# Standard restart (compile + import + tests + restart + auto-continuation)
./deploy/restart.sh

# Skip tests for faster iteration
./deploy/restart.sh --skip-tests

# Custom continuation message (Claude sees this after restart)
./deploy/restart.sh "Deploy complete — verify the new prompt."
```

The script:

1. Compile-checks every `landline/*.py`.
2. Imports `TelegramDaemon` to catch import-time errors.
3. Runs the full pytest suite (unless `--skip-tests`).
4. Writes `<workspace>/cache/restart-continuation.txt` (default or your
   custom message).
5. `launchctl bootout` + `bootstrap` of the daemon label.
6. Tails the log to verify.

Config via env: `LANDLINE_WORKSPACE`, `LANDLINE_REPO`, `LANDLINE_PLIST`,
`LANDLINE_LABEL` — all default to values that match the `docs/SETUP.md`
walkthrough (workspace `~/.landline`, plist
`~/Library/LaunchAgents/com.landline.telegram-daemon.plist`, label
`com.landline.telegram-daemon`), so a stock install runs `./deploy/restart.sh`
with no env vars.

### Restart continuation

After restart, the daemon checks for `cache/restart-continuation.txt`. If
found (and session is unlocked), it injects the file's content as a
synthetic message to Claude — so Claude resumes automatically without the
operator having to message first. If session is locked, continuation is
skipped but the trigger file is **left in place** so the payload survives
until the next unlock/restart.

## Do NOT

- Use `rm` — use `trash` for safe deletion.
- Commit PII, phone numbers, addresses, or secrets. Secrets go in Keychain.
- Add co-author trailers to commits.
- Restart the daemon without compile-checking first.
- Mix `telegram_fmt` helpers with `send_response` — those helpers return
  raw HTML tags and `send_response` runs markdown → HTML which would
  escape them. Use `send_html` for pre-built HTML.
- Add a second reader for the Claude subprocess stdout. StreamPump owns it.
- Add a runtime dependency without a hard, argued reason.
