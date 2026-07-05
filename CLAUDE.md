# Landline — Developer Guide

This file is the invariant rulebook for AI agents (and humans) editing the
`landline` package. Every bullet exists because breaking it caused a real
production incident — the narratives and diagrams live in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); this file is what you
re-read before you touch code.

If you're here to install and run Landline, read
[`README.md`](README.md) and [`docs/SETUP.md`](docs/SETUP.md) first.

## Runtime environment

- **Python 3.9** (the macOS system Python at `/usr/bin/python3`).
- **Zero runtime dependencies.** Standard library only. Do not add a
  requirement without a very good reason.
- Entry point: `python3 -m landline` with the agent workspace as the
  working directory (launchd sets `WorkingDirectory`; interactive runs
  should `cd` into it or set `LANDLINE_WORKSPACE`).
- Managed by launchd via the templates in `deploy/`. `KeepAlive: true` +
  `ThrottleInterval: 30` bounds the crash loop; the watchdog
  re-bootstraps the label if it falls off launchd entirely.

### Python 3.9 syntax cheat sheet

Never use 3.10+ syntax. The daemon starts fine if a file imported before
the edit, then crashes on next restart — sometimes hours later when
nobody's watching. **Always compile-check every `landline/**/*.py` after
editing.**

| Forbidden (3.10+) | Use instead |
|-------------------|-------------|
| `str \| None`     | `Optional[str]` from `typing` |
| `int \| float`    | `Union[int, float]` from `typing` |
| `match x:`        | `if / elif` chains |
| `type Alias = …`  | `Alias = …` (plain assignment) |

Compile-check (recursive over the subpackage tree):

```bash
cd claude-landline && /usr/bin/python3 -c \
  "import py_compile, glob; [py_compile.compile(f, doraise=True) for f in glob.glob('landline/**/*.py', recursive=True)]; print('OK')"
```

## Architecture at a glance

The daemon is a single-threaded main loop with daemon helper threads. The
main loop reads Telegram updates from a background poller, classifies
them, and dispatches to a **persistent** Claude Code subprocess whose
stdout is drained by a single long-lived pump thread. Text replies
stream back to Telegram through a per-chat FIFO sender.

For the module tree, the message-shape diagram, and the design
narratives, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — that's
the canonical home. This file is the checklist you re-read while
editing.

Secrets — bot token, allowlist, passphrase hash, iMessage alert
handle — live in the macOS Keychain, addressed by fixed service names
and the configurable `KEYCHAIN_ACCOUNT` (default `landline`).

## Hard-won invariants

Each bullet below is a rule with a real incident behind it. When two
rules seem to conflict, read the referenced ARCHITECTURE section — the
whole reason it's canonical there is that the trade-offs are subtle.

### StreamPump — one reader, for the process's life

See [ARCHITECTURE → StreamPump](docs/ARCHITECTURE.md#streampump--one-reader-for-the-processs-life)
for the full story (off-by-one desync bug, attribution race,
sole-producer contract, usage-stats fsync hazard). The rules:

- The Claude process's stdout has **exactly ONE reader for the process's
  life** — `StreamPump`. Never add a second reader.
- `StreamPump` is created once per subprocess via
  `get_or_create_pump(proc)` (weak-keyed registry). Never construct one
  directly.
- Turn blocks are delimited `system/init` … `result`. Dispatched turns
  register a `TurnHandle` BEFORE their stdin write; blocks with no
  registered handle are unsolicited and route to the chat sender
  immediately.
- A registered handle is ALWAYS completed (result / EOF / read error) so
  dispatch can never hang.
- If the pump thread dies while the process lives, respawn the process —
  never spin up a second pump.
- The final-result tail + turn-boundary flush happen on the pump thread,
  before the handle completes. `run_claude_streaming` must NOT touch the
  sender after `handle.done.wait()` returns.
- Unsolicited-block `usage_stats.record_turn` MUST be dispatched to a
  short-lived daemon thread; calling it from the pump thread would let
  an SSD stall back-pressure the whole pipe.
- Do NOT "fix" the sub-second attribution race with task-notification
  counting — a miscount can orphan a dispatched turn (a hang), strictly
  worse than cosmetic skew.

### StreamSender — long-lived, per-chat, unified queue

See [ARCHITECTURE → StreamSender](docs/ARCHITECTURE.md#streamsender--one-ordered-queue-per-chat).
Rules:

- Senders are **long-lived, one per chat** — kept in the module-level
  `_senders` registry in `landline/claude/registry.py`. NEVER create
  per-turn.
- End-of-turn calls `sender.flush()` (non-blocking FLUSH boundary),
  **never** `close()`. `close()` runs only at shutdown via
  `_close_all_senders()`.
- The queue is intentionally unbounded — dropping is the bug we avoid.
  `_note_queue_depth` logs once past `_QUEUE_HIGH_WATER` for
  observability.
- Daemon notices ("(Paused.)", context warning, empty/error) enqueue
  through `try_enqueue_chat_notice` so they land after any draining
  bubbles. Out-of-band health alerts (backoff-gate, "Claude
  unavailable") intentionally stay direct-send.
- The sender's own delivery-failure fallback routes through
  `landline.runtime.notifications` (async iMessage) — do NOT re-use the
  failing text-send callable for its own outage notice.

### Session id — single source of truth

See [ARCHITECTURE → Session id](docs/ARCHITECTURE.md#session-id--single-source-of-truth).
Rules:

- `PersistentClaude` owns the live session id
  (`get_session_id()` / `set_session_id()`, guarded by `_lock`).
  `state["session_id"]` is a write-on-save serialization slot,
  lazy-seeded into pc on the dispatcher's first `send_to_claude`.
- `_retry_with_fresh_session` clears pc **before** state.
- `_finalize_response` always mirrors pc into state before save (so an
  interrupted / exit-143 turn can't clobber the session).
- Tests reset the pc singleton via the autouse
  `reset_persistent_claude_singleton` fixture and patch the seam at
  `landline.claude._get_persistent_claude` (the lazy import inside the
  dispatcher), never `landline.claude.dispatch._get_persistent_claude`.

### Stale-resume vs mid-session-error discriminator

A pruned `--resume` (`error_during_execution` result, no init, exit 1,
stderr "No conversation found") routes into `_retry_with_fresh_session`.
Mid-session API errors must NOT match this shape — mark `saw_init`
BEFORE `session_id` in `_open_block`, and demand the stderr marker
before nuking a session. Auth-failure stderr is detected FIRST so it
routes to the auth-alert path unmolested. See
[ARCHITECTURE → Stale-resume discriminator](docs/ARCHITECTURE.md#stale-resume-vs-mid-session-error-discriminator).

### Outbound spool is at-least-once, never at-most-once

`telegram/transport.py` persists the payload before sending and unlinks
only on a confirmed 200. Replay (startup + a 60 s background pass)
honors age and count caps. A rare duplicate send is the accepted trade;
do NOT "optimize" the persist-first ordering away. See
[ARCHITECTURE → Outbound spool](docs/ARCHITECTURE.md#outbound-spool--at-least-once-send).

### Reactions are fire-and-forget and ordered

One persistent worker thread drains a FIFO queue (SET / CLEAR pairs for
the same message must never race). A reaction failure must never delay
or fail message processing. Every 👀 must reach 👌 or CLEAR on EVERY
exit path (locked, paused, overflow, batch-error, brush-off). Consult
the reaction tests before touching any bail-out path. Kill switch:
`reaction_acks_enabled` in `landline.json`. See
[ARCHITECTURE → Reactions](docs/ARCHITECTURE.md#reactions----).

### Voice + `/pause`

A pause set BEFORE whisper starts lets whisper finish and re-anchors the
pause for the Claude turn (voice content is never silently dropped); a
pause DURING whisper kills the subprocess. Transcripts and document
filenames are UNTRUSTED: they go to Claude inside XML delimiters
(close-tag escaped) and MUST NEVER appear in daemon log lines (PII
rule — log `chat_id` / sizes only).

### Poller self-heal + auth-expiry alert

The main loop replaces the poller in-process on staleness while
preserving the dedup-set/cursor contract. Auth-expiry alert is
once-per-incident latched, reset on success, delivered via the async
iMessage path (never blocks dispatch). The 6h floor
(`CLAUDE_AUTH_ALERT_MIN_INTERVAL_SECONDS`) MUST run unconditionally to
defeat the fail → success → fail latch-reset race. See
[ARCHITECTURE → Poller self-healing](docs/ARCHITECTURE.md#poller-self-healing).

### PID-file flock prevents dual instances

Without this, launchd restart races can spawn two daemons polling the
same bot. The lock is in `cache/telegram-daemon.pid` via
`fcntl.flock(LOCK_EX | LOCK_NB)`.

### SIGTERM during Claude call ≠ stale session

If the shutdown handler kills Claude (exit 143 = 128 + 15), the empty
result looks like a pruned session. `looks_like_stale_session` excludes
exit code 143 and interrupted results. `_record_outcome` skips
interrupted results so they don't trip the failure counter.

### Dedup set must NOT be pruned on cursor advance

In-flight long polls can return updates the main thread already
processed. Pruning them from the dedup set re-queues and double-
processes them. The set grows by one int per message — negligible over
the daemon lifetime.

### Keychain allowlist must be cached

`guard.is_allowed()` is called per-message. Without caching, every
message spawns a `security` subprocess. Cache with 60 s TTL via
module-level globals; if you reset globals in tests, use the autouse
`reset_guard_cache` fixture. If Keychain is unavailable (locked after
sleep/wake), the previous cache is preserved rather than blanking out
(which would lock the operator out for 60 s). Cold start with no cache
still fails closed.

### Tests must not write to the real log

Tests that call `log()` write to the real log file unless the logger is
mocked. Rely on the autouse `isolate_daemon_log` conftest fixture — it
points the log at a tmp path. If a test needs to verify logging, mock
`landline.runtime.logging.log`.

### The watchdog must close stdout if the process dies

If Claude's process dies but a grandchild holds the stdout pipe, the
main thread blocks forever in `for raw in proc.stdout:`. The watchdog
detects `proc.poll() is not None` and closes stdout to unbreak the
reader.

### Interrupts must not trigger failure backoff

When a new message arrives and interrupts Claude (SIGINT), the empty
result is NOT a Claude failure. Without this check, fast typing
triggers exponential backoff lockout.

### Never `os.umask`

Process-wide, races concurrent file creation across the poller /
sender / watchdog threads. Set file modes with
`os.open(..., 0o600)` + `os.fchmod`, dir modes with `os.chmod`. Daily
logs `0600`, `memory/daily/` `0700`, state file `0600` (see
`DAILY_LOG_FILE_MODE` / `DAILY_LOG_DIR_MODE` / `STATE_FILE_MODE` in
`config.py`).

### Never log PII or secrets

`chat_id` is semi-public and OK. Message text, passphrases, hashes,
tokens, Keychain values, voice transcripts, document filenames —
never.

### Restart-continuation is two-phase

The trigger file is unlinked ONLY AFTER a successful dispatch handoff
(a dispatch error no longer drops the operator's cross-restart
instruction); a locked session still preserves the trigger. See
[ARCHITECTURE → Restart continuation](docs/ARCHITECTURE.md#restart-continuation--two-phase-commit).

### Cursor advance is durable and pre-notice

Advance in-memory cursor, notify poller, persist to disk immediately;
send notices (skip / LOCKED_HELP / overflow) BEFORE advancing so a
failed send leaves the update un-advanced. No bulk-advance on
batch-level exception. See
[ARCHITECTURE → Cursor advance](docs/ARCHITECTURE.md#cursor-advance--durability--at-least-once).

## Queueing + `/pause` — strict rules

These are the strict rules; the sequence diagram + stranded-flag
consumer live in
[ARCHITECTURE → The queueing + /pause contract](docs/ARCHITECTURE.md#the-queueing---pause-contract).

- Messages received during a Claude call are queued in
  `BackgroundPoller._incoming_updates_queue`, NOT auto-interrupted.
- `/pause` is the ONLY way to interrupt a running Claude call.
- `/pause` is intercepted BEFORE the `/`-prefix routing in
  `process_update_batch` — never reaches `CommandRouter`.
- `_pause_requested` is a `PauseFlag` — generation-aware so stale pauses
  can't interrupt the next call.
- `_pause_requested` is SET only by the poller's `on_update_queued`
  callback.
- `_pause_requested` is CLEARED only in (a) `_finalize_response` when
  `result.interrupted`, and (b) `handle_pause_updates` when no dispatch
  is pending in the same batch.
- NEVER clear `_pause_requested` at the start of `_invoke_claude_call` —
  it races with the watchdog when `/pause` arrives in the same batch as
  text.
- `_batch_dispatch_attempted` flips `True` ONLY when `send_to_claude`
  confirms it reached `_invoke_claude_call` (return `True`). The
  stranded-flag consumer relies on this to know whether to clear the
  pause flag itself.
- Max `MAX_QUEUED_UPDATES` (30) drained updates per loop iteration;
  overflow gets a "dropped N messages" notice. Cap applies in `run()`
  AFTER drain, BEFORE classification — covers text + photos + commands
  under one budget.
- Poller `on_update_queued` callback contract: O(1), non-blocking,
  exceptions isolated from the poll loop (must NOT increment
  `consecutive_error_count`).
- Callback queries are discarded — the orchestrator advances the cursor
  and `continue`s without calling `answerCallbackQuery`. If a button
  flow is ever re-introduced, its ACK must be sent off the main loop.

## Telegram formatting pipeline

Two sending paths — **never mix them.** The shared formatter
(`landline/telegram/fmt.py`) is used by `landline/telegram/transport.py`
and by any external out-of-band delivery script (via the
`landline/telegram_fmt.py` compat shim).

- **`send_response()`** — for markdown text. Runs through
  `md_to_telegram_html()` which converts `**bold**`, `_italic_`,
  `` `code` ``, etc. to HTML tags.
- **`send_html()`** — for pre-built HTML. Bypasses the markdown
  converter. Use when building HTML with `telegram.fmt` helpers
  (`bold()`, `italic()`, `code()`, `pre()`).

**The bug to avoid:** Using `telegram.fmt` helpers (which return raw
`<i>`, `<pre>` tags) and then sending through `send_response()` — the
converter HTML-escapes the tags, showing literal `<i>text</i>` in
Telegram. This has caused bugs twice.

| Building with…                                       | Send via…            |
|------------------------------------------------------|----------------------|
| Markdown (`_italic_`, `**bold**`)                    | `send_response()`    |
| `telegram.fmt` helpers (`italic()`, `pre()`, `bold()`) | `send_html()`      |
| Plain text                                           | Either works         |

## Config

`landline/config.py` is the single source of truth for constants.
Fail-fast tolerant loader for `<WORKSPACE>/landline.json` with a fixed
allowlist of keys — unknown/malformed/type-mismatch raises `SystemExit`.

- **Mechanism** (loader, WORKSPACE seam, fail-fast rationale):
  [ARCHITECTURE → Config](docs/ARCHITECTURE.md#config--mechanism).
- **Reference table** (defaults, meanings, examples):
  [`docs/SETUP.md`](docs/SETUP.md#config-reference).
- Additions require a new `_ALLOWED_KEYS` row, a type-check, a SETUP
  reference row, and the four loader tests.

Keychain **service** names stay fixed constants; only the **account** is
configurable (`KEYCHAIN_ACCOUNT`, default `landline`).

## Tests

Before ANY change, run the full suite and verify it passes:

```bash
cd claude-landline && /usr/bin/python3 -m pytest landline/tests/ -q
```

The suite is 1 079 tests covering unit / integration / regression paths
for every subsystem, including the desync-regression tests that make
`StreamPump` load-bearing. Test isolation (autouse fixtures that
redirect the workspace, log path, Keychain, and network) is itself
load-bearing — the restart script runs the suite inside the live
workspace as a deploy gate, so a leaked write goes straight to
production. Any new subsystem that writes files or hits the network
needs its own autouse fixture. See
[ARCHITECTURE → Test isolation](docs/ARCHITECTURE.md#test-isolation).

## Modules

For the canonical module table and subpackage layout, see
[ARCHITECTURE → Module map](docs/ARCHITECTURE.md#module-map). Don't
duplicate the table here — the ARCHITECTURE version is the one that
gets kept in sync.

## Restart procedure

**Always use `deploy/restart.sh`.** Skipping the compile / import /
test gates causes production outages where the daemon crashes on
restart and is unresponsive for hours. The full walkthrough (env vars,
what the script does step-by-step, continuation semantics, day-two
operations) lives in
[`docs/SETUP.md`](docs/SETUP.md#7-day-two-operations) — the reference
below is the invariant + command list.

**Invariant: never raw `launchctl` for a code deploy.** The pipeline is
compile-check → import-check → tests → `bootout` → `bootstrap` → log
tail. Every step catches a class of production outage that raw
`launchctl bootout && bootstrap` hides.

```bash
# Standard restart (compile + import + tests + restart + auto-continuation)
./deploy/restart.sh

# Skip tests for faster iteration
./deploy/restart.sh --skip-tests

# Custom continuation message (Claude sees this after restart)
./deploy/restart.sh "Deploy complete — verify the new prompt."
```

Config via env: `LANDLINE_WORKSPACE`, `LANDLINE_REPO`, `LANDLINE_PLIST`,
`LANDLINE_LABEL` — all default to values that match the SETUP
walkthrough (workspace `~/.landline`, plist
`~/Library/LaunchAgents/com.landline.telegram-daemon.plist`, label
`com.landline.telegram-daemon`), so a stock install runs
`./deploy/restart.sh` with no env vars.

## Do NOT

- Use `rm` — use `trash` for safe deletion.
- Commit PII, phone numbers, addresses, or secrets. Secrets go in
  Keychain.
- Add co-author trailers to commits.
- Restart the daemon without compile-checking first.
- Mix `telegram.fmt` helpers with `send_response` — those helpers return
  raw HTML tags and `send_response` runs markdown → HTML which would
  escape them. Use `send_html` for pre-built HTML.
- Add a second reader for the Claude subprocess stdout. `StreamPump`
  owns it.
- Touch the per-chat sender from `run_claude_streaming` after
  `handle.done.wait()` returns — the pump is the sole producer of turn
  content on the sender.
- Call `usage_stats.record_turn` synchronously from the pump thread.
- Clear `_pause_requested` at the start of `_invoke_claude_call`.
- Use `os.umask` — race-prone across threads.
- Add a runtime dependency without a hard, argued reason.
