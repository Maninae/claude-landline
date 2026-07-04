# Architecture

Landline is a Python 3.9, stdlib-only, macOS-targeted daemon. It bridges
Telegram to a **persistent** headless Claude Code subprocess so that a phone
becomes the front door to an always-on agent on your workstation. This
document walks the moving parts and the invariants that keep them honest.

If you're new to the codebase, skim this end-to-end before diving in. The
StreamPump section in particular is the load-bearing story of the whole
repo — the design there is not incidental.

---

## The shape of a message

```
Telegram Bot API
     │  long-poll (30s)
     ▼
┌─────────────────────────┐    ┌──────────────────┐
│ BackgroundPoller thread │───▶│ dedup + cursor   │
└─────────────────────────┘    └────────┬─────────┘
                                        │ enqueue
                                        ▼
                             ┌────────────────────────┐
                             │ Orchestrator main loop │
                             │  (single-threaded)     │
                             └─┬──────────────────────┘
              classify batch ──┤
              /pause?         ─┤
              locked?          ─┤    Keychain-cached allowlist
              photo/voice/doc? ─┤    lock state machine
              text?            ─┤    reaction 👀
                                ▼
                       ┌──────────────────┐
                       │ ClaudeDispatcher │  ──▶ persistent stdin
                       └────────┬─────────┘
                                ▼
                       ┌──────────────────┐
                       │  StreamPump      │  ◀── persistent stdout
                       │  (ONE reader for │      (turn demux by
                       │   the process's  │       system/init → result)
                       │   whole life)    │
                       └────────┬─────────┘
                                ▼
                       ┌──────────────────┐
                       │  StreamSender    │  ──▶ Telegram sendMessage
                       │  per-chat FIFO   │      (with 4096-char HTML chunker,
                       │  one worker      │       429/Retry-After, outbound
                       │  thread          │       spool for at-least-once)
                       └──────────────────┘
                                │
                       reaction 👌 on turn completion
```

A dispatched turn round-trips through the same pump and sender that also
carry background-turn output. Everything downstream of the orchestrator is
built to keep those two streams from stepping on each other.

---

## Module map

The package is ~29 modules. `orchestrator.py` is the coordinator that wires
them together and hosts the main loop; the rest are focused on one concern
each. Two thin facade modules (`claude.py`, `client.py`) exist to keep
import paths stable across internal refactors — always import from the
facade, never the sub-module.

| Module | Purpose |
|---|---|
| `__main__.py` | Entry point — PID flock, fatal-crash pause, top-level wiring |
| `orchestrator.py` | `TelegramDaemon` coordinator — main loop, cursor mgmt, dispatch wiring, shutdown |
| `pause_flag.py` | `PauseFlag` — generation-aware `/pause` interrupt flag |
| `batch_classifier.py` | Update-batch classification (Pass 1) + `_is_pause_command` |
| `photo_handler.py` | Photo batch processing + album grouping |
| `image_cache.py` | Age-based retention sweep for cached photos |
| `claude.py` | **Facade** re-exporting the Claude-core sub-modules |
| `types.py` | Leaf module — `ClaudeStreamResult`; breaks the `claude_dispatch`↔`streaming` cycle |
| `tool_status.py` | `tool_use` → status-line formatters |
| `stream_sender.py` | `StreamSender` + worker loop (text + status, one ordered queue) |
| `sender_registry.py` | Per-chat long-lived sender registry + `try_enqueue_chat_notice` |
| `persistent_claude.py` | `PersistentClaude` subprocess manager + singleton |
| `stream_pump.py` | `StreamPump` — the sole stdout reader; turn demux + unsolicited-turn delivery |
| `streaming.py` | `run_claude_streaming` turn lifecycle + watchdog / typing |
| `claude_dispatch.py` | `ClaudeDispatcher` — call lifecycle, backoff, finalization |
| `poller.py` | `BackgroundPoller` — long-poll thread, bounded dedup set, cursor advance |
| `client.py` | **Facade** re-exporting the Telegram I/O sub-modules |
| `telegram_transport.py` | HTTP path — `telegram_api`, `send_response` / `send_typing`, 429 + retry |
| `telegram_download.py` | `download_file` — `getFile` + byte-capped streaming |
| `html_chunker.py` | Tag-aware HTML chunker + `send_html` (Telegram 4096 / UTF-16 safe) |
| `commands.py` | `CommandRouter` — `/new`, `/status`, unknown |
| `lock.py` | `LockManager` — passphrase, lockout, idle expiry |
| `outbound_spool.py` | Disk-backed at-least-once outbound send spool |
| `reactions.py` | 👀 received → 👌 done — single FIFO worker, fire-and-forget |
| `usage_stats.py` | Per-day turn / token / notional-cost counters surfaced in `/status` |
| `voice_transcribe.py` | Local whisper subprocess wrapper (timeout, pause-aware kill) |
| `voice_handler.py` | Voice / audio pipeline — download → transcribe → delimited dispatch |
| `document_handler.py` | PDF / doc pipeline — sanitized basenames, XML-delimited names |
| `inject.py` | Just-in-time context queue prepended to next Claude turn |
| `state.py` | Atomic state save, conversation log append, JSONL usage parse |
| `config.py` | Constants + `landline.json` loader — no side effects on import |
| `failure_tracker.py` | Consecutive-failure tracker + exponential backoff |
| `guard.py` | Allowlist gate — fail-closed, 60s Keychain cache |
| `notifications.py` | iMessage alerts (poller stall, Claude auth expiry) |
| `security.py` | `keychain_get` / `keychain_get_status` wrappers |
| `logging.py` | Rotating file + stdout logger |
| `telegram_fmt.py` | Shared markdown → Telegram HTML formatter |

---

## StreamPump — one reader for the process's life

This is the design decision the rest of the daemon is built around.

### The bug

Claude Code's persistent process is not a request-response protocol. When a
background task (a subagent, or a shell command started with
`run_in_background`) finishes, the harness starts a **turn that nobody
asked for**. That unsolicited turn writes a full `system/init` → assistant
events → `result` block to stdout with no matching stdin write. Verified
empirically against the local `claude` CLI, and reproducible.

The original design attached a fresh reader per dispatched turn and read
"until the first `result` event". When a background task completed while
no turn was in flight, its events piled up unread in the stdout pipe. The
next dispatched turn's reader consumed the stale block, hit the stale
`result`, and returned — leaving that turn's actual response sitting in
the pipe for the next reader to pick up.

The observable symptom: every turn delivered the *previous* turn's answer.
A → nothing → B → A' → C → B' → … until a restart or `/new` killed the
process. It survived for months. It was first misattributed to a Telegram
client bug, then to send-retry drops. Both wrong. The bug was in *our
reader's contract*: readers cannot detach from a shared pipe.

### The fix

`stream_pump.py` inverts the ownership. Exactly one `StreamPump` is
created per subprocess (`get_or_create_pump(proc)`, weak-keyed registry)
and reads stdout continuously for the process's life. Nothing else may
read that pipe.

- **Turn blocks are delimited** by `system/init` … `result`. Every turn
  — dispatched or unsolicited — opens with `init`.
- **Dispatched turns register a `TurnHandle` BEFORE their stdin write.**
  The next `init` after registration is attributed to that handle. Its
  `result` completes the handle.
- **Unregistered blocks are unsolicited.** Their text is routed to the
  chat's `StreamSender` immediately — background subagent results reach
  the user when they finish, not one message later.
- **A registered handle is ALWAYS completed** (result / EOF / read
  error). Dispatch can never hang on an abandoned turn.
- **If the pump thread dies while the process lives**, the pipe's read
  position is unknowable. `run_claude_streaming` kills and respawns the
  process. Session id survives on `PersistentClaude` so continuity is
  preserved. Never create a second pump for a live process.

There is one deliberately-unfixed cosmetic race: if a background turn
begins in the sub-second window between `register_turn` and the
dispatched turn's `init`, attribution can swap. All text is still
delivered either way (both the dispatched and background streams flow to
the sender); only per-turn bookkeeping snapshots the wrong block. The
alternative — counting notifications to disambiguate — can miscount and
orphan a dispatched turn (a hang), which is strictly worse than a
cosmetic skew that self-heals.

### Watchdog note

`_touch()` bumps the pending turn's activity clock on every event,
including another block's events. This is deliberate parity with the old
reader: the shared pipe was always the timing signal, and scoping
"activity" to the owned block would let a long-running background turn
starve out a healthy dispatched turn.

---

## StreamSender — one ordered queue per chat

Claude's output is not just text — a single turn interleaves prose deltas
with tool-status lines ("running Bash: `ls -la`", "reading `foo.py`").
Both go to Telegram, and their relative order matters: a status line
about a tool call needs to arrive before the reply that references its
result.

The chosen design is one FIFO queue and one worker thread per chat, both
long-lived for the daemon's whole life. The queue carries `(tag, payload)`
tuples where `tag` is TEXT or STATUS. The worker coalesces same-type
runs (text with `STREAM_BUFFER_WINDOW`, status with
`STATUS_BUFFER_WINDOW`) and forces a flush on every type transition —
that's what preserves the ordering guarantee across two heterogeneous
streams without any cross-thread synchronization primitives.

### Why long-lived, not per-turn

Senders are **long-lived, one per chat**, kept in a module-level registry
keyed by `chat_id`. They are NOT created per turn. This is what kills the
whole drain-stall / desync class of bugs:

- One FIFO queue + one worker per chat ⇒ bubbles deliver in enqueue order
  **across turns**. Dispatch is single-threaded, so turn N fully enqueues
  (including its trailing FLUSH) before turn N+1 begins.
- End-of-turn calls `sender.flush()` — a non-blocking FLUSH boundary,
  **never** `close()`. The dispatch thread never blocks on Telegram's
  send rate. A slow turn delays the *next* turn's bubbles in order — it
  never freezes the daemon, drops, or reorders them.
- `close()` runs **only at shutdown**, via `_close_all_senders()`.
- The worker is hardened: `_run` wraps each entry in catch-log-continue,
  and `_get_or_create_sender` self-heals by recreating a sender whose
  worker thread has died.
- The queue is intentionally unbounded — dropping is the bug we avoid.
  A backlog past `_QUEUE_HIGH_WATER` logs once so it's observable.

### Daemon notices routed the same way

Notices generated by the daemon itself — "(Paused.)", "(Still working…)",
context-window warnings, empty-response fallbacks — enqueue through
`try_enqueue_chat_notice(chat_id, html=/text=)`. They land **after** any
draining bubbles, preserving the invariant that a status line about a
tool arrives before its reply. Each caller keeps a direct-send fallback
for the case when no live sender exists yet (before the first turn to a
new chat).

Out-of-band **health alerts** (backoff gate, "Claude unavailable") stay
direct-send. Immediate delivery beats ordering when the queue itself
might be the thing that's stuck.

---

## Outbound spool — at-least-once send

Telegram's HTTP API sometimes takes tens of seconds to acknowledge a
send, and the underlying TCP connection can silently drop mid-flight.
Losing a reply is worse than sending a duplicate — the operator can
recognize a duplicate; a missing reply is invisible.

`telegram_transport` handles this with a persist-first outbound spool:

- Every chunk handed to `_send_with_retry` is written to disk at
  `cache/telegram-outbound-spool/{epoch_ns}-{uid}-{state}.json` **before**
  the HTTP send.
- The file is renamed to `-inflight-<pid>.json` while the send is in
  flight, unlinked on a confirmed 200, and renamed back to `-pending`
  on final retry-exhaustion.
- A background thread + a synchronous startup pass replay pending files.
- Age (`SPOOL_MAX_AGE_SECONDS`, 24h) and count (`SPOOL_MAX_FILES`, 500)
  caps prevent a stale morning brief from resurrecting hours later.
- A minimum-age gate (`SPOOL_REPLAY_MIN_AGE_SECONDS`, 5s) keeps the
  replayer from stealing a send that the primary path is still working
  on.

The trade: a rare duplicate send on a timed-out-but-delivered request is
accepted. Optimizing the persist-first ordering away (persist after
send) would silently drop chunks on any crash mid-send. Don't.

The synchronous `replay_all` pass that used to run at startup was
removed: it could block startup for tens of minutes when Telegram was
unreachable (~200 files × 10s urlopen timeout each). The background
replayer's first tick provides identical coverage without the
availability hole.

---

## Reactions — 👀 → 👌

Two Bot API `setMessageReaction` calls per turn:

- 👀 (eyes) at classify time, once the update passes the guard and the
  lock is unlocked. Tells the operator "I saw this; queued or working".
- 👌 (OK hand) on turn completion.

This runs on a single persistent worker thread draining a FIFO queue.
SET / CLEAR pairs for the same message must never race each other on
separate threads (the API is order-sensitive), which is why the queue is
FIFO and single-worker rather than a thread pool.

Reactions are fire-and-forget: a failure must never delay or fail message
processing. The reaction path is entirely off the dispatch critical path.

Every 👀 must reach 👌 or CLEAR on every exit path — locked, paused,
overflow, batch-error, brush-off. Several review rounds have been spent
pinning down every bail-out branch. Consult the reaction tests before
touching any of them.

Kill switch: `REACTION_ACKS_ENABLED = False` in `landline.json` disables
the whole subsystem — useful if Telegram ever removes an emoji from the
allowed set for `setMessageReaction`.

---

## Poller self-healing

The long-poll TCP connection can go stale — the socket sits in
`ESTABLISHED` state indefinitely, no data flows, no error is raised. The
daemon looks healthy (process alive, thread alive) but stops processing
messages.

Two mitigations:

- **Timeout on `urllib`.** The poller passes `POLL_TIMEOUT + 10` to
  `urlopen`. A truly wedged socket can evade this — some macOS TCP
  states don't fire the read timeout — but it catches most cases.
- **In-process replacement.** The main loop tracks
  `poller.last_successful_poll_at`. If no successful poll lands within
  `POLL_STALE_ALERT_THRESHOLD_SECONDS` (7 minutes, comfortably past
  `POLL_TIMEOUT` + backoff cap), the orchestrator swaps the poller in
  place while preserving the dedup-set and cursor state. The replacement
  is invisible to Telegram — no update is lost, no cursor rewinds.

An auth-expiry alert (once-per-incident latched, reset on next success)
fires the async iMessage path if the Claude CLI itself starts returning
401s. This catches the class of failure where `claude -p` jobs die
silently for hours — verified against a real multi-day outage.

---

## The queueing + `/pause` contract

The dispatch thread is single-threaded. When Claude is mid-turn, incoming
Telegram updates queue up on `BackgroundPoller._incoming_updates_queue`
rather than interrupting. Interruption is opt-in — `/pause` is the only
message that interrupts a running turn.

The rules are strict because the race surface is subtle:

- `/pause` is intercepted **before** `/`-prefix routing in
  `_process_update_batch`, so it never reaches `CommandRouter`.
- `_pause_requested` is a `PauseFlag` — generation-aware. A stale pause
  from a previous turn cannot interrupt the next one.
- `_pause_requested` is **set** only by the poller's `on_update_queued`
  callback.
- `_pause_requested` is **cleared** only in (a) `_finalize_response`
  when the result is interrupted, and (b) `_handle_pause_updates` when
  no dispatch is pending in the same batch.
- Never clear `_pause_requested` at the start of `_invoke_claude_call`
  — it races with the watchdog when `/pause` arrives in the same batch
  as text.
- `MAX_QUEUED_UPDATES` caps drained updates per loop iteration.
  Overflow gets a "dropped N messages" notice. The cap is applied after
  drain, before classification, so text + photos + commands share one
  budget.
- The `on_update_queued` callback contract is O(1), non-blocking,
  exception-isolated (exceptions must NOT increment
  `consecutive_error_count`).
- Callback queries are discarded. The orchestrator advances the cursor
  and `continue`s without calling `answerCallbackQuery`. If a button
  flow is ever re-introduced, the ACK must be sent off the main loop —
  Telegram times out button presses after ~30s and the main loop can be
  blocked for minutes during a Claude call.

---

## Voice + documents — untrusted content, delimited

Voice notes and documents are inbound *user content*, not agent context.
They must not be treated as instructions.

### Voice

1. Poller sees an incoming voice / audio / video_note message and
   downloads it via `getFile` (20 MB cap; the daemon rejects anything
   larger without downloading).
2. `voice_transcribe` shells out to a local whisper CLI. The subprocess
   is bounded by a wall-clock cap (`VOICE_TRANSCRIBE_TIMEOUT_SECONDS`)
   and polls the `PauseFlag` between short `proc.wait()` slices — a
   pause during whisper kills the subprocess. A pause **before** whisper
   starts lets whisper finish and re-anchors the pause for the Claude
   turn (voice content is never silently dropped).
3. Transcripts are truncated to
   `VOICE_TRANSCRIBE_MAX_TRANSCRIPT_CHARS` (belt-and-suspenders against
   whisper hallucination loops on silence).
4. The dispatched Claude message wraps the transcript in XML delimiters
   with the tag close-escaped, so the content is unambiguously
   attributed as untrusted operator speech.

Transcripts and document filenames MUST NEVER appear in log lines. Log
`chat_id` and byte counts only. This is enforced by the module docstring
and covered by tests.

### Documents

`document_handler.py` accepts an allowlist of extensions (see
`DOCUMENT_ALLOWED_EXTENSIONS` — PDF, TXT, MD, CSV, JSON, LOG, TSV,
YAML/YML). Belt-and-suspenders MIME check when Telegram supplies
`mime_type`. Downloaded to `cache/telegram_files/` at 0600 with 0700
parent, XML-delimited filename passed to Claude.

---

## Locking + fail-closed guarding

Two independent gates:

- **Allowlist.** `guard.is_allowed(chat_id)` checks against a Keychain
  entry (`telegram-allowed-chat-ids`, comma-separated). Fail-closed:
  empty allowlist blocks everyone. Cached with 60s TTL — a per-message
  Keychain call is too slow. If Keychain is unavailable (locked after
  sleep/wake, `security` timeout), the previous cache is preserved
  rather than blanking to empty (which would lock the operator out for
  60s). Cold start with no cache still fails closed.
- **Session lock.** `LockManager` implements a passphrase-typed-directly
  unlock (no `/unlock` command — the operator types the phrase into the
  chat). Failed attempts trip an exponentially-escalated lockout, hard
  capped at `UNLOCK_LOCKOUT_MAX_SECONDS` (1h) so a legitimate user can
  always recover. An in-memory `time.monotonic()` floor resists forward
  wall-clock jumps; a restart falls back to wall-only. Never persist
  the monotonic value.

`REJECTION_MODE = "silent"` (default) makes unauthorized senders get
nothing back — no enumeration oracle for the bot's privacy gate. The
rejected `chat_id` is still logged so abuse patterns are visible in the
log.

---

## Config

`landline/config.py` is the single source of truth for constants. It is
stateless on import except for the `landline.json` overlay:

1. `WORKSPACE = Path(os.environ.get("LANDLINE_WORKSPACE", os.getcwd())).resolve()`
2. Read `<WORKSPACE>/landline.json` if present, otherwise all defaults.
3. Enforce a fixed allowlist of keys. Unknown key, malformed JSON, or
   type mismatch → `SystemExit` with a one-line error.

Fail-fast on config is deliberate. A half-configured daemon that runs is
harder to diagnose than one that refuses to start; launchd's
`ThrottleInterval` bounds the resulting crash loop.

Keychain **service** names are fixed constants
(`telegram-bot-token`, `telegram-chat-id`, `telegram-allowed-chat-ids`,
`telegram-unlock-hash`, `owner-imsg-handle`) — a config-controlled
service name would let a malicious config point secrets lookups at an
attacker-chosen service. Only the **account** is configurable
(`KEYCHAIN_ACCOUNT`, default `landline`), so several installations on
one Mac can coexist without colliding.

See [`SETUP.md`](SETUP.md) for the full key table.

---

## Test isolation

The suite runs inside the live workspace when it's invoked as a deploy
gate by `deploy/restart.sh`. This means a leaked write during a test
lands in the production workspace. To make that safe, autouse fixtures
in `landline/tests/conftest.py` redirect every file / network side effect
before any test runs:

- `LANDLINE_WORKSPACE` is pointed at a fresh temp dir **at module
  import top** — before any `landline` import loads `config.py`. This is
  the only way to keep a real `landline.json` in the workspace from
  leaking into tests.
- Daemon log path, conversation log
  (`landline.state.WORKSPACE` → tmp), usage-stats file, spool dir,
  reactions network, Keychain, `persistent_claude` singleton — every
  side-effecting surface has an autouse fixture.
- Test additions that touch a new side-effecting surface must add their
  own autouse fixture. Not a nice-to-have — a leak here goes straight to
  production.

The suite is fast (target: seconds, not minutes) and green. `--skip-tests`
exists on `restart.sh` for iteration speed, not for shipping.

---

## What's deliberately not here

- No LLM inside the daemon. The daemon shells out to `claude` and reads
  its stream. No prompt engineering, no eval logic.
- No message store beyond an atomic state file, a Markdown daily log
  (0600), and a per-day usage-stats JSON. No SQLite, no external
  database.
- No cross-platform support. Landline is macOS-only. Keychain, launchd,
  and `osascript` iMessage are load-bearing.
- No runtime dependencies. If you find yourself reaching for `requests`,
  `pydantic`, or `httpx` — reconsider. The stdlib has done fine.
- No inline-keyboard flow. An earlier button-based unlock path was
  removed; if it ever comes back, its callback ACK must be off the main
  loop.

---

## Further reading

- [`SETUP.md`](SETUP.md) — install, configure, and run.
- [`../CLAUDE.md`](../CLAUDE.md) — hard-won invariants (the shorter,
  operational cousin of this document — read both if you plan to edit
  the daemon).
- `landline/stream_pump.py` module docstring — the definitive description
  of the reader contract.
- `landline/stream_sender.py` module docstring — the sender queue design.
