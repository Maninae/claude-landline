# Setup

End-to-end install for Landline: from an empty macOS box to a running daemon
that lets your phone talk to the Claude Code agent on your workstation.

Landline is macOS-only. This guide assumes the operator, the workstation,
and the Telegram account are all yours.

## Prerequisites

- macOS with the system Python (`/usr/bin/python3`, 3.9+). Nothing else to
  install — Landline has zero runtime dependencies.
- [Claude Code](https://claude.com/product/claude-code) installed and
  logged in on the workstation. The daemon shells out to the `claude` CLI
  and relies on your active session; a paid Claude subscription (or an
  API-key configuration) is required for headless jobs to run at all.
- A Telegram account and a phone that can message a bot.
- Optional: `whisper` on your PATH (Homebrew: `brew install openai-whisper`).
  Required only if you want voice-note transcription. Documents (PDF, TXT,
  etc.) work without it.

## 1. Get the code

```bash
git clone https://github.com/Maninae/claude-landline.git ~/claude-landline
```

You will also want a separate **agent workspace directory** where the
daemon reads and writes state — logs, cache, media, the conversation log.
This is the launchd `WorkingDirectory` for the daemon and is where
`landline.json` lives. It can be anywhere; the `deploy/restart.sh` and
`deploy/watchdog.sh` scripts default to `~/.landline`, so using that path
lets you run them with no environment variables.

```bash
mkdir -p ~/.landline/{cache,logs/telegram-daemon,memory/daily}
```

Keep the code and the workspace separate: the code is a checkout you can
`git pull` on; the workspace holds your local state and shouldn't live
inside a git worktree.

## 2. Create the Telegram bot

Message [@BotFather](https://t.me/BotFather) on Telegram:

1. `/newbot`. Pick a display name and a username (the username ends in
   `bot`).
2. BotFather returns a **bot token** of the form
   `1234567890:ABCdefGHIjklmnoPQRstuVWXyz`. Save this — the next step
   moves it into Keychain.
3. Message your new bot **from your own Telegram account** (any message)
   to establish the chat. Then hit
   `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser to find
   your **chat_id** — it's the `chat.id` field on the update that came
   from you. This is the ID Landline will allow to send messages.
4. Send BotFather `/setprivacy` → **Disable** so the bot can read all
   messages you send it (rather than only slash commands). You want this
   for personal-assistant use — the daemon still runs its own allowlist
   at the application layer.

Optional but recommended:

- `/setcommands`: give the operator UI-level auto-complete for
  `/new`, `/status`, `/pause`.

## 3. Store secrets in Keychain

Landline never reads secrets from files or environment variables. All
five entries below are `generic-password` items on the login Keychain,
keyed by a fixed **service** name and a configurable **account** (default
`landline`). If you're running more than one Landline on the same Mac —
e.g. staging and personal — pick a different account per install and set
`keychain_account` in `landline.json` accordingly.

Substitute your values, then run once:

```bash
# 1. Bot token (from BotFather)
security add-generic-password -s telegram-bot-token \
    -a landline -w '1234567890:ABCdefGHIjklmnoPQRstuVWXyz'

# 2. Owner chat_id (integer, quoted as a string).
#    Used by out-of-band delivery paths that need a default destination.
security add-generic-password -s telegram-chat-id \
    -a landline -w '123456789'

# 3. Allowlist (comma-separated Telegram USER ids — the `from.id` on each
#    inbound message, NOT the chat.id). Fail-closed: only these users can
#    send messages to the bot. For a 1:1 owner bot your user id equals your
#    chat id, so the same value works; in a group / multi-user context the
#    two differ and this must be the sender's user id. Include your own.
security add-generic-password -s telegram-allowed-chat-ids \
    -a landline -w '123456789'

# 4. Unlock passphrase hash. SHA-256 of the passphrase you'll type to
#    unlock the session after /new or an idle expiry. Pick a phrase
#    that's easy to type on a phone but hard to guess.
PASSPHRASE='your-passphrase-goes-here'
HASH=$(printf %s "$PASSPHRASE" | shasum -a 256 | awk '{print $1}')
security add-generic-password -s telegram-unlock-hash \
    -a landline -w "$HASH"

# 5. Optional: iMessage handle (phone or Apple ID email) for out-of-band
#    alerts — used when the daemon can't reach Telegram (e.g. Claude auth
#    expired). Skip if you don't use iMessage; the daemon degrades to
#    log-only.
security add-generic-password -s owner-imsg-handle \
    -a landline -w '+15551234567'
```

Verify with `security find-generic-password -s telegram-bot-token -a landline -w`
(prints the token). If the login Keychain is locked (e.g. after
sleep/wake), unlock it with `security unlock-keychain login.keychain`.

**Never** commit these values anywhere. Read them from Keychain at
runtime — that's what the daemon does.

## 4. Write `landline.json`

Create `~/.landline/landline.json`. Every key is optional — omit any key
to accept the default. The full table is below the example.

```json
{
  "keychain_account": "landline",
  "user_name": "Alex",
  "agent_name": "Rook",
  "timezone": "America/Los_Angeles",
  "launchd_label_prefix": "com.landline"
}
```

Start with something this minimal. Add keys as needed.

### Config reference

| JSON key | Default | Meaning | Example |
|---|---|---|---|
| `keychain_account` | `"landline"` | Keychain **account** used with the five fixed service names. Change if you run multiple Landlines on one Mac. | `"landline-staging"` |
| `claude_binary` | `"claude"` | Path to the Claude Code CLI. If not absolute, the daemon resolves it on `PATH` at spawn time and exits with a clear error if missing. Set to an absolute path if launchd's PATH doesn't include your install. | `"/opt/homebrew/bin/claude"` |
| `claude_model` | `null` | Model id passed as `--model`. `null` omits the flag entirely — Claude Code picks its default. | `"claude-opus-4-8"` |
| `claude_permission_mode` | `"bypassPermissions"` | Value passed as `--permission-mode`. **This is a load-bearing security knob** — see the warning below. | `"acceptEdits"` |
| `user_name` | `"User"` | How the daemon addresses you in prompts, log role labels, and the daily Markdown log header. Purely cosmetic; not a security control. | `"Alex"` |
| `agent_name` | `"Assistant"` | How the daemon labels the agent side of the conversation. Purely cosmetic. | `"Rook"` |
| `timezone` | `null` (system zone) | IANA timezone name (via `ZoneInfo`) used for date/time formatting in `/status` and log filenames. `null` reads `/etc/localtime` and falls back to UTC. | `"America/Los_Angeles"` |
| `launchd_label_prefix` | `"com.landline"` | Prefix `/status` matches against `launchctl list` to report which Landline processes are alive. Match your actual plist label prefix. | `"com.landline"` |
| `morning_brief_glob` | `null` | Optional glob relative to the workspace. If set, `/status` reports the newest matching file. `null` skips the briefs line entirely — safe default for public installs. | `"briefs_morning/morning-*.md"` |
| `doctor_script` | `null` | Optional executable behind the `/doctor` command. The router spawns it detached (workspace cwd) with the operator's issue text as `argv[1]`; the script owns its own logging and delivers its report out-of-band (e.g. via the Bot API). `null` makes `/doctor` reply with setup guidance. `~` is expanded. | `"~/.mineru/scripts/doctor.sh"` |
| `whisper_bin` | `"whisper"` | Whisper CLI binary. Absolute path recommended so the launchd-slim `PATH` doesn't need `/opt/homebrew/bin`. | `"/opt/homebrew/bin/whisper"` |
| `whisper_model` | `"base"` | Whisper model name. `"base"` is ~145 MB, ~4× real-time on Apple Silicon — a good default for short voice notes. `"large-v3-turbo"` is higher quality and ~8× slower. | `"base"` |
| `whisper_model_dir` | `"~/.cache/whisper"` | Where whisper looks for the model weights (`--model_dir`). `~` is expanded. | `"~/.cache/whisper"` |
| `whisper_language` | `"en"` | Pinned language (skips auto-detect, faster and more accurate for a known-language operator). | `"en"` |
| `reaction_acks_enabled` | `true` | Sends 👀 on receipt and 👌 on turn completion via `setMessageReaction`. Kill switch — flip to `false` if Telegram ever removes one of the emojis from the allowed set. | `false` |
| `rejection_mode` | `"silent"` | `"silent"` sends nothing to unauthorized senders (removes the enumeration oracle). `"reply"` restores a "This bot is private." reply — useful during incident response. | `"silent"` |
| `profile_name` | `null` | Optional cosmetic label surfaced in the `/status` header (in parentheses after `agent_name`). Purely for eyeballing WHICH daemon replied when you run more than one on the same Mac — never an auth surface. `null` keeps the header plain. | `"mineru"` |

### The `bypassPermissions` warning

The default `claude_permission_mode` is `"bypassPermissions"`. This is
what makes the "always-on agent" experience work — an agent that stops
every 20 seconds to ask you "run this bash command? [y/n]" is not
messageable from your phone.

**What this means in practice:** every message that reaches Claude runs
with full tool access to the machine hosting the daemon. Anyone whose
`chat_id` is in the Keychain allowlist and who knows the passphrase can
read your files, edit code, and run shell commands as your user.

Design implications:

- Dedicate a Mac (or a user account, at minimum) to Landline that you're
  comfortable giving to your agent.
- The allowlist and passphrase are the two things standing between the
  internet and your shell. Set them thoughtfully. Rotate the passphrase
  if you suspect exposure.
- Silent rejection (`rejection_mode: "silent"`) is the default for good
  reason — it makes the bot invisible to anyone not on the allowlist.
- If you're not comfortable with this posture, set
  `claude_permission_mode` to `"acceptEdits"` or one of the other modes
  the Claude Code CLI accepts, and expect a lower-fidelity experience.

## 5. Install the launchd plists

The templates in `deploy/` are what launchd loads. Copy them into
`~/Library/LaunchAgents/`, replace the `YOU`-placeholder paths with your
real ones, then bootstrap. The template filenames and labels
(`com.landline.telegram-daemon`, `com.landline.daemon-watchdog`) match the
defaults baked into `deploy/restart.sh` and `deploy/watchdog.sh`, so no
env vars are required if you keep them as-is.

```bash
cp deploy/com.landline.telegram-daemon.plist \
   ~/Library/LaunchAgents/com.landline.telegram-daemon.plist
cp deploy/com.landline.daemon-watchdog.plist \
   ~/Library/LaunchAgents/com.landline.daemon-watchdog.plist

# Edit both to replace /Users/YOU/.landline and /Users/YOU/claude-landline
# with your real paths.
${EDITOR:-vi} ~/Library/LaunchAgents/com.landline.telegram-daemon.plist
${EDITOR:-vi} ~/Library/LaunchAgents/com.landline.daemon-watchdog.plist

# Load both.
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.landline.telegram-daemon.plist
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.landline.daemon-watchdog.plist

# Confirm.
launchctl list | grep landline
```

The daemon plist uses `KeepAlive: true`, `ThrottleInterval: 30`, and
`ExitTimeOut: 45`: launchd will restart the daemon whenever it exits,
bounding the crash loop to at most one restart per 30 seconds and
allowing 45 seconds for graceful shutdown. The watchdog plist runs every
`StartInterval: 300` seconds (5 minutes) and re-bootstraps the daemon
label if it fell off launchd entirely (which `KeepAlive` alone doesn't
recover).

`deploy/restart.sh` is the operational restart tool — always use it
instead of raw `launchctl` because it compile-checks, imports, and
runs the test suite before touching the process. See
[`../CLAUDE.md`](../CLAUDE.md) → **Restart Procedure** for details.

## 6. Verify

Message your bot from your allowlisted Telegram account. The first
message after a fresh start goes into the "locked" state — the daemon
replies with a lock notice. Type your passphrase (just the phrase, no
`/unlock` command). The daemon replies with "unlocked" and hands the
next message to Claude.

Then:

- Send `/status`. You should get a header (`**<agent_name> System
  Status**`), a count of loaded/running launchd jobs matching
  `launchd_label_prefix`, the last workspace git commit as the last
  backup, the current Claude session id + turn count, today's usage stats
  (turns / tokens / notional USD) if any turns have run, and the lock
  status line. If `morning_brief_glob` is configured, a "Last morning
  brief" line lands above the backup line.
- Watch the log tail: `tail -f ~/.landline/logs/telegram-daemon/daemon.log`.
- Try a voice note (if whisper is installed) — a 👀 should appear on
  your message, transcription runs locally, and the transcript is
  handed to Claude inside XML delimiters.
- Try a PDF: send it, watch the daemon log the byte count (never the
  filename), and see Claude pick it up.

If something isn't working:

- **No response at all.** Check the allowlist Keychain entry —
  `security find-generic-password -s telegram-allowed-chat-ids -a landline -w`
  should list your `chat_id`. An empty or missing allowlist blocks
  everyone.
- **`(Paused.)` on every send.** Restart the daemon; a stale pause flag
  from a crashed turn may be latched.
- **Blank replies.** `tail` the daemon log for stack traces — the most
  common cause is a Python 3.10+ syntax error introduced by an edit.
  Compile-check with the one-liner in [`../CLAUDE.md`](../CLAUDE.md).
- **`launchctl bootstrap` fails with `service already loaded`.** Run
  `launchctl bootout gui/$UID ~/Library/LaunchAgents/com.landline.telegram-daemon.plist`
  first, then bootstrap.

## 7. Running multiple daemons on one Mac

Landline is designed to run one instance per **profile** (agent + workspace
+ bot). Two profiles on the same Mac (e.g. `mineru` for personal work,
`staging` for testing) coexist cleanly as long as **every** per-profile
name is distinct — otherwise they clobber each other's Keychain entries,
launchd label, PID lock, and disk state.

The three axes you MUST separate:

| Axis | Why it must be per-profile | How |
|---|---|---|
| **Workspace directory** | State, cache, media, PID lock, spool, daily log, `landline.json`. Two daemons sharing a workspace fight over the singleton flock and corrupt each other's state. | Point `WorkingDirectory` in each plist at a distinct dir (`~/.mineru`, `~/.landline-staging`, etc.). Export `LANDLINE_WORKSPACE` if you launch the daemon interactively from another cwd. |
| **Keychain account** | The five `telegram-*` services are shared service names; only the **account** disambiguates. Two daemons with the same account would read the same bot token and route each other's traffic. | Set `"keychain_account": "<profile>"` in each `landline.json` and re-add the five secrets under that account. |
| **launchd label** | Two plists with the same `Label` bootstrap-conflict, and `KeepAlive` restarts the wrong one on crash. `/status`'s `launchctl list` filter also depends on the label prefix. | Give each plist a distinct `Label` (`com.mineru.telegram-daemon` vs `com.landline-staging.telegram-daemon`) and set `"launchd_label_prefix"` in each `landline.json` to match. |

Two other knobs that help operationally:

- **`profile_name`** in `landline.json` — cosmetic label surfaced in the
  `/status` header (e.g. `**Rook System Status** (mineru)`). Not an auth
  surface; purely so you can eyeball which daemon replied when you send
  `/status` to both bots.
- **Distinct Telegram bots** — each profile gets its own bot from
  BotFather (one token per profile Keychain entry) and its own allowlist.
  A single bot handling two profiles is not a supported configuration.

### Worked example: adding a `mineru` profile alongside a default `landline`

```bash
# 1. Workspace
mkdir -p ~/.mineru/{cache,logs/telegram-daemon,memory/daily}

# 2. Keychain entries under a distinct account. Substitute your own values.
security add-generic-password -s telegram-bot-token       -a mineru -w '<new-bot-token>'
security add-generic-password -s telegram-chat-id         -a mineru -w '<your-user-id>'
security add-generic-password -s telegram-allowed-chat-ids -a mineru -w '<your-user-id>'
PASSPHRASE='second-passphrase'
HASH=$(printf %s "$PASSPHRASE" | shasum -a 256 | awk '{print $1}')
security add-generic-password -s telegram-unlock-hash    -a mineru -w "$HASH"

# 3. landline.json — every per-profile axis distinct.
cat > ~/.mineru/landline.json <<'JSON'
{
  "keychain_account": "mineru",
  "launchd_label_prefix": "com.mineru",
  "agent_name": "Mineru",
  "profile_name": "mineru"
}
JSON

# 4. Plist — copy the template, retarget WorkingDirectory + Label.
cp deploy/com.landline.telegram-daemon.plist \
   ~/Library/LaunchAgents/com.mineru.telegram-daemon.plist
${EDITOR:-vi} ~/Library/LaunchAgents/com.mineru.telegram-daemon.plist
# Change the <Label> to com.mineru.telegram-daemon, WorkingDirectory to
# /Users/YOU/.mineru, and (recommended) StandardOutPath / StandardErrorPath
# to /Users/YOU/.mineru/logs/telegram-daemon/{stdout,stderr}.log so the
# two daemons never share a log file.

launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.mineru.telegram-daemon.plist
launchctl list | grep -E '(landline|mineru)'
```

At startup each daemon logs its resolved workspace and inject-queue path
(`Inject queue: /Users/YOU/.mineru/cache/inject-queue`) so you can
eyeball which is which in the log tail.

## 8. Day-two operations

- **Restart after an edit:** `./deploy/restart.sh` — compile, import,
  test, `bootout`+`bootstrap`, tail. Never raw `launchctl`.
- **Restart with a continuation message:**
  `./deploy/restart.sh "verify the formatting fix"`. The message is
  injected as a synthetic user turn once the daemon comes back up (or,
  if the session is locked, the trigger file is preserved and the
  message fires on next unlock).
- **Force a fresh Claude session:** send `/new`. This ends the current
  Claude session id and re-locks the daemon; type the passphrase to
  continue.
- **Check what launchd sees:** `launchctl list | grep landline`. The
  daemon's PID column should be a number, not `-`. If it's `-` with a
  non-zero exit code, the watchdog will pick it up within 5 minutes.
- **Rotate the passphrase:** overwrite the Keychain entry
  (`security add-generic-password ... -U`) with the new hash, restart.

Onward: [`ARCHITECTURE.md`](ARCHITECTURE.md) walks the internals.
