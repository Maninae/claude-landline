#!/bin/bash
# Landline daemon watchdog — secondary safety net for when launchd fails.
# Runs on an interval via its own launchd plist. If the daemon plist isn't
# loaded, re-bootstraps it and alerts the operator: Telegram Bot API first
# (Keychain services telegram-bot-token + telegram-chat-id), iMessage as the
# fallback (osascript from launchd contexts times out ~half the time).
#
# Config via env:
#   LANDLINE_WORKSPACE          agent workspace (default: $HOME/.landline)
#   LANDLINE_PLIST              plist to check + boot
#                               (default: $HOME/Library/LaunchAgents/com.landline.telegram-daemon.plist)
#   LANDLINE_LABEL              launchd label
#                               (default: com.landline.telegram-daemon)
#   LANDLINE_WATCHDOG_LOG       watchdog log file
#                               (default: <workspace>/logs/telegram-daemon/watchdog.log)
#   LANDLINE_KEYCHAIN_ACCOUNT   Keychain account holding owner-imsg-handle
#                               (default: landline; MUST match landline.json's
#                               "keychain_account")

LANDLINE_WORKSPACE="${LANDLINE_WORKSPACE:-$HOME/.landline}"
LANDLINE_PLIST="${LANDLINE_PLIST:-$HOME/Library/LaunchAgents/com.landline.telegram-daemon.plist}"
LANDLINE_LABEL="${LANDLINE_LABEL:-com.landline.telegram-daemon}"
LANDLINE_WATCHDOG_LOG="${LANDLINE_WATCHDOG_LOG:-$LANDLINE_WORKSPACE/logs/telegram-daemon/watchdog.log}"
LANDLINE_KEYCHAIN_ACCOUNT="${LANDLINE_KEYCHAIN_ACCOUNT:-landline}"

OWNER_HANDLE=$(security find-generic-password \
    -a "$LANDLINE_KEYCHAIN_ACCOUNT" \
    -s owner-imsg-handle -w 2>/dev/null)
UID_NUM=$(id -u)

mkdir -p "$(dirname "$LANDLINE_WATCHDOG_LOG")" 2>/dev/null || true

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LANDLINE_WATCHDOG_LOG"
}

# Check if the daemon is loaded in launchd
if launchctl list "$LANDLINE_LABEL" &>/dev/null; then
    # Loaded — check if the process is actually running
    PID=$(launchctl list "$LANDLINE_LABEL" 2>/dev/null | awk 'NR==1{print $1}')
    if [[ "$PID" != "-" && "$PID" =~ ^[0-9]+$ ]]; then
        # Process is running, all good
        exit 0
    fi
    # Loaded but PID is "-" (not running) — launchd will restart it via KeepAlive.
    # Just verify it comes back within the next check cycle.
    exit 0
fi

# send_alert <message>
#
# Telegram Bot API first (plain HTTPS; the daemon being down doesn't affect
# the API, and it's the channel the operator actually reads). iMessage is the
# fallback only: AppleEvents from a launchd context time out roughly half the
# time (error -1712), which is why it can't be the primary channel.
# Every outcome is logged truthfully — a failed send must never log as sent.
send_alert() {
    local msg="$1"
    local delivered=0

    local token chat_id
    token=$(security find-generic-password \
        -a "$LANDLINE_KEYCHAIN_ACCOUNT" -s telegram-bot-token -w 2>/dev/null)
    chat_id=$(security find-generic-password \
        -a "$LANDLINE_KEYCHAIN_ACCOUNT" -s telegram-chat-id -w 2>/dev/null)
    if [[ -n "$token" && -n "$chat_id" ]]; then
        # URL via `curl -K -` on stdin keeps the bot token out of argv (ps-visible).
        local response
        response=$(printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$token" | \
            curl -sS -m 15 -K - \
                --data-urlencode "chat_id=${chat_id}" \
                --data-urlencode "text=${msg}" 2>>"$LANDLINE_WATCHDOG_LOG")
        if [[ "$response" == *'"ok":true'* ]]; then
            log "Sent Telegram alert"
            delivered=1
        else
            log "Telegram alert send FAILED"
        fi
    else
        log "Telegram alert skipped (telegram-bot-token / telegram-chat-id not in Keychain)"
    fi

    if [[ "$delivered" -eq 0 && -n "$OWNER_HANDLE" ]]; then
        if osascript -e "tell application \"Messages\" to send \"$msg\" to participant \"$OWNER_HANDLE\"" \
                2>>"$LANDLINE_WATCHDOG_LOG"; then
            log "Sent iMessage alert (fallback)"
        else
            log "iMessage alert send FAILED"
        fi
    fi
}

# NOT loaded in launchd — the failure case this watchdog exists for.
log "ALERT: $LANDLINE_LABEL not loaded in launchd. Re-bootstrapping..."

if launchctl bootstrap "gui/$UID_NUM" "$LANDLINE_PLIST" 2>>"$LANDLINE_WATCHDOG_LOG"; then
    log "Successfully re-bootstrapped $LANDLINE_LABEL"
    MSG="[Landline watchdog] $LANDLINE_LABEL was not loaded in launchd. Re-bootstrapped at $(date '+%Y-%m-%d %H:%M:%S')."
else
    log "ERROR: Failed to bootstrap $LANDLINE_LABEL"
    MSG="[Landline watchdog] $LANDLINE_LABEL crashed and FAILED to relaunch at $(date '+%Y-%m-%d %H:%M:%S'). Manual intervention needed."
fi

send_alert "$MSG"
