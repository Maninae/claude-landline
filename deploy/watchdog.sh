#!/bin/bash
# Landline daemon watchdog — secondary safety net for when launchd fails.
# Runs on an interval via its own launchd plist. If the daemon plist isn't
# loaded, re-bootstraps it and fires an iMessage alert.
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

# NOT loaded in launchd — the failure case this watchdog exists for.
log "ALERT: $LANDLINE_LABEL not loaded in launchd. Re-bootstrapping..."

if launchctl bootstrap "gui/$UID_NUM" "$LANDLINE_PLIST" 2>>"$LANDLINE_WATCHDOG_LOG"; then
    log "Successfully re-bootstrapped $LANDLINE_LABEL"
    MSG="[Landline watchdog] $LANDLINE_LABEL was not loaded in launchd. Re-bootstrapped at $(date '+%Y-%m-%d %H:%M:%S')."
else
    log "ERROR: Failed to bootstrap $LANDLINE_LABEL"
    MSG="[Landline watchdog] $LANDLINE_LABEL crashed and FAILED to relaunch at $(date '+%Y-%m-%d %H:%M:%S'). Manual intervention needed."
fi

# Notify via iMessage
if [[ -n "$OWNER_HANDLE" ]]; then
    osascript -e "tell application \"Messages\" to send \"$MSG\" to participant \"$OWNER_HANDLE\"" 2>>"$LANDLINE_WATCHDOG_LOG" || true
    log "Sent iMessage alert"
fi
