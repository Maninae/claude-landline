#!/bin/bash
# Landline daemon restart — safe restart with compile/import/pytest gates
# and an optional continuation message.
#
# Usage:
#   restart.sh                                # restart, default continuation
#   restart.sh "Continue where you left off"  # restart + custom message
#   restart.sh --skip-tests                   # skip pytest (faster)
#   restart.sh --skip-tests "message"         # both
#
# Steps: compile check → import check → (tests) → write continuation trigger
#        → launchctl bootout → launchctl bootstrap → verify.
#
# Config via env (all defaults match the docs/SETUP.md workflow):
#   LANDLINE_WORKSPACE  agent workspace where the daemon runs
#                       (default: $HOME/.landline)
#   LANDLINE_REPO       path to this checkout — where landline/ lives
#                       (default: this script's ../.. )
#   LANDLINE_PLIST      launchd plist to boot
#                       (default: $HOME/Library/LaunchAgents/com.landline.telegram-daemon.plist)
#   LANDLINE_LABEL      launchd label
#                       (default: com.landline.telegram-daemon)

set -euo pipefail

# Resolve this script's directory so LANDLINE_REPO defaults sanely no matter
# where the script is invoked from.
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LANDLINE_WORKSPACE="${LANDLINE_WORKSPACE:-$HOME/.landline}"
LANDLINE_REPO="${LANDLINE_REPO:-$(cd "$_SCRIPT_DIR/.." && pwd)}"
LANDLINE_PLIST="${LANDLINE_PLIST:-$HOME/Library/LaunchAgents/com.landline.telegram-daemon.plist}"
LANDLINE_LABEL="${LANDLINE_LABEL:-com.landline.telegram-daemon}"

CONTINUATION_FILE="$LANDLINE_WORKSPACE/cache/restart-continuation.txt"
UID_NUM=$(id -u)
PYTHON_BIN="${LANDLINE_PYTHON:-/usr/bin/python3}"

SKIP_TESTS=false
CONTINUATION_MSG=""

for arg in "$@"; do
    case "$arg" in
        --skip-tests) SKIP_TESTS=true ;;
        *)            CONTINUATION_MSG="$arg" ;;
    esac
done

if [ ! -d "$LANDLINE_REPO/landline" ]; then
    echo "ERROR: LANDLINE_REPO ($LANDLINE_REPO) does not contain a landline/ package." >&2
    exit 1
fi

echo "Compile check ($LANDLINE_REPO/landline/**/*.py)..."
(cd "$LANDLINE_REPO" && "$PYTHON_BIN" -c \
    "import py_compile, glob; [py_compile.compile(f, doraise=True) for f in glob.glob('landline/**/*.py', recursive=True)]; print('OK')")

echo "Import check..."
(cd "$LANDLINE_REPO" && PYTHONPATH="$LANDLINE_REPO" LANDLINE_WORKSPACE="$LANDLINE_WORKSPACE" \
    "$PYTHON_BIN" -c "from landline.orchestrator import TelegramDaemon; print('OK')")

if [ "$SKIP_TESTS" = false ]; then
    echo "Running pytest..."
    (cd "$LANDLINE_REPO" && PYTHONPATH="$LANDLINE_REPO" \
        "$PYTHON_BIN" -m pytest landline/tests/ -q 2>&1 | tail -3)
else
    echo "Skipping pytest (--skip-tests)"
fi

mkdir -p "$LANDLINE_WORKSPACE/cache"
if [ -n "$CONTINUATION_MSG" ]; then
    echo "Writing continuation trigger..."
    echo "$CONTINUATION_MSG" > "$CONTINUATION_FILE"
else
    echo "[System] The Telegram daemon was just restarted. Continue from where you left off." > "$CONTINUATION_FILE"
fi

echo "Restarting daemon ($LANDLINE_LABEL)..."
launchctl bootout "gui/$UID_NUM" "$LANDLINE_PLIST" 2>/dev/null || true
sleep 1
launchctl bootstrap "gui/$UID_NUM" "$LANDLINE_PLIST"

echo "Daemon restarted. Recent log:"
sleep 2
tail -5 "$LANDLINE_WORKSPACE/logs/telegram-daemon/daemon.log" 2>/dev/null || \
    echo "(no log yet at $LANDLINE_WORKSPACE/logs/telegram-daemon/daemon.log)"
