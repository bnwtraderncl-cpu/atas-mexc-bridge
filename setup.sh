#!/usr/bin/env bash
# Sets up and launches the ATAS -> MEXC bridge.
# Works in Git Bash / MINGW64 on Windows, and on Linux/macOS.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

CONFIG_FILE="config/config.json"
EXAMPLE_FILE="config/example_config.json"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "==> $CONFIG_FILE not found. Creating it from $EXAMPLE_FILE..."
    cp "$EXAMPLE_FILE" "$CONFIG_FILE"
    echo ""
    echo "############################################################"
    echo "  $CONFIG_FILE was just created with placeholder values."
    echo "  Open it and fill in:"
    echo "    - mexc.api_key"
    echo "    - mexc.api_secret"
    echo "  Then run ./setup.sh again."
    echo "  (trading.paper_trading is 'true' by default - safe to start"
    echo "   with, no real orders will be sent to MEXC.)"
    echo "############################################################"
    exit 1
fi

if grep -q "YOUR_MEXC_API_KEY_HERE\|YOUR_MEXC_API_SECRET_HERE" "$CONFIG_FILE"; then
    echo "############################################################"
    echo "  $CONFIG_FILE still has placeholder API credentials."
    echo "  Edit mexc.api_key and mexc.api_secret before continuing."
    echo "############################################################"
    exit 1
fi

# Pick a Python interpreter - prefer 'python', fall back to 'python3'.
PYTHON_BIN="python"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    PYTHON_BIN="python3"
fi
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "ERROR: no 'python' or 'python3' found on PATH. Install Python 3 first."
    exit 1
fi

echo "==> Installing dependencies..."
"$PYTHON_BIN" -m pip install -r requirements.txt

echo "==> Starting the bridge (Ctrl+C to stop)..."
"$PYTHON_BIN" src/bridge.py --config "$CONFIG_FILE"
