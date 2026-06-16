#!/usr/bin/env bash
# Smoke-tests a running ATAS -> MEXC bridge over HTTP.
# Works in Git Bash / MINGW64 on Windows, and on Linux/macOS.
set -uo pipefail

BASE_URL="http://localhost:5000"
PASS=0
FAIL=0

if ! command -v curl >/dev/null 2>&1; then
    echo "ERROR: curl not found. Git Bash on Windows ships with curl by default -"
    echo "if it's missing, reinstall Git for Windows or run this from WSL."
    exit 1
fi

echo "==> Checking if the bridge is running at $BASE_URL ..."
if ! curl -s -o /dev/null --max-time 3 "$BASE_URL/health"; then
    echo ""
    echo "############################################################"
    echo "  Could not reach $BASE_URL"
    echo "  Start the bridge first, e.g. in another terminal:"
    echo "    ./setup.sh"
    echo "############################################################"
    exit 1
fi
echo "    Bridge is reachable."
echo ""

check() {
    local label="$1"
    local expected_codes="$2"   # space-separated list of acceptable codes
    local method="$3"
    local path="$4"
    local body="${5:-}"

    local response status
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" --max-time 5 "$BASE_URL$path")
    else
        response=$(curl -s -w "\n%{http_code}" --max-time 5 -X "$method" \
            -H "Content-Type: application/json" -d "$body" "$BASE_URL$path")
    fi
    status=$(echo "$response" | tail -n1)
    payload=$(echo "$response" | sed '$d')

    if echo "$expected_codes" | grep -qw "$status"; then
        echo "[PASS] $label -> HTTP $status"
        echo "       $payload"
        PASS=$((PASS+1))
    else
        echo "[FAIL] $label -> HTTP $status (expected one of: $expected_codes)"
        echo "       $payload"
        FAIL=$((FAIL+1))
    fi
    echo ""
}

check "GET /health" "200" "GET" "/health"
check "GET /status" "200" "GET" "/status"

# Small test order: well within default limits (max_order_size, max_order_size_usd).
# 400 is accepted too - e.g. insufficient balance on a fresh/unfunded account is a
# valid, correctly-handled response, not a broken endpoint.
check "POST /order (test order)" "200 400" "POST" "/order" \
    '{"symbol":"BTCUSDT","side":"BUY","price":"50000","quantity":"0.001"}'

echo "============================================================"
echo "Results: $PASS passed, $FAIL failed"
echo "============================================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
