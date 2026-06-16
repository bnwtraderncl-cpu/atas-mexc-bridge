# atas-mexc-bridge

Python bridge routing ATAS orders to MEXC with account sync.

## Quick start

```
./setup.sh
```

On first run this just creates `config/config.json` from the example and
stops, telling you to fill in `mexc.api_key` / `mexc.api_secret`. Edit that
file, then run `./setup.sh` again — it installs dependencies and starts the
bridge in the foreground (leave this terminal open; `paper_trading` is
`true` by default, so this is safe — no real orders go to MEXC).

In a **second terminal**, with the bridge still running:

```
./test.sh
```

This hits `GET /health`, `GET /status`, and `POST /order` (a small test
order) and reports pass/fail for each.

**Windows:** run both scripts from **Git Bash (MINGW64)** — that's what
ships with Git for Windows and is what `setup.sh`/`test.sh` are written
for. Plain `cmd.exe` or PowerShell can't run `.sh` files directly; open
"Git Bash" from the Start menu (or right-click the folder ->
"Git Bash Here") instead.

## What it does

- Accepts normalized order requests (as would come from ATAS) and validates
  them against configurable limits (allowed symbols, max order size, max
  open orders, available balance).
- Routes validated orders to the MEXC spot REST API.
- Keeps a local snapshot of balances, positions, and open orders current via
  MEXC's authenticated user data WebSocket stream, with periodic REST
  reconciliation as a fallback.
- Exposes an HTTP API on `localhost:5000` for ATAS (or anything else) to
  submit orders and check bridge/account state.
- Defaults to **paper trading**: orders are validated and logged but never
  sent to MEXC unless `trading.paper_trading` is explicitly set to `false`.

## HTTP endpoints

The bridge starts a local HTTP server (default `http://127.0.0.1:5000`,
configurable under `http` in `config.json`) as soon as it's launched.

### `POST /order`

Submit an order.

Request body:
```json
{"symbol": "BTCUSDT", "side": "BUY", "price": "50000", "quantity": "0.001"}
```

Success response (`200`):
```json
{"orderId": "...", "status": "PENDING", "timestamp": "..."}
```

Validation failure (`400`) — e.g. insufficient balance, exceeds
`max_order_size`/`max_order_size_usd`, symbol not in `allowed_symbols`,
unrecognized symbol format:
```json
{"error": "..."}
```

A `502` means the order passed validation but MEXC itself rejected the
live submission.

### `GET /status`

Current account state, for ATAS to sync against:
```json
{"balance": {...}, "open_orders": [...], "listen_key_valid": true}
```

### `GET /health`

Bridge/connection health check:
```json
{"status": "running", "mexc_connected": true, "uptime_seconds": 123.4}
```

`mexc_connected` reflects the live state of the user-data WebSocket, not
just that the HTTP server is up.

## Manual setup (without setup.sh)

1. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

2. Copy the example config and fill in your MEXC API credentials:

   ```
   cp config/example_config.json config/config.json
   ```

   Edit `config/config.json`:
   - `mexc.api_key` / `mexc.api_secret`: from your MEXC account API
     management page. Use a key with **spot trading only**, no withdrawal
     permission.
   - `trading.paper_trading`: keep `true` until you've verified behavior.
     Set to `false` only when you intend to submit real orders.
   - `trading.max_order_size_usd`, `trading.max_order_size`,
     `trading.max_open_orders`, `trading.allowed_symbols`: hard limits
     enforced before any order is sent, regardless of what ATAS requests.
   - `http.host` / `http.port`: where the bridge's HTTP API listens
     (default `127.0.0.1:5000`).
   - `sync.*`: WebSocket reconnect/keepalive and REST polling intervals.

3. **Never commit `config/config.json`** — it contains live API secrets.
   It's already listed in `.gitignore`, so a plain `git add` won't pick it
   up — don't override that with `git add -f`.

## Running

```
python src/bridge.py --config config/config.json
```

This starts the account sync (REST snapshot + WebSocket stream), the HTTP
server, and logs a periodic account summary. Logs go to stdout and to the
file configured in `logging.file` (default `logs/bridge.log`).

## Submitting orders programmatically

```python
from src.bridge import Bridge, OrderRequest

bridge = Bridge("config/config.json")
bridge.start()

confirmation = bridge.submit_order(
    OrderRequest(symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                 quantity=0.001, price=60000.0)
)
print(confirmation)

bridge.close()
```

In paper mode, `confirmation["status"]` is `"PAPER_ACCEPTED"` and no network
call to MEXC's order endpoint is made. In live mode, it's
`"LIVE_SUBMITTED"` with MEXC's actual order response. Call `bridge.close()`
on shutdown to stop the HTTP server and the WebSocket sync/listen key
cleanly.

## Safety notes

- **`paper_trading: true` by default** — orders are validated and logged
  but never sent to MEXC until you explicitly flip this to `false`.
- Live trading requires both `trading.paper_trading: false` **and** a
  non-empty API key/secret — missing either raises an error before any
  order is sent.
- Order validation (symbol allowlist, size limits, balance check, open
  order cap) runs identically in paper and live mode, so you can trust
  paper-mode behavior as a preview of live-mode validation.
- Use an MEXC API key scoped to spot trading only, with withdrawals
  disabled, to limit blast radius if the key is ever compromised.
- The HTTP API binds to `127.0.0.1` by default — it's not exposed to the
  network unless you deliberately change `http.host`.
