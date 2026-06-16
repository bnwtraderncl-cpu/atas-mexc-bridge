"""
ATAS -> MEXC order bridge.

Routes orders received from ATAS to MEXC's REST API and keeps local
account state (balances, positions, open orders) in sync via MEXC's
user data WebSocket stream.

Safety: the bridge defaults to paper trading. Live order submission is
only possible when config["trading"]["paper_trading"] is explicitly set
to false AND a valid API key/secret are present. In paper mode, orders
are validated and logged but never sent to MEXC.
"""

import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
import websocket
from flask import Flask, jsonify, request
from werkzeug.serving import make_server

logger = logging.getLogger("atas_mexc_bridge")

# Known MEXC quote assets, longest first, used for lightweight symbol format
# validation without requiring a live exchangeInfo call.
KNOWN_QUOTE_ASSETS = ("USDT", "USDC", "BTC", "ETH", "BNB")


class BridgeError(Exception):
    """Base error for all bridge failures."""


class OrderValidationError(BridgeError):
    """Raised when an incoming order fails validation."""


class LiveTradingDisabledError(BridgeError):
    """Raised if live order submission is attempted while paper mode is on."""


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    required_sections = ("mexc", "trading", "sync", "logging")
    for section in required_sections:
        if section not in config:
            raise BridgeError(f"Config missing required section: {section}")

    return config


def configure_logging(log_config: dict) -> None:
    level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_file = log_config.get("file")

    handlers = [logging.StreamHandler()]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=handlers,
    )


@dataclass
class OrderRequest:
    """Normalized representation of an order coming in from ATAS."""

    symbol: str
    side: str  # "BUY" or "SELL"
    order_type: str  # "LIMIT" or "MARKET"
    quantity: float
    price: Optional[float] = None
    client_order_id: Optional[str] = None


@dataclass
class AccountState:
    """In-memory snapshot of account state, kept current by the WS sync."""

    balances: dict = field(default_factory=dict)
    positions: dict = field(default_factory=dict)
    open_orders: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update_balance(self, asset: str, free: float, locked: float) -> None:
        with self.lock:
            self.balances[asset] = {"free": free, "locked": locked}

    def upsert_order(self, order_id: str, order: dict) -> None:
        with self.lock:
            self.open_orders[order_id] = order

    def remove_order(self, order_id: str) -> None:
        with self.lock:
            self.open_orders.pop(order_id, None)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "balances": dict(self.balances),
                "positions": dict(self.positions),
                "open_orders": dict(self.open_orders),
            }


class MexcClient:
    """Minimal signed REST client for the MEXC spot API."""

    def __init__(self, api_key: str, api_secret: str, base_url: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-MEXC-APIKEY": self.api_key})

    def _sign(self, params: dict) -> str:
        query = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return signature

    def _signed_request(self, method: str, path: str, params: Optional[dict] = None) -> dict:
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)

        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, params=params, timeout=10)

        if not response.ok:
            raise BridgeError(
                f"MEXC API error {response.status_code} on {method} {path}: {response.text}"
            )
        return response.json()

    def place_order(self, order: OrderRequest) -> dict:
        params = {
            "symbol": order.symbol,
            "side": order.side,
            "type": order.order_type,
            "quantity": order.quantity,
        }
        if order.client_order_id:
            params["newClientOrderId"] = order.client_order_id
        if order.order_type == "LIMIT":
            if order.price is None:
                raise OrderValidationError("LIMIT orders require a price")
            params["price"] = order.price
            params["timeInForce"] = "GTC"

        return self._signed_request("POST", "/api/v3/order", params)

    def get_account(self) -> dict:
        return self._signed_request("GET", "/api/v3/account")

    def get_open_orders(self, symbol: Optional[str] = None) -> list:
        params = {"symbol": symbol} if symbol else {}
        return self._signed_request("GET", "/api/v3/openOrders", params)

    def create_listen_key(self) -> str:
        result = self._signed_request("POST", "/api/v3/userDataStream")
        return result["listenKey"]

    def keepalive_listen_key(self, listen_key: str) -> None:
        self._signed_request("PUT", "/api/v3/userDataStream", {"listenKey": listen_key})


class AccountSync:
    """Maintains a live AccountState via MEXC's user data WebSocket stream."""

    def __init__(self, client: MexcClient, ws_url: str, state: AccountState, sync_config: dict):
        self.client = client
        self.ws_url = ws_url
        self.state = state
        self.sync_config = sync_config
        self._listen_key: Optional[str] = None
        self._ws: Optional[websocket.WebSocketApp] = None
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._connected = threading.Event()

    def start(self) -> None:
        self._listen_key = self.client.create_listen_key()
        self._refresh_snapshot()

        ws_thread = threading.Thread(target=self._run_ws_loop, daemon=True)
        ws_thread.start()
        self._threads.append(ws_thread)

        keepalive_thread = threading.Thread(target=self._run_keepalive_loop, daemon=True)
        keepalive_thread.start()
        self._threads.append(keepalive_thread)

        poll_thread = threading.Thread(target=self._run_poll_loop, daemon=True)
        poll_thread.start()
        self._threads.append(poll_thread)

    def stop(self) -> None:
        self._stop_event.set()
        self._connected.clear()
        if self._ws:
            self._ws.close()

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def is_listen_key_valid(self) -> bool:
        return self._listen_key is not None

    def _refresh_snapshot(self) -> None:
        try:
            account = self.client.get_account()
            for balance in account.get("balances", []):
                self.state.update_balance(
                    balance["asset"], float(balance["free"]), float(balance["locked"])
                )
            open_orders = self.client.get_open_orders()
            for order in open_orders:
                self.state.upsert_order(str(order["orderId"]), order)
            logger.info("Account snapshot refreshed")
        except BridgeError as exc:
            logger.error("Failed to refresh account snapshot: %s", exc)

    def _run_poll_loop(self) -> None:
        interval = self.sync_config.get("account_sync_interval_seconds", 30)
        while not self._stop_event.wait(interval):
            self._refresh_snapshot()

    def _run_keepalive_loop(self) -> None:
        interval = self.sync_config.get("listen_key_refresh_interval_seconds", 1800)
        while not self._stop_event.wait(interval):
            try:
                self.client.keepalive_listen_key(self._listen_key)
                logger.debug("Listen key refreshed")
            except BridgeError as exc:
                logger.error("Failed to refresh listen key: %s", exc)

    def _run_ws_loop(self) -> None:
        reconnect_delay = self.sync_config.get("ws_reconnect_delay_seconds", 5)
        while not self._stop_event.is_set():
            try:
                url = f"{self.ws_url}?listenKey={self._listen_key}"
                self._ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                logger.exception("WebSocket loop crashed")

            if not self._stop_event.is_set():
                logger.warning("WebSocket disconnected, reconnecting in %ss", reconnect_delay)
                self._stop_event.wait(reconnect_delay)

    def _on_open(self, ws) -> None:
        self._connected.set()
        subscribe_msg = {
            "method": "SUBSCRIPTION",
            "params": ["spot@private.account.v3.api", "spot@private.orders.v3.api"],
        }
        ws.send(json.dumps(subscribe_msg))
        logger.info("Sent WS subscription: %s", subscribe_msg["params"])

    def _on_message(self, _ws, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("Received non-JSON WS message")
            return

        channel = data.get("channel")
        if channel == "spot@private.account.v3.api":
            account_update = data.get("spotaccountupdate", {})
            for balance in account_update.get("balances", []):
                self.state.update_balance(
                    balance["asset"], float(balance["free"]), float(balance["locked"])
                )
        elif channel == "spot@private.orders.v3.api":
            order = data.get("spotprivateorder", {})
            order_id = str(order.get("orderId"))
            status = order.get("status")
            if status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
                self.state.remove_order(order_id)
            else:
                self.state.upsert_order(order_id, order)
        else:
            logger.debug("Unhandled WS channel: %s", channel)

    def _on_error(self, _ws, error) -> None:
        logger.error("WebSocket error: %s", error)

    def _on_close(self, _ws, status_code, msg) -> None:
        self._connected.clear()
        logger.info("WebSocket closed (%s): %s", status_code, msg)


def split_symbol(symbol: str) -> Optional[tuple]:
    """Split a MEXC symbol like 'BTCUSDT' into (base, quote), or None if unrecognized."""
    for quote in KNOWN_QUOTE_ASSETS:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    return None


class OrderRouter:
    """Validates incoming orders and routes them to MEXC, or to a paper log."""

    def __init__(self, client: MexcClient, trading_config: dict, state: AccountState):
        self.client = client
        self.trading_config = trading_config
        self.state = state
        self.paper_trading = trading_config.get("paper_trading", True)
        self._lock = threading.Lock()

    def validate(self, order: OrderRequest) -> None:
        allowed_symbols = self.trading_config.get("allowed_symbols")
        if allowed_symbols and order.symbol not in allowed_symbols:
            raise OrderValidationError(f"Symbol {order.symbol} is not in allowed_symbols")

        split = split_symbol(order.symbol)
        if split is None:
            raise OrderValidationError(f"Symbol {order.symbol} is not a recognized MEXC symbol")
        base_asset, quote_asset = split

        if order.side not in ("BUY", "SELL"):
            raise OrderValidationError(f"Invalid side: {order.side}")

        if order.order_type not in ("LIMIT", "MARKET"):
            raise OrderValidationError(f"Invalid order type: {order.order_type}")

        if order.quantity <= 0:
            raise OrderValidationError("Quantity must be positive")

        max_order_size = self.trading_config.get("max_order_size")
        if max_order_size and order.quantity > max_order_size:
            raise OrderValidationError(
                f"Order quantity {order.quantity} exceeds max_order_size {max_order_size}"
            )

        if order.order_type == "LIMIT":
            if order.price is None or order.price <= 0:
                raise OrderValidationError("LIMIT orders require a positive price")
            notional = order.quantity * order.price
            max_size_usd = self.trading_config.get("max_order_size_usd")
            if max_size_usd and notional > max_size_usd:
                raise OrderValidationError(
                    f"Order notional {notional:.2f} exceeds max_order_size_usd {max_size_usd}"
                )

            balances = self.state.snapshot()["balances"]
            if order.side == "BUY":
                available = balances.get(quote_asset, {}).get("free", 0.0)
                if available < notional:
                    raise OrderValidationError(
                        f"Insufficient {quote_asset} balance: need {notional:.8f}, "
                        f"have {available:.8f}"
                    )
            else:
                available = balances.get(base_asset, {}).get("free", 0.0)
                if available < order.quantity:
                    raise OrderValidationError(
                        f"Insufficient {base_asset} balance: need {order.quantity}, "
                        f"have {available}"
                    )

        max_open = self.trading_config.get("max_open_orders")
        if max_open is not None and len(self.state.snapshot()["open_orders"]) >= max_open:
            raise OrderValidationError(f"max_open_orders ({max_open}) reached")

    def route_order(self, order: OrderRequest) -> dict:
        """Thread-safe entry point: validate then send the order."""
        with self._lock:
            return self.route(order)

    def route(self, order: OrderRequest) -> dict:
        """Validate then send the order. Returns a confirmation dict."""
        self.validate(order)

        if self.paper_trading:
            logger.info("[PAPER] Order accepted, not sent to MEXC: %s", order)
            return {
                "status": "PAPER_ACCEPTED",
                "paper_trading": True,
                "order": order.__dict__,
            }

        if not self.client.api_key or not self.client.api_secret:
            raise LiveTradingDisabledError(
                "Live trading requires a valid api_key/api_secret in config"
            )

        logger.info("Routing live order to MEXC: %s", order)
        try:
            result = self.client.place_order(order)
        except BridgeError:
            logger.exception("Order submission failed")
            raise

        logger.info("Order confirmed by MEXC: %s", result)
        return {"status": "LIVE_SUBMITTED", "paper_trading": False, "order": result}


class BridgeHttpServer:
    """Flask HTTP server exposing the bridge to ATAS: /order, /status, /health."""

    def __init__(
        self,
        router: OrderRouter,
        state: AccountState,
        sync: AccountSync,
        host: str = "127.0.0.1",
        port: int = 5000,
    ):
        self.router = router
        self.state = state
        self.sync = sync
        self.start_time = time.time()

        self.app = Flask("atas_mexc_bridge")
        self._register_routes()
        self._server = make_server(host, port, self.app)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def _register_routes(self) -> None:
        app = self.app

        @app.post("/order")
        def post_order():
            payload = request.get_json(silent=True) or {}
            symbol = payload.get("symbol")
            side = payload.get("side")
            price_raw = payload.get("price")
            quantity_raw = payload.get("quantity")
            logger.info(
                "Received POST /order: symbol=%s side=%s quantity=%s",
                symbol, side, quantity_raw,
            )

            client_order_id = str(uuid.uuid4())
            try:
                if not symbol or not side or quantity_raw is None:
                    raise OrderValidationError("symbol, side, and quantity are required")
                quantity = float(quantity_raw)
                price = float(price_raw) if price_raw is not None else None
                order = OrderRequest(
                    symbol=symbol,
                    side=side,
                    order_type="LIMIT" if price is not None else "MARKET",
                    quantity=quantity,
                    price=price,
                    client_order_id=client_order_id,
                )
                result = self.router.route_order(order)
            except (OrderValidationError, LiveTradingDisabledError) as exc:
                logger.warning("Order validation failed: %s", exc)
                return jsonify({"error": str(exc)}), 400
            except (ValueError, TypeError) as exc:
                logger.warning("Malformed order request: %s", exc)
                return jsonify({"error": f"Malformed order request: {exc}"}), 400
            except BridgeError as exc:
                logger.error("Order submission failed: %s", exc)
                return jsonify({"error": str(exc)}), 502

            logger.info("Order confirmed: %s", result)
            order_id = result.get("order", {}).get("orderId") or client_order_id
            return jsonify(
                {
                    "orderId": str(order_id),
                    "status": "PENDING",
                    "timestamp": str(int(time.time() * 1000)),
                }
            )

        @app.get("/status")
        def get_status():
            snapshot = self.state.snapshot()
            return jsonify(
                {
                    "balance": snapshot["balances"],
                    "open_orders": list(snapshot["open_orders"].values()),
                    "listen_key_valid": self.sync.is_listen_key_valid(),
                }
            )

        @app.get("/health")
        def get_health():
            return jsonify(
                {
                    "status": "running",
                    "mexc_connected": self.sync.is_connected(),
                    "uptime_seconds": round(time.time() - self.start_time, 1),
                }
            )

    def start(self) -> None:
        self._thread.start()
        logger.info("HTTP server listening on %s", self._server.server_address)

    def stop(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)


class Bridge:
    """Top-level wiring: config, client, state, sync, and router."""

    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        configure_logging(self.config["logging"])

        mexc_config = self.config["mexc"]
        self.client = MexcClient(
            api_key=mexc_config.get("api_key", ""),
            api_secret=mexc_config.get("api_secret", ""),
            base_url=mexc_config.get("base_url", "https://api.mexc.com"),
        )

        self.state = AccountState()
        self.router = OrderRouter(self.client, self.config["trading"], self.state)
        self.sync = AccountSync(
            self.client, mexc_config.get("ws_url"), self.state, self.config["sync"]
        )

        http_config = self.config.get("http", {})
        self.http_server = BridgeHttpServer(
            self.router,
            self.state,
            self.sync,
            host=http_config.get("host", "127.0.0.1"),
            port=http_config.get("port", 5000),
        )

        if self.config["trading"].get("paper_trading", True):
            logger.warning("Bridge running in PAPER TRADING mode. No live orders will be sent.")
        else:
            logger.warning("Bridge running in LIVE TRADING mode. Real orders will be sent to MEXC.")

        self.http_server.start()

    def start(self) -> None:
        self.sync.start()

    def stop(self) -> None:
        self.sync.stop()

    def close(self) -> None:
        """Gracefully shut down the HTTP server and the MEXC WS/listen key sync."""
        self.http_server.stop()
        self.stop()

    def submit_order(self, order: OrderRequest) -> dict:
        return self.router.route_order(order)

    def account_snapshot(self) -> dict:
        return self.state.snapshot()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="ATAS -> MEXC order bridge")
    parser.add_argument(
        "--config", default="config/example_config.json", help="Path to config JSON file"
    )
    args = parser.parse_args()

    bridge = Bridge(args.config)
    bridge.start()

    try:
        while True:
            time.sleep(60)
            logger.info("Account snapshot: %s", bridge.account_snapshot())
    except KeyboardInterrupt:
        logger.info("Shutting down bridge")
        bridge.close()


if __name__ == "__main__":
    main()
