"""
Complete end-to-end test suite for the ATAS-MEXC bridge.
Run with:  python -m pytest tests/test_bridge.py -v
           python tests/test_bridge.py        (no pytest needed)
"""
import hashlib
import hmac
import json
import sys
import time
import threading
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import requests as _requests

from bridge import (
    AccountState,
    AccountSync,
    BridgeHttpServer,
    FuturesAccountSync,
    FuturesOrderRequest,
    FuturesOrderRouter,
    MexcClient,
    MexcFuturesClient,
    OrderRequest,
    OrderRouter,
    OrderValidationError,
)

# ---------------------------------------------------------------------------
# Minimal test harness (no external dependencies)
# ---------------------------------------------------------------------------

PASS = 0
FAIL = 0


def check(label: str, fn):
    global PASS, FAIL
    try:
        fn()
        print(f"  [PASS] {label}")
        PASS += 1
    except Exception as exc:
        print(f"  [FAIL] {label}")
        print(f"         {type(exc).__name__}: {exc}")
        FAIL += 1


def raises(exc_type, fn):
    try:
        fn()
        raise AssertionError(f"Expected {exc_type.__name__} but nothing was raised")
    except exc_type:
        pass


def section(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# 1. Syntax check
# ---------------------------------------------------------------------------

section("1. Syntax check")

import py_compile, tempfile, shutil

check("py_compile src/bridge.py", lambda: py_compile.compile("src/bridge.py", doraise=True))


# ---------------------------------------------------------------------------
# 2a. MexcClient signing
# ---------------------------------------------------------------------------

section("2a. MexcClient signing")

CLIENT = MexcClient("test_key", "test_secret", "https://api.mexc.com")


def test_spot_sign_format():
    params = {"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001", "timestamp": "1234567890000"}
    sig = CLIENT._sign(params)
    assert len(sig) == 64, f"Expected 64-char hex, got {len(sig)}"
    assert all(c in "0123456789abcdef" for c in sig)


def test_spot_sign_deterministic():
    params = {"a": "1", "b": "2"}
    assert CLIENT._sign(params) == CLIENT._sign(params)


def test_spot_sign_correct_value():
    params = {"timestamp": "1000"}
    query = urllib.parse.urlencode(params)
    expected = hmac.new(b"test_secret", query.encode(), hashlib.sha256).hexdigest()
    assert CLIENT._sign(params) == expected


check("spot signature is 64-char hex", test_spot_sign_format)
check("spot signature is deterministic", test_spot_sign_deterministic)
check("spot signature matches reference HMAC-SHA256", test_spot_sign_correct_value)


# ---------------------------------------------------------------------------
# 2b. MexcFuturesClient signing
# ---------------------------------------------------------------------------

section("2b. MexcFuturesClient signing")

FCLIENT = MexcFuturesClient("test_key", "test_secret", "https://contract.mexc.com")


def test_futures_sign_format():
    ts, sig = FCLIENT.login_signature()
    assert ts.isdigit()
    assert len(sig) == 64


def test_futures_sign_correct_value():
    ts = "1234567890000"
    expected_target = f"test_key{ts}"
    expected = hmac.new(b"test_secret", expected_target.encode(), hashlib.sha256).hexdigest()
    actual = FCLIENT._sign(ts, "")
    assert actual == expected, f"{actual} != {expected}"


def test_futures_sign_includes_param_str():
    ts = "1000"
    with_params = FCLIENT._sign(ts, "symbol=BTC_USDT")
    without_params = FCLIENT._sign(ts, "")
    assert with_params != without_params


check("futures login_signature returns (timestamp, 64-char hex)", test_futures_sign_format)
check("futures signature matches reference HMAC-SHA256(secret, key+ts+'')", test_futures_sign_correct_value)
check("futures signature differs when paramString differs", test_futures_sign_includes_param_str)


# ---------------------------------------------------------------------------
# 2c. AccountState
# ---------------------------------------------------------------------------

section("2c. AccountState balance tracking")

def test_account_balance_add():
    state = AccountState()
    state.update_balance("USDT", 1000.0, 50.0)
    snap = state.snapshot()
    assert snap["balances"]["USDT"] == {"free": 1000.0, "locked": 50.0}


def test_account_balance_overwrite():
    state = AccountState()
    state.update_balance("BTC", 1.0, 0.0)
    state.update_balance("BTC", 0.5, 0.5)
    assert state.snapshot()["balances"]["BTC"]["free"] == 0.5
    assert state.snapshot()["balances"]["BTC"]["locked"] == 0.5


def test_account_order_upsert_remove():
    state = AccountState()
    state.upsert_order("42", {"orderId": "42", "status": "NEW"})
    assert "42" in state.snapshot()["open_orders"]
    state.remove_order("42")
    assert "42" not in state.snapshot()["open_orders"]


def test_account_remove_nonexistent_is_noop():
    state = AccountState()
    state.remove_order("doesnotexist")  # should not raise


def test_account_snapshot_is_copy():
    state = AccountState()
    state.update_balance("USDT", 100.0, 0.0)
    snap1 = state.snapshot()
    state.update_balance("USDT", 200.0, 0.0)
    assert snap1["balances"]["USDT"]["free"] == 100.0  # not mutated


def test_account_thread_safety():
    state = AccountState()
    errors = []

    def writer():
        for i in range(500):
            try:
                state.update_balance(f"TOKEN{i}", float(i), 0.0)
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=writer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"Thread safety errors: {errors}"


check("update_balance stores free and locked", test_account_balance_add)
check("update_balance overwrites previous value", test_account_balance_overwrite)
check("upsert_order / remove_order lifecycle", test_account_order_upsert_remove)
check("remove_order on nonexistent key is a no-op", test_account_remove_nonexistent_is_noop)
check("snapshot returns independent copy", test_account_snapshot_is_copy)
check("concurrent balance updates are thread-safe", test_account_thread_safety)


# ---------------------------------------------------------------------------
# 2d. Spot OrderRouter validation
# ---------------------------------------------------------------------------

section("2d. Spot OrderRouter validation")

def _spot_router(extra_trading=None, balance_usdt=10000.0, balance_btc=1.0):
    state = AccountState()
    state.update_balance("USDT", balance_usdt, 0.0)
    state.update_balance("BTC", balance_btc, 0.0)
    config = {
        "paper_trading": True,
        "max_order_size_usd": 1000.0,
        "max_order_size": 5.0,
        "max_open_orders": 3,
        "allowed_symbols": ["BTCUSDT", "ETHUSDT"],
    }
    if extra_trading:
        config.update(extra_trading)
    client = MexcClient("", "", "https://api.mexc.com")
    return OrderRouter(client, config, state), state


def test_spot_valid_buy():
    router, _ = _spot_router()
    order = OrderRequest("BTCUSDT", "BUY", "LIMIT", 0.001, price=50000.0)
    result = router.route_order(order)
    assert result["status"] == "PAPER_ACCEPTED"


def test_spot_valid_sell():
    router, _ = _spot_router()
    order = OrderRequest("BTCUSDT", "SELL", "LIMIT", 0.001, price=50000.0)
    result = router.route_order(order)
    assert result["status"] == "PAPER_ACCEPTED"


def test_spot_invalid_symbol_format():
    router, _ = _spot_router()
    order = OrderRequest("BTCGBP", "BUY", "LIMIT", 0.001, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_symbol_not_in_allowlist():
    router, _ = _spot_router()
    order = OrderRequest("SOLUSDT", "BUY", "LIMIT", 1.0, price=100.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_invalid_side():
    router, _ = _spot_router()
    order = OrderRequest("BTCUSDT", "LONG", "LIMIT", 0.001, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_quantity_exceeds_max():
    router, _ = _spot_router()
    order = OrderRequest("BTCUSDT", "BUY", "LIMIT", 10.0, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_notional_exceeds_max():
    router, _ = _spot_router()
    order = OrderRequest("BTCUSDT", "BUY", "LIMIT", 1.0, price=5000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_buy_insufficient_balance():
    router, _ = _spot_router(balance_usdt=10.0)
    order = OrderRequest("BTCUSDT", "BUY", "LIMIT", 0.001, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_sell_insufficient_balance():
    router, _ = _spot_router(balance_btc=0.0)
    order = OrderRequest("BTCUSDT", "SELL", "LIMIT", 0.5, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_spot_max_open_orders():
    router, state = _spot_router()
    state.upsert_order("1", {}); state.upsert_order("2", {}); state.upsert_order("3", {})
    order = OrderRequest("BTCUSDT", "BUY", "LIMIT", 0.001, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


check("valid BUY LIMIT order is PAPER_ACCEPTED", test_spot_valid_buy)
check("valid SELL LIMIT order is PAPER_ACCEPTED", test_spot_valid_sell)
check("unrecognized symbol format raises OrderValidationError", test_spot_invalid_symbol_format)
check("symbol not in allowlist raises OrderValidationError", test_spot_symbol_not_in_allowlist)
check("invalid side raises OrderValidationError", test_spot_invalid_side)
check("quantity > max_order_size raises OrderValidationError", test_spot_quantity_exceeds_max)
check("notional > max_order_size_usd raises OrderValidationError", test_spot_notional_exceeds_max)
check("BUY with insufficient USDT balance raises OrderValidationError", test_spot_buy_insufficient_balance)
check("SELL with insufficient BTC balance raises OrderValidationError", test_spot_sell_insufficient_balance)
check("too many open orders raises OrderValidationError", test_spot_max_open_orders)


# ---------------------------------------------------------------------------
# 2e. Futures OrderRouter validation
# ---------------------------------------------------------------------------

section("2e. Futures OrderRouter validation")

def _futures_router(balance_usdt=5000.0, extra_trading=None):
    state = AccountState()
    state.update_balance("USDT", balance_usdt, 0.0)
    config = {
        "paper_trading": True,
        "max_order_size_usd": 50000.0,
        "max_order_size": 5.0,
        "max_open_orders": 3,
        "allowed_symbols": ["BTC_USDT", "ETH_USDT"],
    }
    if extra_trading:
        config.update(extra_trading)
    client = MexcFuturesClient("", "", "https://contract.mexc.com")
    return FuturesOrderRouter(client, config, state), state


def test_futures_open_long_valid():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 0.01, price=50000.0, leverage=10)
    assert router.route_order(order)["status"] == "PAPER_ACCEPTED"


def test_futures_open_short_valid():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "OPEN_SHORT", "LIMIT", 0.01, price=50000.0, leverage=10)
    assert router.route_order(order)["status"] == "PAPER_ACCEPTED"


def test_futures_close_long_no_margin_check():
    router, _ = _futures_router(balance_usdt=0.0)
    order = FuturesOrderRequest("BTC_USDT", "CLOSE_LONG", "LIMIT", 0.01, price=50000.0, leverage=10)
    assert router.route_order(order)["status"] == "PAPER_ACCEPTED"


def test_futures_close_short_no_margin_check():
    router, _ = _futures_router(balance_usdt=0.0)
    order = FuturesOrderRequest("BTC_USDT", "CLOSE_SHORT", "LIMIT", 0.01, price=50000.0, leverage=10)
    assert router.route_order(order)["status"] == "PAPER_ACCEPTED"


def test_futures_insufficient_margin():
    router, _ = _futures_router(balance_usdt=100.0)
    # notional=5000, leverage=1 -> required margin=5000 > 100
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 0.1, price=50000.0, leverage=1)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_futures_margin_passes_at_boundary():
    router, _ = _futures_router(balance_usdt=500.0)
    # notional=5000, leverage=10 -> required margin=500 == available
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 0.1, price=50000.0, leverage=10)
    assert router.route_order(order)["status"] == "PAPER_ACCEPTED"


def test_futures_notional_exceeds_max():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 2.0, price=50000.0, leverage=10)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_futures_vol_exceeds_max_order_size():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 10.0, price=1.0, leverage=1)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_futures_invalid_side():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "BUY", "LIMIT", 0.01, price=50000.0)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_futures_symbol_not_in_allowlist():
    router, _ = _futures_router()
    order = FuturesOrderRequest("SOL_USDT", "OPEN_LONG", "LIMIT", 0.01, price=100.0, leverage=5)
    raises(OrderValidationError, lambda: router.route_order(order))


def test_futures_invalid_open_type():
    router, _ = _futures_router()
    order = FuturesOrderRequest("BTC_USDT", "OPEN_LONG", "LIMIT", 0.01, price=50000.0, leverage=10, open_type="NAKED")
    raises(OrderValidationError, lambda: router.route_order(order))


check("OPEN_LONG valid order is PAPER_ACCEPTED", test_futures_open_long_valid)
check("OPEN_SHORT valid order is PAPER_ACCEPTED", test_futures_open_short_valid)
check("CLOSE_LONG skips margin check even at zero balance", test_futures_close_long_no_margin_check)
check("CLOSE_SHORT skips margin check even at zero balance", test_futures_close_short_no_margin_check)
check("OPEN_LONG with insufficient margin raises OrderValidationError", test_futures_insufficient_margin)
check("OPEN_LONG passes when margin exactly meets requirement", test_futures_margin_passes_at_boundary)
check("notional > max_order_size_usd raises OrderValidationError", test_futures_notional_exceeds_max)
check("vol > max_order_size raises OrderValidationError", test_futures_vol_exceeds_max_order_size)
check("invalid side ('BUY') raises OrderValidationError", test_futures_invalid_side)
check("symbol not in allowlist raises OrderValidationError", test_futures_symbol_not_in_allowlist)
check("invalid open_type raises OrderValidationError", test_futures_invalid_open_type)


# ---------------------------------------------------------------------------
# 2f. AccountSync WS message parsing
# ---------------------------------------------------------------------------

section("2f. AccountSync WS message parsing (spot)")

def _spot_sync():
    state = AccountState()
    client = MexcClient("", "", "https://api.mexc.com")
    sync = AccountSync(client, "wss://wbs.mexc.com/ws", state, {})
    return sync, state


def test_spot_ws_account_update():
    sync, state = _spot_sync()
    msg = json.dumps({"channel": "spot@private.account.v3.api", "spotaccountupdate": {
        "balances": [{"asset": "BTC", "free": "1.5", "locked": "0.25"}]
    }})
    sync._on_message(None, msg)
    assert state.snapshot()["balances"]["BTC"] == {"free": 1.5, "locked": 0.25}


def test_spot_ws_order_new():
    sync, state = _spot_sync()
    msg = json.dumps({"channel": "spot@private.orders.v3.api", "spotprivateorder": {
        "orderId": "77", "symbol": "BTCUSDT", "status": "NEW", "side": "BUY"
    }})
    sync._on_message(None, msg)
    assert "77" in state.snapshot()["open_orders"]


def test_spot_ws_order_filled_removed():
    sync, state = _spot_sync()
    state.upsert_order("77", {"orderId": "77"})
    msg = json.dumps({"channel": "spot@private.orders.v3.api", "spotprivateorder": {
        "orderId": "77", "status": "FILLED"
    }})
    sync._on_message(None, msg)
    assert "77" not in state.snapshot()["open_orders"]


def test_spot_ws_order_canceled_removed():
    sync, state = _spot_sync()
    state.upsert_order("88", {"orderId": "88"})
    for status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
        state.upsert_order("88", {"orderId": "88"})
        msg = json.dumps({"channel": "spot@private.orders.v3.api", "spotprivateorder": {
            "orderId": "88", "status": status
        }})
        sync._on_message(None, msg)
        assert "88" not in state.snapshot()["open_orders"], f"Failed for status {status}"


def test_spot_ws_bad_json_ignored():
    sync, state = _spot_sync()
    sync._on_message(None, "not valid json")  # must not raise


def test_spot_ws_unknown_channel_ignored():
    sync, state = _spot_sync()
    sync._on_message(None, json.dumps({"channel": "spot@public.unknown"}))


check("spot WS account update -> balance stored", test_spot_ws_account_update)
check("spot WS order NEW -> added to open_orders", test_spot_ws_order_new)
check("spot WS order FILLED -> removed from open_orders", test_spot_ws_order_filled_removed)
check("spot WS FILLED/CANCELED/REJECTED/EXPIRED all remove order", test_spot_ws_order_canceled_removed)
check("spot WS non-JSON message is silently ignored", test_spot_ws_bad_json_ignored)
check("spot WS unknown channel is silently ignored", test_spot_ws_unknown_channel_ignored)


section("2g. FuturesAccountSync WS message parsing")

def _futures_sync():
    state = AccountState()
    client = MexcFuturesClient("ak", "sk", "https://contract.mexc.com")
    sync = FuturesAccountSync(client, "wss://contract.mexc.com/edge", state, {})
    return sync, state


def test_futures_ws_login_success():
    sync, _ = _futures_sync()
    sync._on_message(None, json.dumps({"channel": "rs.login", "data": "success"}))
    assert sync.is_connected()


def test_futures_ws_login_fail_not_connected():
    sync, _ = _futures_sync()
    sync._on_message(None, json.dumps({"channel": "rs.login", "data": "fail"}))
    assert not sync.is_connected()


def test_futures_ws_asset_push():
    sync, state = _futures_sync()
    msg = json.dumps({"channel": "push.personal.asset", "data": {
        "currency": "USDT", "availableBalance": 999.0, "frozenBalance": 1.0
    }})
    sync._on_message(None, msg)
    assert state.snapshot()["balances"]["USDT"] == {"free": 999.0, "locked": 1.0}


def test_futures_ws_asset_push_short_channel():
    sync, state = _futures_sync()
    msg = json.dumps({"channel": "push.asset", "data": {
        "currency": "BTC", "availableBalance": 2.0, "frozenBalance": 0.0
    }})
    sync._on_message(None, msg)
    assert state.snapshot()["balances"]["BTC"]["free"] == 2.0


def test_futures_ws_order_open():
    sync, state = _futures_sync()
    msg = json.dumps({"channel": "push.personal.order", "data": {
        "orderId": "555", "symbol": "BTC_USDT", "state": 2, "vol": 0.01
    }})
    sync._on_message(None, msg)
    assert "555" in state.snapshot()["open_orders"]


def test_futures_ws_order_terminal_removed():
    sync, state = _futures_sync()
    for terminal_state in (3, 4, 5):
        state.upsert_order("555", {"orderId": "555"})
        msg = json.dumps({"channel": "push.personal.order", "data": {
            "orderId": "555", "state": terminal_state
        }})
        sync._on_message(None, msg)
        assert "555" not in state.snapshot()["open_orders"], f"Failed for state={terminal_state}"


def test_futures_ws_position_stored():
    sync, state = _futures_sync()
    msg = json.dumps({"channel": "push.personal.position", "data": {
        "symbol": "BTC_USDT", "holdVol": 0.01, "positionId": 123
    }})
    sync._on_message(None, msg)
    assert "BTC_USDT" in state.snapshot()["positions"]


def test_futures_ws_bad_json_ignored():
    sync, _ = _futures_sync()
    sync._on_message(None, "not json")


check("futures WS rs.login success -> is_connected True", test_futures_ws_login_success)
check("futures WS rs.login fail -> is_connected False", test_futures_ws_login_fail_not_connected)
check("futures WS push.personal.asset -> balance stored", test_futures_ws_asset_push)
check("futures WS push.asset (short channel) -> balance stored", test_futures_ws_asset_push_short_channel)
check("futures WS push.personal.order state=2 -> added to open_orders", test_futures_ws_order_open)
check("futures WS terminal states 3/4/5 remove order", test_futures_ws_order_terminal_removed)
check("futures WS push.personal.position -> positions updated", test_futures_ws_position_stored)
check("futures WS non-JSON silently ignored", test_futures_ws_bad_json_ignored)


# ---------------------------------------------------------------------------
# 3. HTTP endpoint tests
# ---------------------------------------------------------------------------

section("3. HTTP endpoint tests")

def _make_http_server(port: int):
    state = AccountState()
    state.update_balance("USDT", 10000.0, 0.0)
    state.update_balance("BTC", 1.0, 0.0)

    client = MexcClient("", "", "https://api.mexc.com")
    spot_router = OrderRouter(client, {
        "paper_trading": True,
        "max_order_size_usd": 10000.0,
        "max_order_size": 5.0,
        "max_open_orders": 5,
        "allowed_symbols": ["BTCUSDT", "ETHUSDT"],
    }, state)
    spot_sync = AccountSync(client, "wss://wbs.mexc.com/ws", state, {})

    fstate = AccountState()
    fstate.update_balance("USDT", 5000.0, 0.0)
    fclient = MexcFuturesClient("", "", "https://contract.mexc.com")
    futures_router = FuturesOrderRouter(fclient, {
        "paper_trading": True,
        "max_order_size_usd": 100000.0,
        "max_order_size": 5.0,
        "max_open_orders": 5,
        "allowed_symbols": ["BTC_USDT"],
    }, fstate)
    futures_sync = FuturesAccountSync(fclient, "wss://contract.mexc.com/edge", fstate, {})

    server = BridgeHttpServer(
        spot_router, state, spot_sync,
        host="127.0.0.1", port=port,
        futures_router=futures_router,
        futures_state=fstate,
        futures_sync=futures_sync,
    )
    server.start()
    time.sleep(0.3)
    return server


BASE = "http://127.0.0.1:5100"
_http_server = _make_http_server(5100)


def test_http_health():
    r = _requests.get(f"{BASE}/health", timeout=5)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "running"
    assert "mexc_connected" in body
    assert "mexc_futures_connected" in body
    assert "uptime_seconds" in body


def test_http_status():
    r = _requests.get(f"{BASE}/status", timeout=5)
    assert r.status_code == 200
    body = r.json()
    assert "balance" in body
    assert "open_orders" in body
    assert "listen_key_valid" in body
    assert "futures" in body
    assert "balance" in body["futures"]
    assert "positions" in body["futures"]
    assert "open_orders" in body["futures"]


def test_http_post_order_valid():
    r = _requests.post(f"{BASE}/order", json={
        "symbol": "BTCUSDT", "side": "BUY", "price": "50000", "quantity": "0.001"
    }, timeout=5)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "orderId" in body
    assert body["status"] == "PENDING"
    assert "timestamp" in body


def test_http_post_order_insufficient_balance():
    r = _requests.post(f"{BASE}/order", json={
        "symbol": "BTCUSDT", "side": "BUY", "price": "50000", "quantity": "5"
    }, timeout=5)
    assert r.status_code == 400
    assert "error" in r.json()


def test_http_post_order_bad_symbol():
    r = _requests.post(f"{BASE}/order", json={
        "symbol": "BTCGBP", "side": "BUY", "price": "50000", "quantity": "0.001"
    }, timeout=5)
    assert r.status_code == 400


def test_http_post_order_missing_fields():
    r = _requests.post(f"{BASE}/order", json={"symbol": "BTCUSDT"}, timeout=5)
    assert r.status_code == 400


def test_http_futures_order_valid():
    r = _requests.post(f"{BASE}/futures/order", json={
        "symbol": "BTC_USDT", "side": "OPEN_LONG",
        "price": "50000", "quantity": "0.01", "leverage": 10
    }, timeout=5)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "orderId" in body
    assert body["status"] == "PENDING"


def test_http_futures_order_insufficient_margin():
    r = _requests.post(f"{BASE}/futures/order", json={
        "symbol": "BTC_USDT", "side": "OPEN_LONG",
        "price": "50000", "quantity": "0.5", "leverage": 1
    }, timeout=5)
    assert r.status_code == 400, r.text
    assert "error" in r.json()


def test_http_futures_order_invalid_side():
    r = _requests.post(f"{BASE}/futures/order", json={
        "symbol": "BTC_USDT", "side": "BUY",
        "price": "50000", "quantity": "0.01"
    }, timeout=5)
    assert r.status_code == 400


check("GET /health returns status/mexc_connected/mexc_futures_connected/uptime", test_http_health)
check("GET /status returns balance/open_orders/listen_key_valid + futures section", test_http_status)
check("POST /order valid spot buy returns {orderId, status:PENDING, timestamp}", test_http_post_order_valid)
check("POST /order with insufficient balance returns 400", test_http_post_order_insufficient_balance)
check("POST /order with unrecognized symbol returns 400", test_http_post_order_bad_symbol)
check("POST /order with missing required fields returns 400", test_http_post_order_missing_fields)
check("POST /futures/order valid OPEN_LONG returns {orderId, status:PENDING}", test_http_futures_order_valid)
check("POST /futures/order insufficient margin returns 400", test_http_futures_order_insufficient_margin)
check("POST /futures/order invalid side returns 400", test_http_futures_order_invalid_side)

_http_server.stop()


# ---------------------------------------------------------------------------
# 4. Config validation
# ---------------------------------------------------------------------------

section("4. Config validation")

def test_config_valid():
    import json
    with open("config/config.json") as f:
        cfg = json.load(f)
    required = [
        ("mexc", "api_key"),
        ("mexc", "api_secret"),
        ("trading", "paper_trading"),
        ("futures", "enabled"),
    ]
    for section_key, field in required:
        assert cfg.get(section_key, {}).get(field) is not None, \
            f"config.json missing {section_key}.{field}"


def test_config_example_has_placeholders():
    import json
    with open("config/example_config.json") as f:
        cfg = json.load(f)
    assert "YOUR_MEXC_API_KEY_HERE" in cfg["mexc"]["api_key"]
    assert "YOUR_MEXC_API_SECRET_HERE" in cfg["mexc"]["api_secret"]


def test_config_example_futures_disabled_by_default():
    import json
    with open("config/example_config.json") as f:
        cfg = json.load(f)
    assert cfg["futures"]["enabled"] is False


def test_setup_sh_detects_placeholders():
    """Verify setup.sh's placeholder-detection logic by running it against a config
    that contains placeholder values, using a temp copy so the real config is untouched."""
    import subprocess, shutil, os, tempfile

    # Write a config file that contains placeholder values.
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False,
        dir="config", prefix="test_placeholder_"
    )
    shutil.copyfileobj(open("config/example_config.json"), tmp)
    tmp.close()
    placeholder_path = tmp.name

    try:
        # Run the same grep that setup.sh uses, against the placeholder file.
        result = subprocess.run(
            ["bash", "-c",
             f'grep -q "YOUR_MEXC_API_KEY_HERE\\|YOUR_MEXC_API_SECRET_HERE" '
             f'"{placeholder_path}" && echo "PLACEHOLDER_DETECTED" || echo "NOT_DETECTED"'],
            capture_output=True, text=True,
        )
        assert "PLACEHOLDER_DETECTED" in result.stdout, \
            f"Placeholder detection failed. Got: {result.stdout!r}"
    finally:
        os.unlink(placeholder_path)


check("config/config.json has all required fields", test_config_valid)
check("config/example_config.json has placeholder credentials", test_config_example_has_placeholders)
check("config/example_config.json has futures.enabled=false by default", test_config_example_futures_disabled_by_default)
check("setup.sh placeholder-detection grep correctly identifies placeholder config", test_setup_sh_detects_placeholders)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

total = PASS + FAIL
print(f"\n{'=' * 60}")
print(f"  RESULTS: {PASS}/{total} passed, {FAIL} failed")
print(f"{'=' * 60}")

if FAIL > 0:
    sys.exit(1)
