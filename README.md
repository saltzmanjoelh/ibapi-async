# ibapi-async

`asyncio` wrapper for the [Interactive Brokers Python API](https://github.com/saltzmanjoelh/ibapi).

Provides `AsyncTWSClient` — a drop-in async replacement for `ibapi`'s `TWSSyncWrapper` — using standard Python `asyncio` (async/await). The original `ibapi` package is a dependency and is **never modified**, so it continues to auto-update independently.

## Architecture

```
User code (await)
    ↓
AsyncTWSClient.get_historical_data()
    ↓ calls (sync)
EClient.reqHistoricalData()        ← inherited unchanged from ibapi
    ↓ calls (sync)
AsyncEClient.sendMsg()             ← enqueues to asyncio.Queue
    ↓
_write_loop task                   ← drains queue → StreamWriter
    ↑
asyncio.StreamWriter (TCP)
    ↑ asyncio.StreamReader
_read_loop task                    ← reads → Decoder → EWrapper callbacks
    ↓
AsyncTWSClient.historicalDataEnd() ← sets asyncio.Event
    ↓
User code (receives result)
```

Only 4 methods of `EClient` are overridden: `connect`, `disconnect`, `sendMsg`, `sendMsgProtoBuf`. All 200+ request methods are inherited unchanged.

## Installation

```toml
# In your project's pyproject.toml
[tool.poetry.dependencies]
ibapi-async = { git = "https://github.com/saltzmanjoelh/ibapi-async.git", branch = "main" }
```

## Usage

```python
import asyncio
from ibapi.contract import Contract
from ibapi_async import AsyncTWSClient

async def main():
    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    async with await AsyncTWSClient.create("127.0.0.1", 4002, client_id=0) as client:
        # Get contract details
        details = await client.get_contract_details(contract)
        print(f"Found {len(details)} contracts")

        # Get historical data
        bars = await client.get_historical_data(
            contract=contract,
            end_date_time="",
            duration_str="5 D",
            bar_size_setting="1 hour",
            what_to_show="TRADES",
        )
        print(f"Got {len(bars)} bars")

        # Place an order
        from ibapi.order import Order
        order = Order()
        order.action = "BUY"
        order.orderType = "MKT"
        order.totalQuantity = 1
        status = await client.place_order(contract, order)
        print(f"Order status: {status['status']}")

asyncio.run(main())
```

## Available Methods

| Method | Replaces |
|---|---|
| `get_contract_details(contract)` | `TWSSyncWrapper.get_contract_details()` |
| `get_historical_data(contract, ...)` | `TWSSyncWrapper.get_historical_data()` |
| `get_market_data_snapshot(contract)` | `TWSSyncWrapper.get_market_data_snapshot()` |
| `place_order(contract, order)` | `TWSSyncWrapper.place_order_sync()` |
| `cancel_order(order_id)` | `TWSSyncWrapper.cancel_order_sync()` |
| `get_open_orders()` | `TWSSyncWrapper.get_open_orders()` |
| `get_executions(exec_filter)` | `TWSSyncWrapper.get_executions()` |
| `get_positions()` | `TWSSyncWrapper.get_positions()` |
| `get_portfolio(account_code)` | `TWSSyncWrapper.get_portfolio()` |
| `get_account_summary(tags)` | `TWSSyncWrapper.get_account_summary()` |
| `get_current_time()` | `TWSSyncWrapper.get_current_time()` |
| `get_next_valid_id()` | `TWSSyncWrapper.get_next_valid_id()` |

All `EClient` request methods (`reqMktData`, `placeOrder`, `reqHistoricalData`, etc.) are also available directly as synchronous methods — they enqueue the request, and the response arrives via the corresponding `await get_*` method.

## Relationship to ibkr_core

This package is designed to be the foundation layer for `ibkr_core`. Migration path:

```python
# Before (ibkr_core IBKRClient)
class IBKRClient(EWrapper, EClient): ...

# After
from ibapi_async import AsyncTWSClient
class IBKRClient(AsyncTWSClient): ...
```

## Development

```bash
poetry install
poetry run pytest               # unit tests (default)
```

### Integration tests

`tests/integration/` contains tests that exercise the async stack end-to-end against a **live IB Gateway** (or TWS) running under a paper-trading account. These tests are **opt-in** — they're excluded from the default `pytest` run (and from CI) via the `integration` pytest marker.

To run them:

```bash
# Start IB Gateway with a paper-trading account (port 4002 by default), then:
poetry run pytest -m integration tests/integration/ -v
```

The fixture resolves the Gateway endpoint in this order:

1. **Existing Gateway** at `IBKR_GATEWAY_HOST:IBKR_GATEWAY_PORT` (your local Gateway, TWS, or a `docker-compose` stack).
2. **Docker autostart fallback** — pulls `ghcr.io/saltzmanjoelh/ibgateway:main` and runs it as a container named `ibgateway-test`, mapping the paper port. Requires `IBGATEWAY_USERNAME` and `IBGATEWAY_PASSWORD` in the environment, and `docker` on `PATH`.
3. **Skip with a clear message** if neither is available.

The autostarted container is left running between sessions so the ~5 min Gateway login cost is paid only once. Tear down with `docker rm -f ibgateway-test`.

Override defaults with environment variables:

| Variable | Default | Notes |
|---|---|---|
| `IBKR_GATEWAY_HOST` | `127.0.0.1` | Gateway host |
| `IBKR_GATEWAY_PORT` | `4002` | Paper port. Live Gateway = 4001, paper TWS = 7497, live TWS = 7496 |
| `IBKR_GATEWAY_CLIENT_ID` | `99` | Pick a high number to avoid colliding with TWS itself or other API clients |
| `IBKR_GATEWAY_DOCKER_IMAGE` | `ghcr.io/saltzmanjoelh/ibgateway:main` | Image used by the autostart fallback |
| `IBKR_GATEWAY_DOCKER_AUTOSTART` | `1` | Set to `0` to disable autostart (skip instead) |
| `IBKR_GATEWAY_DOCKER_STARTUP_TIMEOUT` | `600` | Seconds to wait for the container's paper port to come up |
| `IBGATEWAY_USERNAME` / `IBGATEWAY_PASSWORD` | _(unset)_ | Required for the autostart path |

## ibapi Auto-Update

This repo tracks `ibapi`'s `stable` branch. When IBKR releases a new version:

1. `ibapi`'s GitHub Action auto-updates its `stable` branch.
2. This repo's `check-ibapi-updates` workflow runs daily, detects the change, runs tests, and opens a PR bumping `poetry.lock`.
3. Review and merge — no code changes needed.
