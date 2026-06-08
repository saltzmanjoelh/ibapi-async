"""Coverage tests for AsyncTWSClient EWrapper callbacks, high-level methods, and streams.

Uses mocked request methods + synthetic callbacks (no live gateway).
"""

import asyncio
from decimal import Decimal

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.order import Order

from ibapi_async.tws_client import AsyncTWSClient
from ibapi_async.exceptions import ResponseTimeout, TWSError


def connected() -> AsyncTWSClient:
    client = AsyncTWSClient(timeout=5.0)
    client.connState = EClient.CONNECTED
    mc = MagicMock()
    mc.isConnected.return_value = True
    client.conn = mc
    client.serverVersion_ = 176
    client.next_valid_id_value = 1
    client._write_queue = asyncio.Queue()
    return client


def _fire_later(client, req_id, event_name, value, delay=0.02):
    """Set a response event after a short delay (lets _wait_for_response register)."""
    async def _sim():
        await asyncio.sleep(delay)
        client._set_event(req_id, event_name, value)
    return asyncio.create_task(_sim())


# ==================================================================
# EWrapper callbacks — store + signal
# ==================================================================

def test_cb_current_time():
    client = AsyncTWSClient()
    client.currentTime(1700000000)
    assert client.current_time_value == 1700000000


def test_cb_open_orders():
    client = AsyncTWSClient()
    client._response_events["open_orders_0"] = asyncio.Event()
    order = MagicMock()
    client.openOrder(1, Contract(), order, MagicMock())
    assert client.open_orders[1]["order"] is order
    client.openOrderEnd()
    assert client._response_events["open_orders_0"].is_set()


def test_cb_exec_details():
    client = AsyncTWSClient()
    client._response_events["executions_2"] = asyncio.Event()
    client.execDetails(2, Contract(), MagicMock())
    client.execDetails(2, Contract(), MagicMock())
    assert len(client.executions[2]) == 2
    client.execDetailsEnd(2)
    assert client._response_events["executions_2"].is_set()


def test_cb_portfolio():
    client = AsyncTWSClient()
    client._response_events["portfolio_0"] = asyncio.Event()
    client.updatePortfolio(Contract(), Decimal(10), 1.0, 10.0, 1.0, 0.5, 0.2, "DU1")
    assert client.portfolio[0]["accountName"] == "DU1"
    client.accountDownloadEnd("DU1")
    assert client._response_events["portfolio_0"].is_set()


def test_cb_positions():
    client = AsyncTWSClient()
    client._response_events["positions_0"] = asyncio.Event()
    client.position("DU1", Contract(), Decimal(5), 1.5)
    assert client.positions["DU1"][0]["position"] == Decimal(5)
    client.positionEnd()
    assert client._response_events["positions_0"].is_set()


def test_cb_account_summary():
    client = AsyncTWSClient()
    client._response_events["account_summary_3"] = asyncio.Event()
    client.accountSummary(3, "DU1", "NetLiquidation", "1000", "USD")
    assert client.account_summary[3]["DU1"]["NetLiquidation"]["value"] == "1000"
    client.accountSummaryEnd(3)
    assert client._response_events["account_summary_3"].is_set()


def test_cb_ticks():
    # Distinct reqIds so each callback exercises its own market_data init branch.
    client = AsyncTWSClient()
    client.tickPrice(1, 4, 100.5, MagicMock())
    client.tickSize(2, 0, Decimal(7))
    client.tickString(3, 45, "ts")
    client.tickGeneric(4, 23, 1.25)
    assert client.market_data[1] and client.market_data[2]
    assert client.market_data[3] and client.market_data[4]
    client._response_events["market_data_1"] = asyncio.Event()
    client.tickSnapshotEnd(1)
    assert client._response_events["market_data_1"].is_set()


def test_cb_managed_accounts():
    client = AsyncTWSClient()
    client._response_events["managed_accounts_0"] = asyncio.Event()
    client.managedAccounts("DU1,DU2")
    assert client.managed_accounts_value == "DU1,DU2"
    assert client._response_events["managed_accounts_0"].is_set()


def test_cb_symbol_samples():
    client = AsyncTWSClient()
    client._response_events["matching_symbols_4"] = asyncio.Event()
    client.symbolSamples(4, [MagicMock(), MagicMock()])
    assert len(client.matching_symbols[4]) == 2
    assert client._response_events["matching_symbols_4"].is_set()


def test_cb_head_timestamp():
    client = AsyncTWSClient()
    client._response_events["head_timestamp_5"] = asyncio.Event()
    client.headTimestamp(5, "20200101 00:00:00")
    assert client.head_timestamps[5] == "20200101 00:00:00"
    assert client._response_events["head_timestamp_5"].is_set()


def test_cb_option_params():
    client = AsyncTWSClient()
    client._response_events["option_chain_6"] = asyncio.Event()
    client.securityDefinitionOptionParameter(
        6, "SMART", 111, "AAPL", "100", {"20260101", "20260201"}, {100.0, 200.0}
    )
    assert client.option_params[6][0]["exchange"] == "SMART"
    assert client.option_params[6][0]["strikes"] == [100.0, 200.0]
    client.securityDefinitionOptionParameterEnd(6)
    assert client._response_events["option_chain_6"].is_set()


def test_cb_news_providers():
    client = AsyncTWSClient()
    client._response_events["news_providers_0"] = asyncio.Event()
    client.newsProviders([MagicMock()])
    assert len(client.news_providers_value) == 1
    assert client._response_events["news_providers_0"].is_set()


def test_cb_scanner_parameters():
    client = AsyncTWSClient()
    client._response_events["scanner_parameters_0"] = asyncio.Event()
    client.scannerParameters("<xml/>")
    assert client.scanner_xml == "<xml/>"
    assert client._response_events["scanner_parameters_0"].is_set()


def test_cb_histogram():
    client = AsyncTWSClient()
    client._response_events["histogram_7"] = asyncio.Event()
    client.histogramData(7, [MagicMock(), MagicMock()])
    assert len(client.histogram_results[7]) == 2
    assert client._response_events["histogram_7"].is_set()


def test_cb_historical_ticks_variants():
    client = AsyncTWSClient()
    for rid, method in (
        (10, "historicalTicks"),
        (11, "historicalTicksBidAsk"),
        (12, "historicalTicksLast"),
    ):
        client._response_events[f"historical_ticks_{rid}"] = asyncio.Event()
        getattr(client, method)(rid, [MagicMock()], True)
        assert client._response_events[f"historical_ticks_{rid}"].is_set()
        assert len(client.historical_ticks[rid]) == 1


def test_cb_historical_ticks_not_done_does_not_signal():
    client = AsyncTWSClient()
    client._response_events["historical_ticks_13"] = asyncio.Event()
    client.historicalTicks(13, [MagicMock()], False)
    assert not client._response_events["historical_ticks_13"].is_set()


def test_cb_pnl():
    client = AsyncTWSClient()
    client._response_events["pnl_8"] = asyncio.Event()
    client.pnl(8, 1.0, 2.0, 3.0)
    assert client._response_events["pnl_8"].is_set()


def test_cb_fundamental_data():
    client = AsyncTWSClient()
    client._response_events["fundamental_data_9"] = asyncio.Event()
    client.fundamentalData(9, "<report/>")
    assert client._response_events["fundamental_data_9"].is_set()


def test_cb_completed_orders():
    client = AsyncTWSClient()
    client._response_events["completed_orders_0"] = asyncio.Event()
    client.completedOrder(Contract(), MagicMock(), MagicMock())
    assert len(client.completed_orders) == 1
    client.completedOrdersEnd()
    assert client._response_events["completed_orders_0"].is_set()


def test_cb_realtime_bar_to_queue():
    client = AsyncTWSClient()
    q: asyncio.Queue = asyncio.Queue()
    client._stream_queues[1] = q
    client.realtimeBar(1, 100, 1.0, 2.0, 0.5, 1.5, Decimal(10), Decimal(11), 3)
    bar = q.get_nowait()
    assert bar["close"] == 1.5 and bar["count"] == 3


def test_cb_tick_by_tick_variants_to_queue():
    client = AsyncTWSClient()
    q: asyncio.Queue = asyncio.Queue()
    client._stream_queues[1] = q
    client.tickByTickAllLast(1, 1, 100, 1.5, Decimal(2), MagicMock(), "NASDAQ", "")
    client.tickByTickAllLast(1, 2, 100, 1.5, Decimal(2), MagicMock(), "NASDAQ", "")
    client.tickByTickBidAsk(1, 100, 1.0, 1.1, Decimal(2), Decimal(3), MagicMock())
    client.tickByTickMidPoint(1, 100, 1.05)
    assert q.get_nowait()["kind"] == "AllLast"
    assert q.get_nowait()["kind"] == "Last"
    assert q.get_nowait()["kind"] == "BidAsk"
    assert q.get_nowait()["kind"] == "MidPoint"


def test_cb_update_mkt_depth_insert_and_delete():
    client = AsyncTWSClient()
    q: asyncio.Queue = asyncio.Queue()
    client._stream_queues[1] = q
    client.updateMktDepth(1, 0, 0, 1, 9.5, Decimal(5))  # insert bid
    assert client.market_depth[1][1][0]["price"] == 9.5
    assert q.get_nowait()["operation"] == 0
    client.updateMktDepth(1, 0, 2, 1, 9.5, Decimal(5))  # delete
    assert 0 not in client.market_depth[1][1]


def test_cb_update_mkt_depth_l2_insert_and_delete():
    client = AsyncTWSClient()
    q: asyncio.Queue = asyncio.Queue()
    client._stream_queues[1] = q
    client.updateMktDepthL2(1, 0, "MM", 0, 0, 10.0, Decimal(5), False)  # insert ask
    assert client.market_depth[1][0][0]["marketMaker"] == "MM"
    assert q.get_nowait()["marketMaker"] == "MM"
    client.updateMktDepthL2(1, 0, "MM", 2, 0, 10.0, Decimal(5), False)  # delete
    assert 0 not in client.market_depth[1][0]


def test_error_info_code_does_not_signal():
    client = AsyncTWSClient()
    client._response_events["error_1"] = asyncio.Event()
    client.error(1, 0, 2104, "data farm OK", "")
    assert client.errors[1][0]["errorCode"] == 2104
    assert not client._response_events["error_1"].is_set()


# ==================================================================
# Correlation helpers
# ==================================================================

async def test_wait_for_response_raises_twserror_on_error_event():
    client = connected()

    async def sim():
        await asyncio.sleep(0.02)
        client.error(7, 0, 200, "bad thing", "")

    asyncio.create_task(sim())
    with pytest.raises(TWSError) as exc:
        await client._wait_for_response(7, "contract_details", timeout=2.0)
    assert exc.value.code == 200


def test_safe_cancel_swallows_exception():
    client = AsyncTWSClient()
    client._safe_cancel(MagicMock(side_effect=RuntimeError("boom")), 1)  # no raise


async def test_get_next_valid_id_waits_for_handshake():
    client = AsyncTWSClient()  # next_valid_id_value is None → must wait

    async def sim():
        await asyncio.sleep(0.02)
        client.nextValidId(5)

    asyncio.create_task(sim())
    oid = await client.get_next_valid_id(timeout=2.0)
    assert oid == 5
    assert client.next_valid_id_value == 6


# ==================================================================
# High-level async methods
# ==================================================================

async def test_get_current_time():
    client = connected()
    with patch.object(client, "reqCurrentTime", return_value=None) as req:
        _fire_later(client, 0, "current_time", 1700000000)
        result = await client.get_current_time(timeout=2.0)
    assert result == 1700000000
    req.assert_called_once()


async def test_place_order_lmt_default_timeout():
    client = connected()
    order = Order()
    order.orderType = "LMT"
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=50)), \
         patch.object(client, "placeOrder", return_value=None) as po:
        _fire_later(client, 50, "order_status", {"status": "Submitted"})
        result = await client.place_order(Contract(), order)
    assert result["status"] == "Submitted"
    assert order.orderId == 50
    po.assert_called_once()


async def test_place_order_non_lmt_default_timeout():
    client = connected()
    order = Order()
    order.orderType = "STP"
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=51)), \
         patch.object(client, "placeOrder", return_value=None):
        _fire_later(client, 51, "order_status", {"status": "PreSubmitted"})
        result = await client.place_order(Contract(), order)
    assert result["status"] == "PreSubmitted"


async def test_cancel_order():
    client = connected()
    with patch.object(client, "cancelOrder", return_value=None) as co:
        _fire_later(client, 52, "order_status", {"status": "Cancelled"})
        result = await client.cancel_order(52)
    assert result["status"] == "Cancelled"
    co.assert_called_once()


async def test_get_open_orders():
    client = connected()
    with patch.object(client, "reqOpenOrders", return_value=None):
        _fire_later(client, 0, "open_orders", {1: {"status": "Submitted"}})
        result = await client.get_open_orders(timeout=2.0)
    assert result == {1: {"status": "Submitted"}}


async def test_get_executions_happy():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=40)), \
         patch.object(client, "reqExecutions", return_value=None):
        _fire_later(client, 40, "executions", [{"x": 1}])
        result = await client.get_executions(timeout=2.0)
    assert result == [{"x": 1}]


async def test_get_executions_timeout_returns_accumulated():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=41)), \
         patch.object(client, "reqExecutions", return_value=None):
        result = await client.get_executions(timeout=0.1)
    assert result == []


async def test_get_portfolio():
    client = connected()
    with patch.object(client, "reqAccountUpdates", return_value=None) as req:
        _fire_later(client, 0, "portfolio", [{"acct": "DU1"}])
        result = await client.get_portfolio(timeout=2.0)
    assert result == [{"acct": "DU1"}]
    assert req.call_count == 2  # subscribe + unsubscribe


async def test_get_positions():
    client = connected()
    with patch.object(client, "reqPositions", return_value=None), \
         patch.object(client, "cancelPositions", return_value=None) as cancel:
        _fire_later(client, 0, "positions", {"DU1": []})
        result = await client.get_positions(timeout=2.0)
    assert result == {"DU1": []}
    cancel.assert_called_once()


async def test_get_account_summary():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=42)), \
         patch.object(client, "reqAccountSummary", return_value=None), \
         patch.object(client, "cancelAccountSummary", return_value=None) as cancel:
        _fire_later(client, 42, "account_summary", {"DU1": {}})
        result = await client.get_account_summary(timeout=2.0)
    assert result == {"DU1": {}}
    cancel.assert_called_once()


async def test_get_market_data_snapshot():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=43)), \
         patch.object(client, "reqMktData", return_value=None):
        _fire_later(client, 43, "market_data", {"LAST": 100.0})
        result = await client.get_market_data_snapshot(Contract(), timeout=2.0)
    assert result == {"LAST": 100.0}


async def test_get_managed_accounts_cached():
    client = connected()
    client.managed_accounts_value = "DU1,DU2,"
    result = await client.get_managed_accounts()
    assert result == ["DU1", "DU2"]


async def test_get_managed_accounts_fresh():
    client = connected()
    client.managed_accounts_value = None
    with patch.object(client, "reqManagedAccts", return_value=None):
        _fire_later(client, 0, "managed_accounts", "DU9,DU8")
        result = await client.get_managed_accounts(timeout=2.0)
    assert result == ["DU9", "DU8"]


async def test_get_head_timestamp_happy():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=60)), \
         patch.object(client, "reqHeadTimeStamp", return_value=None), \
         patch.object(client, "cancelHeadTimeStamp", return_value=None) as cancel:
        _fire_later(client, 60, "head_timestamp", "20200101 00:00:00")
        ts = await client.get_head_timestamp(Contract(), timeout=2.0)
    assert ts == "20200101 00:00:00"
    cancel.assert_called_once()


async def test_get_head_timestamp_twserror_does_not_cancel():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=61)), \
         patch.object(client, "reqHeadTimeStamp", return_value=None), \
         patch.object(client, "cancelHeadTimeStamp") as cancel:
        async def sim():
            await asyncio.sleep(0.02)
            client.error(61, 0, 200, "bad", "")
        asyncio.create_task(sim())
        with pytest.raises(TWSError):
            await client.get_head_timestamp(Contract(), timeout=2.0)
    cancel.assert_not_called()


async def test_search_symbols():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=62)), \
         patch.object(client, "reqMatchingSymbols", return_value=None):
        _fire_later(client, 62, "matching_symbols", [MagicMock()])
        result = await client.search_symbols("AAPL", timeout=2.0)
    assert len(result) == 1


async def test_get_option_chain():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=63)), \
         patch.object(client, "reqSecDefOptParams", return_value=None):
        _fire_later(client, 63, "option_chain", [{"exchange": "SMART"}])
        result = await client.get_option_chain("AAPL", "STK", 111, timeout=2.0)
    assert result == [{"exchange": "SMART"}]


async def test_get_news_providers():
    client = connected()
    with patch.object(client, "reqNewsProviders", return_value=None):
        _fire_later(client, 0, "news_providers", [MagicMock()])
        result = await client.get_news_providers(timeout=2.0)
    assert len(result) == 1


async def test_get_scanner_parameters():
    client = connected()
    with patch.object(client, "reqScannerParameters", return_value=None):
        _fire_later(client, 0, "scanner_parameters", "<xml/>")
        result = await client.get_scanner_parameters(timeout=2.0)
    assert result == "<xml/>"


async def test_get_histogram_data_happy():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=64)), \
         patch.object(client, "reqHistogramData", return_value=None), \
         patch.object(client, "cancelHistogramData", return_value=None) as cancel:
        _fire_later(client, 64, "histogram", [MagicMock()])
        result = await client.get_histogram_data(Contract(), timeout=2.0)
    assert len(result) == 1
    cancel.assert_called_once()


async def test_get_histogram_data_twserror():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=65)), \
         patch.object(client, "reqHistogramData", return_value=None), \
         patch.object(client, "cancelHistogramData") as cancel:
        async def sim():
            await asyncio.sleep(0.02)
            client.error(65, 0, 200, "bad", "")
        asyncio.create_task(sim())
        with pytest.raises(TWSError):
            await client.get_histogram_data(Contract(), timeout=2.0)
    cancel.assert_not_called()


async def test_get_historical_ticks():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=66)), \
         patch.object(client, "reqHistoricalTicks", return_value=None):
        _fire_later(client, 66, "historical_ticks", [1, 2, 3])
        result = await client.get_historical_ticks(Contract(), timeout=2.0)
    assert result == [1, 2, 3]


async def test_get_pnl_happy():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=67)), \
         patch.object(client, "reqPnL", return_value=None), \
         patch.object(client, "cancelPnL", return_value=None) as cancel:
        _fire_later(client, 67, "pnl", {"dailyPnL": 1.0})
        result = await client.get_pnl("DU1", timeout=2.0)
    assert result == {"dailyPnL": 1.0}
    cancel.assert_called_once()


async def test_get_pnl_twserror():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=68)), \
         patch.object(client, "reqPnL", return_value=None), \
         patch.object(client, "cancelPnL") as cancel:
        async def sim():
            await asyncio.sleep(0.02)
            client.error(68, 0, 200, "bad", "")
        asyncio.create_task(sim())
        with pytest.raises(TWSError):
            await client.get_pnl("DU1", timeout=2.0)
    cancel.assert_not_called()


async def test_get_fundamental_data_happy():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=69)), \
         patch.object(client, "reqFundamentalData", return_value=None), \
         patch.object(client, "cancelFundamentalData", return_value=None) as cancel:
        _fire_later(client, 69, "fundamental_data", "<report/>")
        result = await client.get_fundamental_data(Contract(), timeout=2.0)
    assert result == "<report/>"
    cancel.assert_called_once()


async def test_get_fundamental_data_twserror():
    client = connected()
    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=70)), \
         patch.object(client, "reqFundamentalData", return_value=None), \
         patch.object(client, "cancelFundamentalData") as cancel:
        async def sim():
            await asyncio.sleep(0.02)
            client.error(70, 0, 10358, "no subscription", "")
        asyncio.create_task(sim())
        with pytest.raises(TWSError):
            await client.get_fundamental_data(Contract(), timeout=2.0)
    cancel.assert_not_called()


async def test_get_completed_orders():
    client = connected()
    with patch.object(client, "reqCompletedOrders", return_value=None):
        _fire_later(client, 0, "completed_orders", [{"id": 1}])
        result = await client.get_completed_orders(timeout=2.0)
    assert result == [{"id": 1}]


def test_cancel_all_orders():
    client = connected()
    with patch.object(client, "reqGlobalCancel", return_value=None) as req:
        client.cancel_all_orders()
    req.assert_called_once()


def test_request_market_data_type():
    client = connected()
    with patch.object(client, "reqMarketDataType", return_value=None) as req:
        client.request_market_data_type(3)
    req.assert_called_once_with(3)


# ==================================================================
# Streaming generators
# ==================================================================

async def test_stream_real_time_bars():
    client = connected()
    req_id = 80

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.realtimeBar(req_id, 1, 1.0, 2.0, 0.5, 1.5, Decimal(10), Decimal(11), 3)
        client.realtimeBar(req_id, 2, 1.0, 2.0, 0.5, 1.5, Decimal(10), Decimal(11), 3)

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqRealTimeBars", return_value=None), \
         patch.object(client, "cancelRealTimeBars") as cancel:
        f = asyncio.create_task(feeder())
        gen = client.stream_real_time_bars(Contract())
        received = []
        async for bar in gen:
            received.append(bar)
            if len(received) >= 2:
                break
        await gen.aclose()
        await f

    assert len(received) == 2
    cancel.assert_called_once_with(req_id)
    assert req_id not in client._stream_queues


async def test_stream_real_time_bars_raises_twserror():
    client = connected()
    req_id = 81

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.error(req_id, 0, 200, "permission denied", "")

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqRealTimeBars", return_value=None), \
         patch.object(client, "cancelRealTimeBars") as cancel:
        f = asyncio.create_task(feeder())
        with pytest.raises(TWSError):
            async for _bar in client.stream_real_time_bars(Contract()):
                pass
        await f

    cancel.assert_not_called()  # server already terminated
    assert req_id not in client._stream_queues


async def test_stream_tick_by_tick():
    client = connected()
    req_id = 82

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.tickByTickAllLast(req_id, 1, 100, 1.5, Decimal(2), MagicMock(), "NASDAQ", "")

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqTickByTickData", return_value=None), \
         patch.object(client, "cancelTickByTickData") as cancel:
        f = asyncio.create_task(feeder())
        gen = client.stream_tick_by_tick(Contract(), tick_type="AllLast")
        received = []
        async for tick in gen:
            received.append(tick)
            break
        await gen.aclose()
        await f

    assert received and received[0]["kind"] == "AllLast"
    cancel.assert_called_once()
    assert req_id not in client._stream_queues


async def test_get_market_depth():
    client = connected()
    req_id = 83

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.updateMktDepth(req_id, 0, 0, 0, 10.0, Decimal(5))  # ask
        client.updateMktDepth(req_id, 0, 0, 1, 9.5, Decimal(7))   # bid

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqMktDepth", return_value=None), \
         patch.object(client, "cancelMktDepth") as cancel:
        f = asyncio.create_task(feeder())
        book = await client.get_market_depth(Contract(), settle_time=0.05, timeout=2.0)
        await f

    assert book["asks"][0]["price"] == 10.0
    assert book["bids"][0]["price"] == 9.5
    cancel.assert_called_once()
    assert req_id not in client._stream_queues


async def test_get_market_depth_twserror():
    client = connected()
    req_id = 84

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.error(req_id, 0, 200, "no permission", "")

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqMktDepth", return_value=None), \
         patch.object(client, "cancelMktDepth") as cancel:
        f = asyncio.create_task(feeder())
        with pytest.raises(TWSError):
            await client.get_market_depth(Contract(), settle_time=0.05, timeout=2.0)
        await f

    cancel.assert_not_called()
    assert req_id not in client._stream_queues


async def test_get_market_depth_twserror_during_settle():
    """An error arriving after the first update (during the settle window) raises."""
    client = connected()
    req_id = 87

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.updateMktDepth(req_id, 0, 0, 0, 10.0, Decimal(5))
        await asyncio.sleep(0.01)
        client.error(req_id, 0, 200, "denied mid-settle", "")

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqMktDepth", return_value=None), \
         patch.object(client, "cancelMktDepth") as cancel:
        f = asyncio.create_task(feeder())
        with pytest.raises(TWSError):
            await client.get_market_depth(Contract(), settle_time=2.0, timeout=2.0)
        await f

    cancel.assert_not_called()
    assert req_id not in client._stream_queues


# ==================================================================
# Misc gaps: default timeout, tick-by-tick error, unexpected-exception propagation
# ==================================================================

async def test_wait_for_response_uses_default_timeout():
    """timeout=None falls back to self.timeout."""
    client = connected()  # self.timeout == 5.0

    async def sim():
        await asyncio.sleep(0.02)
        client._set_event(1, "thing", "value")

    asyncio.create_task(sim())
    result = await client._wait_for_response(1, "thing")  # no explicit timeout
    assert result == "value"


async def test_stream_tick_by_tick_raises_twserror():
    client = connected()
    req_id = 85

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client.error(req_id, 0, 200, "denied", "")

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqTickByTickData", return_value=None), \
         patch.object(client, "cancelTickByTickData") as cancel:
        f = asyncio.create_task(feeder())
        with pytest.raises(TWSError):
            async for _ in client.stream_tick_by_tick(Contract()):
                pass
        await f

    cancel.assert_not_called()
    assert req_id not in client._stream_queues


async def test_stream_historical_data_propagates_unexpected_exception():
    client = connected()
    req_id = 90

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client._stream_queues[req_id].put_nowait(ValueError("boom"))

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqHistoricalData", return_value=None), \
         patch.object(client, "cancelHistoricalData"):
        f = asyncio.create_task(feeder())
        with pytest.raises(ValueError):
            async for _ in client.stream_historical_data(
                Contract(), "", "1 D", "1 hour", "TRADES", timeout=2.0
            ):
                pass
        await f

    assert req_id not in client._stream_queues


async def test_stream_real_time_bars_propagates_unexpected_exception():
    client = connected()
    req_id = 91

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client._stream_queues[req_id].put_nowait(ValueError("boom"))

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqRealTimeBars", return_value=None), \
         patch.object(client, "cancelRealTimeBars"):
        f = asyncio.create_task(feeder())
        with pytest.raises(ValueError):
            async for _ in client.stream_real_time_bars(Contract()):
                pass
        await f


async def test_stream_tick_by_tick_propagates_unexpected_exception():
    client = connected()
    req_id = 92

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client._stream_queues[req_id].put_nowait(ValueError("boom"))

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqTickByTickData", return_value=None), \
         patch.object(client, "cancelTickByTickData"):
        f = asyncio.create_task(feeder())
        with pytest.raises(ValueError):
            async for _ in client.stream_tick_by_tick(Contract()):
                pass
        await f


async def test_get_market_depth_first_item_unexpected_exception():
    client = connected()
    req_id = 93

    async def feeder():
        while req_id not in client._stream_queues:
            await asyncio.sleep(0)
        client._stream_queues[req_id].put_nowait(ValueError("boom"))

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqMktDepth", return_value=None), \
         patch.object(client, "cancelMktDepth"):
        f = asyncio.create_task(feeder())
        with pytest.raises(ValueError):
            await client.get_market_depth(Contract(), settle_time=0.05, timeout=2.0)
        await f

    assert req_id not in client._stream_queues
