"""
AsyncTWSClient: async replacement for ibapi.sync_wrapper.TWSSyncWrapper.

Direct 1:1 async mirror of TWSSyncWrapper:
  threading.Event()         → asyncio.Event()
  event.wait(timeout=N)     → await asyncio.wait_for(event.wait(), timeout=N)
  threading.Thread(run)     → asyncio.Task (started in AsyncEClient.connect())

Combines AsyncEClient (I/O) + EWrapper (callbacks) into a single class.
All 200+ EClient request methods are inherited and remain synchronous.
The high-level convenience methods (get_contract_details, place_order, etc.)
are async coroutines that fire the sync request and await the response event.

Usage:
    async with await AsyncTWSClient.create("127.0.0.1", 4002, 0) as client:
        details = await client.get_contract_details(contract)
"""

import asyncio
import logging
from decimal import Decimal
from typing import Any, AsyncIterator

from ibapi.account_summary_tags import AccountSummaryTags
from ibapi.common import (
    BarData,
    HistogramData,
    HistoricalTick,
    HistoricalTickBidAsk,
    HistoricalTickLast,
    ListOfDepthExchanges,
    ListOfNewsProviders,
    OrderId,
    PriceIncrement,
    SmartComponentMap,
    TickAttribBidAsk,
    TickAttribLast,
    TickerId,
)
from ibapi.contract import Contract, ContractDescription, ContractDetails
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import Order
from ibapi.order_cancel import OrderCancel
from ibapi.order_state import OrderState
from ibapi.ticktype import TickTypeEnum
from ibapi.wrapper import EWrapper

from ibapi_async.client import AsyncEClient
from ibapi_async.exceptions import INFO_ERROR_CODES, ResponseTimeout, TWSError

logger = logging.getLogger(__name__)

# Sentinel pushed onto a historical-data stream queue when TWS signals
# ``historicalDataEnd``. Lets ``stream_historical_data`` distinguish a clean
# end-of-stream from a bar or an injected ``TWSError``.
_HISTORICAL_END = object()


class AsyncTWSClient(EWrapper, AsyncEClient):
    """
    High-level asyncio client for Interactive Brokers TWS / IB Gateway.

    Inherits:
      - All EClient request methods (placeOrder, reqMarketData, etc.)
      - All EWrapper callback stubs (overridden below to store + signal)
      - Async connect/disconnect/_write_loop from AsyncEClient

    Typical usage:
        client = await AsyncTWSClient.create("127.0.0.1", 4002, 0)
        async with client:
            details = await client.get_contract_details(my_contract)
    """

    def __init__(self, timeout: float = 30.0) -> None:
        EWrapper.__init__(self)
        AsyncEClient.__init__(self, wrapper=self)

        self.timeout = timeout

        # Correlation: event_key → asyncio.Event / data
        self._response_events: dict[str, asyncio.Event] = {}
        self._response_data: dict[str, Any] = {}

        # Accumulators (same structure as TWSSyncWrapper)
        self.contract_details: dict[int, list[ContractDetails]] = {}
        self.order_status: dict[int, dict] = {}
        self.open_orders: dict[int, dict] = {}
        self.executions: dict[int, list] = {}
        self.portfolio: list[dict] = []
        self.positions: dict[str, list] = {}
        self.account_summary: dict[int, dict] = {}
        self.market_data: dict[int, dict] = {}
        self.historical_data: dict[int, list[BarData]] = {}
        # whatToShow=SCHEDULE requests are answered with a single
        # ``historicalSchedule`` callback (no historicalData/historicalDataEnd),
        # stored here keyed by reqId.
        self.historical_schedule: dict[int, dict] = {}
        self.errors: dict[int, list] = {}
        self.current_time_value: int | None = None
        self.next_valid_id_value: int | None = None

        # Extended accumulators
        self.matching_symbols: dict[int, list[ContractDescription]] = {}
        self.head_timestamps: dict[int, str] = {}
        self.option_params: dict[int, list[dict]] = {}
        self.histogram_results: dict[int, list[HistogramData]] = {}
        self.historical_ticks: dict[int, list] = {}
        self.scanner_xml: str | None = None
        self.news_providers_value: list = []
        self.market_depth: dict[int, dict[int, dict]] = {}  # reqId → {position → row}
        self.completed_orders: list[dict] = []
        self.managed_accounts_value: str | None = None

        # Streaming queues — populated by callbacks, drained by stream_* generators.
        # Each entry maps a reqId to an asyncio.Queue. Generators put a sentinel
        # (None) on stop; errors push an exception so the consumer raises.
        self._stream_queues: dict[int, asyncio.Queue] = {}

    # ------------------------------------------------------------------
    # Factory + context manager
    # ------------------------------------------------------------------

    @classmethod
    async def create(
        cls,
        host: str = "127.0.0.1",
        port: int = 7496,
        client_id: int = 0,
        timeout: float = 30.0,
    ) -> "AsyncTWSClient":
        """
        Connect and return a ready-to-use client.

        Awaits until nextValidId is received (confirms TWS handshake).
        Raises ResponseTimeout if the handshake doesn't complete in time.

        ``timeout`` bounds the handshake wait *and* becomes the default
        per-request timeout of the returned client.

        Example:
            client = await AsyncTWSClient.create("127.0.0.1", 4002, 0)
        """
        client = cls(timeout=timeout)
        await client.connect(host, port, client_id)

        # Wait for nextValidId (handshake complete) OR a connection drop —
        # whichever comes first, bounded by ``timeout``. When the gateway
        # rejects our clientId (326) it sends the error and immediately closes
        # the socket; without racing the drop, we'd block for the full timeout
        # even though the connection is already gone. Racing it surfaces the
        # failure in ~the round-trip time.
        nvi = asyncio.ensure_future(client._next_valid_id_received.wait())
        lost = asyncio.ensure_future(client._connection_lost.wait())
        try:
            done, _pending = await asyncio.wait(
                {nvi, lost}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (nvi, lost):
                if not task.done():
                    task.cancel()

        if nvi in done:
            return client

        # Handshake failed: connection dropped, or nothing arrived in time.
        # Surface the most specific *fatal* server error (e.g. 326) if there was
        # one. errors[-1] also collects benign informational codes (2104/2106/
        # 2158, …) that TWS delivers on the handshake channel, so filter those
        # out — otherwise the message can blame an unrelated "connection is OK".
        await client.disconnect()
        fatal_errors = [
            e for e in (client.errors.get(-1) or [])
            if e.get("errorCode") not in INFO_ERROR_CODES
        ]
        detail = ""
        if fatal_errors:
            last = fatal_errors[-1]
            detail = f" (last server error {last.get('errorCode')}: {last.get('errorString')})"
        if lost in done:
            raise ResponseTimeout(
                "connection closed during handshake before nextValidId" + detail
            )
        raise ResponseTimeout(
            f"nextValidId not received within {timeout:g} seconds — "
            "is TWS/Gateway running and accepting connections?" + detail
        )

    async def __aenter__(self) -> "AsyncTWSClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.disconnect()

    # Async Event for the initial nextValidId handshake
    @property
    def _next_valid_id_received(self) -> asyncio.Event:
        if not hasattr(self, "_nvi_event"):
            self._nvi_event = asyncio.Event()
        return self._nvi_event

    # ------------------------------------------------------------------
    # Core correlation helpers
    # ------------------------------------------------------------------

    async def _wait_for_response(
        self,
        req_id: int,
        event_name: str,
        timeout: float | None = None,
    ) -> Any:
        """
        Async mirror of TWSSyncWrapper._wait_for_response().

        Waits for _set_event(req_id, event_name, data) to be called
        by an EWrapper callback, then returns the stored data.

        Raises ResponseTimeout on timeout.
        """
        if timeout is None:
            timeout = self.timeout

        key = f"{event_name}_{req_id}"
        err_key = f"error_{req_id}"
        if key not in self._response_events:
            self._response_events[key] = asyncio.Event()
        if err_key not in self._response_events:
            self._response_events[err_key] = asyncio.Event()

        success_task = asyncio.create_task(self._response_events[key].wait())
        error_task = asyncio.create_task(self._response_events[err_key].wait())

        try:
            done, pending = await asyncio.wait(
                {success_task, error_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (success_task, error_task):
                if not t.done():
                    t.cancel()

        if not done:
            self._cleanup_event(key)
            self._cleanup_event(err_key)
            raise ResponseTimeout(
                f"No response for '{event_name}' (req_id={req_id}) "
                f"within {timeout}s"
            )

        if error_task in done and success_task not in done:
            err = self._response_data.get(err_key) or {}
            self._cleanup_event(key)
            self._cleanup_event(err_key)
            raise TWSError(
                req_id=err.get("reqId", req_id),
                code=err.get("errorCode", 0),
                message=err.get("errorString", ""),
            )

        data = self._response_data.get(key)
        self._cleanup_event(key)
        self._cleanup_event(err_key)
        return data

    def _set_event(
        self,
        req_id: int,
        event_name: str,
        data: Any = None,
    ) -> None:
        """
        Signal a waiting _wait_for_response() coroutine.

        Called from EWrapper callbacks (synchronous context, same event loop).
        """
        key = f"{event_name}_{req_id}"
        self._response_data[key] = data
        if key in self._response_events:
            self._response_events[key].set()

    def _cleanup_event(self, key: str) -> None:
        self._response_events.pop(key, None)
        self._response_data.pop(key, None)

    def _safe_cancel(self, cancel_fn, *args) -> None:
        """Call a TWS cancel method, swallowing any exception.

        Used for paired-cancel cleanup of one-shot subscriptions
        (head timestamp, histogram, fundamental, pnl). Should NOT be
        called after the server has already terminated the subscription
        with an error — that path causes the next request to misalign
        and produce error 320 ("Unable to parse field").
        """
        try:
            cancel_fn(*args)
        except Exception:
            logger.debug("safe-cancel suppressed", exc_info=True)

    # ------------------------------------------------------------------
    # EWrapper overrides — accumulate data + signal events
    # (mirrors TWSSyncWrapper 1:1)
    # ------------------------------------------------------------------

    def nextValidId(self, orderId: int) -> None:
        self.next_valid_id_value = orderId
        self._set_event(0, "next_valid_id", orderId)
        self._next_valid_id_received.set()
        super().nextValidId(orderId)

    def error(
        self,
        reqId: TickerId,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        error_info = {
            "reqId": reqId,
            "errorTime": errorTime,
            "errorCode": errorCode,
            "errorString": errorString,
            "advancedOrderRejectJson": advancedOrderRejectJson,
        }
        if reqId not in self.errors:
            self.errors[reqId] = []
        self.errors[reqId].append(error_info)
        # Only signal informational codes don't terminate awaiting requests.
        if errorCode not in INFO_ERROR_CODES:
            self._set_event(reqId, "error", error_info)
            # Surface the error to any streaming consumer subscribed to this reqId
            # so it can raise instead of hanging forever.
            q = self._stream_queues.get(reqId)
            if q is not None:
                q.put_nowait(
                    TWSError(req_id=reqId, code=errorCode, message=errorString)
                )
        super().error(reqId, errorTime, errorCode, errorString, advancedOrderRejectJson)

    def currentTime(self, time_value: int) -> None:
        self.current_time_value = time_value
        self._set_event(0, "current_time", time_value)
        super().currentTime(time_value)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        if reqId not in self.contract_details:
            self.contract_details[reqId] = []
        self.contract_details[reqId].append(contractDetails)
        super().contractDetails(reqId, contractDetails)

    def contractDetailsEnd(self, reqId: int) -> None:
        self._set_event(
            reqId, "contract_details", self.contract_details.get(reqId, [])
        )
        super().contractDetailsEnd(reqId)

    def orderStatus(
        self,
        orderId: OrderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        data = {
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFillPrice": avgFillPrice,
            "permId": permId,
            "parentId": parentId,
            "lastFillPrice": lastFillPrice,
            "clientId": clientId,
            "whyHeld": whyHeld,
            "mktCapPrice": mktCapPrice,
        }
        self.order_status[orderId] = data
        self._set_event(orderId, "order_status", data)
        super().orderStatus(
            orderId, status, filled, remaining, avgFillPrice,
            permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice,
        )

    def openOrder(
        self,
        orderId: OrderId,
        contract: Contract,
        order: Order,
        orderState: OrderState,
    ) -> None:
        self.open_orders[orderId] = {
            "orderId": orderId,
            "contract": contract,
            "order": order,
            "orderState": orderState,
        }
        super().openOrder(orderId, contract, order, orderState)

    def openOrderEnd(self) -> None:
        self._set_event(0, "open_orders", self.open_orders)
        super().openOrderEnd()

    def execDetails(
        self, reqId: int, contract: Contract, execution: Execution
    ) -> None:
        if reqId not in self.executions:
            self.executions[reqId] = []
        self.executions[reqId].append({"contract": contract, "execution": execution})
        super().execDetails(reqId, contract, execution)

    def execDetailsEnd(self, reqId: int) -> None:
        self._set_event(reqId, "executions", self.executions.get(reqId, []))
        super().execDetailsEnd(reqId)

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        self.portfolio.append({
            "contract": contract,
            "position": position,
            "marketPrice": marketPrice,
            "marketValue": marketValue,
            "averageCost": averageCost,
            "unrealizedPNL": unrealizedPNL,
            "realizedPNL": realizedPNL,
            "accountName": accountName,
        })
        super().updatePortfolio(
            contract, position, marketPrice, marketValue,
            averageCost, unrealizedPNL, realizedPNL, accountName,
        )

    def accountDownloadEnd(self, accountName: str) -> None:
        self._set_event(0, "portfolio", self.portfolio)
        super().accountDownloadEnd(accountName)

    def position(
        self,
        account: str,
        contract: Contract,
        position: Decimal,
        avgCost: float,
    ) -> None:
        if account not in self.positions:
            self.positions[account] = []
        self.positions[account].append({
            "contract": contract,
            "position": position,
            "avgCost": avgCost,
        })
        super().position(account, contract, position, avgCost)

    def positionEnd(self) -> None:
        self._set_event(0, "positions", self.positions)
        super().positionEnd()

    def accountSummary(
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ) -> None:
        if reqId not in self.account_summary:
            self.account_summary[reqId] = {}
        if account not in self.account_summary[reqId]:
            self.account_summary[reqId][account] = {}
        self.account_summary[reqId][account][tag] = {"value": value, "currency": currency}
        super().accountSummary(reqId, account, tag, value, currency)

    def accountSummaryEnd(self, reqId: int) -> None:
        self._set_event(reqId, "account_summary", self.account_summary.get(reqId, {}))
        super().accountSummaryEnd(reqId)

    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib: Any) -> None:
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        self.market_data[reqId][TickTypeEnum.toStr(tickType)] = price
        super().tickPrice(reqId, tickType, price, attrib)

    def tickSize(self, reqId: TickerId, tickType: int, size: Decimal) -> None:
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        self.market_data[reqId][TickTypeEnum.toStr(tickType)] = size
        super().tickSize(reqId, tickType, size)

    def tickString(self, reqId: TickerId, tickType: int, value: str) -> None:
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        self.market_data[reqId][TickTypeEnum.toStr(tickType)] = value

    def tickGeneric(self, reqId: TickerId, tickType: int, value: float) -> None:
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        self.market_data[reqId][TickTypeEnum.toStr(tickType)] = value

    def tickSnapshotEnd(self, reqId: int) -> None:
        self._set_event(reqId, "market_data", self.market_data.get(reqId, {}))
        super().tickSnapshotEnd(reqId)

    def historicalData(self, reqId: int, bar: BarData) -> None:
        # Streaming consumer (stream_historical_data) registered a queue for
        # this reqId — hand the bar straight over and skip accumulation so a
        # long stream doesn't grow self.historical_data unbounded. The
        # await-the-whole-thing path (get_historical_data) registers no queue
        # and falls through to the accumulator below.
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait(bar)
        else:
            if reqId not in self.historical_data:
                self.historical_data[reqId] = []
            self.historical_data[reqId].append(bar)
        super().historicalData(reqId, bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait(_HISTORICAL_END)
        else:
            self._set_event(
                reqId, "historical_data", self.historical_data.get(reqId, [])
            )
        super().historicalDataEnd(reqId, start, end)

    def historicalSchedule(
        self,
        reqId: int,
        startDateTime: str,
        endDateTime: str,
        timeZone: str,
        sessions: list,
    ) -> None:
        # A whatToShow=SCHEDULE request is answered with this single callback —
        # there is NO historicalData/historicalDataEnd — so this IS the
        # completion signal. Without it the awaiting coroutine would hang until
        # timeout (the ~30s gap-filter stall). Store the payload and fire the
        # ``historical_schedule`` event that ``get_historical_schedule`` awaits.
        payload = {
            "start": startDateTime,
            "end": endDateTime,
            "time_zone": timeZone,
            "sessions": list(sessions),
        }
        self.historical_schedule[reqId] = payload
        self._set_event(reqId, "historical_schedule", payload)
        super().historicalSchedule(
            reqId, startDateTime, endDateTime, timeZone, sessions
        )

    # ------------------------------------------------------------------
    # Extended EWrapper overrides (reference data, news, options, scanner,
    # histogram, ticks, pnl, fundamentals, completed orders, streaming)
    # ------------------------------------------------------------------

    def managedAccounts(self, accountsList: str) -> None:
        self.managed_accounts_value = accountsList
        self._set_event(0, "managed_accounts", accountsList)
        super().managedAccounts(accountsList)

    def symbolSamples(
        self, reqId: int, contractDescriptions: list[ContractDescription]
    ) -> None:
        self.matching_symbols[reqId] = list(contractDescriptions)
        self._set_event(reqId, "matching_symbols", self.matching_symbols[reqId])
        super().symbolSamples(reqId, contractDescriptions)

    def headTimestamp(self, reqId: int, headTimestamp: str) -> None:
        self.head_timestamps[reqId] = headTimestamp
        self._set_event(reqId, "head_timestamp", headTimestamp)
        super().headTimestamp(reqId, headTimestamp)

    def securityDefinitionOptionParameter(
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: set,
        strikes: set,
    ) -> None:
        self.option_params.setdefault(reqId, []).append({
            "exchange": exchange,
            "underlyingConId": underlyingConId,
            "tradingClass": tradingClass,
            "multiplier": multiplier,
            "expirations": sorted(expirations),
            "strikes": sorted(strikes),
        })
        super().securityDefinitionOptionParameter(
            reqId, exchange, underlyingConId, tradingClass,
            multiplier, expirations, strikes,
        )

    def securityDefinitionOptionParameterEnd(self, reqId: int) -> None:
        self._set_event(
            reqId, "option_chain", self.option_params.get(reqId, [])
        )
        super().securityDefinitionOptionParameterEnd(reqId)

    def newsProviders(self, providers: ListOfNewsProviders) -> None:
        self.news_providers_value = list(providers)
        self._set_event(0, "news_providers", self.news_providers_value)
        super().newsProviders(providers)

    def scannerParameters(self, xml: str) -> None:
        self.scanner_xml = xml
        self._set_event(0, "scanner_parameters", xml)
        super().scannerParameters(xml)

    def histogramData(self, reqId: int, items: list[HistogramData]) -> None:
        self.histogram_results[reqId] = list(items)
        self._set_event(reqId, "histogram", self.histogram_results[reqId])
        super().histogramData(reqId, items)

    def historicalTicks(
        self, reqId: int, ticks: list[HistoricalTick], done: bool
    ) -> None:
        self.historical_ticks.setdefault(reqId, []).extend(ticks)
        if done:
            self._set_event(reqId, "historical_ticks", self.historical_ticks[reqId])
        super().historicalTicks(reqId, ticks, done)

    def historicalTicksBidAsk(
        self, reqId: int, ticks: list[HistoricalTickBidAsk], done: bool
    ) -> None:
        self.historical_ticks.setdefault(reqId, []).extend(ticks)
        if done:
            self._set_event(reqId, "historical_ticks", self.historical_ticks[reqId])
        super().historicalTicksBidAsk(reqId, ticks, done)

    def historicalTicksLast(
        self, reqId: int, ticks: list[HistoricalTickLast], done: bool
    ) -> None:
        self.historical_ticks.setdefault(reqId, []).extend(ticks)
        if done:
            self._set_event(reqId, "historical_ticks", self.historical_ticks[reqId])
        super().historicalTicksLast(reqId, ticks, done)

    def pnl(
        self,
        reqId: int,
        dailyPnL: float,
        unrealizedPnL: float,
        realizedPnL: float,
    ) -> None:
        data = {
            "dailyPnL": dailyPnL,
            "unrealizedPnL": unrealizedPnL,
            "realizedPnL": realizedPnL,
        }
        # First-snapshot semantics: signal once per request.
        self._set_event(reqId, "pnl", data)
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)

    def fundamentalData(self, reqId: int, data: str) -> None:
        self._set_event(reqId, "fundamental_data", data)
        super().fundamentalData(reqId, data)

    def completedOrder(
        self,
        contract: Contract,
        order: Order,
        orderState: OrderState,
    ) -> None:
        self.completed_orders.append({
            "contract": contract,
            "order": order,
            "orderState": orderState,
        })
        super().completedOrder(contract, order, orderState)

    def completedOrdersEnd(self) -> None:
        self._set_event(0, "completed_orders", list(self.completed_orders))
        super().completedOrdersEnd()

    # ── streaming callbacks: push onto the matching reqId queue ────────
    def realtimeBar(
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Decimal,
        wap: Decimal,
        count: int,
    ) -> None:
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({
                "time": time, "open": open_, "high": high, "low": low,
                "close": close, "volume": volume, "wap": wap, "count": count,
            })
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)

    def tickByTickAllLast(
        self,
        reqId: int,
        tickType: int,
        time: int,
        price: float,
        size: Decimal,
        tickAttribLast: TickAttribLast,
        exchange: str,
        specialConditions: str,
    ) -> None:
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({
                "kind": "AllLast" if tickType == 1 else "Last",
                "time": time, "price": price, "size": size,
                "exchange": exchange, "specialConditions": specialConditions,
            })
        super().tickByTickAllLast(
            reqId, tickType, time, price, size, tickAttribLast,
            exchange, specialConditions,
        )

    def tickByTickBidAsk(
        self,
        reqId: int,
        time: int,
        bidPrice: float,
        askPrice: float,
        bidSize: Decimal,
        askSize: Decimal,
        tickAttribBidAsk: TickAttribBidAsk,
    ) -> None:
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({
                "kind": "BidAsk", "time": time,
                "bidPrice": bidPrice, "askPrice": askPrice,
                "bidSize": bidSize, "askSize": askSize,
            })
        super().tickByTickBidAsk(
            reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk,
        )

    def tickByTickMidPoint(
        self, reqId: int, time: int, midPoint: float
    ) -> None:
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({"kind": "MidPoint", "time": time, "midPoint": midPoint})
        super().tickByTickMidPoint(reqId, time, midPoint)

    def updateMktDepth(
        self,
        reqId: TickerId,
        position: int,
        operation: int,
        side: int,
        price: float,
        size: Decimal,
    ) -> None:
        # Track an in-memory book for snapshot-style get_market_depth().
        # operation: 0=insert, 1=update, 2=delete; side: 0=ask, 1=bid
        book = self.market_depth.setdefault(reqId, {0: {}, 1: {}})
        side_book = book.setdefault(side, {})
        if operation == 2:
            side_book.pop(position, None)
        else:
            side_book[position] = {"price": price, "size": size}
        # Stream consumers also see the raw event
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({
                "position": position, "operation": operation,
                "side": side, "price": price, "size": size,
            })
        super().updateMktDepth(reqId, position, operation, side, price, size)

    def updateMktDepthL2(
        self,
        reqId: TickerId,
        position: int,
        marketMaker: str,
        operation: int,
        side: int,
        price: float,
        size: Decimal,
        isSmartDepth: bool,
    ) -> None:
        # Reuse the L1 book; marketMaker is informational.
        book = self.market_depth.setdefault(reqId, {0: {}, 1: {}})
        side_book = book.setdefault(side, {})
        if operation == 2:
            side_book.pop(position, None)
        else:
            side_book[position] = {
                "price": price, "size": size, "marketMaker": marketMaker,
            }
        q = self._stream_queues.get(reqId)
        if q is not None:
            q.put_nowait({
                "position": position, "operation": operation, "side": side,
                "price": price, "size": size, "marketMaker": marketMaker,
            })
        super().updateMktDepthL2(
            reqId, position, marketMaker, operation, side,
            price, size, isSmartDepth,
        )

    # ------------------------------------------------------------------
    # High-level async convenience methods
    # (async mirrors of TWSSyncWrapper's sync methods)
    # ------------------------------------------------------------------

    async def get_next_valid_id(self, timeout: float = 5.0) -> int:
        """Return the next order ID, incrementing the locally cached value.

        TWS sends one ``nextValidId`` automatically after the handshake; the
        documented pattern is to cache that value and increment it locally.
        ``reqIds(-1)`` is effectively a no-op on modern TWS/Gateway and is
        also rejected outright when the API is in Read-Only mode.
        """
        if self.next_valid_id_value is None:
            await asyncio.wait_for(
                self._next_valid_id_received.wait(), timeout=timeout
            )
        assert self.next_valid_id_value is not None
        order_id = self.next_valid_id_value
        self.next_valid_id_value += 1
        return order_id

    async def get_current_time(self, timeout: float = 5.0) -> int:
        """Return the current system time on the TWS server (epoch seconds)."""
        self.reqCurrentTime()
        return await self._wait_for_response(0, "current_time", timeout)

    async def get_contract_details(
        self, contract: Contract, timeout: float = 5.0
    ) -> list[ContractDetails]:
        """Return a list of ContractDetails for the given contract."""
        req_id = await self.get_next_valid_id()
        self.contract_details.pop(req_id, None)
        self.reqContractDetails(req_id, contract)
        return await self._wait_for_response(req_id, "contract_details", timeout)

    async def place_order(
        self,
        contract: Contract,
        order: Order,
        timeout: float | None = None,
    ) -> dict:
        """
        Place an order and wait for the initial order status.

        timeout defaults to 5s for LMT/MKT orders, 2s otherwise.
        """
        if timeout is None:
            timeout = 5.0 if order.orderType in ("LMT", "MKT") else 2.0

        order_id = await self.get_next_valid_id()
        order.orderId = order_id
        self.order_status.pop(order_id, None)
        self.placeOrder(order_id, contract, order)
        return await self._wait_for_response(order_id, "order_status", timeout)

    async def cancel_order(
        self,
        order_id: int,
        order_cancel: OrderCancel | None = None,
        timeout: float = 3.0,
    ) -> dict:
        """Cancel an order and return the resulting order status."""
        if order_cancel is None:
            order_cancel = OrderCancel()
        self.cancelOrder(order_id, order_cancel)
        return await self._wait_for_response(order_id, "order_status", timeout)

    async def get_open_orders(self, timeout: float = 3.0) -> dict:
        """Return all currently open orders."""
        self.open_orders = {}
        self.reqOpenOrders()
        return await self._wait_for_response(0, "open_orders", timeout)

    async def get_executions(
        self,
        exec_filter: ExecutionFilter | None = None,
        timeout: float = 5.0,
    ) -> list:
        """Return executions matching the given filter.

        IB's gateway does **not** reliably fire ``execDetailsEnd`` when there
        are zero executions matching the filter — the server simply stays
        silent. To avoid hanging callers in that case we treat a timeout as
        "no executions" and return ``[]``. If you need to distinguish
        "really no executions" from "TWS dropped the response", catch
        ``ResponseTimeout`` directly with ``_wait_for_response`` instead.
        """
        if exec_filter is None:
            exec_filter = ExecutionFilter()
        req_id = await self.get_next_valid_id()
        self.executions.pop(req_id, None)
        self.reqExecutions(req_id, exec_filter)
        try:
            return await self._wait_for_response(req_id, "executions", timeout)
        except ResponseTimeout:
            # Most paper accounts will hit this on a no-executions filter.
            # Surface what we accumulated (typically nothing).
            return self.executions.get(req_id, [])

    async def get_portfolio(
        self, account_code: str = "", timeout: float = 30.0
    ) -> list:
        """Return the current portfolio for the given account (empty = all)."""
        self.portfolio = []
        self.reqAccountUpdates(True, account_code)
        portfolio = await self._wait_for_response(0, "portfolio", timeout)
        self.reqAccountUpdates(False, account_code)
        return portfolio

    async def get_positions(self, timeout: float = 10.0) -> dict:
        """Return current positions grouped by account."""
        self.positions = {}
        self.reqPositions()
        positions = await self._wait_for_response(0, "positions", timeout)
        self.cancelPositions()
        return positions

    async def get_account_summary(
        self,
        tags: str = AccountSummaryTags.AllTags,
        group: str = "All",
        timeout: float = 5.0,
    ) -> dict:
        """Return account summary for the given tags and account group."""
        req_id = await self.get_next_valid_id()
        self.account_summary.pop(req_id, None)
        self.reqAccountSummary(req_id, group, tags)
        summary = await self._wait_for_response(req_id, "account_summary", timeout)
        self.cancelAccountSummary(req_id)
        return summary

    async def get_market_data_snapshot(
        self,
        contract: Contract,
        generic_tick_list: str = "",
        timeout: float = 11.0,
    ) -> dict:
        """Return a snapshot of market data for the given contract."""
        req_id = await self.get_next_valid_id()
        self.market_data.pop(req_id, None)
        self.reqMktData(req_id, contract, generic_tick_list, True, False, [])
        return await self._wait_for_response(req_id, "market_data", timeout)

    async def get_historical_data(
        self,
        contract: Contract,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        timeout: float = 30.0,
    ) -> list[BarData]:
        """Return historical bar data for the given contract and parameters."""
        req_id = await self.get_next_valid_id()
        self.historical_data.pop(req_id, None)
        self.reqHistoricalData(
            req_id, contract, end_date_time, duration_str,
            bar_size_setting, what_to_show, use_rth, format_date, False, [],
        )
        return await self._wait_for_response(req_id, "historical_data", timeout)

    async def get_historical_schedule(
        self,
        contract: Contract,
        end_date_time: str,
        duration_str: str,
        use_rth: bool = True,
        format_date: int = 2,
        timeout: float = 30.0,
    ) -> dict:
        """Return the historical trading-session schedule for a contract.

        Issues a ``whatToShow=SCHEDULE`` / ``barSize=1 day`` historical request.
        TWS answers with ONE ``historicalSchedule`` callback (and no
        ``historicalDataEnd``), so this awaits the ``historical_schedule`` event
        rather than an end-of-stream sentinel — the whole point of the separate
        path. Returns ``{"start", "end", "time_zone", "sessions"}``.
        """
        req_id = await self.get_next_valid_id()
        self.historical_schedule.pop(req_id, None)
        self.reqHistoricalData(
            req_id, contract, end_date_time, duration_str,
            "1 day", "SCHEDULE", use_rth, format_date, False, [],
        )
        return await self._wait_for_response(req_id, "historical_schedule", timeout)

    async def stream_historical_data(
        self,
        contract: Contract,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        keep_up_to_date: bool = False,
        timeout: float = 30.0,
    ) -> AsyncIterator[BarData]:
        """Yield historical bars progressively, as each ``historicalData``
        callback fires, completing when TWS sends ``historicalDataEnd``.

        Unlike :meth:`get_historical_data` (which awaits the whole result),
        this lets a consumer render/persist the newest bars while older
        history is still streaming — the basis for chunked, progressive UI
        delivery.

        Raises :class:`TWSError` if TWS returns an error for this reqId, and
        :class:`ResponseTimeout` if no bar or end-of-stream arrives within
        ``timeout`` seconds (covers IB's server-side prep stalling before the
        first bar). Cancels the underlying request on generator close.

        ``keep_up_to_date=True`` is forwarded to ``reqHistoricalData`` but the
        stream still ends at ``historicalDataEnd``; live ``historicalDataUpdate``
        ticks are out of scope here (use ``get_historical_data`` semantics or a
        dedicated realtime stream).
        """
        req_id = await self.get_next_valid_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues[req_id] = queue
        self.reqHistoricalData(
            req_id, contract, end_date_time, duration_str, bar_size_setting,
            what_to_show, use_rth, format_date, keep_up_to_date, [],
        )
        server_terminated = False
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    raise ResponseTimeout(
                        f"historical data stream for reqId {req_id} timed out "
                        f"after {timeout}s"
                    )
                if item is _HISTORICAL_END:
                    break
                if isinstance(item, TWSError):
                    server_terminated = True
                    raise item
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            if not server_terminated:
                self._safe_cancel(self.cancelHistoricalData, req_id)
            self._stream_queues.pop(req_id, None)
            self.historical_data.pop(req_id, None)

    # ------------------------------------------------------------------
    # Extended async convenience methods
    # ------------------------------------------------------------------

    async def get_managed_accounts(self, timeout: float = 5.0) -> list[str]:
        """Return the list of account codes the API session is permissioned for.

        TWS pushes ``managedAccounts`` automatically once at handshake; this
        method returns the cached value if available, else issues a fresh
        ``reqManagedAccts``.
        """
        if self.managed_accounts_value is not None:
            return [a for a in self.managed_accounts_value.split(",") if a]
        self.reqManagedAccts()
        accounts = await self._wait_for_response(0, "managed_accounts", timeout)
        return [a for a in (accounts or "").split(",") if a]

    async def get_head_timestamp(
        self,
        contract: Contract,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        format_date: int = 1,
        timeout: float = 10.0,
    ) -> str:
        """Return the earliest timestamp for which historical data exists."""
        req_id = await self.get_next_valid_id()
        self.head_timestamps.pop(req_id, None)
        self.reqHeadTimeStamp(
            req_id, contract, what_to_show, int(use_rth), format_date
        )
        try:
            ts = await self._wait_for_response(req_id, "head_timestamp", timeout)
        except TWSError:
            # TWS already terminated this subscription server-side; cancelling
            # again sends a stray message that desyncs the parser.
            raise
        else:
            self._safe_cancel(self.cancelHeadTimeStamp, req_id)
            return ts

    async def search_symbols(
        self, pattern: str, timeout: float = 10.0
    ) -> list[ContractDescription]:
        """Search for instruments matching a substring of name or symbol."""
        req_id = await self.get_next_valid_id()
        self.matching_symbols.pop(req_id, None)
        self.reqMatchingSymbols(req_id, pattern)
        return await self._wait_for_response(req_id, "matching_symbols", timeout)

    async def get_option_chain(
        self,
        underlying_symbol: str,
        underlying_sec_type: str,
        underlying_con_id: int,
        fut_fop_exchange: str = "",
        timeout: float = 20.0,
    ) -> list[dict]:
        """Return option chain expirations / strikes per exchange.

        Each entry: {exchange, underlyingConId, tradingClass, multiplier,
        expirations (sorted), strikes (sorted)}.
        """
        req_id = await self.get_next_valid_id()
        self.option_params.pop(req_id, None)
        self.reqSecDefOptParams(
            req_id, underlying_symbol, fut_fop_exchange,
            underlying_sec_type, underlying_con_id,
        )
        return await self._wait_for_response(req_id, "option_chain", timeout)

    async def get_news_providers(self, timeout: float = 5.0) -> list:
        """Return the list of news providers permissioned for this account."""
        self.news_providers_value = []
        self.reqNewsProviders()
        return await self._wait_for_response(0, "news_providers", timeout)

    async def get_scanner_parameters(self, timeout: float = 30.0) -> str:
        """Return TWS's scanner parameters XML (used to build scanner queries)."""
        self.scanner_xml = None
        self.reqScannerParameters()
        return await self._wait_for_response(0, "scanner_parameters", timeout)

    async def get_histogram_data(
        self,
        contract: Contract,
        period: str = "3 months",
        use_rth: bool = True,
        timeout: float = 30.0,
    ) -> list[HistogramData]:
        """Return a price → traded-volume histogram for the given period."""
        req_id = await self.get_next_valid_id()
        self.histogram_results.pop(req_id, None)
        self.reqHistogramData(req_id, contract, use_rth, period)
        try:
            data = await self._wait_for_response(req_id, "histogram", timeout)
        except TWSError:
            raise
        else:
            self._safe_cancel(self.cancelHistogramData, req_id)
            return data

    async def get_historical_ticks(
        self,
        contract: Contract,
        start_date_time: str = "",
        end_date_time: str = "",
        number_of_ticks: int = 100,
        what_to_show: str = "TRADES",
        use_rth: int = 1,
        ignore_size: bool = True,
        timeout: float = 15.0,
    ) -> list:
        """Return up to ``number_of_ticks`` historical ticks.

        Provide either ``start_date_time`` OR ``end_date_time`` (not both) in
        ``"YYYYMMDD HH:MM:SS US/Eastern"`` format.

        ``ignore_size=True`` (the default) keeps zero-size prints in the
        result. Setting it to ``False`` filters them out — and on quiet
        instruments TWS may then return nothing at all, which surfaces as a
        ``ResponseTimeout``.
        """
        req_id = await self.get_next_valid_id()
        self.historical_ticks.pop(req_id, None)
        self.reqHistoricalTicks(
            req_id, contract, start_date_time, end_date_time,
            number_of_ticks, what_to_show, use_rth, ignore_size, [],
        )
        return await self._wait_for_response(req_id, "historical_ticks", timeout)

    async def get_pnl(
        self,
        account: str,
        model_code: str = "",
        timeout: float = 5.0,
    ) -> dict:
        """Return one PnL snapshot (daily / unrealized / realized) for an account.

        Cancels the streaming subscription before returning.
        """
        req_id = await self.get_next_valid_id()
        self.reqPnL(req_id, account, model_code)
        try:
            data = await self._wait_for_response(req_id, "pnl", timeout)
        except TWSError:
            raise
        else:
            self._safe_cancel(self.cancelPnL, req_id)
            return data

    async def get_fundamental_data(
        self,
        contract: Contract,
        report_type: str = "ReportSnapshot",
        timeout: float = 15.0,
    ) -> str:
        """Return fundamental analyst report XML.

        Raises ``TWSError`` on accounts without a Reuters fundamentals
        subscription (code 10358).
        """
        req_id = await self.get_next_valid_id()
        self.reqFundamentalData(req_id, contract, report_type, [])
        try:
            xml = await self._wait_for_response(
                req_id, "fundamental_data", timeout
            )
        except TWSError:
            # TWS already terminated server-side; cancelling here desyncs
            # the parser (observed: subsequent reqRealTimeBars produces
            # error 320 "Unable to parse field 'Client Req Id'").
            raise
        else:
            self._safe_cancel(self.cancelFundamentalData, req_id)
            return xml

    async def get_completed_orders(
        self, api_only: bool = True, timeout: float = 10.0
    ) -> list[dict]:
        """Return today's completed orders (filled or cancelled)."""
        self.completed_orders = []
        self.reqCompletedOrders(api_only)
        return await self._wait_for_response(0, "completed_orders", timeout)

    def cancel_all_orders(self) -> None:
        """Issue ``reqGlobalCancel`` — cancel every working order in the account.

        Synchronous: TWS doesn't acknowledge global-cancel directly; consumers
        should poll ``get_open_orders`` afterwards if they need confirmation.
        """
        order_cancel = OrderCancel()
        self.reqGlobalCancel(order_cancel)

    def request_market_data_type(self, market_data_type: int) -> None:
        """Set the data feed type for subsequent reqMktData calls.

        1 = live, 2 = frozen, 3 = delayed, 4 = delayed-frozen.
        """
        self.reqMarketDataType(market_data_type)

    # ------------------------------------------------------------------
    # Streaming async generators
    # ------------------------------------------------------------------

    async def stream_real_time_bars(
        self,
        contract: Contract,
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> AsyncIterator[dict]:
        """Yield 5-second real-time bars until the consumer breaks out.

        Cancels the underlying subscription on generator close. Raises
        ``TWSError`` if TWS returns an error (e.g. permission denied).
        """
        req_id = await self.get_next_valid_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues[req_id] = queue
        self.reqRealTimeBars(req_id, contract, 5, what_to_show, use_rth, [])
        server_terminated = False
        try:
            while True:
                item = await queue.get()
                if isinstance(item, TWSError):
                    server_terminated = True
                    raise item
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            if not server_terminated:
                self._safe_cancel(self.cancelRealTimeBars, req_id)
            self._stream_queues.pop(req_id, None)

    async def stream_tick_by_tick(
        self,
        contract: Contract,
        tick_type: str = "Last",
        number_of_ticks: int = 0,
        ignore_size: bool = False,
    ) -> AsyncIterator[dict]:
        """Yield tick-by-tick events until consumer breaks out.

        ``tick_type``: ``"Last"``, ``"AllLast"``, ``"BidAsk"``, or ``"MidPoint"``.
        Raises ``TWSError`` on permission denial / invalid request.
        """
        req_id = await self.get_next_valid_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues[req_id] = queue
        self.reqTickByTickData(
            req_id, contract, tick_type, number_of_ticks, ignore_size
        )
        server_terminated = False
        try:
            while True:
                item = await queue.get()
                if isinstance(item, TWSError):
                    server_terminated = True
                    raise item
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            if not server_terminated:
                self._safe_cancel(self.cancelTickByTickData, req_id)
            self._stream_queues.pop(req_id, None)

    async def get_market_depth(
        self,
        contract: Contract,
        num_rows: int = 5,
        is_smart_depth: bool = False,
        settle_time: float = 2.0,
        timeout: float = 8.0,
    ) -> dict:
        """Subscribe, accumulate updates for ``settle_time`` seconds, then
        cancel and return the resulting book snapshot.

        Returns ``{"bids": [...], "asks": [...]}`` sorted best-first.
        Raises ``TWSError`` on permission denial.
        """
        req_id = await self.get_next_valid_id()
        queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues[req_id] = queue
        self.market_depth.pop(req_id, None)
        self.reqMktDepth(req_id, contract, num_rows, is_smart_depth, [])
        server_terminated = False
        try:
            # Wait for either: first update (then keep accumulating for
            # settle_time), or an error, or overall timeout.
            first = await asyncio.wait_for(queue.get(), timeout=timeout)
            if isinstance(first, TWSError):
                server_terminated = True
                raise first
            if isinstance(first, Exception):
                raise first
            # Drain anything that arrives in the settle window.
            deadline = asyncio.get_event_loop().time() + settle_time
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=remaining)
                    if isinstance(item, TWSError):
                        server_terminated = True
                        raise item
                    if isinstance(item, Exception):
                        raise item
                except asyncio.TimeoutError:
                    break
        finally:
            if not server_terminated:
                self._safe_cancel(self.cancelMktDepth, req_id, is_smart_depth)
            self._stream_queues.pop(req_id, None)

        book = self.market_depth.get(req_id, {0: {}, 1: {}})
        # side 0 = ask, side 1 = bid (per IB docs)
        asks = sorted(
            book.get(0, {}).items(), key=lambda kv: kv[0]
        )
        bids = sorted(
            book.get(1, {}).items(), key=lambda kv: kv[0]
        )
        return {
            "asks": [{"position": p, **row} for p, row in asks],
            "bids": [{"position": p, **row} for p, row in bids],
        }
