"""Tests for AsyncTWSClient."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ibapi.contract import Contract, ContractDetails
from ibapi.common import BarData

from ibapi_async.tws_client import AsyncTWSClient
from ibapi_async.exceptions import ResponseTimeout
from tests.conftest import make_handshake_response, MockStreamReader, MockStreamWriter


def make_connected_client() -> AsyncTWSClient:
    """Return an AsyncTWSClient that appears connected (state mocked)."""
    client = AsyncTWSClient(timeout=5.0)
    from ibapi.client import EClient
    client.connState = EClient.CONNECTED
    mock_conn = MagicMock()
    mock_conn.isConnected.return_value = True
    client.conn = mock_conn
    client.serverVersion_ = 176
    client.next_valid_id_value = 1
    return client


# ------------------------------------------------------------------
# _wait_for_response / _set_event tests
# ------------------------------------------------------------------

async def test_wait_for_response_returns_data():
    """_wait_for_response should return data after _set_event is called."""
    client = make_connected_client()

    async def trigger():
        await asyncio.sleep(0.05)
        client._set_event(42, "contract_details", ["details_data"])

    asyncio.create_task(trigger())
    result = await client._wait_for_response(42, "contract_details", timeout=2.0)
    assert result == ["details_data"]


async def test_wait_for_response_raises_on_timeout():
    """_wait_for_response should raise ResponseTimeout when no event is set."""
    client = make_connected_client()

    with pytest.raises(ResponseTimeout, match="contract_details"):
        await client._wait_for_response(99, "contract_details", timeout=0.1)


async def test_cleanup_after_response():
    """Event keys should be cleaned up after _wait_for_response returns."""
    client = make_connected_client()

    async def trigger():
        await asyncio.sleep(0.02)
        client._set_event(1, "next_valid_id", 5)

    asyncio.create_task(trigger())
    await client._wait_for_response(1, "next_valid_id", timeout=1.0)

    key = "next_valid_id_1"
    assert key not in client._response_events
    assert key not in client._response_data


# ------------------------------------------------------------------
# EWrapper callback tests
# ------------------------------------------------------------------

def test_next_valid_id_sets_value():
    """nextValidId callback should update next_valid_id_value."""
    client = AsyncTWSClient()
    client.nextValidId(42)
    assert client.next_valid_id_value == 42


def test_contract_details_accumulates():
    """contractDetails callback should append to the list."""
    client = AsyncTWSClient()
    cd = MagicMock(spec=ContractDetails)
    client.contractDetails(1, cd)
    client.contractDetails(1, cd)
    assert len(client.contract_details[1]) == 2


def test_contract_details_end_sets_event():
    """contractDetailsEnd should signal the waiting coroutine."""
    client = AsyncTWSClient()
    client._response_events["contract_details_7"] = asyncio.Event()
    cd = MagicMock(spec=ContractDetails)
    client.contractDetails(7, cd)
    client.contractDetailsEnd(7)
    assert client._response_events["contract_details_7"].is_set()


def test_historical_data_accumulates():
    """historicalData callback should accumulate BarData."""
    client = AsyncTWSClient()
    bar = MagicMock(spec=BarData)
    client.historicalData(3, bar)
    client.historicalData(3, bar)
    assert len(client.historical_data[3]) == 2


def test_historical_data_end_sets_event():
    """historicalDataEnd should signal the waiting coroutine."""
    client = AsyncTWSClient()
    client._response_events["historical_data_3"] = asyncio.Event()
    bar = MagicMock(spec=BarData)
    client.historicalData(3, bar)
    client.historicalDataEnd(3, "start", "end")
    assert client._response_events["historical_data_3"].is_set()


def test_order_status_stored_and_event_set():
    """orderStatus callback should store data and signal event."""
    from decimal import Decimal
    client = AsyncTWSClient()
    client._response_events["order_status_10"] = asyncio.Event()

    client.orderStatus(10, "Submitted", Decimal("0"), Decimal("100"),
                       0.0, 0, 0, 0.0, 0, "", 0.0)

    assert client.order_status[10]["status"] == "Submitted"
    assert client._response_events["order_status_10"].is_set()


def test_error_accumulated():
    """error callback should accumulate error info."""
    client = AsyncTWSClient()
    client.error(5, 12345, 200, "No security found", "")
    assert 5 in client.errors
    assert client.errors[5][0]["errorCode"] == 200


# ------------------------------------------------------------------
# High-level method tests (mocked EClient calls)
# ------------------------------------------------------------------

async def test_get_contract_details_happy_path():
    """get_contract_details should call reqContractDetails and await the response."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()

    cd = MagicMock(spec=ContractDetails)

    async def simulate_response():
        await asyncio.sleep(0.05)
        client.contract_details[2] = [cd]
        client._set_event(2, "next_valid_id", 2)
        client._set_event(2, "contract_details", [cd])

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=2)):
        with patch.object(client, "reqContractDetails", return_value=None):
            asyncio.create_task(simulate_response())
            contract = Contract()
            result = await client.get_contract_details(contract, timeout=2.0)

    assert result == [cd]


async def test_get_historical_data_happy_path():
    """get_historical_data should call reqHistoricalData and await the response."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()

    bar = MagicMock(spec=BarData)

    async def simulate_response():
        await asyncio.sleep(0.05)
        client.historical_data[3] = [bar]
        client._set_event(3, "historical_data", [bar])

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=3)):
        with patch.object(client, "reqHistoricalData", return_value=None):
            asyncio.create_task(simulate_response())
            contract = Contract()
            result = await client.get_historical_data(
                contract, "", "1 D", "1 hour", "TRADES", timeout=2.0
            )

    assert result == [bar]


def test_historical_schedule_sets_event():
    """historicalSchedule should signal the waiting coroutine. TWS answers a
    whatToShow=SCHEDULE request with a single historicalSchedule callback and
    NO historicalDataEnd — so without handling it here the request hangs until
    timeout (the ~30s gap-filter stall)."""
    client = AsyncTWSClient()
    client._response_events["historical_schedule_3"] = asyncio.Event()
    sess = MagicMock()
    client.historicalSchedule(3, "20260101", "20260110", "US/Eastern", [sess])
    assert client._response_events["historical_schedule_3"].is_set()
    assert client.historical_schedule[3]["sessions"] == [sess]
    assert client.historical_schedule[3]["time_zone"] == "US/Eastern"


async def test_get_historical_schedule_happy_path():
    """get_historical_schedule should send a SCHEDULE reqHistoricalData and
    return when the historicalSchedule callback fires — not wait for an
    historicalDataEnd that never comes for SCHEDULE requests."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()

    sess = MagicMock()
    payload = {
        "start": "20260101", "end": "20260110",
        "time_zone": "US/Eastern", "sessions": [sess],
    }

    async def simulate_response():
        await asyncio.sleep(0.05)
        client._set_event(3, "historical_schedule", payload)

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=3)):
        with patch.object(client, "reqHistoricalData", return_value=None) as req:
            asyncio.create_task(simulate_response())
            contract = Contract()
            result = await client.get_historical_schedule(
                contract, "", "18 D", timeout=2.0
            )

    assert result == payload
    # Sent as a SCHEDULE / 1-day request.
    sent = req.call_args[0]
    assert "SCHEDULE" in sent
    assert "1 day" in sent


async def test_get_contract_details_timeout():
    """get_contract_details should raise ResponseTimeout when no response arrives."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=99)):
        with patch.object(client, "reqContractDetails", return_value=None):
            contract = Contract()
            with pytest.raises(ResponseTimeout):
                await client.get_contract_details(contract, timeout=0.1)


async def test_aenter_aexit():
    """AsyncTWSClient should support async context manager protocol."""
    client = make_connected_client()

    with patch.object(client, "disconnect", new=AsyncMock()) as mock_disconnect:
        async with client:
            pass
        mock_disconnect.assert_called_once()


# ------------------------------------------------------------------
# stream_historical_data (progressive per-bar generator)
# ------------------------------------------------------------------


def test_historical_data_streams_to_queue_when_registered():
    """With a stream queue registered, historicalData routes the bar to the
    queue and does NOT accumulate; historicalDataEnd pushes the sentinel."""
    from ibapi_async.tws_client import _HISTORICAL_END

    client = AsyncTWSClient()
    q: asyncio.Queue = asyncio.Queue()
    client._stream_queues[5] = q
    bar = MagicMock(spec=BarData)

    client.historicalData(5, bar)
    client.historicalDataEnd(5, "s", "e")

    assert q.get_nowait() is bar
    assert q.get_nowait() is _HISTORICAL_END
    assert 5 not in client.historical_data  # not accumulated while streaming


async def _drive_stream(client, req_id, *, bars=(), error=None, end=True):
    """Wait for the generator to register its queue, then feed callbacks."""
    while req_id not in client._stream_queues:
        await asyncio.sleep(0)
    for bar in bars:
        client.historicalData(req_id, bar)
    if error is not None:
        code, msg = error
        client.error(req_id, 0, code, msg, "")
    if end:
        client.historicalDataEnd(req_id, "s", "e")


async def test_stream_historical_data_yields_incrementally():
    """Bars are yielded as they arrive; the stream ends on historicalDataEnd."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()
    req_id = 7
    bars = [MagicMock(spec=BarData) for _ in range(3)]

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqHistoricalData", return_value=None):
        feeder = asyncio.create_task(_drive_stream(client, req_id, bars=bars))
        received = []
        async for bar in client.stream_historical_data(
            Contract(), "", "1 D", "1 hour", "TRADES", timeout=2.0
        ):
            received.append(bar)
        await feeder

    assert received == bars
    assert req_id not in client._stream_queues  # cleaned up


async def test_stream_historical_data_raises_twserror_mid_stream():
    """A non-info error for the reqId surfaces as TWSError after prior bars."""
    from ibapi_async.exceptions import TWSError

    client = make_connected_client()
    client._write_queue = asyncio.Queue()
    req_id = 8
    bars = [MagicMock(spec=BarData)]

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqHistoricalData", return_value=None), \
         patch.object(client, "cancelHistoricalData", return_value=None):
        feeder = asyncio.create_task(
            _drive_stream(client, req_id, bars=bars, error=(162, "pacing"), end=False)
        )
        received = []
        with pytest.raises(TWSError) as exc:
            async for bar in client.stream_historical_data(
                Contract(), "", "1 D", "1 hour", "TRADES", timeout=2.0
            ):
                received.append(bar)
        await feeder

    assert received == bars
    assert exc.value.code == 162
    assert req_id not in client._stream_queues


async def test_stream_historical_data_cancels_on_early_break():
    """Breaking out of the loop cancels the underlying IB subscription."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()
    req_id = 9
    bars = [MagicMock(spec=BarData) for _ in range(3)]

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqHistoricalData", return_value=None), \
         patch.object(client, "cancelHistoricalData") as mock_cancel:
        feeder = asyncio.create_task(_drive_stream(client, req_id, bars=bars))
        gen = client.stream_historical_data(
            Contract(), "", "1 D", "1 hour", "TRADES", timeout=2.0
        )
        async for _bar in gen:
            break  # consume one, then abandon
        await gen.aclose()  # async-gen contract: cleanup runs on close
        await feeder

    mock_cancel.assert_called_once_with(req_id)
    assert req_id not in client._stream_queues


async def test_stream_historical_data_times_out():
    """No bars and no end → ResponseTimeout, and the subscription is cancelled."""
    client = make_connected_client()
    client._write_queue = asyncio.Queue()
    req_id = 10

    with patch.object(client, "get_next_valid_id", new=AsyncMock(return_value=req_id)), \
         patch.object(client, "reqHistoricalData", return_value=None), \
         patch.object(client, "cancelHistoricalData") as mock_cancel:
        with pytest.raises(ResponseTimeout):
            async for _bar in client.stream_historical_data(
                Contract(), "", "1 D", "1 hour", "TRADES", timeout=0.1
            ):
                pass

    mock_cancel.assert_called_once_with(req_id)
    assert req_id not in client._stream_queues


# ------------------------------------------------------------------
# create() handshake — fail fast on connection drop (e.g. 326)
# ------------------------------------------------------------------


async def test_create_fails_fast_on_connection_drop():
    """When the gateway rejects the clientId (326) and closes the socket during
    the handshake, create() raises in ~the round-trip time — NOT after the full
    10s nextValidId timeout — and includes the server error."""
    async def fake_connect(self, host, port, client_id):
        # Simulate: gateway sends 326 then drops the connection.
        async def _drop():
            await asyncio.sleep(0.01)
            self.errors[-1] = [
                {"errorCode": 326, "errorString": "Unable to connect as the client id is already in use"}
            ]
            self._connection_lost.set()
        asyncio.ensure_future(_drop())

    with patch.object(AsyncTWSClient, "connect", fake_connect), \
         patch.object(AsyncTWSClient, "disconnect", new=AsyncMock()):
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(ResponseTimeout) as exc:
            await AsyncTWSClient.create("127.0.0.1", 4002, 1)
        elapsed = loop.time() - start

    assert elapsed < 2.0, f"should fail fast, took {elapsed:.2f}s"
    msg = str(exc.value)
    assert "connection closed during handshake" in msg
    assert "326" in msg


async def test_create_succeeds_when_nextvalidid_arrives():
    """The happy path still returns the client once nextValidId is signaled."""
    async def fake_connect(self, host, port, client_id):
        async def _nvi():
            await asyncio.sleep(0.01)
            self._next_valid_id_received.set()
        asyncio.ensure_future(_nvi())

    with patch.object(AsyncTWSClient, "connect", fake_connect):
        client = await AsyncTWSClient.create("127.0.0.1", 4002, 1)

    assert client is not None
    assert client._next_valid_id_received.is_set()


async def test_connect_signals_connection_lost_on_oserror():
    """connect() must set _connection_lost when the TCP connect itself fails
    (e.g. ConnectionRefusedError — gateway not running), so the handshake waiter
    in create() can fail fast instead of blocking the whole handshake timeout."""
    client = AsyncTWSClient()
    with patch("ibapi_async.client.AsyncConnection") as MockConn:
        MockConn.return_value.connect = AsyncMock(
            side_effect=ConnectionRefusedError("refused")
        )
        await client.connect("127.0.0.1", 4002, 1)

    assert client._connection_lost.is_set()


async def test_create_fails_fast_on_connection_refused():
    """When the gateway isn't running, connect() hits OSError; create() must
    raise in ~round-trip time (NOT after the full handshake timeout) and surface
    the real CONNECT_FAIL (502) error."""
    with patch("ibapi_async.client.AsyncConnection") as MockConn:
        MockConn.return_value.connect = AsyncMock(
            side_effect=ConnectionRefusedError("refused")
        )
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(ResponseTimeout) as exc:
            await AsyncTWSClient.create("127.0.0.1", 4002, 1, timeout=10.0)
        elapsed = loop.time() - start

    assert elapsed < 2.0, f"should fail fast on refused connect, took {elapsed:.2f}s"
    assert "502" in str(exc.value)


async def test_create_honors_timeout_for_handshake():
    """create()'s timeout argument must bound the handshake wait — a small
    timeout fails quickly instead of blocking the old hardcoded 10s, and the
    message reports the timeout that was actually applied."""
    async def fake_connect(self, host, port, client_id):
        return None  # "connected", but nothing ever signals nvi or a drop

    with patch.object(AsyncTWSClient, "connect", fake_connect), \
         patch.object(AsyncTWSClient, "disconnect", new=AsyncMock()):
        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(ResponseTimeout) as exc:
            await AsyncTWSClient.create("127.0.0.1", 4002, 1, timeout=0.3)
        elapsed = loop.time() - start

    assert 0.2 < elapsed < 2.0, f"should respect timeout=0.3, took {elapsed:.2f}s"
    assert "0.3" in str(exc.value)


async def test_create_detail_ignores_informational_error_codes():
    """The handshake-failure message must name the real fatal error (326), not a
    benign informational code (2104 'data farm connection is OK') that TWS also
    delivers on the handshake channel and that lands last in errors[-1]."""
    async def fake_connect(self, host, port, client_id):
        async def _drop():
            await asyncio.sleep(0.01)
            self.errors[-1] = [
                {"errorCode": 326, "errorString": "clientId already in use"},
                {"errorCode": 2104, "errorString": "Market data farm connection is OK"},
            ]
            self._connection_lost.set()
        asyncio.ensure_future(_drop())

    with patch.object(AsyncTWSClient, "connect", fake_connect), \
         patch.object(AsyncTWSClient, "disconnect", new=AsyncMock()):
        with pytest.raises(ResponseTimeout) as exc:
            await AsyncTWSClient.create("127.0.0.1", 4002, 1)

    msg = str(exc.value)
    assert "326" in msg
    assert "2104" not in msg
