"""Tests for AsyncEClient."""

import asyncio
import struct
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ibapi_async.client import AsyncEClient
from ibapi_async.connection import AsyncConnection
from tests.conftest import MockStreamReader, MockStreamWriter, make_handshake_response


def make_mock_wrapper():
    wrapper = MagicMock()
    wrapper.connectAck = MagicMock()
    wrapper.connectionClosed = MagicMock()
    wrapper.error = MagicMock()
    return wrapper


async def test_run_raises_runtime_error():
    """run() should raise RuntimeError — async clients use connect()."""
    wrapper = make_mock_wrapper()
    client = AsyncEClient(wrapper)
    with pytest.raises(RuntimeError, match="AsyncEClient.run\\(\\) is not available"):
        client.run()


async def test_send_msg_enqueues_when_connected():
    """sendMsg() should enqueue bytes on the write queue when connected."""
    wrapper = make_mock_wrapper()
    client = AsyncEClient(wrapper)

    # Simulate a connected AsyncConnection
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn.isConnected.return_value = True
    client.conn = mock_conn
    client.serverVersion_ = 176  # above MIN_SERVER_VER_PROTOBUF for protobuf path testing

    # Use a field we can check
    client._write_queue = asyncio.Queue()

    # reqCurrentTime calls sendMsg internally; but sendMsg needs serverVersion
    # so we call it directly with a simple string
    client.sendMsg(49, "1\0")  # msg_id=49 (CURRENT_TIME), minimal body

    assert not client._write_queue.empty()
    msg = client._write_queue.get_nowait()
    assert isinstance(msg, bytes)
    assert len(msg) > 0


async def test_send_msg_drops_when_not_connected():
    """sendMsg() should drop bytes when the connection is not live."""
    wrapper = make_mock_wrapper()
    client = AsyncEClient(wrapper)

    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn.isConnected.return_value = False
    client.conn = mock_conn
    client.serverVersion_ = 176
    client._write_queue = asyncio.Queue()

    client.sendMsg(49, "1\0")

    assert client._write_queue.empty()


async def test_write_loop_drains_queue():
    """_write_loop should drain bytes from the queue to the writer."""
    wrapper = make_mock_wrapper()
    client = AsyncEClient(wrapper)

    mock_writer = MockStreamWriter()
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn.isConnected.return_value = True
    mock_conn._writer = mock_writer
    client.conn = mock_conn
    client.serverVersion_ = 100
    client._write_queue = asyncio.Queue()

    # Put a message into the queue, then mark as disconnected so loop exits
    test_bytes = b"\x00\x00\x00\x04test"
    client._write_queue.put_nowait(test_bytes)

    call_count = [0]
    original_is_connected = client.isConnected

    def patched_is_connected():
        call_count[0] += 1
        return call_count[0] <= 3  # runs enough to drain the message

    client.conn.isConnected.side_effect = patched_is_connected

    # Override isConnected on the client itself (it checks both conn state and conn.isConnected)
    with patch.object(client, "isConnected", side_effect=patched_is_connected):
        task = asyncio.create_task(client._write_loop())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    assert test_bytes in mock_writer.written


async def test_connect_sets_connected_state():
    """connect() should transition to CONNECTED state after handshake."""
    wrapper = make_mock_wrapper()
    client = AsyncEClient(wrapper)

    reader_stream = MockStreamReader()
    writer_stream = MockStreamWriter()

    # Feed the handshake response then EOF so background tasks exit cleanly
    reader_stream.feed(make_handshake_response(server_version=176))
    reader_stream.feed_eof()

    tasks: list[asyncio.Task] = []

    def capture_task(coro, **kw):
        t = asyncio.ensure_future(coro)
        tasks.append(t)
        return t

    with patch("ibapi_async.client.asyncio.open_connection", new=AsyncMock(return_value=(reader_stream, writer_stream))):
        with patch("ibapi_async.client.asyncio.create_task", side_effect=capture_task):
            with patch.object(client, "startApi", return_value=None):
                await client.connect("127.0.0.1", 4002, 0)

    from ibapi.client import EClient
    assert client.connState == EClient.CONNECTED
    assert client.serverVersion_ == 176

    # Cancel background tasks so the event loop closes cleanly
    for t in tasks:
        if not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=0.5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
