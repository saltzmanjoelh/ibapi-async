"""Coverage tests for client.py: protobuf send, handshake EOF, write-loop error/cancel."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ibapi.client import EClient

from ibapi_async.client import AsyncEClient
from ibapi_async.connection import AsyncConnection
from tests.conftest import MockStreamReader, MockStreamWriter, make_handshake_response


def make_mock_wrapper():
    return MagicMock()


def _connected_client():
    client = AsyncEClient(make_mock_wrapper())
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn.isConnected.return_value = True
    client.conn = mock_conn
    client.serverVersion_ = 176
    client._write_queue = asyncio.Queue()
    return client


# ------------------------------------------------------------------
# sendMsgProtoBuf
# ------------------------------------------------------------------

def test_send_msg_protobuf_enqueues_when_connected():
    client = _connected_client()
    client.sendMsgProtoBuf(5, b"\x08\x01")
    assert not client._write_queue.empty()


def test_send_msg_protobuf_drops_when_not_connected():
    client = _connected_client()
    client.conn.isConnected.return_value = False
    client.sendMsgProtoBuf(5, b"\x08\x01")
    assert client._write_queue.empty()


# ------------------------------------------------------------------
# connect() — EOF during handshake
# ------------------------------------------------------------------

async def test_connect_eof_during_handshake_disconnects():
    """If the server closes before sending the version, connect() disconnects."""
    client = AsyncEClient(make_mock_wrapper())
    reader = MockStreamReader()
    writer = MockStreamWriter()
    reader.feed_eof()  # immediate EOF, no handshake bytes

    with patch(
        "ibapi_async.client.asyncio.open_connection",
        new=AsyncMock(return_value=(reader, writer)),
    ):
        await client.connect("127.0.0.1", 4002, 0)

    assert client.connState == EClient.DISCONNECTED


# ------------------------------------------------------------------
# _write_loop — write error + cancel flush
# ------------------------------------------------------------------

async def test_write_loop_disconnects_on_write_error():
    """A write/drain error tears the connection down via disconnect()."""
    client = AsyncEClient(make_mock_wrapper())
    writer = MockStreamWriter()

    async def bad_drain():
        raise ConnectionResetError("reset")

    writer.drain = bad_drain
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn.isConnected.return_value = True
    mock_conn._writer = writer
    client.conn = mock_conn
    client._write_queue = asyncio.Queue()
    client._write_queue.put_nowait(b"hello")

    with patch.object(client, "disconnect", new=AsyncMock()) as disc, \
         patch.object(client, "isConnected", return_value=True):
        await client._write_loop()

    disc.assert_called_once()


async def test_write_loop_flushes_remaining_on_cancel():
    """On cancellation the loop best-effort flushes any still-queued messages.

    The cancel is delivered while the loop awaits the *first* ``writer.drain()``
    (a never-completing await), so it reaches the cancel handler
    deterministically on every Python version. The earlier version raced
    ``put_nowait()`` + ``cancel()`` against ``wait_for(queue.get())``; on Python
    3.11 ``wait_for`` swallows the cancellation when the inner get already
    resolved, so (with isConnected pinned True) the loop spun forever and hung CI.
    """
    client = AsyncEClient(make_mock_wrapper())
    writer = MockStreamWriter()

    drain_started = asyncio.Event()
    drain_calls = [0]

    async def drain():
        drain_calls[0] += 1
        if drain_calls[0] == 1:
            # Block the first drain so pending2 stays queued and the cancel
            # lands here, on an await that never completes on its own.
            drain_started.set()
            await asyncio.Event().wait()

    writer.drain = drain
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn._writer = writer
    client.conn = mock_conn
    client._write_queue = asyncio.Queue()
    client._write_queue.put_nowait(b"pending1")
    client._write_queue.put_nowait(b"pending2")

    with patch.object(client, "isConnected", return_value=True):
        task = asyncio.create_task(client._write_loop())
        await asyncio.wait_for(drain_started.wait(), timeout=2.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)

    assert b"pending1" in writer.written  # written before the blocking drain
    assert b"pending2" in writer.written  # flushed during cancel handling


async def test_write_loop_clean_exit_when_disconnected():
    """The loop drains a message then exits cleanly when isConnected() goes False."""
    client = AsyncEClient(make_mock_wrapper())
    writer = MockStreamWriter()
    mock_conn = MagicMock(spec=AsyncConnection)
    mock_conn._writer = writer
    client.conn = mock_conn
    client._write_queue = asyncio.Queue()
    client._write_queue.put_nowait(b"one")

    calls = [0]

    def is_conn():
        calls[0] += 1
        return calls[0] <= 1  # True once (drain), then False → clean exit

    with patch.object(client, "isConnected", side_effect=is_conn):
        await client._write_loop()

    assert b"one" in writer.written


async def test_connect_includes_connect_options_in_handshake():
    """connectOptions are appended to the version string sent during handshake."""
    client = AsyncEClient(make_mock_wrapper())
    client.connectOptions = "+PACEAPI"
    reader = MockStreamReader()
    writer = MockStreamWriter()
    reader.feed(make_handshake_response(server_version=176))
    reader.feed_eof()

    tasks: list[asyncio.Task] = []

    def capture_task(coro, **kw):
        t = asyncio.ensure_future(coro)
        tasks.append(t)
        return t

    with patch(
        "ibapi_async.client.asyncio.open_connection",
        new=AsyncMock(return_value=(reader, writer)),
    ), patch("ibapi_async.client.asyncio.create_task", side_effect=capture_task), \
            patch.object(client, "startApi", return_value=None):
        await client.connect("127.0.0.1", 4002, 0)

    assert any(b"+PACEAPI" in chunk for chunk in writer.written)

    for t in tasks:
        if not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=0.5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
