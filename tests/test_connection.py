"""Tests for AsyncConnection."""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from ibapi_async.connection import AsyncConnection


async def test_connect_opens_tcp_stream():
    """connect() should call asyncio.open_connection with host and port."""
    mock_reader = MagicMock()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False

    write_queue = asyncio.Queue()
    conn = AsyncConnection("127.0.0.1", 4002, write_queue)

    with patch("ibapi_async.connection.asyncio.open_connection", new=AsyncMock(return_value=(mock_reader, mock_writer))):
        await conn.connect()

    assert conn._reader is mock_reader
    assert conn._writer is mock_writer


async def test_is_connected_true_after_connect():
    """isConnected() returns True when writer is open."""
    mock_reader = MagicMock()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False

    write_queue = asyncio.Queue()
    conn = AsyncConnection("127.0.0.1", 4002, write_queue)

    with patch("ibapi_async.connection.asyncio.open_connection", new=AsyncMock(return_value=(mock_reader, mock_writer))):
        await conn.connect()

    assert conn.isConnected() is True


async def test_is_connected_false_before_connect():
    """isConnected() returns False before connect() is called."""
    conn = AsyncConnection("127.0.0.1", 4002, asyncio.Queue())
    assert conn.isConnected() is False


async def test_send_msg_enqueues_bytes():
    """sendMsg() should enqueue bytes onto the write queue."""
    mock_reader = MagicMock()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False

    write_queue = asyncio.Queue()
    conn = AsyncConnection("127.0.0.1", 4002, write_queue)

    with patch("ibapi_async.connection.asyncio.open_connection", new=AsyncMock(return_value=(mock_reader, mock_writer))):
        await conn.connect()

    msg = b"\x00\x00\x00\x05hello"
    result = conn.sendMsg(msg)

    assert result == len(msg)
    assert not write_queue.empty()
    queued = write_queue.get_nowait()
    assert queued == msg


async def test_send_msg_returns_zero_when_not_connected():
    """sendMsg() returns 0 and drops the message when not connected."""
    write_queue = asyncio.Queue()
    conn = AsyncConnection("127.0.0.1", 4002, write_queue)

    result = conn.sendMsg(b"some bytes")

    assert result == 0
    assert write_queue.empty()


async def test_disconnect_clears_streams():
    """disconnect() should close the writer and clear _reader/_writer."""
    mock_reader = MagicMock()
    mock_writer = MagicMock()
    mock_writer.is_closing.return_value = False
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()

    write_queue = asyncio.Queue()
    conn = AsyncConnection("127.0.0.1", 4002, write_queue)

    with patch("ibapi_async.connection.asyncio.open_connection", new=AsyncMock(return_value=(mock_reader, mock_writer))):
        await conn.connect()

    await conn.disconnect()

    assert conn._reader is None
    assert conn._writer is None
    assert conn.isConnected() is False
    mock_writer.close.assert_called_once()
    mock_writer.wait_closed.assert_called_once()


def test_recv_msg_raises():
    """recvMsg() should raise NotImplementedError — it's not used in async mode."""
    conn = AsyncConnection("127.0.0.1", 4002, asyncio.Queue())
    with pytest.raises(NotImplementedError):
        conn.recvMsg()
