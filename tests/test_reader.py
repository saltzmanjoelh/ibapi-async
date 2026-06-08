"""Tests for _read_loop and _dispatch_message."""

import asyncio
import struct
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ibapi_async.reader import _dispatch_message, _read_loop
from tests.conftest import MockStreamReader, make_legacy_message, make_handshake_response


def make_mock_decoder():
    dec = MagicMock()
    dec.interpret = MagicMock()
    dec.processProtoBuf = MagicMock()
    return dec


# ------------------------------------------------------------------
# _dispatch_message unit tests
# ------------------------------------------------------------------

def test_dispatch_legacy_message():
    """Legacy messages (no protobuf) should route to decoder.interpret()."""
    dec = make_mock_decoder()
    server_version = 100  # below MIN_SERVER_VER_PROTOBUF

    # Build a legacy message: "9\0<reqId>\0" (msg 9 = REQ_CURRENT_TIME response style)
    # We'll use msg_id=49 (CURRENT_TIME) with one field
    msg_id = 49
    body = f"20250101 12:00:00\0".encode()
    raw = f"{msg_id}\0".encode() + body

    _dispatch_message(raw, dec, server_version)

    dec.interpret.assert_called_once()
    dec.processProtoBuf.assert_not_called()


def test_dispatch_ignores_oversized_messages():
    """Messages exceeding MAX_MSG_LEN should be silently dropped."""
    from ibapi.const import MAX_MSG_LEN
    dec = make_mock_decoder()

    big_msg = b"x" * (MAX_MSG_LEN + 1)
    _dispatch_message(big_msg, dec, 100)

    dec.interpret.assert_not_called()
    dec.processProtoBuf.assert_not_called()


# ------------------------------------------------------------------
# _read_loop unit tests
# ------------------------------------------------------------------

async def test_read_loop_dispatches_messages():
    """_read_loop should dispatch a complete framed message from the stream."""
    dec = make_mock_decoder()
    server_version_val = 100

    # Build a valid framed legacy message
    msg_id = 49
    body = b"20250101 12:00:00\0"
    text = f"{msg_id}\0".encode() + body
    framed = struct.pack(f"!I{len(text)}s", len(text), text)

    stream = MockStreamReader()
    stream.feed(framed)
    stream.feed_eof()

    connected = [True]

    async def on_disconnect():
        connected[0] = False

    await _read_loop(
        stream_reader=stream,
        decoder=dec,
        is_connected=lambda: connected[0],
        server_version=lambda: server_version_val,
        on_disconnect=on_disconnect,
    )

    dec.interpret.assert_called_once()


async def test_read_loop_handles_eof():
    """_read_loop should call on_disconnect() when the server closes the connection."""
    dec = make_mock_decoder()
    disconnected = False

    async def on_disconnect():
        nonlocal disconnected
        disconnected = True

    stream = MockStreamReader()
    stream.feed_eof()

    await _read_loop(
        stream_reader=stream,
        decoder=dec,
        is_connected=lambda: True,
        server_version=lambda: 100,
        on_disconnect=on_disconnect,
    )

    assert disconnected


async def test_read_loop_stops_when_not_connected():
    """_read_loop should exit cleanly when is_connected() returns False."""
    dec = make_mock_decoder()
    stream = MockStreamReader()

    # The loop checks is_connected() before each read; return False immediately
    call_count = [0]

    def is_connected():
        call_count[0] += 1
        return call_count[0] < 2  # True once, then False

    async def on_disconnect():
        pass

    # Feed a timeout-causing empty queue so the loop hits the is_connected check
    await asyncio.wait_for(
        _read_loop(
            stream_reader=stream,
            decoder=dec,
            is_connected=is_connected,
            server_version=lambda: 100,
            on_disconnect=on_disconnect,
        ),
        timeout=2.0,
    )
    # No crash = pass


async def test_read_loop_handles_fragmented_messages():
    """_read_loop should buffer incomplete messages and reassemble them."""
    dec = make_mock_decoder()

    # Build a valid framed message and split it in two chunks
    msg_id = 49
    body = b"20250101 12:00:00\0"
    text = f"{msg_id}\0".encode() + body
    framed = struct.pack(f"!I{len(text)}s", len(text), text)

    half = len(framed) // 2
    chunk1 = framed[:half]
    chunk2 = framed[half:]

    stream = MockStreamReader()
    stream.feed(chunk1)
    stream.feed(chunk2)
    stream.feed_eof()

    async def on_disconnect():
        pass

    await _read_loop(
        stream_reader=stream,
        decoder=dec,
        is_connected=lambda: True,
        server_version=lambda: 100,
        on_disconnect=on_disconnect,
    )

    dec.interpret.assert_called_once()
