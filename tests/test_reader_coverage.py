"""Coverage tests for reader.py: protobuf dispatch, parse errors, loop error/cancel paths."""

import asyncio
import pytest
from unittest.mock import MagicMock

from ibapi.common import PROTOBUF_MSG_ID
from ibapi.utils import BadMessage

from ibapi_async.reader import _dispatch_message, _read_loop
from tests.conftest import MockStreamReader


def make_mock_decoder():
    dec = MagicMock()
    dec.interpret = MagicMock()
    dec.processProtoBuf = MagicMock()
    return dec


# ------------------------------------------------------------------
# _dispatch_message — protobuf path + error swallowing
# ------------------------------------------------------------------

def test_dispatch_protobuf_message_routes_to_processprotobuf():
    """server_version >= protobuf threshold + 4-byte msgId > PROTOBUF_MSG_ID."""
    dec = make_mock_decoder()
    real_id = 5
    msg = (PROTOBUF_MSG_ID + real_id).to_bytes(4, "big") + b"payload"

    _dispatch_message(msg, dec, server_version=300)

    dec.processProtoBuf.assert_called_once()
    body, rid = dec.processProtoBuf.call_args[0]
    assert body == b"payload"
    assert rid == real_id
    dec.interpret.assert_not_called()


def test_dispatch_protobuf_framing_with_legacy_id_uses_interpret():
    """High server version parses the 4-byte prefix; msgId <= PROTOBUF_MSG_ID → legacy interpret."""
    dec = make_mock_decoder()
    msg = (5).to_bytes(4, "big") + b"a\0b\0"

    _dispatch_message(msg, dec, server_version=300)

    dec.interpret.assert_called_once()
    dec.processProtoBuf.assert_not_called()


def test_dispatch_bad_message_is_swallowed():
    """A BadMessage raised during dispatch is logged and swallowed, not raised."""
    dec = make_mock_decoder()
    dec.interpret.side_effect = BadMessage("bad")
    _dispatch_message(b"49\0x\0", dec, server_version=100)  # no exception = pass


def test_dispatch_unhandled_exception_is_swallowed():
    """A non-numeric legacy msgId raises ValueError, caught by the broad handler."""
    dec = make_mock_decoder()
    _dispatch_message(b"notanint\0body\0", dec, server_version=100)  # no exception = pass


# ------------------------------------------------------------------
# _read_loop — connection error / cancel / unhandled exception
# ------------------------------------------------------------------

async def test_read_loop_connection_error_disconnects():
    """A ConnectionResetError from the stream triggers on_disconnect and returns."""
    dec = make_mock_decoder()
    disconnected = []

    class ErrReader:
        async def read(self, n=-1):
            raise ConnectionResetError("reset")

    async def on_disconnect():
        disconnected.append(True)

    await _read_loop(
        stream_reader=ErrReader(),
        decoder=dec,
        is_connected=lambda: True,
        server_version=lambda: 100,
        on_disconnect=on_disconnect,
    )
    assert disconnected == [True]


async def test_read_loop_cancelled_reraises():
    """Cancelling the read-loop task propagates CancelledError."""
    dec = make_mock_decoder()

    class BlockReader:
        async def read(self, n=-1):
            await asyncio.sleep(10)
            return b""

    async def on_disconnect():
        pass

    task = asyncio.create_task(
        _read_loop(
            stream_reader=BlockReader(),
            decoder=dec,
            is_connected=lambda: True,
            server_version=lambda: 100,
            on_disconnect=on_disconnect,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_read_loop_unhandled_exception_disconnects():
    """An unexpected exception in the loop body triggers on_disconnect."""
    dec = make_mock_decoder()
    disconnected = []

    async def on_disconnect():
        disconnected.append(True)

    def boom():
        raise RuntimeError("boom")

    await _read_loop(
        stream_reader=MockStreamReader(),
        decoder=dec,
        is_connected=boom,
        server_version=lambda: 100,
        on_disconnect=on_disconnect,
    )
    assert disconnected == [True]
