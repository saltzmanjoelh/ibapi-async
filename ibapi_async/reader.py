"""
_read_loop: asyncio replacement for ibapi.reader.EReader thread.

Runs as a background asyncio Task. Reads bytes from asyncio.StreamReader,
applies ibapi's length-prefix framing (comm.read_msg), then dispatches
each message to the Decoder inline (no intermediate queue needed).

The dispatch logic mirrors EClient.run() lines 569-620 exactly.
"""

import asyncio
import logging
from typing import Awaitable, Callable

from ibapi import comm, decoder as ibapi_decoder
from ibapi.common import PROTOBUF_MSG_ID
from ibapi.const import MAX_MSG_LEN, NO_VALID_ID
from ibapi.errors import BAD_LENGTH
from ibapi.server_versions import MIN_SERVER_VER_PROTOBUF
from ibapi.utils import BadMessage, currentTimeMillis

logger = logging.getLogger(__name__)


def _dispatch_message(
    msg: bytes,
    dec: ibapi_decoder.Decoder,
    server_version: int,
) -> None:
    """
    Parse a single framed message and route it to the Decoder.

    Mirrors EClient.run()'s dispatch block (lines 590-608 of client.py):
      - legacy path: null-terminated ASCII msgId → Decoder.interpret()
      - protobuf path: 4-byte big-endian msgId → Decoder.processProtoBuf()
    """
    if len(msg) > MAX_MSG_LEN:
        logger.error(
            "_dispatch_message: message too long (%d bytes) — dropping", len(msg)
        )
        return

    try:
        if server_version >= MIN_SERVER_VER_PROTOBUF:
            # 4-byte big-endian msgId prefix
            s_msg_id = msg[:4]
            msg_id = int.from_bytes(s_msg_id, "big")
            body = msg[4:]
        else:
            # Null-terminated ASCII msgId prefix (legacy)
            null_pos = msg.index(b"\0")
            s_msg_id = msg[:null_pos]
            body = msg[null_pos + 1:]
            msg_id = int(s_msg_id)

        if msg_id > PROTOBUF_MSG_ID:
            # Protobuf message
            real_id = msg_id - PROTOBUF_MSG_ID
            logger.debug("_dispatch_message: protobuf msgId=%d", real_id)
            dec.processProtoBuf(body, real_id)
        else:
            # Legacy message
            fields = comm.read_fields(body)
            logger.debug("_dispatch_message: legacy msgId=%d fields=%s", msg_id, fields)
            dec.interpret(fields, msg_id)

    except BadMessage:
        logger.warning("_dispatch_message: BadMessage — skipping")
    except Exception:
        logger.exception("_dispatch_message: unhandled exception")


async def _read_loop(
    stream_reader: asyncio.StreamReader,
    decoder: ibapi_decoder.Decoder,
    is_connected: Callable[[], bool],
    server_version: Callable[[], int],
    on_disconnect: Callable[[], Awaitable[None]],
) -> None:
    """
    Continuously read from the TCP stream and dispatch decoded messages.

    Replaces EReader(Thread) + EClient.run() message dispatch.
    Runs as an asyncio.Task started inside AsyncEClient.connect().

    Args:
        stream_reader:  asyncio.StreamReader from the open TCP connection.
        decoder:        ibapi.decoder.Decoder instance (stateful).
        is_connected:   Callable returning True while the client is live.
        server_version: Callable returning the negotiated server version.
        on_disconnect:  Async callable to trigger graceful shutdown.
    """
    logger.debug("_read_loop: started")
    buf = b""

    try:
        while is_connected():
            try:
                chunk = await asyncio.wait_for(
                    stream_reader.read(4096), timeout=1.0
                )
            except asyncio.TimeoutError:
                # No data in 1 second — check is_connected() and loop
                continue
            except (ConnectionResetError, EOFError, OSError) as exc:
                logger.error("_read_loop: connection error — %s", exc)
                await on_disconnect()
                return

            if not chunk:
                # EOF — server closed the connection
                logger.info("_read_loop: EOF from server — disconnecting")
                await on_disconnect()
                return

            buf += chunk
            logger.debug("_read_loop: received %d bytes (buf=%d)", len(chunk), len(buf))

            # Extract all complete messages from the buffer
            while buf:
                size, msg, buf = comm.read_msg(buf)
                if not msg:
                    # Need more bytes to complete the next message
                    logger.debug("_read_loop: incomplete message — waiting for more data")
                    break

                _dispatch_message(msg, decoder, server_version())

    except asyncio.CancelledError:
        logger.debug("_read_loop: cancelled")
        raise
    except Exception:
        logger.exception("_read_loop: unhandled exception")
        await on_disconnect()

    logger.debug("_read_loop: finished")
