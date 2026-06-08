"""
AsyncConnection: asyncio replacement for ibapi.connection.Connection.

Uses asyncio.StreamReader / StreamWriter instead of a blocking socket.
The sendMsg() method stays synchronous (puts bytes onto an asyncio.Queue)
so that EClient's 200+ request methods can call it without modification.
The write queue is drained to the TCP stream by AsyncEClient._write_loop().
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncConnection:
    """
    Thin async wrapper around asyncio TCP streams.

    sendMsg() is intentionally synchronous: it enqueues bytes for the
    _write_loop coroutine to drain, preserving EClient's sync call sites.

    recvMsg() is NOT used — reading is done directly via the StreamReader
    in the _read_loop coroutine. It raises NotImplementedError to catch
    accidental misuse.
    """

    def __init__(self, host: str, port: int, write_queue: asyncio.Queue) -> None:
        self.host = host
        self.port = port
        self._write_queue = write_queue
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self.wrapper = None  # set by AsyncEClient after construction

    async def connect(self) -> None:
        """Open the TCP connection to TWS / IB Gateway."""
        logger.debug("AsyncConnection: connecting to %s:%d", self.host, self.port)
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        logger.debug("AsyncConnection: connected")

    async def disconnect(self) -> None:
        """Close the TCP connection gracefully."""
        if self._writer is not None and not self._writer.is_closing():
            logger.debug("AsyncConnection: disconnecting")
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass  # already closed
            logger.debug("AsyncConnection: disconnected")
            if self.wrapper:
                self.wrapper.connectionClosed()
        self._reader = None
        self._writer = None

    def isConnected(self) -> bool:
        """Return True if the TCP stream is open and not closing."""
        return (
            self._writer is not None
            and not self._writer.is_closing()
        )

    def sendMsg(self, msg: bytes) -> int:
        """
        Enqueue bytes for the async write loop.

        Stays synchronous so EClient's request methods (placeOrder,
        reqMarketData, etc.) can call it without any await changes.

        NOTE: put_nowait() is safe when called from within the asyncio
        event loop thread (the normal case). If you ever call EClient
        request methods from a worker thread, use:
            loop.call_soon_threadsafe(self._write_queue.put_nowait, msg)
        """
        if not self.isConnected():
            logger.debug("sendMsg called while not connected — dropped")
            return 0
        self._write_queue.put_nowait(msg)
        logger.debug("sendMsg: enqueued %d bytes", len(msg))
        return len(msg)

    def recvMsg(self) -> bytes:
        """Not used in the async path. Reading is done in _read_loop."""
        raise NotImplementedError(
            "recvMsg() is not available on AsyncConnection. "
            "Reading is handled by the _read_loop coroutine."
        )
