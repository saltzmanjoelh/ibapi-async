"""
AsyncEClient: asyncio replacement for ibapi.client.EClient I/O layer.

Subclasses EClient and overrides exactly 4 methods + adds _write_loop:
  1. connect()         — async TCP handshake, starts background tasks
  2. disconnect()      — cancels tasks, closes stream
  3. sendMsg()         — enqueues to asyncio.Queue (sync bridge)
  4. sendMsgProtoBuf() — same bridge for protobuf path

All 200+ EClient request methods (placeOrder, reqMarketData, etc.) are
inherited unchanged. They call self.sendMsg() / self.sendMsgProtoBuf()
which enqueue bytes that _write_loop drains to the TCP stream.
"""

import asyncio
import logging

from ibapi import comm, decoder
from ibapi.client import EClient
from ibapi.common import PROTOBUF_MSG_ID
from ibapi.const import NO_VALID_ID
from ibapi.server_versions import MIN_CLIENT_VER, MAX_CLIENT_VER, MIN_SERVER_VER_PROTOBUF
from ibapi.utils import currentTimeMillis
from ibapi.errors import CONNECT_FAIL

from ibapi_async.connection import AsyncConnection
from ibapi_async.reader import _read_loop

logger = logging.getLogger(__name__)


class AsyncEClient(EClient):
    """
    asyncio I/O layer for EClient.

    Replaces blocking socket + threading.Lock + EReader thread with:
      - asyncio.StreamReader / StreamWriter  (AsyncConnection)
      - asyncio.Queue + _write_loop task     (send path)
      - _read_loop coroutine task            (receive path)

    Usage: do not call connect_and_start() or run(). Instead:
        await client.connect(host, port, client_id)
        # background tasks are running; use await on AsyncTWSClient methods
        await client.disconnect()
    """

    def __init__(self, wrapper) -> None:
        super().__init__(wrapper)
        self._write_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._write_task: asyncio.Task | None = None
        self._read_task: asyncio.Task | None = None

    @property
    def _connection_lost(self) -> asyncio.Event:
        """Set when the connection drops (read-loop EOF → disconnect). Lets the
        handshake wait in ``AsyncTWSClient.create()`` fail fast instead of
        blocking for the full nextValidId timeout when the gateway closes the
        socket — e.g. after rejecting our clientId with error 326. Lazily
        created (mirrors ``_next_valid_id_received``) to avoid binding an Event
        to a loop at construction time."""
        if not hasattr(self, "_conn_lost_event"):
            self._conn_lost_event = asyncio.Event()
        return self._conn_lost_event

    # ------------------------------------------------------------------
    # Override 1 & 2: sendMsg / sendMsgProtoBuf  (the sync bridge)
    # ------------------------------------------------------------------

    def sendMsg(self, msgId: int, msg: str) -> None:
        """
        Build the wire frame and enqueue it for _write_loop.

        Signature matches EClient.sendMsg() exactly so all request
        methods (reqMarketData, placeOrder, etc.) call this transparently.
        """
        use_raw = (
            self.serverVersion() is not None
            and self.serverVersion() >= MIN_SERVER_VER_PROTOBUF
        )
        full_msg = comm.make_msg(msgId, use_raw, msg)
        logger.debug("sendMsg: enqueuing msgId=%d (%d bytes)", msgId, len(full_msg))
        if self.conn and self.conn.isConnected():
            self._write_queue.put_nowait(full_msg)
        else:
            logger.warning("sendMsg called while not connected — dropped")

    def sendMsgProtoBuf(self, msgId: int, msg: bytes) -> None:
        """Protobuf path — same bridge as sendMsg."""
        full_msg = comm.make_msg_proto(msgId + PROTOBUF_MSG_ID, msg)
        logger.debug("sendMsgProtoBuf: enqueuing msgId=%d (%d bytes)", msgId, len(full_msg))
        if self.conn and self.conn.isConnected():
            self._write_queue.put_nowait(full_msg)
        else:
            logger.warning("sendMsgProtoBuf called while not connected — dropped")

    # ------------------------------------------------------------------
    # Override 3: connect()
    # ------------------------------------------------------------------

    async def connect(self, host: str, port: int, clientId: int) -> None:
        """
        Async TCP handshake — replaces EClient.connect().

        1. Opens the TCP stream via AsyncConnection.
        2. Sends the API version prefix (v{MIN}..{MAX}).
        3. Reads back the server version + connection time.
        4. Sets CONNECTED state, creates Decoder.
        5. Starts _write_loop and _read_loop as asyncio Tasks.
        6. Calls startApi() (enqueues START_API via write queue).
        """
        # Basic validation (mirrors EClient.connect checks)
        self.host = host
        self.port = port
        self._connection_lost.clear()  # fresh handshake — clear any prior drop
        self.clientId = clientId

        self.conn = AsyncConnection(host, port, self._write_queue)
        self.conn.wrapper = self.wrapper

        try:
            await self.conn.connect()
        except OSError as exc:
            logger.error("Could not connect to %s:%d — %s", host, port, exc)
            if self.wrapper:
                self.wrapper.error(
                    NO_VALID_ID, currentTimeMillis(),
                    CONNECT_FAIL.code(), CONNECT_FAIL.msg()
                )
            # Wake create()'s handshake waiter so it fails fast instead of
            # blocking for the full handshake timeout. This mirrors the
            # read-loop-EOF path (which signals via disconnect()); we can't rely
            # on disconnect() here because connState is still DISCONNECTED at
            # this point and disconnect() would early-return without setting the
            # event — so set it directly.
            self._connection_lost.set()
            return

        self.setConnState(EClient.CONNECTING)

        # Build and send the version handshake.
        # We cap the upper bound at 200 — one below MIN_SERVER_VER_PROTOBUF
        # (201) — to force legacy null-terminated msgId framing on every
        # response. Without this cap the server picks v203 and switches to
        # the 4-byte big-endian protobuf framing; that path reproducibly
        # stalls handshake response delivery to AsyncTWSClient on Linux
        # CPython 3.11/3.12 (CI), even though structurally-identical
        # asyncio probes from the same shell read the bytes within ~50ms.
        # Local macOS Python 3.14 happens to mask the bug. Capping at 200
        # is a behavioural rollback only — none of the protobuf-only
        # request types are exercised by this codebase yet.
        max_negotiated = min(MAX_CLIENT_VER, MIN_SERVER_VER_PROTOBUF - 1)
        v100prefix = b"API\0"
        v100version = f"v{MIN_CLIENT_VER}..{max_negotiated}"
        if self.connectOptions:
            v100version = v100version + " " + self.connectOptions
        handshake = v100prefix + comm.make_initial_msg(v100version)
        logger.debug("Sending handshake: %s", handshake)

        writer = self.conn._writer
        writer.write(handshake)
        await writer.drain()

        # Read server version (may arrive in fragments; loop until we have 2 fields)
        reader_stream = self.conn._reader
        raw = b""
        fields = []
        while len(fields) != 2:
            chunk = await asyncio.wait_for(reader_stream.read(4096), timeout=10.0)
            if not chunk:
                logger.error("Connection closed during handshake")
                await self.disconnect()
                return
            raw += chunk
            (size, msg, rest) = comm.read_msg(raw)
            if msg:
                fields = comm.read_fields(msg)
                raw = rest

        server_version, conn_time = int(fields[0]), fields[1]
        logger.debug("Server version: %d  conn_time: %s", server_version, conn_time)

        self.connTime = conn_time
        self.serverVersion_ = server_version
        self.decoder = decoder.Decoder(self.wrapper, self.serverVersion())
        self.setConnState(EClient.CONNECTED)

        # Start background tasks
        self._write_task = asyncio.create_task(
            self._write_loop(), name="ibapi-write-loop"
        )
        self._read_task = asyncio.create_task(
            _read_loop(
                stream_reader=reader_stream,
                decoder=self.decoder,
                is_connected=self.isConnected,
                server_version=self.serverVersion,
                on_disconnect=self.disconnect,
            ),
            name="ibapi-read-loop",
        )

        # Send START_API (goes through write queue → _write_loop)
        self.startApi()
        self.wrapper.connectAck()
        logger.info("Connected to TWS/Gateway at %s:%d (server v%d)", host, port, server_version)

    # ------------------------------------------------------------------
    # Override 4: disconnect()
    # ------------------------------------------------------------------

    async def disconnect(self) -> None:  # type: ignore[override]
        """
        Cancel background tasks and close the TCP stream.

        Safe to call multiple times.
        """
        if self.connState == EClient.DISCONNECTED:
            return

        self.setConnState(EClient.DISCONNECTED)
        self._connection_lost.set()  # wake any handshake waiter (create())
        logger.info("Disconnecting from TWS/Gateway")

        # Cancel tasks
        for task in (self._write_task, self._read_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        self._write_task = None
        self._read_task = None

        if self.conn is not None:
            await self.conn.disconnect()

        self.reset()

    # ------------------------------------------------------------------
    # run() guard
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Not used in async mode. Call await connect() instead."""
        raise RuntimeError(
            "AsyncEClient.run() is not available. "
            "Use `await client.connect(host, port, client_id)` to start the async I/O loop."
        )

    # ------------------------------------------------------------------
    # _write_loop: drains the write queue to the TCP stream
    # ------------------------------------------------------------------

    async def _write_loop(self) -> None:
        """
        Drain the write queue to the TCP StreamWriter.

        Runs as a background asyncio Task. Applies backpressure via
        writer.drain() so large messages don't overwhelm the TCP buffer.
        """
        assert self.conn is not None and self.conn._writer is not None
        writer = self.conn._writer

        logger.debug("_write_loop: started")
        try:
            while self.isConnected():
                try:
                    msg = await asyncio.wait_for(
                        self._write_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                try:
                    writer.write(msg)
                    await writer.drain()
                    self._write_queue.task_done()
                    logger.debug("_write_loop: sent %d bytes", len(msg))
                except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                    logger.error("_write_loop: write error — %s", exc)
                    await self.disconnect()
                    return

        except asyncio.CancelledError:
            # Drain any remaining messages before exiting (best-effort)
            logger.debug("_write_loop: cancelled — flushing remaining messages")
            while not self._write_queue.empty():
                try:
                    msg = self._write_queue.get_nowait()
                    if not writer.is_closing():
                        writer.write(msg)
                except (asyncio.QueueEmpty, Exception):
                    break
            try:
                if not writer.is_closing():
                    await writer.drain()
            except Exception:
                pass
            raise

        logger.debug("_write_loop: finished")
