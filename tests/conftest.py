"""
Shared test fixtures for ibapi-async tests.

Provides mock asyncio stream pairs for unit testing without a real TWS connection.
"""

import asyncio
import struct
import pytest

from ibapi import comm
from ibapi.server_versions import MIN_CLIENT_VER, MAX_CLIENT_VER


def make_handshake_response(server_version: int = 176, conn_time: str = "20250101 12:00:00") -> bytes:
    """
    Build the bytes TWS would send back as the initial handshake response.
    Format: length-prefixed message containing two null-terminated fields:
      <server_version>\0<conn_time>\0
    """
    body = f"{server_version}\0{conn_time}\0".encode()
    return struct.pack(f"!I{len(body)}s", len(body), body)


def make_legacy_message(msg_id: int, *fields) -> bytes:
    """Build a legacy (non-protobuf) IB wire message for testing."""
    body = "\0".join(str(f) for f in fields) + "\0"
    text = f"{msg_id}\0{body}".encode()
    return struct.pack(f"!I{len(text)}s", len(text), text)


class MockStreamReader:
    """Async stream reader backed by a bytes queue."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._eof = False

    def feed(self, data: bytes) -> None:
        self._queue.put_nowait(data)

    def feed_eof(self) -> None:
        self._eof = True
        self._queue.put_nowait(b"")

    async def read(self, n: int = -1) -> bytes:
        if self._queue.empty() and self._eof:
            return b""
        data = await self._queue.get()
        return data


class MockStreamWriter:
    """Collects everything written to it."""

    def __init__(self) -> None:
        self.written: list[bytes] = []
        self._closing = False

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self._closing = True

    async def wait_closed(self) -> None:
        pass

    def is_closing(self) -> bool:
        return self._closing

    @property
    def all_written(self) -> bytes:
        return b"".join(self.written)


@pytest.fixture
def mock_streams():
    """Return a (MockStreamReader, MockStreamWriter) pair."""
    return MockStreamReader(), MockStreamWriter()
