"""Smoke test: round-trip a single request against a live Gateway.

This is the smallest possible end-to-end exercise of the async stack:

    AsyncTWSClient.create()        — TCP open + API handshake
        → AsyncEClient.connect()   — sends version prefix, reads server version
        → _write_loop task         — drains the write queue
        → _read_loop task          — reads framed messages, dispatches to Decoder
    client.get_current_time()      — sync EClient.reqCurrentTime() enqueues the request
        → server replies           — bytes arrive on the StreamReader
        → _dispatch_message        — splits frame, calls Decoder.interpret(fields, msgId=49)
        → AsyncTWSClient.currentTime callback fires _set_event(0, "current_time", t)
    awaiting completes             — _wait_for_response returns the timestamp
    client.disconnect()            — cancels tasks, closes stream

If this test passes against a real Gateway, every layer of the wrapper is
known-good for read+write+request+callback. New integration tests should be
introduced one at a time on top of this baseline.
"""

import time

import pytest

pytestmark = pytest.mark.integration


async def test_get_current_time(gateway_client):
    """Fetch server time and confirm it's within ±60s of local clock."""
    server_time = await gateway_client.get_current_time()

    assert isinstance(server_time, int), f"expected int epoch seconds, got {type(server_time).__name__}"

    drift = abs(server_time - time.time())
    assert drift < 60, (
        f"server time {server_time} differs from local time by {drift:.1f}s "
        f"— likely a clock-sync issue on either the Gateway host or this machine"
    )
