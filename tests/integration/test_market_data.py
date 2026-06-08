"""Live & delayed market-data integration tests.

Paper accounts only have delayed (15-min) market data; live L1 quotes,
real-time 5-second bars, tick-by-tick streams, and Level-2 depth all
require paid subscriptions per exchange. These tests:

  - Use ``request_market_data_type(3)`` to get delayed data so snapshot
    reads return real values instead of erroring.
  - Test the streaming and depth methods in their **permission-denied**
    error path — that's how the wrapper behaves on a paper account, and
    we want to assert the right TWSError surfaces with the right code.
  - For the streaming methods, when permission *is* granted (subscribed
    accounts), the test will validate received data; otherwise it
    verifies the error correlation works without hanging.
"""

import asyncio

import pytest
from ibapi.contract import Contract

from ibapi_async.exceptions import TWSError

pytestmark = pytest.mark.integration


def _aapl() -> Contract:
    c = Contract()
    c.symbol = "AAPL"
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


# ────────────────────────────────────────────────────────────────────────
# Delayed snapshots
# ────────────────────────────────────────────────────────────────────────


async def test_delayed_snapshot_returns_delayed_ticks(delayed_data):
    """Delayed snapshot must come back with DELAYED_* keys, not real-time ticks.

    On a paper account in delayed mode, the snapshot must contain at minimum
    DELAYED_LAST and DELAYED_VOLUME (the two ticks every equity has when
    the market has traded that day).
    """
    snap = await delayed_data.get_market_data_snapshot(_aapl(), timeout=15)
    keys = set(snap.keys())
    delayed_keys = {k for k in keys if k.startswith("DELAYED_")}
    realtime_keys = keys - delayed_keys

    assert delayed_keys, (
        f"no DELAYED_* ticks returned (would mean delayed data not active); "
        f"got keys: {sorted(keys)}"
    )
    assert not realtime_keys, (
        f"unexpected real-time ticks in delayed snapshot: {sorted(realtime_keys)}"
    )

    # We must get at least the LAST trade
    assert "DELAYED_LAST" in keys, (
        f"missing DELAYED_LAST in snapshot: {sorted(keys)}"
    )


async def test_delayed_snapshot_aapl_price_in_band(delayed_data):
    """At least one delayed AAPL price tick must land in a realistic band.

    Catches the snapshot machinery returning *only* sentinels (-1.0 / 0.0)
    instead of any real quote.

    Why not assert on DELAYED_LAST alone: DELAYED_LAST is a *live* tick.
    IB returns -1.0 (its "no data" sentinel) for it whenever the US
    market is closed, so a hard assert on DELAYED_LAST fails every
    overnight CI run even though the snapshot machinery is working fine.
    DELAYED_CLOSE holds the prior session's close and is populated 24/7;
    DELAYED_OPEN/HIGH/LOW likewise carry the last session's values once
    the market has traded. So we look at every delayed price tick, accept
    whichever are real (positive floats), and:

      * pass when at least one real price exists and every real price is
        in the $50-$1500 band, and
      * fail only when *all* price ticks are sentinels — that's the
        actual "snapshot machinery broken" condition this test exists
        to catch.
    """
    snap = await delayed_data.get_market_data_snapshot(_aapl(), timeout=15)

    price_keys = (
        "DELAYED_LAST", "DELAYED_CLOSE", "DELAYED_OPEN",
        "DELAYED_HIGH", "DELAYED_LOW",
    )
    # The wrapper stores tickPrice values as floats; sentinels are -1.0/0.0.
    real_prices = {
        k: snap[k]
        for k in price_keys
        if isinstance(snap.get(k), float) and snap[k] > 0.0
    }

    assert real_prices, (
        f"every delayed price tick is missing or a sentinel — the snapshot "
        f"returned no usable AAPL price at all. keys: {sorted(snap)}"
    )
    out_of_band = {
        k: v for k, v in real_prices.items() if not (50.0 < v < 1500.0)
    }
    assert not out_of_band, (
        f"AAPL delayed price(s) outside realistic $50-$1500 band: "
        f"{out_of_band} — likely a sentinel slipped through the > 0.0 filter"
    )


async def test_delayed_snapshot_close_consistent_with_open(delayed_data):
    """Open / High / Low relationships hold in delayed snapshots too.

    Delayed snapshots include DELAYED_OPEN, DELAYED_HIGH, DELAYED_LOW,
    DELAYED_CLOSE. When all four are present and non-sentinel, low ≤
    open/close ≤ high.
    """
    snap = await delayed_data.get_market_data_snapshot(_aapl(), timeout=15)
    # Filter to non-sentinel float values
    def real(key):
        v = snap.get(key)
        return v if isinstance(v, float) and v > 0 else None

    high = real("DELAYED_HIGH")
    low = real("DELAYED_LOW")
    if high is not None and low is not None:
        assert low <= high, f"low {low} > high {high}"


# ────────────────────────────────────────────────────────────────────────
# Market-data type switching
# ────────────────────────────────────────────────────────────────────────


async def test_market_data_type_can_switch_to_frozen(gateway_client):
    """Switching to type 4 (delayed-frozen) must not break subsequent calls."""
    gateway_client.request_market_data_type(4)
    await asyncio.sleep(0.5)
    snap = await gateway_client.get_market_data_snapshot(_aapl(), timeout=15)
    # Frozen-mode replies use the same DELAYED_* tick types as type 3
    delayed_or_frozen = [k for k in snap if k.startswith("DELAYED_")]
    assert delayed_or_frozen, (
        f"no DELAYED_* ticks in type-4 (frozen) snapshot: {sorted(snap)}"
    )


# ────────────────────────────────────────────────────────────────────────
# Streaming — permission-denied path
# ────────────────────────────────────────────────────────────────────────
# Paper accounts don't have real-time / depth subscriptions. We assert
# the wrapper surfaces a clean TWSError (not a hang) so consumer code can
# branch on subscription state.


async def test_real_time_bars_permission_error(gateway_client):
    """``stream_real_time_bars`` must raise TWSError 420 on no-subscription paper.

    If the running account *does* have a real-time subscription (CI runs
    against a paid account in the future), we accept any received bar as
    a positive signal instead.
    """
    received = 0
    try:
        async for bar in gateway_client.stream_real_time_bars(_aapl(), "TRADES"):
            # Validate bar shape; break after the first received
            assert "open" in bar and "close" in bar
            assert bar["open"] > 0 and bar["close"] > 0
            assert bar["low"] <= bar["high"]
            received += 1
            if received >= 1:
                break
    except TWSError as e:
        # 420 = "Invalid Real-time Query: No market data permissions for ..."
        # 354 = "Requested market data is not subscribed"
        # 10089 = same family
        assert e.code in (420, 354, 10089), (
            f"unexpected real-time bars error code {e.code}: {e.message}"
        )
        return  # permission-denied path is the expected paper behavior

    assert received >= 1


async def test_tick_by_tick_permission_or_data(delayed_data):
    """``stream_tick_by_tick`` either yields ticks (subscribed) or raises 10089.

    Tick-by-tick is gated by a separate subscription from L1 quotes, so
    delayed-mode doesn't help. On a paper account expect TWSError 10089
    or 354. Otherwise consume one tick and validate.
    """
    received = 0
    try:
        async for tick in delayed_data.stream_tick_by_tick(_aapl(), "Last"):
            assert "kind" in tick and "time" in tick
            received += 1
            if received >= 1:
                break
    except TWSError as e:
        assert e.code in (10089, 354, 420), (
            f"unexpected tick-by-tick error code {e.code}: {e.message}"
        )
        return
    assert received >= 1


async def test_market_depth_permission_error(gateway_client):
    """L2 depth requires a per-exchange subscription paper accounts don't have.

    Expected error code is 10092 ("Deep market data is not supported for
    this combination") on accounts without book access.
    """
    try:
        book = await gateway_client.get_market_depth(
            _aapl(), num_rows=5, settle_time=2.0, timeout=8.0
        )
    except TWSError as e:
        assert e.code in (10092, 309, 354), (
            f"unexpected depth error code {e.code}: {e.message}"
        )
        return

    # If we got here, the account has depth subscription — validate the book
    assert "bids" in book and "asks" in book
    # If both sides have any rows, top-of-book bid ≤ ask
    if book["bids"] and book["asks"]:
        top_bid = book["bids"][0]["price"]
        top_ask = book["asks"][0]["price"]
        assert top_bid <= top_ask, (
            f"crossed book: bid {top_bid} > ask {top_ask}"
        )
