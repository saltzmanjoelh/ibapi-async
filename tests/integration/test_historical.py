"""Historical-data integration tests.

Bars across multiple resolutions, what-to-show modes, RTH vs extended
hours, historical ticks, and the price-volume histogram. All read-only.

These tests pin to known historical events / known reference contracts so
the assertions stay meaningful as time moves forward — e.g. a daily bar
for IBM dated 20260508 must match the *actual* IBM daily bar IB has on
file, not just any non-empty list.
"""

from decimal import Decimal

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


def _ibm() -> Contract:
    c = Contract()
    c.symbol = "IBM"
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


# ────────────────────────────────────────────────────────────────────────
# Daily bars
# ────────────────────────────────────────────────────────────────────────


async def test_daily_bars_one_month(gateway_client):
    """1 month of IBM daily bars: should be ~22 trading days, OHLC self-consistent.

    Each bar must satisfy:  low ≤ open, close ≤ high  AND  low ≤ high.
    Bars must be in chronological order. Date strings are YYYYMMDD.
    Volume is positive.
    """
    bars = await gateway_client.get_historical_data(
        _ibm(),
        end_date_time="",
        duration_str="1 M",
        bar_size_setting="1 day",
        what_to_show="TRADES",
        use_rth=True,
        timeout=20,
    )
    # 21–23 US trading days in any 1-month window
    assert 18 <= len(bars) <= 25, (
        f"expected ~22 daily bars in 1 month, got {len(bars)}"
    )

    prev_date = ""
    for bar in bars:
        # Date is YYYYMMDD (RTH-only daily bars)
        assert len(bar.date) == 8 and bar.date.isdigit(), (
            f"unexpected daily date format: {bar.date!r}"
        )
        assert bar.date > prev_date, (
            f"bars not in chronological order: {prev_date} → {bar.date}"
        )
        prev_date = bar.date

        # OHLC consistency
        assert bar.low <= bar.high
        assert bar.low <= bar.open <= bar.high, (
            f"open {bar.open} outside [low {bar.low}, high {bar.high}] on {bar.date}"
        )
        assert bar.low <= bar.close <= bar.high, (
            f"close {bar.close} outside [low {bar.low}, high {bar.high}] on {bar.date}"
        )
        # IBM has been in the $50–$500 band for a long time
        assert 30 < bar.close < 1000
        # Volume must be positive on a regular trading day
        assert bar.volume > 0, f"zero volume on {bar.date}"


async def test_daily_bars_known_reference_date(gateway_client):
    """Hard reference: IBM closed at 229.76 on 2026-05-08 (per probe).

    Pulling 1 day of data ending on that date must surface that bar with
    matching close price (within ¢-level rounding).
    """
    bars = await gateway_client.get_historical_data(
        _ibm(),
        end_date_time="20260508 23:59:59 US/Eastern",
        duration_str="1 D",
        bar_size_setting="1 day",
        what_to_show="TRADES",
        use_rth=True,
        timeout=20,
    )
    assert bars, "no bars returned for 2026-05-08"
    bar = bars[-1]  # last bar is the 5/8 close
    assert bar.date == "20260508", f"expected 20260508; got {bar.date!r}"
    # Close was 229.76; allow ±0.5 for end-of-day adjustments / dividend strips
    assert 229.0 <= bar.close <= 230.5, (
        f"IBM 2026-05-08 close drifted: got {bar.close}, expected ≈229.76"
    )


# ────────────────────────────────────────────────────────────────────────
# Intraday bars
# ────────────────────────────────────────────────────────────────────────


async def test_intraday_5min_bars_count(gateway_client):
    """5-minute RTH bars on a single trading day: 78 bars (390 min / 5).

    Pulling 1 trading day of 5-minute bars on a known open day (Friday
    2026-05-08) must yield exactly 78 bars when use_rth=True.
    """
    bars = await gateway_client.get_historical_data(
        _aapl(),
        end_date_time="20260508 16:00:00 US/Eastern",  # market close
        duration_str="1 D",
        bar_size_setting="5 mins",
        what_to_show="TRADES",
        use_rth=True,
        timeout=20,
    )
    # 6.5h × 12 = 78 bars exactly
    assert 75 <= len(bars) <= 80, (
        f"expected ~78 5-min RTH bars on a US trading day, got {len(bars)}"
    )
    # Bar dates contain time component (epoch seconds as string in newer
    # ibapi versions, or "YYYYMMDD HH:MM:SS" in older).
    first, last = bars[0], bars[-1]
    # Spot OHLC consistency on first and last
    assert first.low <= first.high
    assert last.low <= last.high
    # Each bar should have non-zero volume during RTH (allow occasional
    # zero-volume bars at the open/close auctions)
    nonzero = sum(1 for b in bars if b.volume > 0)
    assert nonzero >= len(bars) - 5, (
        f"too many zero-volume bars: only {nonzero}/{len(bars)} had volume"
    )


async def test_extended_hours_returns_more_bars(gateway_client):
    """useRTH=False must yield more bars than useRTH=True for the same window.

    Pre-market starts at 4:00 ET, after-hours runs to 20:00 ET → 16h ×
    12 = 192 bars vs 78 RTH bars.
    """
    rth = await gateway_client.get_historical_data(
        _aapl(),
        end_date_time="20260508 20:00:00 US/Eastern",
        duration_str="1 D",
        bar_size_setting="5 mins",
        what_to_show="TRADES",
        use_rth=True,
        timeout=20,
    )
    eth = await gateway_client.get_historical_data(
        _aapl(),
        end_date_time="20260508 20:00:00 US/Eastern",
        duration_str="1 D",
        bar_size_setting="5 mins",
        what_to_show="TRADES",
        use_rth=False,
        timeout=20,
    )
    assert len(eth) > len(rth), (
        f"useRTH=False should yield more bars; got rth={len(rth)} eth={len(eth)}"
    )
    # ETH should be >2× RTH (16h vs 6.5h)
    assert len(eth) >= len(rth) * 2, (
        f"ETH count {len(eth)} unexpectedly close to RTH count {len(rth)}"
    )


# ────────────────────────────────────────────────────────────────────────
# Different what_to_show modes
# ────────────────────────────────────────────────────────────────────────


async def test_midpoint_bars_have_zero_volume(gateway_client):
    """MIDPOINT bars are derived from bid/ask only — volume should be 0."""
    bars = await gateway_client.get_historical_data(
        _aapl(),
        end_date_time="20260508 16:00:00 US/Eastern",
        duration_str="1 D",
        bar_size_setting="5 mins",
        what_to_show="MIDPOINT",
        use_rth=True,
        timeout=20,
    )
    assert bars, "no MIDPOINT bars returned"
    # MIDPOINT volume is always zero — these are quote-derived bars
    nonzero = [b for b in bars if b.volume > 0]
    assert not nonzero, (
        f"MIDPOINT bars should have zero volume, found {len(nonzero)} non-zero"
    )
    # Prices still self-consistent (low ≤ open/close ≤ high)
    for bar in bars:
        assert bar.low <= bar.high


async def test_bid_bars_are_below_ask_bars(gateway_client):
    """For the same time window, BID bar close ≤ ASK bar close (approximately).

    Spread is positive on average. We compare medians of close prices to
    sidestep the rare locked-market cross.
    """
    end = "20260508 16:00:00 US/Eastern"
    bid = await gateway_client.get_historical_data(
        _aapl(), end_date_time=end, duration_str="1 D",
        bar_size_setting="5 mins", what_to_show="BID",
        use_rth=True, timeout=20,
    )
    ask = await gateway_client.get_historical_data(
        _aapl(), end_date_time=end, duration_str="1 D",
        bar_size_setting="5 mins", what_to_show="ASK",
        use_rth=True, timeout=20,
    )
    assert bid and ask
    bid_closes = sorted(b.close for b in bid)
    ask_closes = sorted(b.close for b in ask)
    bid_med = bid_closes[len(bid_closes) // 2]
    ask_med = ask_closes[len(ask_closes) // 2]
    assert bid_med <= ask_med, (
        f"median BID close {bid_med} > median ASK close {ask_med} — spread inverted?"
    )


# ────────────────────────────────────────────────────────────────────────
# Error paths
# ────────────────────────────────────────────────────────────────────────


async def test_historical_data_invalid_duration_raises(gateway_client):
    """A bogus duration must surface as ``TWSError``, not a hung wait."""
    with pytest.raises(TWSError) as exc_info:
        await gateway_client.get_historical_data(
            _aapl(),
            end_date_time="",
            duration_str="999 BANANAS",  # invalid unit
            bar_size_setting="1 day",
            what_to_show="TRADES",
            use_rth=True,
            timeout=10,
        )
    # IB error code for bad historical-data parameters is in the 162..200 range.
    # Different gateway versions return slightly different codes:
    # 162 = "historical market data Service error message: HMDS"
    # 200 = "no security definition found"
    # 321 = "Error reading request"
    assert exc_info.value.code in (162, 200, 321, 366, 102), (
        f"unexpected error code for bad duration: {exc_info.value.code} "
        f"({exc_info.value.message})"
    )


# ────────────────────────────────────────────────────────────────────────
# Historical ticks
# ────────────────────────────────────────────────────────────────────────


async def test_historical_ticks_during_market_hours(gateway_client):
    """Pulling AAPL ticks during a known open-market period must yield > 0 ticks.

    We anchor at Friday 2026-05-08 14:00 ET (RTH). Asserts:
      - ≥ 50 ticks returned
      - All tick times fall within a sensible window of the start time
      - All prices are positive AAPL-range values
    """
    ticks = await gateway_client.get_historical_ticks(
        _aapl(),
        start_date_time="20260508 14:00:00 US/Eastern",
        number_of_ticks=100,
        what_to_show="TRADES",
        timeout=15,
    )
    assert len(ticks) >= 50, f"expected ≥50 ticks, got {len(ticks)}"

    # Tick times are unix epochs (int seconds). 2026-05-08 14:00 ET = ~1778263200.
    start_epoch = 1778263200
    for t in ticks:
        # Tick should be at or after our start time, within the same trading day
        assert t.time >= start_epoch - 60, (
            f"tick time {t.time} predates request start {start_epoch}"
        )
        assert t.time < start_epoch + 86_400, (
            f"tick time {t.time} more than a day after start"
        )
        # AAPL price band — anything outside this is a data error
        assert 50 < t.price < 1000, f"unrealistic AAPL tick price {t.price}"

    # Ticks must be in chronological order
    for prev, curr in zip(ticks, ticks[1:]):
        assert prev.time <= curr.time


async def test_historical_ticks_quoted_bid_ask(gateway_client):
    """``BID_ASK`` ticks include both prices; spread is non-negative on average."""
    ticks = await gateway_client.get_historical_ticks(
        _aapl(),
        start_date_time="20260508 14:00:00 US/Eastern",
        number_of_ticks=50,
        what_to_show="BID_ASK",
        timeout=15,
    )
    assert ticks, "no BID_ASK ticks returned"

    # Each tick has priceBid and priceAsk attributes
    spreads = []
    for t in ticks:
        assert hasattr(t, "priceBid") and hasattr(t, "priceAsk"), (
            f"tick missing bid/ask attributes: {dir(t)}"
        )
        if t.priceBid > 0 and t.priceAsk > 0:
            spreads.append(t.priceAsk - t.priceBid)
    assert spreads, "no ticks with valid bid AND ask prices"
    # The median spread is non-negative
    spreads.sort()
    median_spread = spreads[len(spreads) // 2]
    assert median_spread >= 0, f"median spread negative: {median_spread}"
    # And bounded: AAPL spread shouldn't exceed $1 in active hours
    assert median_spread < 1.0, f"unrealistic median spread {median_spread}"


# ────────────────────────────────────────────────────────────────────────
# Histogram
# ────────────────────────────────────────────────────────────────────────


async def test_histogram_3_months(gateway_client):
    """Histogram of AAPL price → traded-volume over the last 3 months.

    Must contain hundreds of bins, prices are positive AAPL-range, sizes
    are positive Decimals. Sum of bin sizes should be substantial (millions
    of shares over a quarter).
    """
    items = await gateway_client.get_histogram_data(
        _aapl(), period="3 months", timeout=30
    )
    assert len(items) >= 100, f"too few histogram bins: {len(items)}"

    # Every bin must have positive price and size
    total_size = Decimal(0)
    for item in items:
        assert item.price > 0, f"non-positive price bin: {item.price}"
        assert item.size > 0, f"non-positive size bin: {item.size}"
        # AAPL band sanity check
        assert 50 < item.price < 1000
        total_size += Decimal(item.size)

    # IB reports the histogram in a downsampled unit (not raw shares — empirically
    # the totals come back ~1 000× smaller than aggregate volume). We just check
    # the total is non-trivial relative to bin count.
    assert total_size > len(items) * 100, (
        f"3-month total histogram volume {total_size} suspiciously low for "
        f"{len(items)} bins"
    )


async def test_histogram_short_period(gateway_client):
    """1-week histogram has fewer bins than 3-month histogram (less price coverage).

    Validates the period parameter actually changes the result.
    """
    week = await gateway_client.get_histogram_data(
        _aapl(), period="1 week", timeout=30
    )
    quarter = await gateway_client.get_histogram_data(
        _aapl(), period="3 months", timeout=30
    )
    # 1 week of price action covers a narrower band than 3 months
    assert len(week) < len(quarter), (
        f"1-week bins {len(week)} should be < 3-month bins {len(quarter)}"
    )
    assert week, "empty 1-week histogram"


# ────────────────────────────────────────────────────────────────────────
# Head timestamp ↔ historical-data alignment
# ────────────────────────────────────────────────────────────────────────


async def test_head_timestamp_predates_historical_data(gateway_client):
    """Earliest historical bar must NOT predate the head timestamp.

    Pull the head timestamp, then ask for bars going back further than
    the head timestamp — IB should clamp to the actual earliest data.
    """
    head_ts = await gateway_client.get_head_timestamp(
        _ibm(), what_to_show="TRADES", timeout=15
    )
    # Asking for 50 years of monthly bars way exceeds IBM's history
    bars = await gateway_client.get_historical_data(
        _ibm(),
        end_date_time="",
        duration_str="50 Y",
        bar_size_setting="1 month",
        what_to_show="TRADES",
        use_rth=True,
        timeout=30,
    )
    assert bars, "no monthly bars returned"
    earliest = bars[0].date  # YYYYMM or YYYYMMDD
    # Head timestamp is YYYYMMDD-HH:MM:SS; compare year portions
    head_year = head_ts[:4]
    earliest_year = earliest[:4]
    assert earliest_year >= head_year, (
        f"historical bars start {earliest_year} predates head ts year {head_year}"
    )
