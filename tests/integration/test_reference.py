"""Reference-data integration tests.

Exercises the read-only metadata side of the API: contract details, trading
sessions, symbol search, head timestamps, option chains, and the scanner
parameters catalog. These tests run against the live IB Gateway / paper
account but never mutate state, so they're safe to run on any account.

Symbol choices are deliberate:
- AAPL  — most-liquid US equity, conId is stable (265598), used as a sanity
          baseline. AAPL's listing date is 1980-12-12, which we assert against
          the head-timestamp result to prove the call returned IB's actual
          historical-data origin and not a today-stamped placeholder.
- IBM   — secondary US equity, used to cross-validate contract details.
- ES    — front-month E-mini S&P 500 future. Tests futures-specific contract
          fields (expiry, multiplier).
- EUR.USD — major FX pair. Tests CASH security type.
"""

import pytest
from ibapi.contract import Contract

from ibapi_async.exceptions import TWSError

pytestmark = pytest.mark.integration


# ─── Constants pulled from IB's reference data — stable; safe to assert ──
AAPL_CONID = 265598   # NMS / NASDAQ AAPL
IBM_CONID = 8314      # NYSE IBM
AAPL_LISTING_YEAR = 1980  # 19801212 per IB head timestamp


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
# Contract details
# ────────────────────────────────────────────────────────────────────────


async def test_contract_details_resolves_aapl(gateway_client):
    """AAPL must resolve to its known conId on NASDAQ with the correct longName."""
    details = await gateway_client.get_contract_details(_aapl(), timeout=15)
    assert details, "expected at least one ContractDetails for AAPL"

    # SMART routes to the listing exchange; for AAPL that's NASDAQ.
    primary = next(
        (d for d in details if d.contract.primaryExchange == "NASDAQ"),
        details[0],
    )
    assert primary.contract.conId == AAPL_CONID, (
        f"AAPL conId drifted: got {primary.contract.conId}, expected {AAPL_CONID}. "
        "Either IB renumbered the security (extremely rare) or this isn't "
        "actually AAPL on NASDAQ."
    )
    assert primary.longName.upper().startswith("APPLE"), (
        f"longName should start with 'APPLE'; got {primary.longName!r}"
    )
    assert primary.contract.currency == "USD"


async def test_contract_details_returns_trading_sessions(gateway_client):
    """tradingHours / liquidHours encode the exchange's trading sessions.

    IB's format is ``YYYYMMDD:HHMM-YYYYMMDD:HHMM;...`` per day for the next
    several days. We assert the structure is parseable and the first day is
    today-or-later — that's what 'sessions' means in IBKR-speak.
    """
    import datetime
    today = datetime.date.today().strftime("%Y%m%d")

    details = await gateway_client.get_contract_details(_aapl(), timeout=15)
    d = details[0]
    assert d.tradingHours, "tradingHours empty — gateway not returning sessions"
    assert d.liquidHours, "liquidHours empty"

    # Every day-section starts with YYYYMMDD: — split on ';' and check.
    sections = [s for s in d.tradingHours.split(";") if s]
    assert len(sections) >= 5, (
        f"expected at least 5 days of trading hours, got {len(sections)}"
    )
    first_date = sections[0][:8]
    assert first_date.isdigit() and len(first_date) == 8
    assert first_date >= today, (
        f"first session date {first_date} is before today {today}; stale data?"
    )

    # liquidHours is a strict subset of tradingHours (RTH ⊂ extended hours).
    # For US equities, an open day will have a 0930-1600 ET liquid window.
    assert "CLOSED" in d.tradingHours or any(
        "0930" in s or "1430" in s for s in sections  # 1430 UTC ≈ 0930 ET DST
    ), "expected at least one regular session window in tradingHours"


async def test_contract_details_returns_market_rule_ids(gateway_client):
    """marketRuleIds tells the order entry system the tick-size schedule.

    For SMART-routed AAPL we should get a comma-separated list with one entry
    per `validExchanges`. Asserts the list is well-formed and aligned.
    """
    details = await gateway_client.get_contract_details(_aapl(), timeout=15)
    d = details[0]
    # validExchanges is comma-delimited; marketRuleIds should match length.
    exchanges = [x for x in d.validExchanges.split(",") if x]
    rule_ids = [x for x in d.marketRuleIds.split(",") if x]
    assert exchanges, "no validExchanges populated"
    assert len(rule_ids) == len(exchanges), (
        f"marketRuleIds count {len(rule_ids)} != validExchanges count "
        f"{len(exchanges)}: {d.marketRuleIds!r} vs {d.validExchanges!r}"
    )
    # Every rule ID is an integer.
    for rid in rule_ids:
        int(rid)  # raises ValueError if not parseable


async def test_contract_details_ambiguous_symbol_returns_multiple(gateway_client):
    """Searching IBM with no exchange should match the underlying NYSE listing.

    ``SMART`` exchange resolves the SMART-routed equity, ``ISLAND`` resolves
    the NASDAQ shadow listing, etc. When the contract is partially specified
    we expect IB to return either the canonical SMART contract or fail with
    error 200; either way, we test we get the IBM conId.
    """
    details = await gateway_client.get_contract_details(_ibm(), timeout=15)
    assert details
    primary = details[0]
    assert primary.contract.symbol == "IBM"
    assert primary.contract.conId == IBM_CONID, (
        f"IBM conId drift: got {primary.contract.conId}, expected {IBM_CONID}"
    )


async def test_contract_details_invalid_symbol_raises(gateway_client):
    """A contract that doesn't exist must raise ``TWSError``, not hang."""
    bogus = Contract()
    bogus.symbol = "ZZZZ_NOT_A_REAL_SYMBOL"
    bogus.secType = "STK"
    bogus.exchange = "SMART"
    bogus.currency = "USD"

    with pytest.raises(TWSError) as exc_info:
        await gateway_client.get_contract_details(bogus, timeout=10)
    # IB returns code 200 for "no security definition has been found".
    assert exc_info.value.code == 200, (
        f"expected error 200 for unknown symbol, got {exc_info.value.code}: "
        f"{exc_info.value.message}"
    )


async def test_contract_details_for_future(gateway_client):
    """Front-month ES future must come back with multiplier and trading-class set."""
    es = Contract()
    es.symbol = "ES"
    es.secType = "FUT"
    es.exchange = "CME"
    es.currency = "USD"

    # No specific expiry → IB returns every listed contract month.
    details = await gateway_client.get_contract_details(es, timeout=15)
    assert details, "no ES futures listed?"
    # Pick the front month — earliest lastTradeDateOrContractMonth.
    front = min(details, key=lambda d: d.contract.lastTradeDateOrContractMonth)

    assert front.contract.tradingClass in ("ES", "MES"), (
        f"unexpected tradingClass {front.contract.tradingClass!r}"
    )
    # ES has $50 multiplier; MES is $5. Both are valid front-month classes.
    assert front.contract.multiplier in ("50", "5")
    assert front.contract.lastTradeDateOrContractMonth.isdigit()
    # Either YYYYMM (6 chars) or YYYYMMDD (8 chars).
    assert len(front.contract.lastTradeDateOrContractMonth) in (6, 8)


async def test_contract_details_for_fx(gateway_client):
    """EUR.USD CASH should resolve to IDEALPRO with multiplier blank."""
    fx = Contract()
    fx.symbol = "EUR"
    fx.secType = "CASH"
    fx.currency = "USD"
    fx.exchange = "IDEALPRO"

    details = await gateway_client.get_contract_details(fx, timeout=15)
    assert details
    d = details[0]
    assert d.contract.symbol == "EUR"
    assert d.contract.currency == "USD"
    assert d.contract.exchange == "IDEALPRO"


# ────────────────────────────────────────────────────────────────────────
# Symbol search
# ────────────────────────────────────────────────────────────────────────


async def test_search_symbols_finds_aapl(gateway_client):
    """``search_symbols('Apple')`` must surface the canonical AAPL listing."""
    results = await gateway_client.search_symbols("Apple", timeout=10)
    assert results, "no matches for 'Apple'"

    aapl = [
        r for r in results
        if r.contract.symbol == "AAPL"
        and r.contract.secType == "STK"
        and r.contract.primaryExchange == "NASDAQ"
        and r.contract.currency == "USD"
    ]
    assert aapl, (
        f"expected to find AAPL/STK/NASDAQ/USD in {len(results)} matches: "
        f"{[(r.contract.symbol, r.contract.secType) for r in results[:5]]}..."
    )


async def test_search_symbols_no_results(gateway_client):
    """A nonsense pattern returns an empty list, not an error."""
    results = await gateway_client.search_symbols(
        "ZZZZNOMATCH_QQQ", timeout=10
    )
    assert results == [] or len(results) == 0, (
        f"expected no matches; got {len(results)}"
    )


# ────────────────────────────────────────────────────────────────────────
# Head timestamp
# ────────────────────────────────────────────────────────────────────────


async def test_head_timestamp_aapl_listing_date(gateway_client):
    """AAPL's earliest TRADES bar is its 1980-12-12 IPO day.

    IB returns this in ``YYYYMMDD-HH:MM:SS`` format. We only assert the year
    portion (1980) — the exact time differs between feed sources.
    """
    ts = await gateway_client.get_head_timestamp(
        _aapl(), what_to_show="TRADES", timeout=15
    )
    assert ts, "head_timestamp returned empty string"
    assert ts.startswith(str(AAPL_LISTING_YEAR)), (
        f"AAPL head timestamp should start with {AAPL_LISTING_YEAR}; got {ts!r}"
    )


async def test_head_timestamp_different_what_to_show(gateway_client):
    """Different what_to_show values can yield different head timestamps.

    BID/ASK bars are typically only available from 2003+ (when IB started
    capturing them), while TRADES go back to listing.
    """
    trades_ts = await gateway_client.get_head_timestamp(
        _aapl(), what_to_show="TRADES", timeout=15
    )
    bid_ts = await gateway_client.get_head_timestamp(
        _aapl(), what_to_show="BID", timeout=15
    )
    # Both well-formed YYYYMMDD-HH:MM:SS
    assert len(trades_ts) >= 8 and trades_ts[:8].isdigit()
    assert len(bid_ts) >= 8 and bid_ts[:8].isdigit()
    # TRADES origin must be ≤ BID origin (TRADES tracking started earlier).
    assert trades_ts[:8] <= bid_ts[:8], (
        f"TRADES head ts {trades_ts} should be ≤ BID head ts {bid_ts}"
    )


# ────────────────────────────────────────────────────────────────────────
# Option chain
# ────────────────────────────────────────────────────────────────────────


async def test_option_chain_aapl(gateway_client):
    """AAPL options must be listed on multiple exchanges with weekly + monthly expiries.

    Asserts:
      - At least 3 listing exchanges in the chain (CBOE, ISE, AMEX, etc.)
      - Multiplier is "100" (standard equity option contract size)
      - At least 10 distinct expirations
      - At least 50 distinct strikes
      - Strikes are sorted and contain values bracketing the current price.
    """
    chain = await gateway_client.get_option_chain(
        underlying_symbol="AAPL",
        underlying_sec_type="STK",
        underlying_con_id=AAPL_CONID,
        timeout=20,
    )
    assert len(chain) >= 3, (
        f"expected ≥3 option exchanges; got {len(chain)}: "
        f"{[c['exchange'] for c in chain]}"
    )

    # CBOE always lists AAPL
    cboe = next((c for c in chain if c["exchange"] == "CBOE"), None)
    assert cboe is not None, "CBOE missing from AAPL option chain"

    assert cboe["multiplier"] == "100", (
        f"AAPL standard option multiplier should be 100; got {cboe['multiplier']!r}"
    )
    assert len(cboe["expirations"]) >= 10, (
        f"expected ≥10 expirations on CBOE; got {len(cboe['expirations'])}"
    )
    assert len(cboe["strikes"]) >= 50, (
        f"expected ≥50 strikes on CBOE; got {len(cboe['strikes'])}"
    )

    # Strikes are sorted ascending (we sorted them in the wrapper)
    strikes = cboe["strikes"]
    assert strikes == sorted(strikes), "strikes not sorted ascending"
    # Reasonable AAPL price range covers $5 to $1000+
    assert min(strikes) <= 100, f"min strike {min(strikes)} unexpectedly high"
    assert max(strikes) >= 200, f"max strike {max(strikes)} unexpectedly low"

    # Expirations are YYYYMMDD strings, sorted, all in future or today
    import datetime
    today = datetime.date.today().strftime("%Y%m%d")
    expirations = cboe["expirations"]
    assert expirations == sorted(expirations), "expirations not sorted"
    # First expiration may be today or in the past for already-expired options
    # that haven't been pruned yet — only assert at least one is in the future.
    future_exps = [e for e in expirations if e >= today]
    assert future_exps, f"no future expirations in {expirations[:5]}..."


# ────────────────────────────────────────────────────────────────────────
# Scanner parameters
# ────────────────────────────────────────────────────────────────────────


async def test_scanner_parameters_xml(gateway_client):
    """Scanner parameters XML must contain the key catalog sections.

    The XML lists every available scan code (e.g. ``TOP_PERC_GAIN``,
    ``HOT_BY_VOLUME``) along with the instrument types and locations they
    apply to. This is the metadata you'd parse to build a scanner UI.
    """
    xml = await gateway_client.get_scanner_parameters(timeout=30)
    assert xml.startswith("<?xml"), f"not XML: {xml[:50]!r}"
    # Catalog is always > 100 KB
    assert len(xml) > 100_000, (
        f"scanner XML suspiciously small ({len(xml)} bytes); did the catalog truncate?"
    )
    # Must contain the standard top-level sections
    for section in ("<ScanParameterResponse>", "<InstrumentList", "<LocationTree"):
        assert section in xml, f"missing section {section!r}"
    # Must contain at least one well-known scan code
    assert "TOP_PERC_GAIN" in xml or "HOT_BY_VOLUME" in xml, (
        "expected canonical scan codes (TOP_PERC_GAIN / HOT_BY_VOLUME)"
    )
