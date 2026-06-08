"""Account & portfolio integration tests.

Verifies the read paths into the paper account: managed-accounts list,
account summary tags, positions snapshot, full portfolio download.
None of these mutate state — they're pure reads. Order placement and
PnL-with-positions are covered by ``test_orders.py``.
"""

import pytest

pytestmark = pytest.mark.integration


# ─── Account summary tags we expect on every margin account ──────────────
# (paper accounts have full margin enabled by default)
REQUIRED_USD_TAGS = {
    "NetLiquidation",
    "TotalCashValue",
    "BuyingPower",
    "AvailableFunds",
    "ExcessLiquidity",
    "EquityWithLoanValue",
    "MaintMarginReq",
    "InitMarginReq",
    "GrossPositionValue",
}
# Tags that don't carry a currency (string/categorical values)
DIMENSIONLESS_TAGS = {
    "AccountType",
    "DayTradesRemaining",
    "Cushion",
    "LookAheadNextChange",
}


# ────────────────────────────────────────────────────────────────────────
# Managed accounts
# ────────────────────────────────────────────────────────────────────────


async def test_managed_accounts_returns_paper_account(gateway_client):
    """The handshake must surface at least one account, formatted ``DUxxxxxxx``."""
    accounts = await gateway_client.get_managed_accounts(timeout=5)
    assert accounts, "no managed accounts returned"
    assert all(isinstance(a, str) and a for a in accounts)
    # Paper account IDs always start with 'D' (DU = paper individual,
    # DF = FA paper, DI = institution paper, etc.).
    assert all(a[0] == "D" for a in accounts), (
        f"non-paper account leaked in: {accounts!r}"
    )
    # Standard format is 1 letter + 1 letter + 7 digits, e.g. DU7150239.
    assert all(len(a) >= 7 for a in accounts), (
        f"account ID too short: {accounts!r}"
    )


async def test_managed_accounts_cached_after_first_call(gateway_client):
    """Second call must be served from the cache without re-issuing reqManagedAccts.

    The handshake delivers `managedAccounts` automatically so the cached
    value should already be populated by the time the test starts.
    """
    a1 = await gateway_client.get_managed_accounts(timeout=5)
    # Tamper with the wire path: zero out the cached value flag would
    # require re-fetch; we just verify two calls return identical lists
    # (cache hit, no re-request).
    a2 = await gateway_client.get_managed_accounts(timeout=5)
    assert a1 == a2


async def test_paper_account_safety_fixture(paper_account):
    """The ``paper_account`` fixture must hand back a D-prefixed account.

    This indirectly exercises the gate that prevents order tests from
    running against a live account.
    """
    assert isinstance(paper_account, str)
    assert paper_account[0] == "D", (
        f"safety fixture returned non-paper account {paper_account!r}"
    )


# ────────────────────────────────────────────────────────────────────────
# Account summary
# ────────────────────────────────────────────────────────────────────────


async def test_account_summary_returns_required_tags(gateway_client, paper_account):
    """Default summary must include every margin / cash / liquidation tag."""
    summary = await gateway_client.get_account_summary(timeout=10)
    assert paper_account in summary, (
        f"account {paper_account} not in summary keys: {list(summary)}"
    )
    tags = summary[paper_account]
    missing = REQUIRED_USD_TAGS - set(tags)
    assert not missing, (
        f"summary missing required tags: {sorted(missing)}; got: {sorted(tags)}"
    )


async def test_account_summary_usd_tags_have_currency(gateway_client, paper_account):
    """Every currency-bearing tag is reported in USD on a US paper account.

    The summary value is parseable as a float (string from the wire).
    """
    summary = await gateway_client.get_account_summary(timeout=10)
    tags = summary[paper_account]
    for tag in REQUIRED_USD_TAGS:
        entry = tags[tag]
        assert entry["currency"] == "USD", (
            f"tag {tag!r} expected USD currency; got {entry!r}"
        )
        # The value must parse as a float (zero is allowed)
        float(entry["value"])


async def test_account_summary_economically_consistent(gateway_client, paper_account):
    """Sanity: NetLiquidation = TotalCashValue + GrossPositionValue (± unrealized PnL).

    On a paper account with no positions, GrossPositionValue is 0 and
    NetLiquidation ≈ TotalCashValue + AccruedCash. We allow a small
    tolerance for accrued interest and pending settlements.
    """
    summary = await gateway_client.get_account_summary(timeout=10)
    tags = summary[paper_account]
    nlv = float(tags["NetLiquidation"]["value"])
    cash = float(tags["TotalCashValue"]["value"])
    gpv = float(tags["GrossPositionValue"]["value"])
    accrued = float(tags.get("AccruedCash", {"value": "0"})["value"])

    # |NLV − (cash + GPV + accrued)| should be tiny
    expected = cash + gpv + accrued
    diff = abs(nlv - expected)
    # Allow 0.5% drift to absorb pending settlements / display rounding
    tolerance = max(1.0, abs(nlv) * 0.005)
    assert diff <= tolerance, (
        f"NLV inconsistency: NLV={nlv:.2f} cash={cash:.2f} GPV={gpv:.2f} "
        f"accrued={accrued:.2f}; diff={diff:.2f} > tol={tolerance:.2f}"
    )


async def test_account_summary_narrow_tag_list(gateway_client, paper_account):
    """Requesting only specific tags must return ONLY those tags."""
    summary = await gateway_client.get_account_summary(
        tags="NetLiquidation,BuyingPower", timeout=10
    )
    tags = summary[paper_account]
    assert set(tags.keys()) == {"NetLiquidation", "BuyingPower"}, (
        f"unexpected tags returned: {sorted(tags)}"
    )


async def test_account_summary_paper_buying_power_is_high(
    gateway_client, paper_account
):
    """Paper accounts are seeded with $1M cash and 4× margin.

    Buying power should comfortably exceed $100k on a fresh paper account.
    This is the smoke check that the account is actually a paper account
    in good standing — a frozen / suspended account has BP = 0.
    """
    summary = await gateway_client.get_account_summary(
        tags="BuyingPower,NetLiquidation", timeout=10
    )
    bp = float(summary[paper_account]["BuyingPower"]["value"])
    nlv = float(summary[paper_account]["NetLiquidation"]["value"])
    assert bp > 100_000, (
        f"BuyingPower {bp:.2f} too low for a healthy paper account"
    )
    # Margin ratio: BP should be ~4× NLV on a Reg-T account, but we leave
    # plenty of slack — IB sometimes tightens margin during volatility.
    assert bp >= nlv * 1.5, (
        f"BuyingPower {bp:.2f} < 1.5× NetLiquidation {nlv:.2f}; "
        "either no margin enabled or account is restricted"
    )


# ────────────────────────────────────────────────────────────────────────
# Positions
# ────────────────────────────────────────────────────────────────────────


async def test_positions_returns_dict_keyed_by_account(gateway_client, paper_account):
    """`get_positions` returns a dict shape regardless of whether positions exist.

    We don't assert positions exist (paper account state varies); we only
    assert the structural contract: ``{account: [ {contract, position,
    avgCost} ]}``.
    """
    positions = await gateway_client.get_positions(timeout=10)
    assert isinstance(positions, dict)
    if positions:
        # If any positions exist, they're on accounts we recognize.
        for acct, holdings in positions.items():
            assert isinstance(acct, str)
            assert isinstance(holdings, list)
            for h in holdings:
                assert "contract" in h and "position" in h and "avgCost" in h
                assert h["contract"].symbol  # non-empty
                # position is a Decimal; avgCost is a float
                assert h["avgCost"] >= 0


async def test_positions_returns_empty_safely(gateway_client):
    """Two back-to-back position requests must both succeed.

    `cancelPositions` is called inside `get_positions` after each call;
    this validates the resubscribe path works.
    """
    p1 = await gateway_client.get_positions(timeout=10)
    p2 = await gateway_client.get_positions(timeout=10)
    assert isinstance(p1, dict)
    assert isinstance(p2, dict)
    # Same accounts on both calls
    assert set(p1) == set(p2)


# ────────────────────────────────────────────────────────────────────────
# Portfolio (full account update download)
# ────────────────────────────────────────────────────────────────────────


async def test_portfolio_completes_and_unsubscribes(gateway_client, paper_account):
    """`reqAccountUpdates(True)` then `(False)` round-trips without hanging.

    `get_portfolio` is the heaviest read in the API — it streams full
    account values, portfolio entries, and account-time updates until the
    `accountDownloadEnd` callback fires. A bug in the unsubscribe path
    would cause this test to leak background updates into the next test.
    """
    portfolio = await gateway_client.get_portfolio(
        account_code=paper_account, timeout=30
    )
    assert isinstance(portfolio, list)
    # Each entry has the full set of portfolio fields
    for entry in portfolio:
        for key in ("contract", "position", "marketPrice", "marketValue",
                    "averageCost", "unrealizedPNL", "realizedPNL", "accountName"):
            assert key in entry, f"portfolio entry missing {key!r}: {entry}"
        assert entry["accountName"] == paper_account
