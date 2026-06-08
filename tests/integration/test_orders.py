"""Order-management integration tests.

Mutates state on the paper account. Every test uses ``cleanup_orders``,
which transitively depends on ``paper_account``, which fails the test
fast if the connected account isn't a paper account (D-prefixed). That's
the sole guard preventing real trades — do not remove it.

Strategy for safety:
- Far-out limit prices (BUY $1.00) so an accidental fill is impossible
  at any market state.
- Sell side only when explicitly placing an order to be cancelled, never
  with marketable prices.
- ``cleanup_orders`` runs ``reqGlobalCancel`` after every test as a
  belt-and-suspenders cleanup.
"""

import asyncio

import pytest
from ibapi.contract import Contract
from ibapi.order import Order

from ibapi_async.exceptions import ResponseTimeout, TWSError

pytestmark = pytest.mark.integration


def _aapl_stock() -> Contract:
    c = Contract()
    c.symbol = "AAPL"
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def _far_out_buy_lmt(qty: int = 1, price: float = 1.00) -> Order:
    """A limit-buy at $1 is unfillable for any liquid US equity (no chance
    of accidental execution even in extreme market crashes)."""
    o = Order()
    o.action = "BUY"
    o.orderType = "LMT"
    o.totalQuantity = qty
    o.lmtPrice = price
    o.tif = "DAY"
    # Paper accounts have outsideRth toggled off by default; explicit:
    o.outsideRth = False
    # Prevent firm-account warnings on routes that require account tag
    o.transmit = True
    return o


# ────────────────────────────────────────────────────────────────────────
# Place / cancel one order
# ────────────────────────────────────────────────────────────────────────


async def test_place_far_out_limit_returns_status(cleanup_orders):
    """Placing a $1 BUY LMT must return an order_status dict with the same orderId."""
    client = cleanup_orders
    contract = _aapl_stock()
    order = _far_out_buy_lmt()

    status = await client.place_order(contract, order, timeout=10)
    assert isinstance(status, dict)
    assert status["orderId"] == order.orderId
    # Status is one of the legal lifecycle values
    legal = {
        "PendingSubmit", "PendingCancel", "PreSubmitted", "Submitted",
        "ApiCancelled", "Cancelled", "Filled", "Inactive", "ApiPending"
    }
    assert status["status"] in legal, f"unknown order status: {status['status']!r}"
    # No fills at $1.00 limit — these MUST be zero
    assert status["filled"] == 0
    assert status["remaining"] == 1
    assert status["avgFillPrice"] == 0.0
    # permId is assigned by TWS post-placement and must be non-zero
    assert status["permId"] > 0, f"permId not assigned: {status}"


async def test_place_then_cancel_round_trip(cleanup_orders):
    """Place order → cancel → final status reaches Cancelled or ApiCancelled."""
    client = cleanup_orders
    order = _far_out_buy_lmt()

    placed = await client.place_order(_aapl_stock(), order, timeout=10)
    assert placed["orderId"] == order.orderId

    cancelled = await client.cancel_order(order.orderId, timeout=10)
    assert cancelled["orderId"] == order.orderId
    assert cancelled["status"] in ("Cancelled", "ApiCancelled", "PendingCancel"), (
        f"unexpected cancel-final status: {cancelled['status']!r}"
    )


async def test_open_orders_includes_placed_order(cleanup_orders):
    """After placing an order, ``get_open_orders`` must include it.

    Cancel before exiting (cleanup_orders also issues globalCancel as
    belt-and-suspenders).
    """
    client = cleanup_orders
    order = _far_out_buy_lmt()
    await client.place_order(_aapl_stock(), order, timeout=10)

    # Give TWS a beat to register the order in the open-orders list
    await asyncio.sleep(0.5)

    open_orders = await client.get_open_orders(timeout=10)
    assert order.orderId in open_orders, (
        f"placed order {order.orderId} missing from open orders: {sorted(open_orders)}"
    )

    entry = open_orders[order.orderId]
    assert entry["contract"].symbol == "AAPL"
    assert entry["order"].action == "BUY"
    assert entry["order"].orderType == "LMT"
    assert float(entry["order"].lmtPrice) == 1.00


async def test_global_cancel_clears_open_orders(cleanup_orders):
    """Place 2 orders, fire ``cancel_all_orders``, then ``get_open_orders`` empties.

    Verifies the global-cancel pathway actually unwinds working orders
    (not just our cleanup fixture's belt-and-suspenders).
    """
    client = cleanup_orders
    o1 = _far_out_buy_lmt(qty=1, price=1.00)
    o2 = _far_out_buy_lmt(qty=1, price=1.50)

    await client.place_order(_aapl_stock(), o1, timeout=10)
    await client.place_order(_aapl_stock(), o2, timeout=10)
    await asyncio.sleep(0.3)

    pre = await client.get_open_orders(timeout=10)
    # Both our orders should be visible (and possibly others left over —
    # we only assert on ours)
    assert o1.orderId in pre
    assert o2.orderId in pre

    client.cancel_all_orders()
    # Global cancel propagates async; give TWS time to process
    await asyncio.sleep(2.0)

    post = await client.get_open_orders(timeout=10)
    assert o1.orderId not in post, (
        f"order {o1.orderId} still open after global cancel"
    )
    assert o2.orderId not in post, (
        f"order {o2.orderId} still open after global cancel"
    )


# ────────────────────────────────────────────────────────────────────────
# Order rejection paths
# ────────────────────────────────────────────────────────────────────────


async def test_invalid_contract_order_raises(cleanup_orders):
    """An order on a contract that doesn't exist must raise ``TWSError``."""
    bogus = Contract()
    bogus.symbol = "ZZZZ_NOT_A_REAL_SYMBOL"
    bogus.secType = "STK"
    bogus.exchange = "SMART"
    bogus.currency = "USD"

    with pytest.raises((TWSError, ResponseTimeout)):
        await cleanup_orders.place_order(
            bogus, _far_out_buy_lmt(), timeout=8
        )


# ────────────────────────────────────────────────────────────────────────
# Executions / completed orders
# ────────────────────────────────────────────────────────────────────────


async def test_get_executions_returns_clean_when_none(gateway_client):
    """``get_executions`` returns a list (possibly empty) without raising.

    Unfilled orders never produce executions, so on a fresh paper session
    this is typically empty. The wrapper internally converts the no-end-fired
    timeout case to ``[]`` (see docstring on ``get_executions``).
    """
    execs = await gateway_client.get_executions(timeout=4)
    assert isinstance(execs, list)
    # Each entry has 'contract' and 'execution' keys
    for entry in execs:
        assert "contract" in entry and "execution" in entry


async def test_completed_orders_returns_list(cleanup_orders):
    """``get_completed_orders(api_only=True)`` returns a list shape.

    On a paper account that hasn't traded today this is usually empty.
    Requires order-write permission because TWS treats reqCompletedOrders
    as a write-mode request even though it's a pure read.
    """
    completed = await cleanup_orders.get_completed_orders(api_only=True, timeout=10)
    assert isinstance(completed, list)
    for entry in completed:
        for key in ("contract", "order", "orderState"):
            assert key in entry
        assert entry["contract"].symbol  # non-empty
