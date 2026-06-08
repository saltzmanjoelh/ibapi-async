"""
Basic usage example for ibapi-async.

Demonstrates connecting to IB Gateway / TWS, fetching contract details,
requesting a market data snapshot, and getting historical bars.

Prerequisites:
  - IB Gateway or TWS running and accepting API connections
  - Port 4002 open (IB Gateway paper) or 7496 (TWS paper)

Run:
  poetry run python examples/basic_usage.py
"""

import asyncio
import logging

from ibapi.contract import Contract

from ibapi_async import AsyncTWSClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def aapl_contract() -> Contract:
    c = Contract()
    c.symbol = "AAPL"
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


async def main() -> None:
    # --- Connect ---
    print("Connecting to IB Gateway...")
    async with await AsyncTWSClient.create(
        host="127.0.0.1",
        port=4002,       # IB Gateway paper port; use 7496 for TWS paper
        client_id=0,
        timeout=30.0,
    ) as client:
        print(f"Connected. Server version: {client.serverVersion()}")
        print(f"Next valid order ID: {client.next_valid_id_value}")

        # --- Contract details ---
        print("\nFetching AAPL contract details...")
        details = await client.get_contract_details(aapl_contract(), timeout=10.0)
        if details:
            d = details[0]
            print(f"  Long name:   {d.longName}")
            print(f"  Exchange:    {d.contract.primaryExchange}")
            print(f"  Currency:    {d.contract.currency}")
            print(f"  ConId:       {d.contract.conId}")
        else:
            print("  No contract details returned.")

        # --- Market data snapshot ---
        # Use delayed data (type 3) so this works without a real-time subscription.
        client.reqMarketDataType(3)
        print("\nFetching AAPL market data snapshot (delayed)...")
        snap = await client.get_market_data_snapshot(aapl_contract(), timeout=15.0)
        if snap:
            for tick_name, value in list(snap.items())[:5]:
                print(f"  {tick_name}: {value}")
        else:
            print("  No market data returned (market may be closed).")

        # --- Historical data ---
        print("\nFetching AAPL 5 days of hourly bars...")
        bars = await client.get_historical_data(
            contract=aapl_contract(),
            end_date_time="",          # empty = now
            duration_str="5 D",
            bar_size_setting="1 hour",
            what_to_show="TRADES",
            use_rth=True,
            timeout=30.0,
        )
        print(f"  Received {len(bars)} bars")
        if bars:
            b = bars[-1]
            print(f"  Last bar — date:{b.date}  open:{b.open}  close:{b.close}  volume:{b.volume}")

        # --- Account summary ---
        print("\nFetching account summary...")
        summary = await client.get_account_summary(
            tags="NetLiquidation,TotalCashValue",
            timeout=10.0,
        )
        for account, data in summary.items():
            print(f"  Account: {account}")
            for tag, val in data.items():
                print(f"    {tag}: {val['value']} {val['currency']}")

    print("\nDisconnected. Done.")


if __name__ == "__main__":
    asyncio.run(main())
