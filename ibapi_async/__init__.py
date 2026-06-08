"""
ibapi-async: asyncio wrapper for the Interactive Brokers Python API.

Provides AsyncTWSClient — a drop-in async replacement for ibapi's TWSSyncWrapper.
The original ibapi package is a dependency and is never modified.

Usage:
    from ibapi_async import AsyncTWSClient
    from ibapi.contract import Contract

    async def main():
        async with await AsyncTWSClient.create("127.0.0.1", 4002, 0) as client:
            contract = Contract()
            contract.symbol = "AAPL"
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            details = await client.get_contract_details(contract)
            print(details)
"""

from ibapi_async.exceptions import ResponseTimeout
from ibapi_async.tws_client import AsyncTWSClient

__all__ = ["AsyncTWSClient", "ResponseTimeout"]
__version__ = "0.1.0"
