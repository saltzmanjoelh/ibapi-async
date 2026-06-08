"""Exceptions for ibapi-async."""


class ResponseTimeout(Exception):
    """Raised when a TWS response is not received within the timeout period."""
    pass


# TWS error codes that are informational (connection status, etc.) and should
# not cause an awaiting request to fail. Mirrors the conventional noise set
# used in the official ibapi samples.
INFO_ERROR_CODES = frozenset({
    2100, 2104, 2106, 2107, 2108, 2119, 2137, 2158, 2168, 2169,
    10167,  # "Displaying delayed market data" — informational, not a failure
})


class TWSError(Exception):
    """Raised when TWS returns an error for an awaited request."""

    def __init__(self, req_id: int, code: int, message: str):
        self.req_id = req_id
        self.code = code
        self.message = message
        super().__init__(f"TWS error {code} (reqId={req_id}): {message}")
