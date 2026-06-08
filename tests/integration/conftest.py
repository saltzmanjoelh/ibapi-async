"""Fixtures for live-Gateway integration tests.

These tests need a running Interactive Brokers Gateway on a paper-trading
account. The `gateway_client` fixture resolves the endpoint in this order:

  1. **Existing Gateway** at `IBKR_GATEWAY_HOST:IBKR_GATEWAY_PORT`
     (default 127.0.0.1:4002 — your local Gateway or a docker-compose stack).
  2. **Autostart a container** if the existing one isn't reachable: pulls
     `ghcr.io/saltzmanjoelh/ibgateway:main` and `docker run`s it with
     `IBGATEWAY_USERNAME` / `IBGATEWAY_PASSWORD` from the environment, mapping
     paper-trading port 4004 → host port `IBKR_GATEWAY_PORT`. Container name
     `ibgateway-test` so it doesn't conflict with your prod compose stack.
  3. **Skip** with an actionable message if neither (a real Gateway nor
     credentials + Docker) is available.

Once started, the container is left running between sessions for speed
(first start can take ~5 minutes for the IB login to complete; subsequent
sessions reuse it instantly). To tear it down:  `docker rm -f ibgateway-test`.

Configuration via env vars:

    IBKR_GATEWAY_HOST                 default 127.0.0.1
    IBKR_GATEWAY_PORT                 default 4002              (paper)
    IBKR_GATEWAY_CLIENT_ID            default 99
    IBKR_GATEWAY_DOCKER_IMAGE         default ghcr.io/saltzmanjoelh/ibgateway:main
    IBKR_GATEWAY_DOCKER_AUTOSTART     default 1                 (0 to disable)
    IBKR_GATEWAY_DOCKER_STARTUP_TIMEOUT  default 600 (seconds)
    IBGATEWAY_USERNAME                required for autostart
    IBGATEWAY_PASSWORD                required for autostart

Run integration tests:
    poetry run pytest -m integration tests/integration/ -v
"""

import asyncio
import itertools
import os
import socket
import subprocess
import time

import pytest

from ibapi_async import AsyncTWSClient
from ibapi_async.exceptions import ResponseTimeout


HOST = os.environ.get("IBKR_GATEWAY_HOST", "127.0.0.1")
PORT = int(os.environ.get("IBKR_GATEWAY_PORT", "4002"))
# Starting client ID. Each connection attempt picks the next free slot from
# `_client_id_seq` (see `_connect`). The gateway keeps a connection alive
# server-side for ~10–30 seconds after a client disconnects, which means
# reusing the same ID across rapidly-running tests fails with a 10-second
# handshake timeout. By advancing the ID per attempt we sidestep that.
CLIENT_ID_BASE = int(os.environ.get("IBKR_GATEWAY_CLIENT_ID", "99"))
# IB API supports up to 32 simultaneous clients per gateway. We probe within
# a much smaller window per test (3 attempts) but the underlying counter
# wraps around the full range.
_CLIENT_ID_RANGE = 64
_client_id_seq = itertools.cycle(
    range(CLIENT_ID_BASE, CLIENT_ID_BASE + _CLIENT_ID_RANGE)
)

# Session-level circuit breaker. The first time `_connect` exhausts its
# probing budget, the diagnostic is recorded here and every subsequent
# call skips immediately. Without this, 50+ tests would each burn the
# full handshake timeout against a dead gateway, turning a broken-CI run
# into a 45-minute wait. Reset between processes (module-level state).
_gateway_circuit_breaker: str | None = None

DOCKER_IMAGE = os.environ.get(
    "IBKR_GATEWAY_DOCKER_IMAGE", "ghcr.io/saltzmanjoelh/ibgateway:main"
)
DOCKER_AUTOSTART = os.environ.get("IBKR_GATEWAY_DOCKER_AUTOSTART", "1") == "1"
DOCKER_STARTUP_TIMEOUT = int(
    os.environ.get("IBKR_GATEWAY_DOCKER_STARTUP_TIMEOUT", "600")
)
CONTAINER_NAME = "ibgateway-test"
CONTAINER_PAPER_PORT = 4004  # internal port the image listens on for paper

CONNECT_TIMEOUT = 30.0  # client-side handshake timeout once the port is open


# ------------------------------------------------------------------
# Endpoint resolution helpers (sync, called from session-scoped fixture)
# ------------------------------------------------------------------


def _is_reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _is_reachable(host, port, timeout=2.0):
            return True
        time.sleep(2.0)
    return False


def _docker_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _container_running(name: str) -> bool:
    result = subprocess.run(
        [
            "docker", "ps",
            "--filter", f"name=^{name}$",
            "--filter", "status=running",
            "--format", "{{.Names}}",
        ],
        capture_output=True, text=True,
    )
    return name in result.stdout.splitlines()


def _start_container() -> None:
    """Run the ibgateway image in a detached container. Skip if creds missing."""
    user = os.environ.get("IBGATEWAY_USERNAME")
    pw = os.environ.get("IBGATEWAY_PASSWORD")
    if not user or not pw:
        pytest.skip(
            "Cannot autostart ibgateway container: set IBGATEWAY_USERNAME and "
            "IBGATEWAY_PASSWORD, or start a Gateway manually and point "
            f"IBKR_GATEWAY_HOST/IBKR_GATEWAY_PORT at it (currently {HOST}:{PORT})."
        )

    # Remove any leftover stopped container with the same name
    subprocess.run(
        ["docker", "rm", "-f", CONTAINER_NAME],
        capture_output=True,
    )

    cmd = [
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "--platform", "linux/amd64",
        "--pull", "missing",
        "-p", f"{PORT}:{CONTAINER_PAPER_PORT}",
        "-e", f"IBGATEWAY_USERNAME={user}",
        "-e", f"IBGATEWAY_PASSWORD={pw}",
        DOCKER_IMAGE,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip(
            f"Failed to `docker run {DOCKER_IMAGE}`: "
            f"{(result.stderr or result.stdout).strip()}"
        )


def _container_logs_tail(name: str, lines: int = 30) -> str:
    result = subprocess.run(
        ["docker", "logs", "--tail", str(lines), name],
        capture_output=True, text=True,
    )
    return (result.stdout + result.stderr).strip()


def _resolve_gateway_endpoint(announce) -> tuple[str, int]:
    """Return (host, port) of a working Gateway, autostarting a container if needed.

    Calls pytest.skip() if no Gateway is reachable and we can't or won't autostart.
    `announce(msg, **kw)` is a callback used to surface progress to the terminal.
    """
    # 1. Existing Gateway / compose stack / previously-started container
    if _is_reachable(HOST, PORT):
        if _container_running(CONTAINER_NAME):
            announce(
                f"Reusing already-running '{CONTAINER_NAME}' Docker container "
                f"at {HOST}:{PORT}",
                cyan=True,
            )
        else:
            announce(
                f"Using existing Gateway at {HOST}:{PORT} "
                "(production / compose stack / TWS — not started by this fixture)",
                cyan=True,
            )
        return HOST, PORT

    # 2. Autostart container fallback
    if not DOCKER_AUTOSTART:
        pytest.skip(
            f"Gateway unreachable at {HOST}:{PORT} and "
            "IBKR_GATEWAY_DOCKER_AUTOSTART=0 disables container autostart."
        )

    if not _docker_available():
        pytest.skip(
            f"Gateway unreachable at {HOST}:{PORT} and `docker` is not "
            f"available to autostart {DOCKER_IMAGE}."
        )

    if not _container_running(CONTAINER_NAME):
        announce(
            f"Starting Docker container '{CONTAINER_NAME}' from {DOCKER_IMAGE} "
            f"(paper port {CONTAINER_PAPER_PORT} → host {PORT})...",
            yellow=True,
        )
        _start_container()
    else:
        announce(
            f"Container '{CONTAINER_NAME}' is running but port {PORT} isn't "
            "responding yet — waiting for the Gateway to finish coming up.",
            yellow=True,
        )

    announce(
        f"Waiting up to {DOCKER_STARTUP_TIMEOUT}s for Gateway to be ready on "
        f"{HOST}:{PORT} (first run usually takes 3-5 minutes for IB login)...",
        yellow=True,
    )
    if not _wait_for_port(HOST, PORT, DOCKER_STARTUP_TIMEOUT):
        logs = _container_logs_tail(CONTAINER_NAME)
        pytest.skip(
            f"ibgateway container started but port {PORT} not reachable "
            f"after {DOCKER_STARTUP_TIMEOUT}s. Last logs:\n{logs}"
        )

    announce(
        f"Docker Gateway ready at {HOST}:{PORT} "
        f"(container '{CONTAINER_NAME}' from {DOCKER_IMAGE})",
        green=True,
    )
    return HOST, PORT


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


def _make_announcer(request):
    """Return a function that prints to the terminal, capture-bypass."""
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return lambda msg, **_: None
    return lambda msg, **kw: reporter.write_line(f"[ibgateway] {msg}", **kw)


@pytest.fixture(scope="session")
def gateway_endpoint(request) -> tuple[str, int]:
    """Session-scoped: resolve the Gateway endpoint and warm up its API.

    Resolution is sync (port reachability + container autostart). The
    warmup probe is async (it actually completes a handshake), so we
    call it via ``asyncio.run`` — safe here because nothing else has
    spun up an event loop yet at session-fixture time.

    If the warmup can't get a handshake within the budget, we
    ``pytest.fail`` from this fixture so the session reports an error
    (CI exits non-zero). Tests are marked as fixture-setup errors
    rather than 54 individual silent skips.
    """
    announce = _make_announcer(request)
    host, port = _resolve_gateway_endpoint(announce)
    asyncio.run(_warmup_handshake(host, port, announce))
    if _gateway_circuit_breaker is not None:
        pytest.fail(_gateway_circuit_breaker, pytrace=False)
    return host, port


async def _warmup_handshake(host: str, port: int, announce) -> None:
    """Validate the gateway can complete an API handshake before tests start.

    A freshly-started SSM tunnel or a gateway that just rebooted may take
    20-60s before its API server is actually willing to issue
    ``nextValidId``. The TCP listener appears immediately, so naïve
    fixtures think everything is ready and start hammering the dead
    socket — turning a transient cold-start into a wave of test
    failures.

    This warmup tries up to 6 handshakes spaced 10s apart (~70s total
    budget). On success we return immediately; on full exhaustion we
    trip the session circuit breaker so the rest of the suite skips
    fast instead of each test paying the same wall.
    """
    global _gateway_circuit_breaker
    attempts: list[str] = []
    for attempt in range(1, 7):
        client_id = next(_client_id_seq)
        try:
            client = await asyncio.wait_for(
                AsyncTWSClient.create(host, port, client_id),
                timeout=CONNECT_TIMEOUT,
            )
            await client.disconnect()
            announce(
                f"Gateway warmup OK on attempt {attempt} "
                f"(client_id={client_id}, server v{client.serverVersion()})",
                green=True,
            )
            return
        except (OSError, asyncio.TimeoutError, ResponseTimeout) as exc:
            attempts.append(f"  attempt {attempt} (client_id={client_id}): "
                            f"{type(exc).__name__}: {exc}")
            announce(
                f"warmup attempt {attempt}/6 (client_id={client_id}) failed "
                f"({type(exc).__name__}); waiting before next try",
                yellow=True,
            )
            await asyncio.sleep(10)

    summary = "\n".join(attempts)
    _gateway_circuit_breaker = (
        f"Session warmup failed: gateway at {host}:{port} did not handshake "
        f"after 6 attempts spaced 10s apart.\n{summary}"
    )


async def _connect(
    host: str, port: int, announce, max_attempts: int = 2
) -> AsyncTWSClient:
    """Open a fresh API session, probing for an unused client ID.

    The gateway holds a connection slot for 10-30s server-side after the
    client disconnects, so reusing a static client ID across back-to-back
    tests would routinely time out the next test's handshake. We pull
    successive IDs from ``_client_id_seq`` and retry up to
    ``max_attempts`` times until one handshakes cleanly.

    Two attempts is enough: probing is to step past *one* lingering stale
    slot, not to boot a dead gateway. Once the second attempt also fails,
    the session-level circuit breaker trips so every subsequent test in
    the run skips immediately instead of each one paying the full
    timeout cost.
    """
    global _gateway_circuit_breaker
    if _gateway_circuit_breaker is not None:
        pytest.skip(
            "Gateway tripped the connection circuit breaker earlier in this "
            f"session — skipping to avoid burning the handshake timeout "
            f"again.\n\nFirst failure was:\n{_gateway_circuit_breaker}"
        )

    attempts: list[tuple[int, str]] = []
    for attempt in range(max_attempts):
        client_id = next(_client_id_seq)
        try:
            client = await asyncio.wait_for(
                AsyncTWSClient.create(host, port, client_id),
                timeout=CONNECT_TIMEOUT,
            )
            announce(
                f"API session established (server v{client.serverVersion()}, "
                f"client_id={client_id})",
                green=True,
            )
            return client
        except (OSError, asyncio.TimeoutError, ResponseTimeout) as exc:
            attempts.append((client_id, f"{type(exc).__name__}: {exc}"))
            announce(
                f"client_id={client_id} unavailable on attempt {attempt + 1}/"
                f"{max_attempts} ({type(exc).__name__}); probing next ID",
                yellow=True,
            )

    summary = "\n".join(f"  client_id={cid}: {err}" for cid, err in attempts)
    diagnostic = (
        f"Could not establish API session to {host}:{port} after "
        f"{max_attempts} probes:\n{summary}\n\n"
        "Common causes:\n"
        "  - Gateway 'Settings → API → Settings → Enable ActiveX and Socket "
        "Clients' is not checked.\n"
        "  - Gateway 'Settings → API → Settings → Trusted IPs' doesn't "
        f"include this client's IP (try adding 127.0.0.1).\n"
        f"  - Wrong port: paper Gateway = 4002, live Gateway = 4001, paper "
        "TWS = 7497, live TWS = 7496. Override with IBKR_GATEWAY_PORT.\n"
        "  - Gateway is still starting up / failed to log in (the autostart "
        "container's first login can take 3-5 min, and a stuck password "
        "prompt leaves it permanently unable to handshake)."
    )
    # Trip the breaker so subsequent tests short-circuit, then fail the
    # test that uncovered the problem so CI exits non-zero.
    _gateway_circuit_breaker = diagnostic
    pytest.fail(diagnostic, pytrace=False)


@pytest.fixture
async def gateway_client(gateway_endpoint, request):
    """Per-test connected client.

    Each test gets a fresh handshake. Costs ~1s per case but sidesteps the
    pytest-asyncio session-loop dance and gives every test full state
    isolation (no leaked subscriptions, no stale market-data type, no
    accidental cross-test order interaction).
    """
    host, port = gateway_endpoint
    announce = _make_announcer(request)
    client = await _connect(host, port, announce)
    try:
        yield client
    finally:
        await client.disconnect()


# ──────────────────────────────────────────────────────────────────────
# Per-test safety / market-data fixtures (built on top of gateway_client)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
async def paper_account(gateway_client) -> str:
    """Resolve and return the active paper account ID.

    Refuses to run if the account does not look like a paper account.
    Paper IB account IDs always start with ``D`` (``DU`` for Universal,
    ``DF`` for FA paper, etc.). This is the order-test safety net — it
    fails the test instead of placing real trades on a live account.
    """
    accounts = await gateway_client.get_managed_accounts()
    if not accounts:
        pytest.fail(
            "Gateway returned no managed accounts. Cannot run integration "
            "tests without an active account."
        )
    acct = accounts[0]
    if not acct.startswith("D"):
        pytest.fail(
            f"Account {acct!r} does not look like a paper account "
            "(expected ID starting with 'D'). Refusing to run integration "
            "tests against a non-paper account.",
            pytrace=False,
        )
    return acct


@pytest.fixture
async def delayed_data(gateway_client):
    """Switch the connection to delayed (type 3) market data and yield the client.

    Real-time L1 quotes require a paid subscription per exchange; paper
    accounts typically only get delayed. With type 3 set,
    ``get_market_data_snapshot`` returns ``DELAYED_*`` ticks instead of
    erroring with code 10089/354.
    """
    gateway_client.request_market_data_type(3)
    # Tiny pause so the data-type switch reaches the server before any
    # subsequent reqMktData request races it.
    await asyncio.sleep(0.5)
    yield gateway_client


async def _api_is_writable(client) -> tuple[bool, str]:
    """Place a guaranteed-unfillable test order and see whether TWS accepts it.

    Returns ``(writable, reason)``. When ``writable`` is False, ``reason``
    is the human-readable diagnostic to surface in the skip / fail message.
    """
    from ibapi.contract import Contract
    from ibapi.order import Order
    from ibapi_async.exceptions import ResponseTimeout, TWSError

    contract = Contract()
    contract.symbol = "AAPL"
    contract.secType = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"

    order = Order()
    order.action = "BUY"
    order.orderType = "LMT"
    order.totalQuantity = 1
    order.lmtPrice = 1.00  # unfillable; never fills accidentally
    order.tif = "DAY"
    order.outsideRth = False
    order.transmit = True

    try:
        status = await client.place_order(contract, order, timeout=4.0)
        # Got a status — order accepted. Cancel it immediately as a courtesy.
        try:
            await client.cancel_order(order.orderId, timeout=3.0)
        except Exception:
            pass
        return True, f"order accepted (status={status['status']})"
    except (ResponseTimeout, TWSError) as exc:
        # Read-only mode emits error 321 on reqId=-1 ("API interface is in
        # Read-Only mode"), which doesn't correlate to our request — we
        # see it as a timeout. Scan the recent uncorrelated errors for the
        # diagnostic.
        readonly_msg = None
        for err in client.errors.get(-1, []):
            if "Read-Only" in err.get("errorString", "") or err.get("errorCode") == 321:
                readonly_msg = err["errorString"]
                break
        if readonly_msg:
            return False, f"Read-Only API mode: {readonly_msg}"
        return False, f"order placement failed: {type(exc).__name__}: {exc}"


@pytest.fixture
async def writable_client(gateway_client, paper_account):
    """Yield a client only if the API is in writable mode; else skip the test.

    Order tests use this fixture instead of ``gateway_client`` directly. When
    the gateway has "Read-Only API" enabled (Configure → Settings → API →
    Settings → Read-Only API), every place/cancel call hangs because TWS
    emits the rejection on reqId=-1 (not the request's reqId). Detecting it
    upfront fails fast with an actionable message instead of letting tests
    time out one-by-one.
    """
    writable, reason = await _api_is_writable(gateway_client)
    if not writable:
        pytest.skip(
            f"Gateway is not order-writable. {reason}\n\n"
            "Fix: in the IB Gateway window, Configure → Settings → API → "
            "Settings → uncheck 'Read-Only API', then restart Gateway.",
        )
    yield gateway_client


@pytest.fixture
async def cleanup_orders(writable_client):
    """Yield a writable client, then global-cancel after the test.

    Even if a test crashes mid-placement, this runs and cancels every
    working order on the paper account so the next test starts clean.
    Depends on ``writable_client``, which depends on ``paper_account`` —
    so the read-only probe and the paper-account safety check both fire
    before any test using this fixture can place an order.
    """
    yield writable_client
    try:
        writable_client.cancel_all_orders()
        await asyncio.sleep(0.5)
    except Exception:
        pass
