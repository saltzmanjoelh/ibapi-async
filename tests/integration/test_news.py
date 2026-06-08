"""News-data integration tests.

Paper accounts always get the free Briefing.com (BRFG / BRFUPDN) and
Dow Jones (DJ-*) feeds. Premium news (Reuters, Benzinga, etc.) needs a
subscription. We assert the free providers are present and verify the
provider records are well-formed.
"""

import pytest

pytestmark = pytest.mark.integration


# Free providers shipped to every paper account
EXPECTED_FREE_PROVIDERS = {
    "BRFG",       # Briefing.com General Market Commentary
    "BRFUPDN",    # Briefing.com Analyst Actions
    "DJ-N",       # Dow Jones Newswire
    "DJ-RT",      # Dow Jones Real-Time News
}


async def test_news_providers_returns_free_set(gateway_client):
    """The free DJ + Briefing providers must be present on a paper account."""
    providers = await gateway_client.get_news_providers(timeout=10)
    assert providers, "no news providers returned"

    codes = {p.code for p in providers}
    missing = EXPECTED_FREE_PROVIDERS - codes
    assert not missing, (
        f"expected free news providers missing: {sorted(missing)}; got: {sorted(codes)}"
    )


async def test_news_providers_records_are_well_formed(gateway_client):
    """Each provider has a non-empty code and human-readable name."""
    providers = await gateway_client.get_news_providers(timeout=10)
    for p in providers:
        assert isinstance(p.code, str) and p.code, f"empty code: {vars(p)}"
        assert isinstance(p.name, str) and p.name, f"empty name: {vars(p)}"
        # Codes are short alphanumeric tokens; names are longer descriptions
        assert len(p.code) <= 16
        assert p.name != p.code, (
            f"name {p.name!r} same as code {p.code!r} — name not populated"
        )


async def test_news_providers_idempotent(gateway_client):
    """Two consecutive calls return the same provider set."""
    first = await gateway_client.get_news_providers(timeout=10)
    second = await gateway_client.get_news_providers(timeout=10)
    assert {p.code for p in first} == {p.code for p in second}
