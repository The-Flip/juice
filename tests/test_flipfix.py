"""Tests for juice.flipfix — FlipFix API client."""

from __future__ import annotations

import pytest
from aioresponses import aioresponses

from juice.flipfix import get_machines


API_URL = "https://flipfix.example.com/api/v1/"
API_KEY = "test-key-abc123"


@pytest.fixture
def mock_api():
    with aioresponses() as m:
        yield m


class TestGetMachines:
    @pytest.mark.asyncio
    async def test_success(self, mock_api) -> None:
        mock_api.get(
            f"{API_URL}machines/",
            payload={
                "machines": [
                    {"asset_id": "M0001", "name": "Medieval Madness", "slug": "medieval-madness"},
                    {"asset_id": "M0013", "name": "Blackout", "slug": "blackout"},
                ],
            },
        )
        result = await get_machines(API_URL, API_KEY)
        assert result == {"M0001": "Medieval Madness", "M0013": "Blackout"}

    @pytest.mark.asyncio
    async def test_empty_list(self, mock_api) -> None:
        mock_api.get(f"{API_URL}machines/", payload={"machines": []})
        result = await get_machines(API_URL, API_KEY)
        assert result == {}

    @pytest.mark.asyncio
    async def test_network_error(self, mock_api) -> None:
        mock_api.get(f"{API_URL}machines/", exception=ConnectionError("offline"))
        result = await get_machines(API_URL, API_KEY)
        assert result == {}
