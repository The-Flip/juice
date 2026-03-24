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
                    {
                        "asset_id": "M0001",
                        "name": "Medieval Madness",
                        "slug": "medieval-madness",
                        "model": {
                            "name": "Medieval Madness",
                            "manufacturer": "Williams",
                            "year": 1997,
                        },
                    },
                    {
                        "asset_id": "M0013",
                        "name": "Blackout",
                        "slug": "blackout",
                        "model": {"name": "Blackout", "manufacturer": "Williams", "year": 1980},
                    },
                ],
            },
        )
        result = await get_machines(API_URL, API_KEY)
        assert result == {
            "M0001": {"name": "Medieval Madness", "year": 1997},
            "M0013": {"name": "Blackout", "year": 1980},
        }

    @pytest.mark.asyncio
    async def test_null_year(self, mock_api) -> None:
        mock_api.get(
            f"{API_URL}machines/",
            payload={
                "machines": [
                    {
                        "asset_id": "M0001",
                        "name": "Mystery Machine",
                        "slug": "mystery",
                        "model": {"name": "Mystery", "manufacturer": "Unknown", "year": None},
                    },
                ],
            },
        )
        result = await get_machines(API_URL, API_KEY)
        assert result == {"M0001": {"name": "Mystery Machine", "year": None}}

    @pytest.mark.asyncio
    async def test_missing_model(self, mock_api) -> None:
        mock_api.get(
            f"{API_URL}machines/",
            payload={
                "machines": [
                    {"asset_id": "M0001", "name": "Old Machine", "slug": "old"},
                ],
            },
        )
        result = await get_machines(API_URL, API_KEY)
        assert result == {"M0001": {"name": "Old Machine", "year": None}}

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
