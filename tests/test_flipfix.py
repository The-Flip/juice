"""Tests for juice.flipfix — FlipFix API client."""

from __future__ import annotations

import pytest
from aioresponses import aioresponses

from juice.flipfix import get_machines, report_unplayable

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


class TestReportUnplayable:
    ENDPOINT = f"{API_URL}machines/M0003/problem-reports/"

    @pytest.mark.asyncio
    async def test_posts_unplayable_and_mark_broken(self, mock_api) -> None:
        captured = {}

        def _cb(url, **kwargs):
            captured.update(kwargs.get("json") or {})

        mock_api.post(
            self.ENDPOINT,
            status=201,
            payload={"problem_report": {"id": 42, "priority": "unplayable"}},
            callback=_cb,
        )
        ok = await report_unplayable(
            API_URL, API_KEY, "M0003", "Auto power-off: 175W vs 49W baseline"
        )
        assert ok is True
        assert captured["priority"] == "unplayable"
        assert captured["mark_broken"] is True
        assert "175W" in captured["description"]
        assert captured["reported_by_name"]

    @pytest.mark.asyncio
    async def test_existing_open_report_200_is_success(self, mock_api) -> None:
        # Idempotent path: server returns the existing open report with 200.
        mock_api.post(self.ENDPOINT, status=200, payload={"problem_report": {"id": 7}})
        ok = await report_unplayable(API_URL, API_KEY, "M0003", "again")
        assert ok is True

    @pytest.mark.asyncio
    async def test_forbidden_key_returns_false(self, mock_api) -> None:
        # Read-only key (no write capability) → 403, best-effort returns False.
        mock_api.post(self.ENDPOINT, status=403, payload={"success": False, "error": "forbidden"})
        ok = await report_unplayable(API_URL, API_KEY, "M0003", "x")
        assert ok is False

    @pytest.mark.asyncio
    async def test_network_error_returns_false(self, mock_api) -> None:
        mock_api.post(self.ENDPOINT, exception=ConnectionError("offline"))
        ok = await report_unplayable(API_URL, API_KEY, "M0003", "x")
        assert ok is False

    @pytest.mark.asyncio
    async def test_passes_occurred_at_when_given(self, mock_api) -> None:
        captured = {}
        mock_api.post(
            self.ENDPOINT,
            status=201,
            payload={"problem_report": {}},
            callback=lambda url, **kw: captured.update(kw.get("json") or {}),
        )
        await report_unplayable(
            API_URL, API_KEY, "M0003", "x", occurred_at="2026-06-13T20:54:00+00:00"
        )
        assert captured["occurred_at"] == "2026-06-13T20:54:00+00:00"
