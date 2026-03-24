"""Tests for juice.recorder."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from juice.collector import Account, Strip
from juice.recorder import PlugState, extract_asset_tag, poll_once, refresh_metadata
from juice.store import Store

# ---------------------------------------------------------------------------
# Asset tag extraction
# ---------------------------------------------------------------------------


class TestExtractAssetTag:
    def test_standard_format(self) -> None:
        assert extract_asset_tag("Blackout - M0013") == "M0013"

    def test_tag_at_end(self) -> None:
        assert extract_asset_tag("M0001") == "M0001"

    def test_tag_in_middle(self) -> None:
        assert extract_asset_tag("foo M0042 bar") == "M0042"

    def test_no_tag(self) -> None:
        assert extract_asset_tag("cooktop") is None

    def test_generic_plug_name(self) -> None:
        assert extract_asset_tag("Plug 2") is None

    def test_multiple_tags_returns_first(self) -> None:
        assert extract_asset_tag("M0001 and M0002") == "M0001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_strip(device_id: str, children: list[dict], account: Account | None = None) -> Strip:
    """Create a Strip with a mocked _sysinfo and _passthrough."""
    strip = Strip.__new__(Strip)
    strip.device_id = device_id
    strip.alias = f"Strip {device_id}"
    strip.model = "HS300(US)"
    strip._server_url = "https://example.com"
    strip._account = account or MagicMock()
    strip._plugs = None

    sysinfo = {"children": children, "child_num": len(children)}

    async def _sysinfo():
        from juice.collector import Plug

        strip._plugs = [Plug(child_id=c["id"], alias=c["alias"], strip=strip) for c in children]
        return sysinfo

    strip._sysinfo = _sysinfo
    return strip


def _emeter_data(
    power_mw: int = 100_000, voltage_mv: int = 120_000, current_ma: int = 833, total_wh: int = 5_000
) -> dict:
    return {
        "emeter": {
            "get_realtime": {
                "power_mw": power_mw,
                "voltage_mv": voltage_mv,
                "current_ma": current_ma,
                "total_wh": total_wh,
            },
        },
    }


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


# ---------------------------------------------------------------------------
# poll_once
# ---------------------------------------------------------------------------


class TestPollOnce:
    @pytest.mark.asyncio
    async def test_polls_active_plug(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=500_000))

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts)

        readings = store._conn.execute("SELECT watts FROM readings").fetchall()
        assert len(readings) == 1
        assert readings[0][0] == pytest.approx(500.0)

    @pytest.mark.asyncio
    async def test_off_plug_records_zero_watts(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 0}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts)

        # First OFF poll writes 0W to DB
        readings = store._conn.execute("SELECT watts FROM readings").fetchall()
        assert len(readings) == 1
        assert readings[0][0] == 0.0
        strip._passthrough.assert_not_called()

    @pytest.mark.asyncio
    async def test_off_plug_rate_limits_db_writes(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 0}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}

        # First poll — writes to DB
        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts1)
        assert len(store._conn.execute("SELECT * FROM readings").fetchall()) == 1

        # Second poll 10s later — skips DB write
        ts2 = datetime(2026, 3, 15, 12, 0, 10, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts2)
        assert len(store._conn.execute("SELECT * FROM readings").fetchall()) == 1

        # Third poll 61s after first — writes again
        ts3 = datetime(2026, 3, 15, 12, 1, 1, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts3)
        assert len(store._conn.execute("SELECT * FROM readings").fetchall()) == 2

    @pytest.mark.asyncio
    async def test_skips_idle_plug(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock()

        # Simulate: last reading was 0W, checked 10s ago
        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(
                last_watts=0.0, last_check=datetime(2026, 3, 15, 11, 59, 50, tzinfo=UTC)
            ),
        }
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts)

        readings = store._conn.execute("SELECT * FROM readings").fetchall()
        assert len(readings) == 0
        strip._passthrough.assert_not_called()

    @pytest.mark.asyncio
    async def test_rechecks_idle_after_60s(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=0))

        # Last reading was 0W, checked 61s ago
        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(
                last_watts=0.0, last_check=datetime(2026, 3, 15, 11, 58, 59, tzinfo=UTC)
            ),
        }
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts)

        readings = store._conn.execute("SELECT * FROM readings").fetchall()
        assert len(readings) == 1
        strip._passthrough.assert_called_once()

    @pytest.mark.asyncio
    async def test_survives_emeter_error(self, store: Store) -> None:
        children = [
            {"id": "c01", "alias": "Blackout - M0013", "state": 1},
            {"id": "c02", "alias": "Hyperball - M0014", "state": 1},
        ]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock(
            side_effect=[RuntimeError("device offline"), _emeter_data(power_mw=200_000)]
        )

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts)

        # Second plug should still be recorded despite first failing
        readings = store._conn.execute("SELECT * FROM readings").fetchall()
        assert len(readings) == 1

    @pytest.mark.asyncio
    async def test_force_poll_overrides_idle_skip(self, store: Store) -> None:
        """A plug in force_poll should be read even if idle-skipped normally."""
        from juice.server import RecorderState

        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=500_000))

        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")

        recorder_state = RecorderState()
        recorder_state.force_poll.add(plug_id)

        # Simulate: last reading was 0W, checked 10s ago (normally skipped)
        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(
                last_watts=0.0, last_check=datetime(2026, 3, 15, 11, 59, 50, tzinfo=UTC)
            ),
        }
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip], store, plug_states, ts, recorder_state)

        # Should have been polled despite idle timer
        readings = store._conn.execute("SELECT watts FROM readings").fetchall()
        assert len(readings) == 1
        assert readings[0][0] == pytest.approx(500.0)

        # Should be removed from force_poll after reading
        assert plug_id not in recorder_state.force_poll


# ---------------------------------------------------------------------------
# refresh_metadata
# ---------------------------------------------------------------------------


class TestRefreshMetadata:
    @pytest.mark.asyncio
    async def test_detects_assignment(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)

        account = MagicMock()
        account.strips = AsyncMock(return_value=[strip])

        machines = {"M0013": {"name": "Blackout", "year": 1980}}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)

        await refresh_metadata(account, store, machines, ts)

        # Machine should be in the DB
        rows = store._conn.execute("SELECT * FROM machines").fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "M0013"

        # Assignment should be created
        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 1
        assert rows[0][3] is None  # assigned_until is NULL (current)

    @pytest.mark.asyncio
    async def test_no_assignment_for_non_tagged_plug(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "cooktop", "state": 1}]
        strip = _make_strip("d1", children)

        account = MagicMock()
        account.strips = AsyncMock(return_value=[strip])

        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await refresh_metadata(account, store, {}, ts)

        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 0
