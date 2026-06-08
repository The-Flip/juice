"""Tests for juice.recorder."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from juice.collector import Account, Outlet, Strip
from juice.recorder import (
    OFFLINE_FAILURE_THRESHOLD,
    PlugState,
    extract_asset_tag,
    hydrate_assignments,
    note_device_failure,
    note_device_ok,
    poll_once,
    refresh_metadata,
)
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


def _make_outlet(
    device_id: str,
    alias: str = "Snack Machine",
    relay_state: int = 1,
    account: Account | None = None,
) -> Outlet:
    """Create an Outlet (e.g. EP10) with mocked _sysinfo and _passthrough."""
    outlet = Outlet.__new__(Outlet)
    outlet.device_id = device_id
    outlet.alias = alias
    outlet.model = "EP10(US)"
    outlet.has_emeter = False
    outlet._server_url = "https://example.com"
    outlet._account = account or MagicMock()
    outlet._plug = None

    async def _sysinfo():
        return {"model": "EP10(US)", "alias": alias, "relay_state": relay_state}

    outlet._sysinfo = _sysinfo
    return outlet


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
# poll_once — EP10 outlets (no emeter)
# ---------------------------------------------------------------------------


class TestPollOnceOutlet:
    @pytest.mark.asyncio
    async def test_on_outlet_records_null_watts_no_emeter_call(self, store: Store) -> None:
        outlet = _make_outlet("ep10-a", alias="Snack Machine", relay_state=1)
        outlet._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts)

        rows = store._conn.execute(
            "SELECT watts, voltage, amps, total_kwh FROM readings"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == (None, None, None, None)
        # No emeter passthrough should have happened
        outlet._passthrough.assert_not_called()
        # Plug row records has_emeter=False
        plug_rows = store._conn.execute("SELECT has_emeter FROM plugs").fetchall()
        assert plug_rows == [(False,)]

    @pytest.mark.asyncio
    async def test_off_outlet_records_zero_watts(self, store: Store) -> None:
        outlet = _make_outlet("ep10-b", alias="Garage", relay_state=0)
        outlet._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts)

        rows = store._conn.execute("SELECT watts FROM readings").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 0.0
        outlet._passthrough.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_outlet_rate_limits_db_writes(self, store: Store) -> None:
        outlet = _make_outlet("ep10-c", relay_state=1)
        outlet._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}

        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts1)
        # 10s later — same ON state, rate-limited, no new row
        ts2 = datetime(2026, 3, 15, 12, 0, 10, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts2)
        assert len(store._conn.execute("SELECT * FROM readings").fetchall()) == 1
        # 61s after first — writes again
        ts3 = datetime(2026, 3, 15, 12, 1, 1, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts3)
        assert len(store._conn.execute("SELECT * FROM readings").fetchall()) == 2

    @pytest.mark.asyncio
    async def test_outlet_state_transition_writes_immediately(self, store: Store) -> None:
        outlet = _make_outlet("ep10-d", relay_state=1)
        outlet._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}
        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts1)

        # Flip to OFF — should write a new row immediately, not wait for rate-limit
        async def _new_sysinfo():
            return {"model": "EP10(US)", "alias": outlet.alias, "relay_state": 0}

        outlet._sysinfo = _new_sysinfo
        ts2 = datetime(2026, 3, 15, 12, 0, 5, tzinfo=UTC)
        await poll_once([outlet], store, plug_states, ts2)

        rows = store._conn.execute("SELECT watts FROM readings ORDER BY ts").fetchall()
        assert len(rows) == 2
        assert rows[0][0] is None  # ON
        assert rows[1][0] == 0.0  # OFF

    @pytest.mark.asyncio
    async def test_mixed_strip_and_outlet(self, store: Store) -> None:
        strip_children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("hs300-d1", strip_children)
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=300_000))

        outlet = _make_outlet("ep10-x", alias="Snack", relay_state=1)
        outlet._passthrough = AsyncMock()

        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await poll_once([strip, outlet], store, plug_states, ts)

        rows = store._conn.execute(
            "SELECT p.has_emeter, r.watts FROM readings r JOIN plugs p USING (plug_id) ORDER BY r.plug_id"
        ).fetchall()
        assert len(rows) == 2
        # HS300 row: has_emeter True, watts ~ 300
        assert rows[0][0] is True
        assert rows[0][1] == pytest.approx(300.0)
        # EP10 row: has_emeter False, watts NULL
        assert rows[1][0] is False
        assert rows[1][1] is None


# ---------------------------------------------------------------------------
# refresh_metadata
# ---------------------------------------------------------------------------


class TestRefreshMetadata:
    @pytest.mark.asyncio
    async def test_detects_assignment(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)

        account = MagicMock()
        account.devices = AsyncMock(return_value=[strip])

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
    async def test_refreshes_strip_names_from_store(self, store: Store) -> None:
        from juice.server import RecorderState

        store.set_strip_name("d1", "Back Wall")

        account = MagicMock()
        account.devices = AsyncMock(return_value=[])

        state = RecorderState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await refresh_metadata(account, store, {}, ts, state)

        assert state.strip_names == {"d1": "Back Wall"}

    @pytest.mark.asyncio
    async def test_one_device_failure_does_not_abort_refresh(self, store: Store) -> None:
        """A device whose child_states() raises should be skipped, not block others."""
        good = _make_strip("good-d", [{"id": "c01", "alias": "Working - M0013", "state": 1}])

        # A broken device — child_states raises.
        broken = _make_strip("broken-d", [])

        async def _boom():
            raise RuntimeError("device offline")

        broken._sysinfo = _boom

        account = MagicMock()
        account.devices = AsyncMock(return_value=[broken, good])

        machines = {"M0013": {"name": "Working", "year": 1980}}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)

        # Must not raise.
        await refresh_metadata(account, store, machines, ts)

        # Good device's assignment still created.
        rows = store._conn.execute("SELECT machine_id FROM assignments").fetchall()
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_no_assignment_for_non_tagged_plug(self, store: Store) -> None:
        children = [{"id": "c01", "alias": "cooktop", "state": 1}]
        strip = _make_strip("d1", children)

        account = MagicMock()
        account.devices = AsyncMock(return_value=[strip])

        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        await refresh_metadata(account, store, {}, ts)

        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Device health (offline tracking)
# ---------------------------------------------------------------------------


class TestDeviceHealth:
    def test_marks_offline_at_threshold(self) -> None:
        from juice.server import RecorderState

        state = RecorderState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        exc = RuntimeError("Passthrough failed: Device is offline")

        for _ in range(OFFLINE_FAILURE_THRESHOLD - 1):
            note_device_failure(state, "d1", ts, exc)
        assert "d1" not in state.offline_since  # not yet

        note_device_failure(state, "d1", ts, exc)
        assert "d1" in state.offline_since

    def test_ok_clears_offline(self) -> None:
        from juice.server import RecorderState

        state = RecorderState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        for _ in range(OFFLINE_FAILURE_THRESHOLD):
            note_device_failure(state, "d1", ts, RuntimeError("offline"))
        assert "d1" in state.offline_since

        note_device_ok(state, "d1")
        assert "d1" not in state.offline_since
        assert "d1" not in state.device_failures

    def test_helpers_noop_without_state(self) -> None:
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        note_device_failure(None, "d1", ts, RuntimeError("offline"))  # must not raise
        note_device_ok(None, "d1")

    @pytest.mark.asyncio
    async def test_poll_once_skips_offline_device(self, store: Store) -> None:
        from juice.server import RecorderState

        strip = _make_strip("d1", [{"id": "c01", "alias": "Blackout - M0013", "state": 1}])
        calls = {"n": 0}

        async def _boom():
            calls["n"] += 1
            raise RuntimeError("Passthrough failed: Device is offline")

        strip._sysinfo = _boom

        state = RecorderState()
        plug_states: dict[str, PlugState] = {}
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)

        # Fail enough times to trip the offline threshold.
        for _ in range(OFFLINE_FAILURE_THRESHOLD):
            await poll_once([strip], store, plug_states, ts, state)
        assert "d1" in state.offline_since
        attempts_when_offline = calls["n"]

        # Once offline, the fast loop must not touch it again.
        await poll_once([strip], store, plug_states, ts, state)
        assert calls["n"] == attempts_when_offline

    @pytest.mark.asyncio
    async def test_refresh_metadata_clears_offline_on_recovery(self, store: Store) -> None:
        from juice.server import RecorderState

        state = RecorderState()
        state.offline_since["d1"] = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        state.device_failures["d1"] = 5

        strip = _make_strip("d1", [{"id": "c01", "alias": "Blackout - M0013", "state": 1}])
        account = MagicMock()
        account.devices = AsyncMock(return_value=[strip])
        machines = {"M0013": {"name": "Blackout", "year": 1980}}
        ts = datetime(2026, 3, 15, 12, 1, 0, tzinfo=UTC)

        await refresh_metadata(account, store, machines, ts, state)

        assert "d1" not in state.offline_since
        assert "d1" not in state.device_failures


# ---------------------------------------------------------------------------
# hydrate_assignments
# ---------------------------------------------------------------------------


class TestHydrateAssignments:
    def test_fills_state_from_open_assignments(self, store: Store) -> None:
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.assignments[plug_id] == ("Blackout", "M0013", None)
        assert state.plugs[plug_id] == ("d-ep10", "", "Blackout - M0013")
        assert state.plug_has_emeter[plug_id] is False

    def test_noop_without_state(self, store: Store) -> None:
        hydrate_assignments(None, store)  # must not raise

    def test_populates_locked_assets(self, store: Store) -> None:
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))
        store.set_machine_locked(mid, True)

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.locked_assets == {"M0013"}

    def test_populates_strip_names(self, store: Store) -> None:
        from juice.server import RecorderState

        store.set_strip_name("d1", "Back Wall")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_names == {"d1": "Back Wall"}

    def test_populates_circuit_devices(self, store: Store) -> None:
        from juice.server import RecorderState

        cid = store.create_circuit("P1", "B20", "coin-op", 20.0)
        store.set_device_circuit("d1", cid)

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.circuit_devices == {"d1": cid}
        assert state.circuits[cid]["panel"] == "P1"

    def test_populates_unassigned_plugs_too(self, store: Store) -> None:
        # The strip outlet map must show every outlet of an offline-at-boot
        # strip, not just the assigned ones — so plugs hydrate from the full
        # plugs table, not only open assignments.
        from juice.server import RecorderState

        assigned = store.ensure_plug("d1", "c00", "Blackout - M0013")
        unassigned = store.ensure_plug("d1", "c01", "Unused", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(assigned, mid, datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.plugs[unassigned] == ("d1", "c01", "Unused")
        assert state.plug_has_emeter[unassigned] is False
        assert unassigned not in state.assignments
        assert state.plugs[assigned] == ("d1", "c00", "Blackout - M0013")
