"""Tests for juice.recorder."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from juice.collector import Account, Outlet, Strip
from juice.flipfix import ReportResult
from juice.recorder import (
    OFFLINE_FAILURE_THRESHOLD,
    RETRO_PLAY_HOURS_MIGRATION,
    PlugState,
    apply_retro_play_hours_migration,
    check_overload,
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
    async def test_watch_window_overrides_idle_skip(self, store: Store) -> None:
        """A plug inside its watch window is read even when idle-skip would apply."""
        from juice.server import RecorderState

        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=500_000))

        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")

        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        recorder_state = RecorderState()
        recorder_state.watch_until[plug_id] = ts + timedelta(seconds=10)

        # Last reading was 0W, checked 10s ago — normally idle-skipped.
        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(
                last_watts=0.0, last_check=datetime(2026, 3, 15, 11, 59, 50, tzinfo=UTC)
            ),
        }
        await poll_once([strip], store, plug_states, ts, recorder_state)

        # Polled despite the idle timer.
        readings = store._conn.execute("SELECT watts FROM readings").fetchall()
        assert len(readings) == 1
        assert readings[0][0] == pytest.approx(500.0)
        # Still watched (window not yet expired): unlike the old one-shot poll,
        # the deadline isn't cleared on read.
        assert plug_id in recorder_state.watch_until

    @pytest.mark.asyncio
    async def test_watch_window_reads_every_cycle_then_expires(self, store: Store) -> None:
        """Within the window the plug is read each cycle; once expired it idle-skips again."""
        from juice.server import RecorderState

        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        # Plug keeps reading 0W (no load behind it yet).
        strip._passthrough = AsyncMock(return_value=_emeter_data(power_mw=0))

        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")
        t0 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        recorder_state = RecorderState()
        recorder_state.watch_until[plug_id] = t0 + timedelta(seconds=10)

        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(last_watts=0.0, last_check=t0 - timedelta(seconds=5)),
        }

        # Two cycles inside the window → two reads (one-shot would only read once).
        await poll_once([strip], store, plug_states, t0, recorder_state)
        await poll_once([strip], store, plug_states, t0 + timedelta(seconds=3), recorder_state)
        assert strip._passthrough.await_count == 2

        # A cycle past the deadline → idle-skip resumes (no further read) and the
        # expired entry is pruned.
        await poll_once([strip], store, plug_states, t0 + timedelta(seconds=11), recorder_state)
        assert strip._passthrough.await_count == 2
        assert plug_id not in recorder_state.watch_until

    @pytest.mark.asyncio
    async def test_delayed_load_captured_within_window(self, store: Store) -> None:
        """The gap this fixes: a load appearing a few seconds after energizing is recorded."""
        from juice.server import RecorderState

        children = [{"id": "c01", "alias": "Blackout - M0013", "state": 1}]
        strip = _make_strip("d1", children)
        # First read 0W (machine not drawing yet), second read 250W (load came up).
        strip._passthrough = AsyncMock(
            side_effect=[_emeter_data(power_mw=0), _emeter_data(power_mw=250_000)]
        )

        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")
        t0 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        recorder_state = RecorderState()
        recorder_state.watch_until[plug_id] = t0 + timedelta(seconds=10)

        # Energized just now: last reading 0W, checked moments ago (would idle-skip).
        plug_states: dict[str, PlugState] = {
            "d1:c01": PlugState(last_watts=0.0, last_check=t0 - timedelta(seconds=1)),
        }
        await poll_once([strip], store, plug_states, t0, recorder_state)
        await poll_once([strip], store, plug_states, t0 + timedelta(seconds=3), recorder_state)

        watts = [
            r[0] for r in store._conn.execute("SELECT watts FROM readings ORDER BY ts").fetchall()
        ]
        assert watts == pytest.approx([0.0, 250.0])  # the delayed load was captured, not hidden

    @pytest.mark.asyncio
    async def test_expired_watch_pruned_even_without_device(self, store: Store) -> None:
        """A stale deadline (e.g. plug went offline) is pruned each cycle."""
        from juice.server import RecorderState

        t0 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        recorder_state = RecorderState()
        recorder_state.watch_until[999] = t0 - timedelta(seconds=1)  # already expired

        await poll_once([], store, {}, t0, recorder_state)
        assert 999 not in recorder_state.watch_until


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
    async def test_reassignment_clears_overload_window(self, store: Store) -> None:
        from juice.overload import OverloadWindow
        from juice.server import RecorderState

        account = MagicMock()
        state = RecorderState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)

        # Plug starts assigned to M0013, with an accumulated overload window.
        account.devices = AsyncMock(
            return_value=[
                _make_strip("d1", [{"id": "c01", "alias": "Blackout - M0013", "state": 1}])
            ]
        )
        await refresh_metadata(
            account, store, {"M0013": {"name": "Blackout", "year": 1980}}, ts, state
        )
        plug_id = next(iter(state.assignments))
        state.overload_windows[plug_id] = OverloadWindow()
        state.overload_windows[plug_id].add(ts, 200.0)

        # Same outlet relabeled to a different machine -> window must be dropped.
        account.devices = AsyncMock(
            return_value=[
                _make_strip("d1", [{"id": "c01", "alias": "Pin-Bot - M0099", "state": 1}])
            ]
        )
        await refresh_metadata(
            account, store, {"M0099": {"name": "Pin-Bot", "year": 1986}}, ts, state
        )

        assert state.assignments[plug_id][1] == "M0099"
        assert plug_id not in state.overload_windows

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

    def test_populates_lock_modes(self, store: Store) -> None:
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))
        store.set_machine_lock_mode(mid, "off")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.lock_modes == {"M0013": "off"}

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

    def test_populates_strip_orders(self, store: Store) -> None:
        from juice.server import RecorderState

        store.set_strip_orders(["d1", "d2"])

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_orders == {"d1": 0, "d2": 1}

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


# ---------------------------------------------------------------------------
# Overload detection + auto-shutdown
# ---------------------------------------------------------------------------


class TestCheckOverload:
    BASE_TS = datetime(2026, 6, 13, 20, 0, 0, tzinfo=UTC)

    def _setup(self, store: Store, *, baseline: float | None = 49.0, mode: str = "live"):
        from juice.overload import SUSTAIN_SECONDS
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d1", "c01", "Trade Winds - M0003")
        store.ensure_machine("M0003", "Trade Winds")
        state = RecorderState()
        state.overload_mode = mode
        state.assignments[plug_id] = ("Trade Winds", "M0003", None)
        if baseline is not None:
            state.power_baselines["M0003"] = baseline
        fake = AsyncMock()
        state.plug_objects[plug_id] = fake
        self._sustain = SUSTAIN_SECONDS
        return state, plug_id, fake

    async def _feed(self, state, store, plug_id, watts, *, seconds=None):
        """Feed a constant load across a span longer than the sustain window."""
        seconds = seconds if seconds is not None else self._sustain + 30
        t = 0
        while t <= seconds:
            await check_overload(state, store, plug_id, self.BASE_TS + timedelta(seconds=t), watts)
            t += 5

    @pytest.mark.asyncio
    async def test_sustained_overload_shuts_down_and_locks_off(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        q: asyncio.Queue = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert state.lock_modes["M0003"] == "off"
        assert store.get_lock_modes() == {"M0003": "off"}
        rows = store.recent_power_events(limit=10)
        overload_row = next(r for r in rows if r["source"] == "overload")
        assert overload_row["action"] == "turn_off"
        assert overload_row["result"] == "ok"
        # FlipFix isn't configured here, so the skip is surfaced as an error audit row.
        assert any(r["source"] == "flipfix" and r["result"] == "error" for r in rows)
        ev = q.get_nowait()
        assert ev["type"] == "overload_shutdown"
        assert ev["asset_id"] == "M0003"
        assert ev["shadow"] is False

    @pytest.mark.asyncio
    async def test_normal_play_does_not_shut_down(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        await self._feed(state, store, plug_id, 45.0)
        fake.turn_off.assert_not_called()
        assert "M0003" not in state.lock_modes
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_shadow_mode_audits_but_does_not_act(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store, mode="shadow")
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()
        assert "M0003" not in state.lock_modes
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "shadow"

    @pytest.mark.asyncio
    async def test_off_mode_does_nothing(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store, mode="off")
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_unarmed_machine_skipped(self, store: Store) -> None:
        # No baseline yet -> not armed -> never auto-shut-down.
        state, plug_id, fake = self._setup(store, baseline=None)
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_locked_off_skipped(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        state.lock_modes["M0003"] = "off"
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()

    @pytest.mark.asyncio
    async def test_turn_off_failure_audited_as_error(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        fake.turn_off.side_effect = RuntimeError("Device is offline")
        await self._feed(state, store, plug_id, 175.0)
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "error"
        # Lock NOT engaged when the power-off didn't succeed.
        assert "M0003" not in state.lock_modes

    def _patch_flipfix(self, monkeypatch, *, report=None, log_ok=True):
        """Patch the FlipFix client; return (report_calls, logentry_calls)."""
        if report is None:
            report = ReportResult(201, 42)
        report_calls: list = []
        log_calls: list = []

        async def _report(url, key, asset_id, description, *, occurred_at=None, mark_broken=True):
            report_calls.append({"asset_id": asset_id, "description": description})
            return report

        async def _log(url, key, report_id, text, *, occurred_at=None):
            log_calls.append({"report_id": report_id, "text": text})
            return log_ok

        monkeypatch.setattr("juice.recorder.report_unplayable", _report)
        monkeypatch.setattr("juice.recorder.add_log_entry", _log)
        return report_calls, log_calls

    @pytest.mark.asyncio
    async def test_files_new_report_on_first_overload(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(201, 42))

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert len(reports) == 1 and reports[0]["asset_id"] == "M0003"
        assert "overload" in reports[0]["description"]
        assert logs == []  # 201 created -> no separate log entry
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "ok" and "#42" in flip["error"]

    @pytest.mark.asyncio
    async def test_recurrence_appends_log_entry(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(200, 7))

        await self._feed(state, store, plug_id, 175.0)

        # 200 = an open unplayable report already exists -> log the recurrence onto it.
        assert len(logs) == 1 and logs[0]["report_id"] == 7
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert (
            flip["result"] == "ok" and "#7" in flip["error"] and "append" in flip["error"].lower()
        )

    @pytest.mark.asyncio
    async def test_report_failure_audited_as_error(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(403, None))

        await self._feed(state, store, plug_id, 175.0)

        assert logs == []
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "error" and "403" in flip["error"]

    @pytest.mark.asyncio
    async def test_description_has_duration_peak_and_link(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        state.public_url = "https://juice.example.com"
        reports, _ = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        desc = reports[0]["description"]
        assert "peak 175W" in desc
        assert "for " in desc and "m " in desc  # a formatted duration like "2m 00s"
        assert f"https://juice.example.com/machine/{plug_id}" in desc

    @pytest.mark.asyncio
    async def test_no_flipfix_report_when_unconfigured(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)  # no flipfix creds set
        reports, logs = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert reports == [] and logs == []
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "error" and "not configured" in flip["error"].lower()

    @pytest.mark.asyncio
    async def test_shadow_mode_does_not_report(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store, mode="shadow")
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_not_called()
        assert reports == [] and logs == []


# ---------------------------------------------------------------------------
# Air monitoring poll
# ---------------------------------------------------------------------------


class TestAirPollOnce:
    @pytest.mark.asyncio
    async def test_persists_sensors_and_readings(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_poll_once

        ts = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        reading = AirReading(
            mac="MAC1",
            ts=ts,
            temperature=22.5,
            humidity=45.0,
            co2=620.0,
            pm25=8.0,
            pm10=12.0,
            tvoc=130.0,
            noise=None,
            battery=88.0,
        )
        air_account = MagicMock()
        air_account.devices = AsyncMock(
            return_value=[(AirSensor(mac="MAC1", name="Main Floor", online=True), reading)]
        )

        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, ts)
            assert count == 1
            sensors = store.list_air_sensors()
            assert sensors[0]["mac"] == "MAC1"
            assert sensors[0]["name"] == "Main Floor"
            latest = store.air_latest()
            assert latest["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_no_sensors_is_a_noop(self) -> None:
        from juice.recorder import air_poll_once

        air_account = MagicMock()
        air_account.devices = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, datetime.now(UTC))
            assert count == 0
            assert store.list_air_sensors() == []


class TestAirBackfill:
    @pytest.mark.asyncio
    async def test_inserts_history(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        hist = [
            AirReading(mac="MAC1", ts=t, co2=600.0),
            AirReading(mac="MAC1", ts=t + timedelta(minutes=15), co2=620.0),
        ]
        acct = MagicMock()
        acct.history = AsyncMock(return_value=hist)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
            assert n == 2
            assert store.air_latest()["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_first_run_uses_default_lookback(self) -> None:
        from juice.air_collector import AirSensor
        from juice.recorder import air_backfill

        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        now = datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)
        with Store(":memory:") as store:
            await air_backfill(acct, store, [AirSensor("MAC1", "Main", True)], now, default_days=30)
        _mac, start_unix, end_unix = acct.history.call_args.args[:3]
        assert end_unix == int(now.timestamp())
        assert start_unix == end_unix - 30 * 86_400

    @pytest.mark.asyncio
    async def test_gap_fill_starts_after_last_stored(self) -> None:
        from juice.air_collector import AirSensor
        from juice.recorder import air_backfill

        last = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            # (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
            store.insert_air_readings(
                [(last, "MAC1", 22.0, 44.0, 600.0, 7.0, 10.0, 120.0, None, 90.0)]
            )
            await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
        _mac, start_unix, _end = acct.history.call_args.args[:3]
        assert start_unix == int(last.timestamp()) + 1

    @pytest.mark.asyncio
    async def test_one_sensor_failure_does_not_abort_others(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)

        async def _history(mac, *_args, **_kw):
            if mac == "BAD":
                raise RuntimeError("boom")
            return [AirReading(mac="GOOD", ts=t, co2=500.0)]

        acct = MagicMock()
        acct.history = AsyncMock(side_effect=_history)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct,
                store,
                [AirSensor("BAD", "Bad", True), AirSensor("GOOD", "Good", True)],
                datetime(2026, 6, 21, tzinfo=UTC),
            )
            assert n == 1
            assert "GOOD" in store.air_latest()


class TestRetroPlayHoursMigration:
    """The one-off startup migration that reapplies current calibrations to the
    frozen historical play-hours rollup (fixes Indiana Jones' stale Jul 6-12)."""

    @staticmethod
    def _seed_stale_rollup(store: Store) -> tuple[int, int]:
        """A calibrated+assigned machine with a historical hourly_play_seconds row
        that its readings (all ATTRACT under the current calibration) don't
        justify — i.e. a leftover from an older, laxer calibration."""
        from juice.state import Calibration

        pid = store.ensure_plug("d1", "c01", "Blackout - M0013")
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(pid, mid, datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=50.0))  # strict
        t0 = datetime(2026, 7, 6, 20, 0, 0, tzinfo=UTC)
        store.insert_readings(
            [
                (t0 + timedelta(seconds=i), pid, 300.0 * (1 + 0.003), 120.0, 2.5, 0.0)
                for i in range(120)
            ]
        )
        # Stale inflated rollup row (as if rolled up under a lenient calibration).
        store._conn.execute(
            "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)",
            [mid, datetime(2026, 7, 6, 15, 0, 0), 3000.0, 3000.0],
        )
        return pid, mid

    @pytest.mark.asyncio
    async def test_rebuilds_history_and_marks_once(self, store: Store) -> None:
        _pid, mid = self._seed_stale_rollup(store)
        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is False

        await apply_retro_play_hours_migration(store)

        # The stale play_seconds is gone — recomputed under the strict calibration.
        play = store._conn.execute(
            "SELECT COALESCE(SUM(play_seconds), 0) FROM hourly_play_seconds WHERE machine_id = ?",
            [mid],
        ).fetchone()[0]
        assert play == pytest.approx(0.0, abs=1.0)
        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is True

    @pytest.mark.asyncio
    async def test_failure_leaves_migration_unmarked_for_retry(
        self, store: Store, monkeypatch
    ) -> None:
        self._seed_stale_rollup(store)

        def boom(_mid: int) -> int:
            raise RuntimeError("rebuild blew up")

        monkeypatch.setattr(store, "rebuild_play_hours", boom)
        await apply_retro_play_hours_migration(store)  # swallows, doesn't mark

        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is False

    @pytest.mark.asyncio
    async def test_is_noop_when_already_applied(self, store: Store) -> None:
        _pid, mid = self._seed_stale_rollup(store)
        store.mark_migration(RETRO_PLAY_HOURS_MIGRATION)

        await apply_retro_play_hours_migration(store)

        # Marker was already set, so the stale row is left untouched.
        play = store._conn.execute(
            "SELECT SUM(play_seconds) FROM hourly_play_seconds WHERE machine_id = ?", [mid]
        ).fetchone()[0]
        assert play == pytest.approx(3000.0)
