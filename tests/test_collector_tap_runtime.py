"""The tap-driven floor as a running collector: what `juice serve --collector
tap` starts instead of `recorder.record`.

`record()` did four things besides polling -- hydrate, resolve the overload
mode, fetch the FlipFix roster, roll up -- and one thing every minute: refresh
the roster and the operator state and reconcile assignments. The tap collector
must do the same, from the store's aliases rather than a device probe, or a
machine renamed in FlipFix would sit unassigned until an outlet was relabelled.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta

import pytest

from juice.collector_tap import (
    TapControl,
    TapPlug,
    housekeeping_loop,
    housekeeping_pass,
    hydrate_assignments,
    reconcile_from_store,
    roster_projection,
    run_tap_collector,
    tap_collector_startup,
)
from juice.rollups import RETRO_PLAY_HOURS_MIGRATION, RollupWorker
from juice.server import RecorderState
from juice.store import Store

DEV = "STRIP1"
NOW = datetime(2026, 9, 13, 16, 0, 0, tzinfo=UTC)
MACHINES = {
    "M0013": {"name": "Blackout", "year": 1980},
    "M0099": {"name": "Lightning", "year": 2024},
}


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


@pytest.fixture
def state():
    return RecorderState()


def _seed(store: Store) -> tuple[int, int]:
    a = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013", has_emeter=True)
    b = store.ensure_plug(DEV, f"{DEV}01", "spare", has_emeter=True)
    return a, b


class TestReconcileFromTheStore:
    def test_tagged_aliases_become_assignments_and_plugs_become_controllable(
        self, store, state
    ) -> None:
        a, b = _seed(store)
        control = TapControl()

        reconcile_from_store(state, store, MACHINES, NOW, control=control)

        assert state.assignments[a] == ("Blackout", "M0013", 1980)
        assert b not in state.assignments
        assert isinstance(state.plug_objects[a], TapPlug)
        assert isinstance(state.plug_objects[b], TapPlug), "untagged outlets are still switchable"
        assert state.plugs[a] == (DEV, f"{DEV}00", "Blackout - M0013")

    def test_a_flipfix_rename_lands_without_a_roster_frame(self, store, state) -> None:
        """The cloud recorder picks a FlipFix change up on its 60 s refresh.
        tap re-sends its roster only when an *outlet* changes, so the store's
        aliases are the only path that keeps parity."""
        a, _ = _seed(store)
        reconcile_from_store(state, store, MACHINES, NOW, control=None)
        assert state.assignments[a][0] == "Blackout"

        renamed = {**MACHINES, "M0013": {"name": "Blackout (1980)", "year": 1980}}
        reconcile_from_store(state, store, renamed, NOW + timedelta(minutes=1), control=None)

        assert state.assignments[a][0] == "Blackout (1980)"

    def test_an_empty_roster_unassigns_nothing(self, store, state) -> None:
        """The same guard as the frame path, for the same reason: FlipFix down
        at boot must not clear the floor."""
        a, _ = _seed(store)
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(a, machine_id, NOW - timedelta(days=1))

        reconcile_from_store(state, store, {}, NOW, control=None)

        assert store.list_open_assignments(), "the assignment survived"

    def test_the_roster_projection_reads_the_live_flipfix_roster(self, store, state) -> None:
        """What `create_app` gets as `tap_devices`: a frame is projected
        against whatever FlipFix said *most recently*, not at wiring time."""
        control = TapControl()
        project = roster_projection(state, store, control)
        state.flipfix_machines = {}
        project([{"device_id": DEV, "child_id": f"{DEV}00", "alias": "Blackout - M0013"}])
        assert state.assignments == {}

        state.flipfix_machines = MACHINES
        project([{"device_id": DEV, "child_id": f"{DEV}00", "alias": "Blackout - M0013"}])
        plug = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        assert state.assignments[plug][1] == "M0013"
        assert isinstance(state.plug_objects[plug], TapPlug)


class TestStartup:
    async def test_it_does_what_record_did_before_the_first_poll(
        self, store, state, monkeypatch
    ) -> None:
        a, _ = _seed(store)
        t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=30)
        store.insert_readings(
            [(t0 + timedelta(seconds=i), a, 250.0, 120.0, 2.1, 0.0) for i in range(180)]
        )
        monkeypatch.setenv("JUICE_OVERLOAD_PROTECTION", "shadow")

        async def machines(_url, _key):
            return MACHINES

        monkeypatch.setattr("juice.flipfix.get_machines", machines)
        rollups = RollupWorker(store)
        try:
            await tap_collector_startup(
                state,
                store,
                rollups,
                flipfix_url="https://flipfix.test",
                flipfix_key="k",
                public_url="https://juice.test/",
                control=TapControl(),
            )
        finally:
            rollups.close()

        assert state.overload_mode == "shadow"
        from juice.overload import TAP_MAX_GAP_S

        assert state.overload_max_gap_s == TAP_MAX_GAP_S, "windows fed at 1 Hz get the tight bound"
        assert (
            state.flipfix_url == "https://flipfix.test" and state.public_url == "https://juice.test"
        )
        assert state.flipfix_machines == MACHINES
        assert state.assignments[a][1] == "M0013"
        assert isinstance(state.plug_objects[a], TapPlug)
        assert list(state.watt_buffers[a]), "sparklines are seeded from recent readings"
        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION)
        assert store._conn.execute("SELECT count(*) FROM hourly_usage").fetchone()[0] > 0

    async def test_flipfix_down_at_boot_leaves_the_hydrated_floor_alone(
        self, store, state, monkeypatch
    ) -> None:
        a, _ = _seed(store)
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(a, machine_id, NOW - timedelta(days=1))

        async def down(_url, _key):
            return {}

        monkeypatch.setattr("juice.flipfix.get_machines", down)
        rollups = RollupWorker(store)
        try:
            await tap_collector_startup(
                state,
                store,
                rollups,
                flipfix_url="u",
                flipfix_key="k",
                public_url=None,
                control=None,
            )
        finally:
            rollups.close()

        assert state.flipfix_machines == {}
        assert a in state.assignments, "hydrated, and not unassigned by an empty roster"


class TestHousekeeping:
    async def test_a_pass_refreshes_the_roster_and_operator_state_and_reconciles(
        self, store, state, monkeypatch
    ) -> None:
        a, _ = _seed(store)
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.set_machine_lock_mode(machine_id, "off")

        async def machines(_url, _key):
            return MACHINES

        monkeypatch.setattr("juice.flipfix.get_machines", machines)
        await housekeeping_pass(
            state, store, flipfix_url="u", flipfix_key="k", control=None, now=NOW
        )

        assert state.flipfix_machines == MACHINES
        assert state.lock_modes == {"M0013": "off"}, "operator state is re-read from the store"
        assert state.assignments[a][1] == "M0013"

    async def test_a_flipfix_blip_keeps_the_last_roster(
        self, store, state, monkeypatch, caplog
    ) -> None:
        """The bug the cloud path had until #103: `get_machines` answers `{}`
        for any failure, and an empty roster must never replace a good one."""
        a, _ = _seed(store)
        state.flipfix_machines = MACHINES
        reconcile_from_store(state, store, MACHINES, NOW, control=None)

        async def blip(_url, _key):
            return {}

        monkeypatch.setattr("juice.flipfix.get_machines", blip)
        with caplog.at_level(logging.WARNING, logger="juice.collector_tap"):
            await housekeeping_pass(
                state, store, flipfix_url="u", flipfix_key="k", control=None, now=NOW
            )

        assert state.flipfix_machines == MACHINES
        assert a in state.assignments
        assert any("keeping the last roster" in r.getMessage() for r in caplog.records)

    async def test_without_flipfix_the_pass_still_refreshes_operator_state(
        self, store, state
    ) -> None:
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.set_machine_lock_mode(machine_id, "on")
        await housekeeping_pass(
            state, store, flipfix_url=None, flipfix_key=None, control=None, now=NOW
        )
        assert state.lock_modes == {"M0013": "on"}

    async def test_the_loop_runs_passes_on_its_interval_and_survives_a_failure(
        self, store, state, monkeypatch
    ) -> None:
        calls = []

        async def flaky(_url, _key):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("flipfix exploded")
            return MACHINES

        monkeypatch.setattr("juice.flipfix.get_machines", flaky)
        task = asyncio.create_task(
            housekeeping_loop(
                state, store, flipfix_url="u", flipfix_key="k", control=None, interval=0.02
            )
        )
        await asyncio.sleep(0.15)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert len(calls) >= 3
        assert state.flipfix_machines == MACHINES


class TestRunTapCollector:
    async def test_it_starts_up_then_keeps_house_and_sweeps(
        self, store, state, monkeypatch
    ) -> None:
        from juice.collector_tap import LiveProjector

        a, _ = _seed(store)

        async def machines(_url, _key):
            return MACHINES

        monkeypatch.setattr("juice.flipfix.get_machines", machines)
        monkeypatch.setattr("juice.collector_tap.LIVE_STALE_S", 0.01)
        rollups = RollupWorker(store)
        projector = LiveProjector(state, store)
        task = asyncio.create_task(
            run_tap_collector(
                state,
                store,
                rollups,
                TapControl(),
                projector,
                flipfix_url="u",
                flipfix_key="k",
                public_url=None,
                interval=0.02,
            )
        )
        try:
            for _ in range(100):
                await asyncio.sleep(0.02)
                if DEV in state.offline_since and projector.sweeps > 2:
                    break
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            rollups.close()

        assert state.assignments[a][1] == "M0013"
        assert DEV in state.offline_since, "no tap ever spoke: the strip is unreachable"


class TestHydrateAssignments:
    def test_fills_state_from_open_assignments(self, store: Store) -> None:
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
        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))
        store.set_machine_lock_mode(mid, "off")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.lock_modes == {"M0013": "off"}

    def test_populates_strip_names(self, store: Store) -> None:
        store.set_strip_name("d1", "Back Wall")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_names == {"d1": "Back Wall"}

    def test_populates_circuit_devices(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "coin-op", 20.0)
        store.set_device_circuit("d1", cid)

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.circuit_devices == {"d1": cid}
        assert state.circuits[cid]["panel"] == "P1"

    def test_populates_strip_orders(self, store: Store) -> None:
        store.set_strip_orders(["d1", "d2"])

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_orders == {"d1": 0, "d2": 1}

    def test_populates_unassigned_plugs_too(self, store: Store) -> None:
        # The strip outlet map must show every outlet of an offline-at-boot
        # strip, not just the assigned ones — so plugs hydrate from the full
        # plugs table, not only open assignments.
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
