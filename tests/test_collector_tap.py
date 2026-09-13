"""The roster projection: tap's `devices` frame becoming plugs and assignments.

This is the half of the cutover that decides which machine is which. The cloud
recorder does it in `refresh_metadata` from a device it just polled; here the same
work is driven by a frame, which changes one thing that matters a great deal: the
frame can arrive before juice has ever spoken to FlipFix.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from juice.collector_tap import apply_devices
from juice.store import Store

DEV = "STRIP1"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _entry(child_id: str, alias: str, **extra) -> dict:
    return {"device_id": DEV, "child_id": child_id, "alias": alias, **extra}


class TestTheFlipFixGuard:
    """The scariest failure in the design, and it is a one-line branch.

    `refresh_metadata` closes the assignment of every outlet whose asset tag is
    not in the machine roster. Under the cloud recorder that is safe by accident:
    `record()` awaits `get_machines` before the first refresh, so the roster is
    never empty when the unassign branch runs. A `devices` frame has no such
    ordering -- it arrives when tap connects, which can be before the first
    FlipFix call, or during a FlipFix outage, or with a misconfigured API key.

    Unassigning on an empty roster would clear every machine on the floor, and
    the dashboard is keyed off assignments, so the museum would open to a wall of
    unassigned outlets.
    """

    def test_an_empty_roster_unassigns_nothing(self, store: Store) -> None:
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, machine_id, datetime(2026, 9, 1, tzinfo=UTC))

        # FlipFix has not answered yet -- or answered with nothing.
        apply_devices(None, store, [_entry(f"{DEV}00", "Blackout - M0013")], {}, datetime.now(UTC))

        still_assigned = store._conn.execute(
            "SELECT machine_id FROM assignments WHERE plug_id = ? AND assigned_until IS NULL",
            [plug_id],
        ).fetchone()
        assert still_assigned is not None, (
            "an empty FlipFix roster must never clear an existing assignment"
        )
        assert still_assigned[0] == machine_id

    def test_an_empty_roster_still_records_the_alias(self, store: Store) -> None:
        """Skipping assignment is not skipping the roster: the alias is what a
        later pass assigns *from*, and ingest writes an empty one for outlets it
        creates itself."""
        apply_devices(
            None, store, [_entry(f"{DEV}05", "Duck Locker - M0037")], {}, datetime.now(UTC)
        )

        alias = store._conn.execute(
            "SELECT alias FROM plugs WHERE device_id = ? AND child_id = ?", [DEV, f"{DEV}05"]
        ).fetchone()
        assert alias is not None and alias[0] == "Duck Locker - M0037"

    def test_a_populated_roster_does_unassign_an_untagged_outlet(self, store: Store) -> None:
        """The guard must not become a blanket refusal: an outlet whose tag really
        is gone -- relabelled to something with no `M\\d+` -- has to be released,
        or a machine can never be moved off it."""
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, machine_id, datetime(2026, 9, 1, tzinfo=UTC))

        apply_devices(
            None,
            store,
            [_entry(f"{DEV}00", "spare outlet")],
            {"M0013": {"name": "Blackout", "year": 1980}},
            datetime.now(UTC),
        )

        assert (
            store._conn.execute(
                "SELECT count(*) FROM assignments WHERE plug_id = ? AND assigned_until IS NULL",
                [plug_id],
            ).fetchone()[0]
            == 0
        )


@pytest.fixture
def state():
    from juice.server import RecorderState

    return RecorderState()


class TestTheRosterReproducesRefreshMetadata:
    """The roster projection has to land where the cloud recorder's did.

    Every difference is a machine that shows up somewhere unexpected on the floor,
    so these mirror `TestRefreshMetadata` in `tests/test_recorder.py` case for case.
    """

    def _machines(self) -> dict:
        return {"M0013": {"name": "Blackout", "year": 1980}}

    def test_it_assigns_a_tagged_outlet(self, store: Store, state) -> None:
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013")],
            self._machines(),
            datetime.now(UTC),
        )

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        assert state.assignments[plug_id] == ("Blackout", "M0013", 1980)
        assert (
            store._conn.execute(
                "SELECT count(*) FROM assignments WHERE plug_id = ? AND assigned_until IS NULL",
                [plug_id],
            ).fetchone()[0]
            == 1
        )

    def test_a_relabel_moves_the_machine(self, store: Store, state) -> None:
        """The whole reason the roster has to be re-sent: relabelling the outlet is
        how an operator moves a machine."""
        machines = {
            "M0013": {"name": "Blackout", "year": 1980},
            "M0099": {"name": "Lightning", "year": 2024},
        }
        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines, ts)
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")

        apply_devices(state, store, [_entry(f"{DEV}00", "Lightning - M0099")], machines, ts)

        assert state.assignments[plug_id][1] == "M0099"

    def test_a_reassignment_clears_the_overload_window(self, store: Store, state) -> None:
        """A plug moving to a different machine must not inherit the previous
        machine's accumulated load -- that window is what fires a shutdown."""
        from juice.overload import OverloadWindow

        machines = {
            "M0013": {"name": "Blackout", "year": 1980},
            "M0099": {"name": "Lightning", "year": 2024},
        }
        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines, ts)
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        state.overload_windows[plug_id] = OverloadWindow()
        state.overload_onsets[plug_id] = ts

        apply_devices(state, store, [_entry(f"{DEV}00", "Lightning - M0099")], machines, ts)

        assert plug_id not in state.overload_windows
        assert plug_id not in state.overload_onsets

    def test_the_same_machine_keeps_its_overload_window(self, store: Store, state) -> None:
        """Only a *change* invalidates it. tap re-sends the roster whenever
        anything in it changes, so an unchanged entry must not keep resetting the
        window -- that would disarm overload protection every time."""
        from juice.overload import OverloadWindow

        ts = datetime.now(UTC)
        entries = [_entry(f"{DEV}00", "Blackout - M0013")]
        apply_devices(state, store, entries, self._machines(), ts)
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        window = OverloadWindow()
        state.overload_windows[plug_id] = window

        apply_devices(state, store, entries, self._machines(), ts)

        assert state.overload_windows.get(plug_id) is window

    def test_it_picks_up_the_calibration(self, store: Store, state) -> None:
        from juice.state import Calibration

        machine_id = store.ensure_machine("M0013", "Blackout")
        store.set_calibration(machine_id, Calibration(idle_max_rsd=None, play_min_rsd=12.0))

        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013")],
            self._machines(),
            datetime.now(UTC),
        )

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        assert state.calibrations[plug_id].play_min_rsd == 12.0

    def test_an_untagged_outlet_is_left_unassigned(self, store: Store, state) -> None:
        apply_devices(
            state, store, [_entry(f"{DEV}01", "spare outlet")], self._machines(), datetime.now(UTC)
        )

        plug_id = store.ensure_plug(DEV, f"{DEV}01", "spare outlet")
        assert plug_id not in state.assignments

    def test_a_tag_with_no_flipfix_machine_is_left_unassigned(self, store: Store, state) -> None:
        """A tag juice does not recognise is not the same as no roster at all --
        the roster is populated here, so the outlet really is unassigned."""
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}02", "Mystery - M4242")],
            self._machines(),
            datetime.now(UTC),
        )

        plug_id = store.ensure_plug(DEV, f"{DEV}02", "Mystery - M4242")
        assert plug_id not in state.assignments


class TestTheOptionalRosterFields:
    """Both are optional on the wire, and both defaults have to be the safe error:
    an older tap omits them entirely."""

    def test_has_emeter_defaults_to_metered(self, store: Store, state) -> None:
        """`refresh_hourly_usage` filters on this, so a wrong FALSE would hide the
        outlet from every energy chart."""
        apply_devices(state, store, [_entry(f"{DEV}00", "a")], {}, datetime.now(UTC))

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "a")
        assert state.plug_has_emeter[plug_id] is True
        assert (
            store._conn.execute(
                "SELECT has_emeter FROM plugs WHERE plug_id = ?", [plug_id]
            ).fetchone()[0]
            is True
        )

    def test_has_emeter_false_is_honoured(self, store: Store, state) -> None:
        apply_devices(
            state, store, [_entry(f"{DEV}00", "a", has_emeter=False)], {}, datetime.now(UTC)
        )

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "a", has_emeter=False)
        assert state.plug_has_emeter[plug_id] is False

    def test_the_device_alias_is_recorded(self, store: Store, state) -> None:
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "a", device_alias="Front Row")],
            {},
            datetime.now(UTC),
        )

        assert state.strip_aliases[DEV] == "Front Row"

    def test_an_absent_device_alias_does_not_blank_a_known_one(self, store: Store, state) -> None:
        """An older tap omits it. Overwriting with "" would blank the dashboard's
        strip heading, which is worse than leaving the cloud recorder's value."""
        state.strip_aliases[DEV] = "Front Row"

        apply_devices(state, store, [_entry(f"{DEV}00", "a")], {}, datetime.now(UTC))

        assert state.strip_aliases[DEV] == "Front Row"

    def test_an_entry_with_no_device_id_is_skipped(self, store: Store, state) -> None:
        """Nothing can be keyed off it, and a plug created for one is unreachable
        forever."""
        apply_devices(
            state, store, [{"device_id": "", "child_id": "x", "alias": "a"}], {}, datetime.now(UTC)
        )

        assert store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0] == 0


class TestShadowMode:
    """The highest-value pre-cutover artefact, and it writes nothing.

    Shadow mode runs with the cloud recorder still authoritative: tap's roster is
    diffed against the live cloud-driven state and the discrepancies are reported.
    The gate before flipping the collector is **zero** roster diffs over 48h,
    because every difference is a machine that would land somewhere unexpected the
    moment tap becomes the source of truth.

    The diff that matters most is an outlet the cloud never saw at all: each one is
    a machine that would silently vanish from the floor.
    """

    def test_an_agreeing_roster_reports_nothing(self, store: Store, state) -> None:
        from juice.collector_tap import shadow_devices

        machines = {"M0013": {"name": "Blackout", "year": 1980}}
        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines, ts)

        report = shadow_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines)

        assert report.clean, report.describe()

    def test_it_writes_nothing(self, store: Store, state) -> None:
        """The whole point: this runs against a production floor the cloud recorder
        is still driving. A shadow pass that mutated anything would be a cutover,
        not a rehearsal."""
        from juice.collector_tap import shadow_devices

        before_plugs = store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0]
        before_assign = store._conn.execute("SELECT count(*) FROM assignments").fetchone()[0]

        shadow_devices(
            state,
            store,
            [_entry(f"{DEV}09", "Ghost - M9999")],
            {"M9999": {"name": "Ghost", "year": 2000}},
        )

        assert store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0] == before_plugs
        assert (
            store._conn.execute("SELECT count(*) FROM assignments").fetchone()[0] == before_assign
        )
        assert state.assignments == {}

    def test_an_outlet_the_cloud_never_saw_is_reported(self, store: Store, state) -> None:
        """The one that would silently lose a machine at cutover."""
        from juice.collector_tap import shadow_devices

        report = shadow_devices(state, store, [_entry(f"{DEV}03", "Lightning - M0099")], {})

        assert not report.clean
        assert (DEV, f"{DEV}03") in report.unknown_outlets
        assert "never seen" in report.describe()

    def test_a_disagreeing_assignment_is_reported(self, store: Store, state) -> None:
        """tap's alias would move the machine somewhere the cloud did not have it."""
        from juice.collector_tap import shadow_devices

        machines = {
            "M0013": {"name": "Blackout", "year": 1980},
            "M0099": {"name": "Lightning", "year": 2024},
        }
        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines, ts)

        report = shadow_devices(state, store, [_entry(f"{DEV}00", "Lightning - M0099")], machines)

        assert not report.clean
        assert report.assignment_changes == ((f"{DEV}00", "M0013", "M0099"),)

    def test_a_metering_disagreement_is_reported(self, store: Store, state) -> None:
        """`refresh_hourly_usage` filters on `has_emeter`, so a disagreement here is
        an energy chart that changes at cutover."""
        from juice.collector_tap import shadow_devices

        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "a")], {}, ts)

        report = shadow_devices(state, store, [_entry(f"{DEV}00", "a", has_emeter=False)], {})

        assert not report.clean
        assert report.metering_changes == ((f"{DEV}00", True, False),)

    def test_an_outlet_the_cloud_has_and_tap_lacks_is_reported(self, store: Store, state) -> None:
        """The mirror case: tap cannot reach a device the cloud can, so at cutover
        those machines would go dark rather than move."""
        from juice.collector_tap import shadow_devices

        ts = datetime.now(UTC)
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013"), _entry(f"{DEV}01", "Lightning - M0099")],
            {},
            ts,
        )

        report = shadow_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], {})

        assert not report.clean
        assert (DEV, f"{DEV}01") in report.missing_outlets


class TestTheShadowProjector:
    """What `serve --tap-shadow` actually wires in: a callable that takes a roster
    frame, diffs it with `shadow_devices`, logs the result, and tracks how long
    the roster has agreed -- which is the 48h gate made observable."""

    def test_it_logs_a_clean_roster_at_info(self, store: Store, state, caplog) -> None:
        import logging

        from juice.collector_tap import ShadowProjector

        state.flipfix_machines = {"M0013": {"name": "Blackout", "year": 1980}}
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013")],
            state.flipfix_machines,
            datetime.now(UTC),
        )
        projector = ShadowProjector(state, store)

        with caplog.at_level(logging.INFO, logger="juice.collector_tap"):
            projector([_entry(f"{DEV}00", "Blackout - M0013")])

        assert projector.clean_since is not None
        assert any("agrees" in r.getMessage() for r in caplog.records), caplog.text

    def test_it_logs_a_disagreement_at_warning_and_resets_the_clock(
        self, store: Store, state, caplog
    ) -> None:
        import logging

        from juice.collector_tap import ShadowProjector

        state.flipfix_machines = {"M0013": {"name": "Blackout", "year": 1980}}
        projector = ShadowProjector(state, store)
        projector([_entry(f"{DEV}00", "Blackout - M0013")])  # agrees? no plug -> unknown
        assert projector.clean_since is None, "an unknown outlet is not clean"

        with caplog.at_level(logging.WARNING, logger="juice.collector_tap"):
            projector([_entry(f"{DEV}00", "Blackout - M0013")])

        assert any("never seen" in r.getMessage() for r in caplog.records), caplog.text
        assert projector.last_diff is not None and not projector.last_diff.clean

    def test_it_reads_the_flipfix_roster_from_state(self, store: Store, state) -> None:
        """The projector must see the roster `record()` keeps current, not a
        snapshot taken when the projector was built -- FlipFix is refetched every
        60s and a machine added there mid-rehearsal must count."""
        from juice.collector_tap import ShadowProjector

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Lightning - M0099")
        projector = ShadowProjector(state, store)
        state.flipfix_machines = {}
        projector([_entry(f"{DEV}00", "Lightning - M0099")])
        assert projector.last_diff.assignment_changes == (), "no roster, nothing to disagree"

        state.flipfix_machines = {"M0099": {"name": "Lightning", "year": 2024}}
        projector([_entry(f"{DEV}00", "Lightning - M0099")])
        assert projector.last_diff.assignment_changes == ((f"{DEV}00", None, "M0099"),)
        assert plug_id  # the plug existed throughout; only the roster changed

    def test_it_never_writes(self, store: Store, state) -> None:
        from juice.collector_tap import ShadowProjector

        state.flipfix_machines = {"M9999": {"name": "Ghost", "year": 2000}}
        projector = ShadowProjector(state, store)
        projector([_entry(f"{DEV}09", "Ghost - M9999")])

        assert store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0] == 0
        assert state.assignments == {}
