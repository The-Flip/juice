"""The roster projection: tap's `devices` frame becoming plugs and assignments.

This is the half of the cutover that decides which machine is which. The cloud
recorder does it in `refresh_metadata` from a device it just polled; here the same
work is driven by a frame, which changes one thing that matters a great deal: the
frame can arrive before juice has ever spoken to FlipFix.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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

        report = shadow_devices(store, [_entry(f"{DEV}00", "Blackout - M0013")], machines)

        assert report.clean, report.describe()

    def test_it_writes_nothing(self, store: Store, state) -> None:
        """The whole point: this runs against a production floor the cloud recorder
        is still driving. A shadow pass that mutated anything would be a cutover,
        not a rehearsal."""
        from juice.collector_tap import shadow_devices

        before_plugs = store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0]
        before_assign = store._conn.execute("SELECT count(*) FROM assignments").fetchone()[0]

        shadow_devices(
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

        report = shadow_devices(store, [_entry(f"{DEV}03", "Lightning - M0099")], {})

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

        report = shadow_devices(store, [_entry(f"{DEV}00", "Lightning - M0099")], machines)

        assert not report.clean
        assert report.assignment_changes == ((f"{DEV}00", "M0013", "M0099"),)

    def test_a_metering_disagreement_is_reported(self, store: Store, state) -> None:
        """`refresh_hourly_usage` filters on `has_emeter`, so a disagreement here is
        an energy chart that changes at cutover."""
        from juice.collector_tap import shadow_devices

        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "a")], {}, ts)

        report = shadow_devices(store, [_entry(f"{DEV}00", "a", has_emeter=False)], {})

        assert not report.clean
        assert report.metering_changes == ((f"{DEV}00", True, False),)

    def test_an_outlet_the_cloud_has_and_tap_lacks_is_reported(self, store: Store, state) -> None:
        """The mirror case: tap cannot reach a device the cloud is *currently*
        reading, so at cutover those machines would go dark rather than move."""
        from juice.collector_tap import shadow_devices

        ts = datetime.now(UTC)
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013"), _entry(f"{DEV}01", "Lightning - M0099")],
            {},
            ts,
        )
        lost = store.ensure_plug(DEV, f"{DEV}01", "Lightning - M0099")
        # The cloud recorder heard from it a minute ago.
        store.insert_readings([(ts - timedelta(minutes=1), lost, 120.0, 120.0, 1.0, 0.0)])

        report = shadow_devices(store, [_entry(f"{DEV}00", "Blackout - M0013")], {})

        assert not report.clean
        assert (DEV, f"{DEV}01") in report.missing_outlets
        assert report.stale_outlets == ()

    def test_a_plug_dead_for_months_does_not_block_the_gate(self, store: Store, state) -> None:
        """Production has two plugs that last reported in May. Counting them as
        "missing from tap" would make a perfect roster read as disagreeing for as
        long as those rows exist -- the 48h gate could never pass. They are dead
        as far as either collector is concerned, so they are named and not
        counted."""
        from juice.collector_tap import STALE_AFTER, shadow_devices

        ts = datetime.now(UTC)
        apply_devices(
            state,
            store,
            [_entry(f"{DEV}00", "Blackout - M0013"), _entry(f"{DEV}01", "Star Trip - M0009")],
            {},
            ts,
        )
        dead = store.ensure_plug(DEV, f"{DEV}01", "Star Trip - M0009")
        store.insert_readings(
            [(ts - STALE_AFTER - timedelta(days=90), dead, 5.0, 120.0, 0.04, 0.0)]
        )

        report = shadow_devices(store, [_entry(f"{DEV}00", "Blackout - M0013")], {})

        assert report.clean, report.describe()
        assert (DEV, f"{DEV}01") in report.stale_outlets
        assert "not counted" in report.describe()


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
        """The gate is about the roster being right *continuously*: one
        disagreement after two clean days is a fresh clock, not a blip."""
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
        projector([_entry(f"{DEV}00", "Blackout - M0013")])
        assert projector.clean_since is not None, "precondition: the clock is running"

        # Then tap reports an outlet the cloud has never seen.
        with caplog.at_level(logging.WARNING, logger="juice.collector_tap"):
            projector([_entry(f"{DEV}00", "Blackout - M0013"), _entry(f"{DEV}05", "New - M0555")])

        assert any("never seen" in r.getMessage() for r in caplog.records), caplog.text
        assert projector.last_diff is not None and not projector.last_diff.clean
        assert projector.clean_since is None, "a disagreement must reset the clock"

    def test_it_reads_the_flipfix_roster_from_state(self, store: Store, state) -> None:
        """The projector must see the roster `record()` keeps current, not a
        snapshot taken when the projector was built -- FlipFix is refetched every
        60s and a machine added there mid-rehearsal must count."""
        from juice.collector_tap import ShadowProjector

        store.ensure_plug(DEV, f"{DEV}00", "Lightning - M0099")
        projector = ShadowProjector(state, store)
        state.flipfix_machines = {}
        projector([_entry(f"{DEV}00", "Lightning - M0099")])
        assert projector.last_diff.assignment_changes == (), "no roster, nothing to disagree"

        state.flipfix_machines = {"M0099": {"name": "Lightning", "year": 2024}}
        projector([_entry(f"{DEV}00", "Lightning - M0099")])
        assert projector.last_diff.assignment_changes == ((f"{DEV}00", None, "M0099"),)

    def test_it_never_writes(self, store: Store, state) -> None:
        from juice.collector_tap import ShadowProjector

        state.flipfix_machines = {"M9999": {"name": "Ghost", "year": 2000}}
        projector = ShadowProjector(state, store)
        projector([_entry(f"{DEV}09", "Ghost - M9999")])

        assert store._conn.execute("SELECT count(*) FROM plugs").fetchone()[0] == 0
        assert state.assignments == {}


class TestShadowDiffsAgainstTheStore:
    """`shadow_devices` deliberately takes no `RecorderState`. In-memory state is
    whatever the cloud recorder happens to hold right now; the store is what
    survives a restart and what `hydrate_assignments` reads back. A diff against
    memory would report clean for a roster that diverges the moment juice
    restarts -- so this makes the two disagree and checks which one wins."""

    def test_the_store_wins_when_memory_disagrees(self, store: Store, state) -> None:
        from juice.collector_tap import shadow_devices

        machines = {"M0013": {"name": "Blackout", "year": 1980}}
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, machine_id, datetime.now(UTC))
        # Memory says something else entirely -- and must be ignored.
        state.assignments[plug_id] = ("Wrong", "M9999", None)

        report = shadow_devices(store, [_entry(f"{DEV}00", "Blackout - M0013")], machines)

        assert report.clean, report.describe()


class TestOneBadEntryDoesNotCostTheRest:
    """`refresh_metadata` isolates one device's failure from the others and has a
    test for it; the projection needs the same. tap re-sends the roster only when
    it changes, so entries lost to a neighbour's malformed field would not come
    back on their own."""

    def test_an_entry_that_raises_is_skipped_and_the_rest_applied(
        self, store: Store, state, caplog
    ) -> None:
        """Field coercion handles the merely odd (a numeric alias becomes its
        string); this is the case that genuinely raises -- a FlipFix machine
        record with no `name`, which `_assign` indexes unconditionally."""
        import logging

        machines = {
            "M0001": {},  # FlipFix returned a record with no name: KeyError
            "M0013": {"name": "Blackout", "year": 1980},
        }
        raising = _entry(f"{DEV}00", "Broken - M0001")
        good = _entry(f"{DEV}01", "Blackout - M0013")

        with caplog.at_level(logging.WARNING, logger="juice.collector_tap"):
            apply_devices(state, store, [raising, good], machines, datetime.now(UTC))

        assert any("skipping an unusable entry" in r.getMessage() for r in caplog.records)
        plug_id = store.ensure_plug(DEV, f"{DEV}01", "Blackout - M0013")
        assert state.assignments.get(plug_id) == ("Blackout", "M0013", 1980), (
            "the entry after the one that raised must still be applied"
        )

    def test_odd_but_coercible_fields_do_not_raise(self, store: Store, state) -> None:
        """The reviewer's original example: a numeric alias. Coerced, not fatal."""
        apply_devices(
            state,
            store,
            [{"device_id": DEV, "child_id": f"{DEV}00", "alias": 12345}],
            {},
            datetime.now(UTC),
        )
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "12345")
        assert state.plugs[plug_id] == (DEV, f"{DEV}00", "12345")


class TestNoFlipFixIsNotAVerdict:
    """A frame that arrives before juice has a FlipFix roster is the *normal*
    case at startup -- the server is up before `record()` has fetched it -- and
    tap reconnects within seconds. Treating that frame as "clean" would set the
    clock on a comparison that skipped assignments entirely, and with tap only
    re-sending on change, nothing would ever revisit it."""

    def test_the_clock_does_not_start(self, store: Store, state, caplog) -> None:
        import logging

        from juice.collector_tap import ShadowProjector

        store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        state.flipfix_machines = {}
        projector = ShadowProjector(state, store)

        with caplog.at_level(logging.INFO, logger="juice.collector_tap"):
            projector([_entry(f"{DEV}00", "Blackout - M0013")])

        assert projector.clean_since is None, "no comparison happened, so no verdict"
        assert any("NO FlipFix roster" in r.getMessage() for r in caplog.records), caplog.text

    def test_a_rediff_after_flipfix_answers_gives_the_real_verdict(
        self, store: Store, state
    ) -> None:
        """The frame judged before FlipFix answered would otherwise be the verdict
        for the whole rehearsal: tap will not send another until a relabel."""
        from juice.collector_tap import ShadowProjector

        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, machine_id, datetime.now(UTC))
        state.flipfix_machines = {}
        projector = ShadowProjector(state, store)
        projector([_entry(f"{DEV}00", "Lightning - M0099")])  # tap disagrees, unseen
        assert projector.clean_since is None

        state.flipfix_machines = {
            "M0013": {"name": "Blackout", "year": 1980},
            "M0099": {"name": "Lightning", "year": 2024},
        }
        projector.rediff()

        assert projector.last_diff is not None and not projector.last_diff.clean, (
            "the re-diff must see the disagreement the first pass could not"
        )
        assert projector.clean_since is None


class TestTheVerdictIsRevisited:
    """tap re-sends its roster only when it changes, so for a healthy fleet the
    verdict on one frame would otherwise stand forever. A momentary disagreement
    -- tap's 60s heartbeat seeing a relabel before the cloud's 60s refresh does --
    must clear once the store catches up, without waiting for another relabel."""

    def test_a_sticky_disagreement_clears_on_rediff(self, store: Store, state) -> None:
        from juice.collector_tap import ShadowProjector

        machines = {
            "M0013": {"name": "Blackout", "year": 1980},
            "M0099": {"name": "Lightning", "year": 2024},
        }
        state.flipfix_machines = machines
        ts = datetime.now(UTC)
        apply_devices(state, store, [_entry(f"{DEV}00", "Blackout - M0013")], machines, ts)
        projector = ShadowProjector(state, store)

        # tap saw the relabel first.
        projector([_entry(f"{DEV}00", "Lightning - M0099")])
        assert projector.clean_since is None

        # A minute later the cloud recorder catches up; tap sends nothing new.
        apply_devices(state, store, [_entry(f"{DEV}00", "Lightning - M0099")], machines, ts)
        projector.rediff()

        assert projector.last_diff is not None and projector.last_diff.clean
        assert projector.clean_since is not None, "agreement must be observable without a new frame"

    async def test_the_loop_rediffs_on_its_interval(self, store: Store, state) -> None:
        import asyncio
        import contextlib

        from juice.collector_tap import ShadowProjector, shadow_loop

        state.flipfix_machines = {"M0013": {"name": "Blackout", "year": 1980}}
        projector = ShadowProjector(state, store)
        projector([_entry(f"{DEV}00", "Blackout - M0013")])
        before = projector.evaluations

        task = asyncio.create_task(shadow_loop(projector, interval=0.02))
        await asyncio.sleep(0.15)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert projector.evaluations >= before + 3, "the loop must re-evaluate on its own clock"

    def test_rediff_before_any_frame_is_a_no_op(self, store: Store, state) -> None:
        from juice.collector_tap import ShadowProjector

        projector = ShadowProjector(state, store)
        projector.rediff()
        assert projector.evaluations == 0 and projector.last_diff is None
