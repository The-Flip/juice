"""Tests for juice.store — DuckDB storage layer."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

import pytest

from juice.collector import PlugReading, StripReading
from juice.state import Calibration
from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


class TestOpen:
    def test_creates_tables(self) -> None:
        with Store(":memory:") as store:
            tables = store._conn.sql("SHOW TABLES").fetchall()
            table_names = {row[0] for row in tables}
            assert table_names == {
                "plugs",
                "readings",
                "machines",
                "assignments",
                "calibrations",
                "power_events",
                "hourly_usage",
                "daily_play_seconds",
            }


class TestPowerEvents:
    def test_record_and_recent_roundtrip(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")
        mid = store.ensure_machine("M0013", "Blackout")
        ts0 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(plug_id, mid, ts0)

        ts = datetime(2026, 5, 25, 12, 5, 0, tzinfo=UTC)
        event_id = store.record_power_event(
            ts=ts,
            plug_id=plug_id,
            action="turn_on",
            source="individual",
            actor="william@theflip.museum",
            result="ok",
        )
        assert isinstance(event_id, int) and event_id >= 1

        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        r = rows[0]
        assert r["event_id"] == event_id
        assert r["plug_id"] == plug_id
        assert r["action"] == "turn_on"
        assert r["source"] == "individual"
        assert r["actor"] == "william@theflip.museum"
        assert r["result"] == "ok"
        assert r["operation_id"] is None
        assert r["error"] is None
        assert r["machine_name"] == "Blackout"  # joined from assignments + machines
        assert r["plug_alias"] == "Blackout - M0013"

    def test_recent_returns_newest_first(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "P1")
        t0 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 5, 25, 12, 1, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 25, 12, 2, 0, tzinfo=UTC)
        store.record_power_event(t0, pid, "turn_on", "individual", "a", "ok")
        store.record_power_event(t1, pid, "turn_off", "all_off", "b", "ok", operation_id="op1")
        store.record_power_event(t2, pid, "turn_on", "all_on", "c", "ok", operation_id="op2")

        rows = store.recent_power_events(limit=10)
        assert [r["actor"] for r in rows] == ["c", "b", "a"]
        assert rows[0]["operation_id"] == "op2"
        assert rows[1]["source"] == "all_off"

    def test_recent_respects_limit(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "P1")
        for i in range(5):
            ts = datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC)
            store.record_power_event(ts, pid, "turn_on", "individual", "a", "ok")
        rows = store.recent_power_events(limit=3)
        assert len(rows) == 3

    def test_recent_pagination_with_before(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "P1")
        ids = []
        for i in range(5):
            ts = datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC)
            ids.append(store.record_power_event(ts, pid, "turn_on", "individual", "a", "ok"))
        # Pass the *oldest* of the first page as `before`; expect strictly older ids.
        rows = store.recent_power_events(limit=10, before=ids[2])
        assert [r["event_id"] for r in rows] == [ids[1], ids[0]]

    def test_records_error_with_message(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "P1")
        ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.record_power_event(
            ts,
            pid,
            "turn_off",
            "all_off",
            "a",
            "error",
            operation_id="op9",
            error="device offline",
        )
        rows = store.recent_power_events(limit=10)
        assert rows[0]["result"] == "error"
        assert rows[0]["error"] == "device offline"
        assert rows[0]["operation_id"] == "op9"

    def test_machine_name_null_for_unassigned_plug(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "Unassigned")
        ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.record_power_event(ts, pid, "turn_on", "individual", "a", "ok")
        rows = store.recent_power_events(limit=10)
        assert rows[0]["machine_name"] is None
        assert rows[0]["plug_alias"] == "Unassigned"


class TestEnsurePlug:
    def test_inserts_and_returns_id(self, store: Store) -> None:
        plug_id = store.ensure_plug("device1", "child01", "Plug 1")
        assert isinstance(plug_id, int)
        assert plug_id >= 1

    def test_idempotent(self, store: Store) -> None:
        id1 = store.ensure_plug("device1", "child01", "Plug 1")
        id2 = store.ensure_plug("device1", "child01", "Plug 1")
        assert id1 == id2

    def test_updates_alias(self, store: Store) -> None:
        store.ensure_plug("device1", "child01", "Old Name")
        store.ensure_plug("device1", "child01", "New Name")
        row = store._conn.execute(
            "SELECT alias FROM plugs WHERE device_id = ? AND child_id = ?",
            ["device1", "child01"],
        ).fetchone()
        assert row[0] == "New Name"

    def test_different_plugs_get_different_ids(self, store: Store) -> None:
        id1 = store.ensure_plug("device1", "child01", "Plug 1")
        id2 = store.ensure_plug("device1", "child02", "Plug 2")
        assert id1 != id2


class TestInsertReadings:
    def test_roundtrip(self, store: Store) -> None:
        plug_id = store.ensure_plug("device1", "child01", "Plug 1")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(ts, plug_id, 100.0, 120.0, 0.833, 5.0)])

        rows = store._conn.execute("SELECT * FROM readings").fetchall()
        assert len(rows) == 1
        assert rows[0][1] == plug_id
        assert rows[0][2] == pytest.approx(100.0)  # watts

    def test_batch_insert(self, store: Store) -> None:
        pid1 = store.ensure_plug("d1", "c1", "P1")
        pid2 = store.ensure_plug("d1", "c2", "P2")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.insert_readings(
            [
                (ts, pid1, 100.0, 120.0, 0.833, 5.0),
                (ts, pid2, 200.0, 121.0, 1.653, 10.0),
            ]
        )

        count = store._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
        assert count == 2


class TestRecordStrip:
    def test_stores_plugs_and_readings(self, store: Store) -> None:
        reading = StripReading(
            alias="Strip 1",
            device_id="device1",
            plugs=[
                PlugReading(
                    child_id="c01",
                    alias="Blackout - M0013",
                    is_on=True,
                    watts=100.0,
                    voltage=120.0,
                    amps=0.833,
                    total_kwh=5.0,
                ),
                PlugReading(
                    child_id="c02",
                    alias="Hyperball - M0014",
                    is_on=False,
                    watts=0.0,
                    voltage=120.0,
                    amps=0.0,
                    total_kwh=2.0,
                ),
            ],
        )
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.record_strip(reading, ts)

        plugs = store._conn.execute("SELECT * FROM plugs ORDER BY plug_id").fetchall()
        assert len(plugs) == 2
        assert plugs[0][1] == "device1"  # device_id
        assert plugs[0][2] == "c01"  # child_id

        readings = store._conn.execute("SELECT * FROM readings ORDER BY plug_id").fetchall()
        assert len(readings) == 2
        assert readings[0][2] == pytest.approx(100.0)  # watts
        assert readings[1][2] == pytest.approx(0.0)


class TestEnsureMachine:
    def test_inserts_and_returns_id(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Medieval Madness")
        assert isinstance(mid, int)
        assert mid >= 1

    def test_idempotent(self, store: Store) -> None:
        id1 = store.ensure_machine("M0001", "Medieval Madness")
        id2 = store.ensure_machine("M0001", "Medieval Madness")
        assert id1 == id2

    def test_updates_name(self, store: Store) -> None:
        store.ensure_machine("M0001", "Old Name")
        store.ensure_machine("M0001", "New Name")
        row = store._conn.execute(
            "SELECT name FROM machines WHERE asset_id = ?", ["M0001"]
        ).fetchone()
        assert row[0] == "New Name"


class TestUpdateAssignment:
    def test_creates_assignment(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        mid = store.ensure_machine("M0001", "Medieval Madness")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(plug_id, mid, ts)

        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == plug_id
        assert rows[0][1] == mid
        assert rows[0][3] is None  # assigned_until

    def test_changes_machine(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        mid1 = store.ensure_machine("M0001", "Medieval Madness")
        mid2 = store.ensure_machine("M0002", "Addams Family")
        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 3, 15, 13, 0, 0, tzinfo=UTC)

        store.update_assignment(plug_id, mid1, ts1)
        store.update_assignment(plug_id, mid2, ts2)

        rows = store._conn.execute("SELECT * FROM assignments ORDER BY assigned_from").fetchall()
        assert len(rows) == 2
        # Old assignment is closed
        assert rows[0][1] == mid1
        assert rows[0][3] is not None  # assigned_until set
        # New assignment is open
        assert rows[1][1] == mid2
        assert rows[1][3] is None

    def test_idempotent(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        mid = store.ensure_machine("M0001", "Medieval Madness")
        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 3, 15, 12, 0, 1, tzinfo=UTC)

        store.update_assignment(plug_id, mid, ts1)
        store.update_assignment(plug_id, mid, ts2)

        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 1  # no duplicate

    def test_idempotent_across_restart(self, tmp_path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        # First process creates an assignment
        with Store(db_path) as s1:
            plug_id = s1.ensure_plug("d1", "c1", "Plug 1")
            mid = s1.ensure_machine("M0001", "Medieval Madness")
            ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
            s1.update_assignment(plug_id, mid, ts1)

        # Second process (fresh cache) assigns the same machine
        with Store(db_path) as s2:
            plug_id = s2.ensure_plug("d1", "c1", "Plug 1")
            mid = s2.ensure_machine("M0001", "Medieval Madness")
            ts2 = datetime(2026, 3, 15, 13, 0, 0, tzinfo=UTC)
            s2.update_assignment(plug_id, mid, ts2)

            rows = s2._conn.execute("SELECT * FROM assignments").fetchall()
            assert len(rows) == 1  # no duplicate

    def test_none_closes_without_opening(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        mid = store.ensure_machine("M0001", "Medieval Madness")
        ts1 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        ts2 = datetime(2026, 3, 15, 13, 0, 0, tzinfo=UTC)

        store.update_assignment(plug_id, mid, ts1)
        store.update_assignment(plug_id, None, ts2)

        rows = store._conn.execute("SELECT * FROM assignments").fetchall()
        assert len(rows) == 1
        assert rows[0][3] is not None  # closed


class TestListOpenAssignments:
    def test_returns_open_with_machine(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(plug_id, mid, ts)

        rows = store.list_open_assignments()
        assert rows == [(plug_id, "d1", "", "Blackout - M0013", False, "M0013", "Blackout")]

    def test_excludes_closed_assignments(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        mid = store.ensure_machine("M0001", "Medieval Madness")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))
        store.update_assignment(plug_id, None, datetime(2026, 3, 15, 13, 0, 0, tzinfo=UTC))

        assert store.list_open_assignments() == []


class TestCalibration:
    def test_set_and_get(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Test Machine")
        cal = Calibration(idle_max_rsd=2.0, play_min_rsd=10.0)
        store.set_calibration(mid, cal)
        assert store.get_calibration(mid) == cal

    def test_get_nonexistent(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Test Machine")
        assert store.get_calibration(mid) is None

    def test_upsert(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Test Machine")
        store.set_calibration(mid, Calibration(idle_max_rsd=1.0, play_min_rsd=5.0))
        store.set_calibration(mid, Calibration(idle_max_rsd=2.0, play_min_rsd=10.0))
        assert store.get_calibration(mid) == Calibration(idle_max_rsd=2.0, play_min_rsd=10.0)

    def test_null_idle_max_rsd(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Test Machine")
        cal = Calibration(idle_max_rsd=None, play_min_rsd=13.0)
        store.set_calibration(mid, cal)
        result = store.get_calibration(mid)
        assert result == cal
        assert result.idle_max_rsd is None

    def test_seed_calibrations(self, store: Store) -> None:
        store.ensure_machine("M0001", "Godzilla (Premium)")
        store.ensure_machine("M0002", "Hyperball")
        store.seed_calibrations(
            {
                "Godzilla (Premium)": Calibration(idle_max_rsd=2.0, play_min_rsd=12.0),
                "Hyperball": Calibration(idle_max_rsd=None, play_min_rsd=13.0),
                "Nonexistent Machine": Calibration(idle_max_rsd=1.0, play_min_rsd=5.0),
            }
        )
        # Seeded machines get calibrations
        godzilla_mid = store._machine_cache["M0001"][0]
        hyperball_mid = store._machine_cache["M0002"][0]
        assert store.get_calibration(godzilla_mid) == Calibration(
            idle_max_rsd=2.0, play_min_rsd=12.0
        )
        assert store.get_calibration(hyperball_mid) == Calibration(
            idle_max_rsd=None, play_min_rsd=13.0
        )


class TestHasEmeterColumn:
    def test_default_true_when_omitted(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        row = store._conn.execute(
            "SELECT has_emeter FROM plugs WHERE plug_id = ?", [plug_id]
        ).fetchone()
        assert row[0] is True

    def test_explicit_false_for_outlet(self, store: Store) -> None:
        plug_id = store.ensure_plug("ep10-id", "", "Snack Machine", has_emeter=False)
        row = store._conn.execute(
            "SELECT has_emeter FROM plugs WHERE plug_id = ?", [plug_id]
        ).fetchone()
        assert row[0] is False

    def test_explicit_true(self, store: Store) -> None:
        plug_id = store.ensure_plug("hs300", "child01", "P1", has_emeter=True)
        row = store._conn.execute(
            "SELECT has_emeter FROM plugs WHERE plug_id = ?", [plug_id]
        ).fetchone()
        assert row[0] is True


class TestMachineLock:
    def test_defaults_unlocked(self, store: Store) -> None:
        store.ensure_machine("M0013", "Blackout")
        assert store.get_locked_asset_ids() == set()

    def test_set_locked_roundtrip(self, store: Store) -> None:
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.set_machine_locked(machine_id, True)
        assert store.get_locked_asset_ids() == {"M0013"}
        store.set_machine_locked(machine_id, False)
        assert store.get_locked_asset_ids() == set()

    def test_only_locked_machines_returned(self, store: Store) -> None:
        locked_id = store.ensure_machine("M0013", "Blackout")
        store.ensure_machine("M0009", "Star Trip")
        store.set_machine_locked(locked_id, True)
        assert store.get_locked_asset_ids() == {"M0013"}

    def test_lock_survives_reopen_and_ensure_machine(self, tmp_path) -> None:
        # The recorder upserts machines every refresh; the upsert must not
        # clobber the lock, and the lock must persist across restarts.
        db_path = str(tmp_path / "test.duckdb")
        with Store(db_path) as s:
            machine_id = s.ensure_machine("M0013", "Blackout")
            s.set_machine_locked(machine_id, True)
        with Store(db_path) as s:
            s.ensure_machine("M0013", "Blackout")
            assert s.get_locked_asset_ids() == {"M0013"}


class TestSchemaMigration:
    def test_adds_locked_column_to_existing_machines_table(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a pre-migration DB: machines table without locked.
        conn = duckdb.connect(db_path)
        conn.execute(
            """
            CREATE SEQUENCE machine_id_seq START 1;
            CREATE TABLE machines (
                machine_id SMALLINT PRIMARY KEY,
                asset_id   VARCHAR NOT NULL UNIQUE,
                name       VARCHAR NOT NULL
            );
            """
        )
        conn.execute("INSERT INTO machines VALUES (1, 'M0013', 'Blackout')")
        conn.close()

        with Store(db_path) as s:
            # locked column now exists, defaults FALSE for the legacy row
            row = s._conn.execute("SELECT locked FROM machines WHERE machine_id = 1").fetchone()
            assert row[0] is False
            assert s.get_locked_asset_ids() == set()

    def test_adds_has_emeter_column_to_existing_db(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a pre-migration DB: plugs table without has_emeter, readings
        # NOT NULL on power columns.
        legacy_schema = """
        CREATE SEQUENCE plug_id_seq START 1;
        CREATE TABLE plugs (
            plug_id   SMALLINT PRIMARY KEY,
            device_id VARCHAR NOT NULL,
            child_id  VARCHAR NOT NULL,
            alias     VARCHAR NOT NULL,
            UNIQUE (device_id, child_id)
        );
        CREATE TABLE readings (
            ts        TIMESTAMP NOT NULL,
            plug_id   SMALLINT  NOT NULL,
            watts     FLOAT     NOT NULL,
            voltage   FLOAT     NOT NULL,
            amps      FLOAT     NOT NULL,
            total_kwh FLOAT     NOT NULL
        );
        """
        conn = duckdb.connect(db_path)
        conn.execute(legacy_schema)
        conn.execute(
            "INSERT INTO plugs (plug_id, device_id, child_id, alias) VALUES (1, 'd1', 'c1', 'Old')"
        )
        conn.close()

        with Store(db_path) as s:
            # has_emeter column now exists, defaults TRUE for the legacy row
            row = s._conn.execute("SELECT has_emeter FROM plugs WHERE plug_id = 1").fetchone()
            assert row[0] is True
            # readings power columns are nullable now
            ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
            s.insert_readings([(ts, 1, None, None, None, None)])
            r = s._conn.execute("SELECT watts, voltage, amps, total_kwh FROM readings").fetchone()
            assert r == (None, None, None, None)


class TestNullableReadings:
    def test_can_insert_null_power_fields(self, store: Store) -> None:
        plug_id = store.ensure_plug("ep10", "", "Snack", has_emeter=False)
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(ts, plug_id, None, None, None, None)])
        row = store._conn.execute(
            "SELECT watts, voltage, amps, total_kwh FROM readings WHERE plug_id = ?",
            [plug_id],
        ).fetchone()
        assert row == (None, None, None, None)


class TestListUnassignedOutlets:
    def test_returns_only_unassigned_outlets(self, store: Store) -> None:
        now = datetime.now(UTC)
        # An HS300 child plug that drew power — should not appear (it's assigned)
        hs300_id = store.ensure_plug("hs300", "child01", "Pinball M0001", has_emeter=True)
        hs_mid = store.ensure_machine("M0001", "Pinball")
        store.update_assignment(hs300_id, hs_mid, now)
        store.insert_readings([(now, hs300_id, 80.0, 120.0, 0.7, 1.0)])
        # An unassigned EP10 that recently drew power — should appear
        ep10_id = store.ensure_plug("ep10-a", "", "Snack Machine", has_emeter=False)
        store.insert_readings([(now, ep10_id, None, None, None, None)])
        # An assigned EP10 — should NOT appear
        ep10_assigned = store.ensure_plug("ep10-b", "", "Tagged Machine M9999", has_emeter=False)
        mid = store.ensure_machine("M9999", "Tagged Machine")
        store.update_assignment(ep10_assigned, mid, now)
        store.insert_readings([(now, ep10_assigned, None, None, None, None)])

        rows = store.list_unassigned_outlets()
        ids = [r[0] for r in rows]
        assert ids == [ep10_id]
        # Tuple shape: (plug_id, device_id, alias, is_on_latest)
        plug_id, device_id, alias, is_on_latest = rows[0]
        assert device_id == "ep10-a"
        assert alias == "Snack Machine"
        assert is_on_latest is True  # watts IS NULL → on for a no-emeter plug

    def test_is_on_latest_reflects_most_recent_reading(self, store: Store) -> None:
        pid = store.ensure_plug("ep10-x", "", "X", has_emeter=False)
        now = datetime.now(UTC)
        t0 = now - timedelta(minutes=2)
        t1 = now - timedelta(minutes=1)
        # First reading: ON (watts=NULL is the on-without-emeter signal)
        store.insert_readings([(t0, pid, None, None, None, None)])
        # Latest reading: OFF (watts=0)
        store.insert_readings([(t1, pid, 0.0, 0.0, 0.0, 0.0)])

        rows = store.list_unassigned_outlets()
        assert len(rows) == 1
        assert rows[0][3] is False

        # Now insert a more recent ON reading
        store.insert_readings([(now, pid, None, None, None, None)])
        rows = store.list_unassigned_outlets()
        assert rows[0][3] is True

    def test_excludes_plugs_with_no_recent_power(self, store: Store) -> None:
        now = datetime.now(UTC)
        # Plugged in but never drew power (only watts=0 readings) — excluded.
        idle = store.ensure_plug("ep10-idle", "", "Idle", has_emeter=False)
        store.insert_readings([(now, idle, 0.0, 0.0, 0.0, 0.0)])
        # Drew power, but two days ago — outside the 24h window, excluded.
        stale = store.ensure_plug("ep10-stale", "", "Stale", has_emeter=False)
        store.insert_readings([(now - timedelta(days=2), stale, None, None, None, None)])
        # Plug with no readings at all — excluded.
        store.ensure_plug("ep10-empty", "", "Empty", has_emeter=False)

        assert store.list_unassigned_outlets() == []

    def test_includes_emeter_plug_that_drew_power(self, store: Store) -> None:
        now = datetime.now(UTC)
        # Unassigned emeter outlet (e.g. a sign on a spare HS300 outlet) drawing power.
        pid = store.ensure_plug("hs300", "c06", "Marquee Sign", has_emeter=True)
        store.insert_readings([(now, pid, 42.0, 120.0, 0.35, 1.0)])

        rows = store.list_unassigned_outlets()
        assert [r[0] for r in rows] == [pid]
        assert rows[0][3] is True  # watts > 0 → on for an emeter plug

    def test_respects_recent_seconds_param(self, store: Store) -> None:
        pid = store.ensure_plug("ep10-y", "", "Y", has_emeter=False)
        store.insert_readings(
            [(datetime.now(UTC) - timedelta(hours=12), pid, None, None, None, None)]
        )
        # 12h-old reading is inside the default 24h window but outside a 1h window.
        assert [r[0] for r in store.list_unassigned_outlets()] == [pid]
        assert store.list_unassigned_outlets(recent_seconds=3600) == []


class TestGetRecentWatts:
    def test_returns_recent_readings(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        now = datetime.now(UTC)
        # Insert readings spanning the last 30 minutes
        rows = [(now, plug_id, float(i), 120.0, 0.5, 1.0) for i in range(10)]
        store.insert_readings(rows)
        result = store.get_recent_watts(plug_id, seconds=3600)
        assert len(result) == 10
        assert result == [float(i) for i in range(10)]

    def test_empty_for_no_readings(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        assert store.get_recent_watts(plug_id) == []


# ---------------------------------------------------------------------------
# Hourly usage rollup
# ---------------------------------------------------------------------------


def _insert_reading(store: Store, plug_id: int, ts: datetime, watts: float) -> None:
    store.insert_readings([(ts, plug_id, watts, 120.0, watts / 120.0, 0.0)])


class TestRefreshHourlyUsage:
    def test_trapezoidal_math(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # 12:00:00 → 100W (first row, dt=NULL, excluded from sum)
        # 12:00:30 → 200W, dt=30s → 200 × 30 = 6000 Ws
        # 12:01:00 → 200W, dt=30s → 200 × 30 = 6000 Ws
        # Hour 12:00 total = 12000 Ws / 3600 / 1000 = 0.003333… kWh
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 200.0)
        _insert_reading(store, pid, h.replace(minute=1, second=0), 200.0)

        store.refresh_hourly_usage()

        rows = store._conn.execute(
            "SELECT plug_id, hour_ts, kwh, samples FROM hourly_usage ORDER BY hour_ts"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pid
        # samples counts rows with prev_dt not null
        assert rows[0][3] == 2
        assert rows[0][2] == pytest.approx(12000 / 3600 / 1000, rel=1e-6)

    def test_dt_cap_at_60s(self, store: Store) -> None:
        """A gap longer than 60s is treated as 60s of the current row's watts."""
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h12, 100.0)
        # Huge gap, 50W at 13:00:00; capped to 60s → 50 × 60 = 3000 Ws
        _insert_reading(store, pid, h13, 50.0)

        store.refresh_hourly_usage()

        rows = store._conn.execute(
            "SELECT hour_ts, kwh FROM hourly_usage ORDER BY hour_ts"
        ).fetchall()
        assert len(rows) == 1
        # Only the 13:00 bucket exists (the 12:00 row's prev_dt is NULL).
        assert rows[0][0] == h13.replace(tzinfo=None)
        assert rows[0][1] == pytest.approx(3000 / 3600 / 1000, rel=1e-6)

    def test_idempotent(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 200.0)

        store.refresh_hourly_usage()
        first = store._conn.execute(
            "SELECT plug_id, hour_ts, kwh, samples FROM hourly_usage"
        ).fetchall()
        store.refresh_hourly_usage()
        second = store._conn.execute(
            "SELECT plug_id, hour_ts, kwh, samples FROM hourly_usage"
        ).fetchall()
        assert first == second

    def test_excludes_no_emeter_plugs(self, store: Store) -> None:
        ep10 = store.ensure_plug("ep", "", "Snack", has_emeter=False)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # EP10s only ever record NULL watts (ON) or 0 (OFF); regardless,
        # has_emeter=False should keep them out of the rollup.
        store.insert_readings([(h, ep10, None, None, None, None)])
        store.insert_readings([(h.replace(second=30), ep10, None, None, None, None)])

        store.refresh_hourly_usage()
        rows = store._conn.execute("SELECT * FROM hourly_usage").fetchall()
        assert rows == []

    def test_backfills_history_on_empty_table(self, store: Store) -> None:
        """First refresh after data already exists should fill the whole range."""
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h12 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 1, 13, 0, 0, tzinfo=UTC)
        h14 = datetime(2026, 5, 1, 14, 0, 0, tzinfo=UTC)
        # Three hours of data spread out
        _insert_reading(store, pid, h12, 100.0)
        _insert_reading(store, pid, h12.replace(second=10), 100.0)
        _insert_reading(store, pid, h13.replace(second=10), 200.0)
        _insert_reading(store, pid, h14.replace(second=10), 300.0)

        store.refresh_hourly_usage()
        rows = store._conn.execute("SELECT hour_ts FROM hourly_usage ORDER BY hour_ts").fetchall()
        # All three hour buckets present (the very first row at h12:00 has
        # prev_dt=NULL, but h12:00 still has h12:00:10's contribution).
        hour_ts_list = [r[0] for r in rows]
        assert hour_ts_list == [
            h12.replace(tzinfo=None),
            h13.replace(tzinfo=None),
            h14.replace(tzinfo=None),
        ]

    def test_no_undercount_at_lookback_boundary(self, store: Store) -> None:
        """A subsequent narrow refresh must not lose the first sample's energy.

        Before the fix, `prev_dt` was computed AFTER the ts >= window_start
        filter, so the boundary row's LAG predecessor was excluded and its
        watts*dt contribution was dropped. The destructive upsert then
        overwrote the previously-correct bucket with the undercount.
        """
        from datetime import timedelta

        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        base = datetime(2026, 5, 25, 10, 0, 0, tzinfo=UTC)
        # 4 hours of data, one reading every 30 minutes.
        for offset_min in range(0, 240, 30):
            _insert_reading(store, pid, base + timedelta(minutes=offset_min), 200.0)

        # First refresh: full backfill (window starts at the oldest reading).
        store.refresh_hourly_usage()
        baseline = dict(
            store._conn.execute("SELECT hour_ts, kwh FROM hourly_usage ORDER BY hour_ts").fetchall()
        )

        # Second refresh with lookback_hours=2 starts the window inside the
        # data — the boundary row's predecessor is now outside the inner
        # window. The fix preserves it via a wider LAG-input window.
        store.refresh_hourly_usage(lookback_hours=2)
        after = dict(
            store._conn.execute("SELECT hour_ts, kwh FROM hourly_usage ORDER BY hour_ts").fetchall()
        )

        for hour, kwh in baseline.items():
            assert after[hour] == pytest.approx(kwh, rel=1e-9), (
                f"hour {hour} undercounted on re-refresh: baseline={kwh} new={after[hour]}"
            )

    def test_refreshes_current_hour_on_repeat_calls(self, store: Store) -> None:
        """A second refresh after new samples in the same hour updates the row."""
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 200.0)
        store.refresh_hourly_usage()
        first_kwh = store._conn.execute("SELECT kwh FROM hourly_usage").fetchone()[0]

        # More samples roll in same hour.
        _insert_reading(store, pid, h.replace(minute=1, second=0), 200.0)
        store.refresh_hourly_usage()
        second_kwh = store._conn.execute("SELECT kwh FROM hourly_usage").fetchone()[0]
        assert second_kwh > first_kwh


class TestUsageByMachine:
    def test_basic_attribution(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "Blackout - M0013", has_emeter=True)
        mid = store.ensure_machine("M0013", "Blackout")
        t0 = datetime(2026, 5, 25, 0, 0, 0, tzinfo=UTC)
        store.update_assignment(pid, mid, t0)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 200.0)
        store.refresh_hourly_usage()

        rows = store.usage_by_machine(h, h.replace(hour=13))
        assert len(rows) == 1
        assert rows[0]["machine_id"] == mid
        assert rows[0]["machine_name"] == "Blackout"
        assert rows[0]["kwh"] > 0

    def test_unassigned_bucket(self, store: Store) -> None:
        # Plug never assigned to a machine.
        pid = store.ensure_plug("d1", "c1", "Spare", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 100.0)
        store.refresh_hourly_usage()

        rows = store.usage_by_machine(h, h.replace(hour=13))
        assert len(rows) == 1
        assert rows[0]["machine_id"] is None
        assert rows[0]["machine_name"] == "Unassigned"

    def test_reassignment_uses_start_of_hour(self, store: Store) -> None:
        """If a plug was reassigned mid-hour, the hour's kWh attributes to the
        assignment active at the START of the hour."""
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        m1 = store.ensure_machine("M0001", "Old Machine")
        m2 = store.ensure_machine("M0002", "New Machine")
        t_start = datetime(2026, 5, 25, 11, 0, 0, tzinfo=UTC)
        t_mid = datetime(2026, 5, 25, 12, 30, 0, tzinfo=UTC)  # mid-hour swap
        store.update_assignment(pid, m1, t_start)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 100.0)
        store.update_assignment(pid, m2, t_mid)
        store.refresh_hourly_usage()

        rows = store.usage_by_machine(h, h.replace(hour=13))
        assert len(rows) == 1
        # Start-of-hour assignment wins the whole hour.
        assert rows[0]["machine_id"] == m1
        assert rows[0]["machine_name"] == "Old Machine"

    def test_empty_window(self, store: Store) -> None:
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        assert store.usage_by_machine(start, end) == []


# ---------------------------------------------------------------------------
# Daily play-hours rollup
# ---------------------------------------------------------------------------


def _attract_watts(n: int, level: float = 50.0) -> list[float]:
    """Tight cluster around `level` — low RSD, classifies as ATTRACT."""
    # Tiny ±0.3% wobble so RSD is well below the play threshold but non-zero.
    return [level * (1 + 0.003 * math.sin(i * 0.3)) for i in range(n)]


def _playing_watts(n: int, base: float = 100.0, swing: float = 60.0) -> list[float]:
    """Wildly varying — high RSD, classifies as PLAYING."""
    # Mix of two unrelated frequencies for noise-like variance.
    return [base + swing * math.sin(i * 0.7) + swing * 0.5 * math.cos(i * 1.3) for i in range(n)]


def _insert_series(
    store: Store, plug_id: int, base_ts: datetime, watts_list: list[float], dt: float = 1.0
) -> None:
    rows = []
    for i, w in enumerate(watts_list):
        ts = base_ts + timedelta(seconds=i * dt)
        rows.append((ts, plug_id, w, 120.0, w / 120.0, 0.0))
    store.insert_readings(rows)


_DEFAULT_CAL = Calibration(idle_max_rsd=None, play_min_rsd=10.0)


def _setup_calibrated(
    store: Store,
    plug_seed: tuple[str, str, str] = ("d1", "c1", "Blackout - M0013"),
    asset: str = "M0013",
    name: str = "Blackout",
    cal: Calibration = _DEFAULT_CAL,
    assign_at: datetime | None = None,
) -> tuple[int, int]:
    pid = store.ensure_plug(*plug_seed, has_emeter=True)
    mid = store.ensure_machine(asset, name)
    store.update_assignment(pid, mid, assign_at or datetime(2026, 5, 24, 0, 0, 0, tzinfo=UTC))
    store.set_calibration(mid, cal)
    return pid, mid


class TestRefreshDailyPlaySeconds:
    def test_detects_playing_segment(self, store: Store) -> None:
        pid, mid = _setup_calibrated(store)
        # 60s ATTRACT primes the rolling classifier, then 120s PLAYING.
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60))
        _insert_series(store, pid, t0 + timedelta(seconds=60), _playing_watts(120))

        store.refresh_daily_play_seconds()

        rows = store._conn.execute(
            "SELECT machine_id, day_local, seconds FROM daily_play_seconds"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == mid
        # PLAYING segment is 120s. The first few PLAYING readings may still
        # classify as ATTRACT while the rolling window is mixing — be lenient
        # but verify the order of magnitude.
        assert 60.0 <= rows[0][2] <= 130.0

    def test_ignores_uncalibrated_machines(self, store: Store) -> None:
        # Plug assigned to a machine that has NO calibration row.
        pid = store.ensure_plug("d1", "c1", "Uncalibrated - M9999", has_emeter=True)
        mid = store.ensure_machine("M9999", "Uncalibrated")
        store.update_assignment(pid, mid, datetime(2026, 5, 24, 0, 0, 0, tzinfo=UTC))
        # Lots of varying watts — would classify as PLAYING if calibration existed.
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))

        store.refresh_daily_play_seconds()

        assert store._conn.execute("SELECT COUNT(*) FROM daily_play_seconds").fetchone()[0] == 0

    def test_dt_cap_at_60s(self, store: Store) -> None:
        """A long gap between consecutive PLAYING samples caps dt at 60s."""
        pid, mid = _setup_calibrated(store)
        # Build a clean PLAYING window, then leave a 30-minute gap, then one
        # more PLAYING reading. The gap row contributes at most 60s.
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60))
        _insert_series(store, pid, t0 + timedelta(seconds=60), _playing_watts(120))
        # 30-minute gap, then continued play (no warmup needed — rolling
        # window persists from the prior segment if dense enough).
        gap_end = t0 + timedelta(seconds=60 + 120 + 1800)
        _insert_series(store, pid, gap_end, _playing_watts(60))

        store.refresh_daily_play_seconds()

        seconds = store._conn.execute(
            "SELECT seconds FROM daily_play_seconds WHERE machine_id = ?",
            [mid],
        ).fetchone()[0]
        # 120s of dense play + at most 60s contribution from the gap-spanning
        # row + ~60s of trailing play.
        assert seconds <= 60 + 120 + 60 + 5  # +5s tolerance for classifier edges

    def test_idempotent(self, store: Store) -> None:
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))
        store.refresh_daily_play_seconds()
        first = store._conn.execute(
            "SELECT machine_id, day_local, seconds FROM daily_play_seconds"
        ).fetchall()
        store.refresh_daily_play_seconds()
        second = store._conn.execute(
            "SELECT machine_id, day_local, seconds FROM daily_play_seconds"
        ).fetchall()
        assert first == second

    def test_recalibration_clears_stale_play_time(self, store: Store) -> None:
        """If a machine's play_min_rsd is raised so that yesterday's readings
        no longer classify as PLAYING, yesterday's row must be wiped — not
        left stale at the previously-computed seconds."""
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))

        # First pass with the default (loose) calibration: should detect play.
        store.refresh_daily_play_seconds()
        first = store._conn.execute(
            "SELECT seconds FROM daily_play_seconds WHERE machine_id = ?", [mid]
        ).fetchone()
        assert first is not None and first[0] > 0

        # Recalibrate so the threshold is impossibly high — nothing in those
        # readings classifies as PLAYING anymore.
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=10000.0))
        store.refresh_daily_play_seconds()

        # The old non-zero row must not survive.
        rows = store._conn.execute(
            "SELECT seconds FROM daily_play_seconds WHERE machine_id = ?", [mid]
        ).fetchall()
        # Either no row at all, or a row with seconds == 0 (both are correct).
        assert all(r[0] == 0 for r in rows), f"stale rows: {rows}"

    def test_buckets_into_central_local_days(self, store: Store) -> None:
        """A PLAYING segment that straddles local midnight ends up in two days."""
        pid, mid = _setup_calibrated(store)
        # 04:50 UTC on 5/25 = 23:50 CDT on 5/24.
        # Build a sequence: warmup, then a long PLAYING burst crossing local midnight.
        warm_start = datetime(2026, 5, 25, 4, 49, 0, tzinfo=UTC)  # 23:49 CDT 5/24
        _insert_series(store, pid, warm_start, _attract_watts(60))
        play_start = warm_start + timedelta(seconds=60)  # 23:50 CDT 5/24
        # 25 minutes of play — straddles midnight (00:00 CDT 5/25 ≈ 05:00 UTC).
        _insert_series(store, pid, play_start, _playing_watts(25 * 60))

        store.refresh_daily_play_seconds()

        rows = store._conn.execute(
            "SELECT day_local, seconds FROM daily_play_seconds "
            "WHERE machine_id = ? ORDER BY day_local",
            [mid],
        ).fetchall()
        days = {r[0] for r in rows}
        assert date(2026, 5, 24) in days
        assert date(2026, 5, 25) in days
        # Both days have nonzero play time.
        for _, seconds in rows:
            assert seconds > 0


class TestPlayHoursByMachine:
    def test_basic_query(self, store: Store) -> None:
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))
        store.refresh_daily_play_seconds()

        rows = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 27))
        assert len(rows) == 1
        assert rows[0]["machine_id"] == mid
        assert rows[0]["machine_name"] == "Blackout"
        assert rows[0]["day_local"] == date(2026, 5, 25)
        assert rows[0]["hours"] > 0

    def test_empty_window(self, store: Store) -> None:
        rows = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 27))
        assert rows == []
