"""Tests for juice.store — DuckDB storage layer."""

from __future__ import annotations

from datetime import UTC, datetime

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


class TestSchemaMigration:
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
        # An HS300 child plug — should not appear
        store.ensure_plug("hs300", "child01", "Pinball", has_emeter=True)
        # An unassigned EP10 — should appear
        ep10_id = store.ensure_plug("ep10-a", "", "Snack Machine", has_emeter=False)
        # An assigned EP10 — should NOT appear
        ep10_assigned = store.ensure_plug("ep10-b", "", "Tagged Machine M9999", has_emeter=False)
        mid = store.ensure_machine("M9999", "Tagged Machine")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(ep10_assigned, mid, ts)

        rows = store.list_unassigned_outlets()
        ids = [r[0] for r in rows]
        assert ids == [ep10_id]
        # Tuple shape: (plug_id, device_id, alias, is_on_latest)
        plug_id, device_id, alias, is_on_latest = rows[0]
        assert device_id == "ep10-a"
        assert alias == "Snack Machine"
        assert is_on_latest is None  # no readings yet

    def test_is_on_latest_reflects_most_recent_reading(self, store: Store) -> None:
        pid = store.ensure_plug("ep10-x", "", "X", has_emeter=False)
        t0 = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 3, 15, 12, 1, 0, tzinfo=UTC)
        # First reading: ON (watts=NULL is the on-without-emeter signal)
        store.insert_readings([(t0, pid, None, None, None, None)])
        # Latest reading: OFF (watts=0)
        store.insert_readings([(t1, pid, 0.0, 0.0, 0.0, 0.0)])

        rows = store.list_unassigned_outlets()
        assert len(rows) == 1
        assert rows[0][3] is False

        # Now insert a more recent ON reading
        t2 = datetime(2026, 3, 15, 12, 2, 0, tzinfo=UTC)
        store.insert_readings([(t2, pid, None, None, None, None)])
        rows = store.list_unassigned_outlets()
        assert rows[0][3] is True


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
