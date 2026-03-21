"""Tests for juice.store — DuckDB storage layer."""

from __future__ import annotations

from datetime import datetime, UTC

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
            assert table_names == {"plugs", "readings", "machines", "assignments", "calibrations"}


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
        store.insert_readings([
            (ts, pid1, 100.0, 120.0, 0.833, 5.0),
            (ts, pid2, 200.0, 121.0, 1.653, 10.0),
        ])

        count = store._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
        assert count == 2


class TestRecordStrip:
    def test_stores_plugs_and_readings(self, store: Store) -> None:
        reading = StripReading(
            alias="Strip 1",
            device_id="device1",
            plugs=[
                PlugReading(child_id="c01", alias="Blackout - M0013", is_on=True,
                            watts=100.0, voltage=120.0, amps=0.833, total_kwh=5.0),
                PlugReading(child_id="c02", alias="Hyperball - M0014", is_on=False,
                            watts=0.0, voltage=120.0, amps=0.0, total_kwh=2.0),
            ],
        )
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.record_strip(reading, ts)

        plugs = store._conn.execute("SELECT * FROM plugs ORDER BY plug_id").fetchall()
        assert len(plugs) == 2
        assert plugs[0][1] == "device1"  # device_id
        assert plugs[0][2] == "c01"      # child_id

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

        rows = store._conn.execute(
            "SELECT * FROM assignments ORDER BY assigned_from"
        ).fetchall()
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
        store.seed_calibrations({
            "Godzilla (Premium)": Calibration(idle_max_rsd=2.0, play_min_rsd=12.0),
            "Hyperball": Calibration(idle_max_rsd=None, play_min_rsd=13.0),
            "Nonexistent Machine": Calibration(idle_max_rsd=1.0, play_min_rsd=5.0),
        })
        # Seeded machines get calibrations
        godzilla_mid = store._machine_cache["M0001"][0]
        hyperball_mid = store._machine_cache["M0002"][0]
        assert store.get_calibration(godzilla_mid) == Calibration(idle_max_rsd=2.0, play_min_rsd=12.0)
        assert store.get_calibration(hyperball_mid) == Calibration(idle_max_rsd=None, play_min_rsd=13.0)
