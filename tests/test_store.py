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
                "power_baselines",
                "power_events",
                "hourly_usage",
                "hourly_strip_peak",
                "hourly_play_seconds",
                "strips",
                "circuits",
                "circuit_devices",
                "hourly_circuit_peak",
                "air_sensors",
                "air_readings",
                "applied_migrations",
            }


class TestSnapshotTo:
    def _seed(self, store: Store) -> int:
        plug_id = store.ensure_plug("d1", "c01", "Blackout - M0013")
        mid = store.ensure_machine("M0013", "Blackout")
        ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(plug_id, mid, ts)
        store.insert_readings([(ts, plug_id, 120.0, 120.0, 1.0, 0.0)])
        return plug_id

    def test_row_count_parity(self, tmp_path) -> None:
        dest = str(tmp_path / "snap.duckdb")
        with Store(str(tmp_path / "src.duckdb")) as src:
            self._seed(src)
            src.snapshot_to(dest)
            counts = {
                t: src._conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]  # noqa: S608
                for t in ("plugs", "machines", "assignments", "readings")
            }
        with Store(dest) as snap:
            for table, n in counts.items():
                got = snap._conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608
                assert got == n, table

    def test_dest_standalone_without_wal(self, tmp_path) -> None:
        dest = tmp_path / "snap.duckdb"
        with Store(str(tmp_path / "src.duckdb")) as src:
            self._seed(src)
            src.snapshot_to(str(dest))
        assert dest.exists()
        assert not (tmp_path / "snap.duckdb.wal").exists()

    def test_source_still_writable_after_snapshot(self, tmp_path) -> None:
        with Store(str(tmp_path / "src.duckdb")) as src:
            plug_id = self._seed(src)
            before = src._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
            src.snapshot_to(str(tmp_path / "snap.duckdb"))
            ts = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
            src.insert_readings([(ts, plug_id, 90.0, 120.0, 0.8, 0.0)])
            after = src._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
            assert after == before + 1

    def test_snapshot_is_usable_by_a_recorder(self, tmp_path) -> None:
        # Schema + sequences must carry over so dev can keep recording into it.
        dest = str(tmp_path / "snap.duckdb")
        with Store(str(tmp_path / "src.duckdb")) as src:
            self._seed(src)
            src.snapshot_to(dest)
        with Store(dest) as snap:
            new_plug = snap.ensure_plug("d2", "c02", "Star Trip - M0009")
            mid = snap.ensure_machine("M0009", "Star Trip")
            assert new_plug > 0 and mid > 0
            ts = datetime(2026, 5, 25, 14, 0, 0, tzinfo=UTC)
            snap.record_power_event(ts, new_plug, "turn_on", "individual", "tester", "ok")


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

    def test_recent_filters_by_plug_id(self, store: Store) -> None:
        pid_a = store.ensure_plug("d1", "c01", "P1")
        pid_b = store.ensure_plug("d1", "c02", "P2")
        t0 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 5, 25, 12, 1, 0, tzinfo=UTC)
        t2 = datetime(2026, 5, 25, 12, 2, 0, tzinfo=UTC)
        store.record_power_event(t0, pid_a, "turn_on", "individual", "a", "ok")
        store.record_power_event(t1, pid_b, "turn_on", "individual", "b", "ok")
        store.record_power_event(t2, pid_a, "turn_off", "individual", "a", "ok")

        rows = store.recent_power_events(limit=10, plug_id=pid_a)
        assert [r["plug_id"] for r in rows] == [pid_a, pid_a]
        assert [r["action"] for r in rows] == ["turn_off", "turn_on"]

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


class TestGetMachineId:
    def test_returns_id_for_known_asset(self, store: Store) -> None:
        mid = store.ensure_machine("M0001", "Medieval Madness")
        assert store.get_machine_id("M0001") == mid

    def test_none_for_unknown_asset(self, store: Store) -> None:
        assert store.get_machine_id("M9999") is None

    def test_reads_without_cache(self, store: Store) -> None:
        # A fresh Store (cold cache) still resolves via the DB query path.
        mid = store.ensure_machine("M0001", "Medieval Madness")
        store._machine_cache.clear()
        assert store.get_machine_id("M0001") == mid


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


class TestPowerBaselines:
    NOW = datetime(2026, 6, 14, 0, 0, 0, tzinfo=UTC)

    def _seed_machine(self, store: Store, asset_id: str, name: str) -> int:
        plug_id = store.ensure_plug("d1", asset_id, f"{name} - {asset_id}")
        mid = store.ensure_machine(asset_id, name)
        store.update_assignment(plug_id, mid, self.NOW - timedelta(days=60))
        return plug_id

    def _insert_minutes(self, store: Store, plug_id: int, watts_per_minute: list[float]) -> None:
        """One reading per minute, ending just before NOW."""
        rows = []
        for i, w in enumerate(reversed(watts_per_minute)):
            ts = self.NOW - timedelta(minutes=i + 1)
            rows.append((ts, plug_id, w, 120.0, w / 120.0, 0.0))
        store.insert_readings(rows)

    def test_roundtrip_and_keyed_by_asset_id(self, store: Store) -> None:
        plug = self._seed_machine(store, "M0003", "Trade Winds")
        self._insert_minutes(store, plug, [45.0] * 50)
        result = store.refresh_power_baselines(min_minutes=10, now=self.NOW)
        assert set(result) == {store._machine_cache["M0003"][0]}
        baselines = store.get_power_baselines()
        assert set(baselines) == {"M0003"}
        assert baselines["M0003"] == pytest.approx(45.0, abs=1.0)

    def test_excludes_low_history_machine(self, store: Store) -> None:
        plug = self._seed_machine(store, "M0003", "Trade Winds")
        self._insert_minutes(store, plug, [45.0] * 4)  # only 4 on-minutes
        result = store.refresh_power_baselines(min_minutes=10, now=self.NOW)
        assert result == {}
        assert store.get_power_baselines() == {}

    def test_p99_robust_to_brief_incident(self, store: Store) -> None:
        # 200 normal minutes + 1 incident minute => incident is <1% so p99 ignores it.
        plug = self._seed_machine(store, "M0003", "Trade Winds")
        self._insert_minutes(store, plug, [45.0] * 200 + [180.0])
        store.refresh_power_baselines(min_minutes=10, now=self.NOW)
        assert store.get_power_baselines()["M0003"] == pytest.approx(45.0, abs=2.0)

    def test_prunes_machine_that_no_longer_qualifies(self, store: Store) -> None:
        # A machine with enough history gets a baseline; once its readings age out
        # of the window it should be dropped so it's no longer armed.
        plug = self._seed_machine(store, "M0003", "Trade Winds")
        self._insert_minutes(store, plug, [45.0] * 50)
        store.refresh_power_baselines(min_minutes=10, now=self.NOW)
        assert "M0003" in store.get_power_baselines()
        # Recompute "30 days later" — all readings now older than the window.
        store.refresh_power_baselines(min_minutes=10, now=self.NOW + timedelta(days=30))
        assert store.get_power_baselines() == {}

    def test_ignores_off_minutes_and_old_readings(self, store: Store) -> None:
        plug = self._seed_machine(store, "M0003", "Trade Winds")
        # 30 on-minutes, plus zero-watt (off) minutes that must not drag it down.
        self._insert_minutes(store, plug, [0.0] * 100 + [48.0] * 30)
        # An old high reading outside the window must be ignored.
        store.insert_readings([(self.NOW - timedelta(days=45), plug, 300.0, 120.0, 2.5, 0.0)])
        store.refresh_power_baselines(days=30, min_minutes=10, now=self.NOW)
        assert store.get_power_baselines()["M0003"] == pytest.approx(48.0, abs=1.0)


class TestMachineLock:
    def test_defaults_unlocked(self, store: Store) -> None:
        store.ensure_machine("M0013", "Blackout")
        assert store.get_lock_modes() == {}

    def test_set_lock_mode_roundtrip(self, store: Store) -> None:
        machine_id = store.ensure_machine("M0013", "Blackout")
        store.set_machine_lock_mode(machine_id, "on")
        assert store.get_lock_modes() == {"M0013": "on"}
        store.set_machine_lock_mode(machine_id, "off")
        assert store.get_lock_modes() == {"M0013": "off"}
        store.set_machine_lock_mode(machine_id, None)
        assert store.get_lock_modes() == {}

    def test_only_locked_machines_returned(self, store: Store) -> None:
        on_id = store.ensure_machine("M0013", "Blackout")
        off_id = store.ensure_machine("M0009", "Star Trip")
        store.ensure_machine("M0042", "Twilight Zone")
        store.set_machine_lock_mode(on_id, "on")
        store.set_machine_lock_mode(off_id, "off")
        assert store.get_lock_modes() == {"M0013": "on", "M0009": "off"}

    def test_lock_survives_reopen_and_ensure_machine(self, tmp_path) -> None:
        # The recorder upserts machines every refresh; the upsert must not
        # clobber the lock, and the lock must persist across restarts.
        db_path = str(tmp_path / "test.duckdb")
        with Store(db_path) as s:
            machine_id = s.ensure_machine("M0013", "Blackout")
            s.set_machine_lock_mode(machine_id, "off")
        with Store(db_path) as s:
            s.ensure_machine("M0013", "Blackout")
            assert s.get_lock_modes() == {"M0013": "off"}


class TestListPlugs:
    def test_returns_all_plugs_including_unassigned(self, store: Store) -> None:
        p1 = store.ensure_plug("d1", "c00", "Blackout - M0013")
        p2 = store.ensure_plug("d1", "c01", "Unused", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(p1, mid, datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))

        rows = store.list_plugs()
        assert rows == [
            (p1, "d1", "c00", "Blackout - M0013", True),
            (p2, "d1", "c01", "Unused", False),
        ]

    def test_empty(self, store: Store) -> None:
        assert store.list_plugs() == []


class TestStripNames:
    def test_set_and_get_roundtrip(self, store: Store) -> None:
        store.set_strip_name("dev1", "Back Wall")
        assert store.get_strip_names() == {"dev1": "Back Wall"}

    def test_upsert_overwrites(self, store: Store) -> None:
        store.set_strip_name("dev1", "Back Wall")
        store.set_strip_name("dev1", "Front Window")
        assert store.get_strip_names() == {"dev1": "Front Window"}

    def test_empty_name_clears_override(self, store: Store) -> None:
        store.set_strip_name("dev1", "Back Wall")
        store.set_strip_name("dev1", "")
        assert store.get_strip_names() == {}

    def test_whitespace_only_clears_override(self, store: Store) -> None:
        store.set_strip_name("dev1", "Back Wall")
        store.set_strip_name("dev1", "   ")
        assert store.get_strip_names() == {}

    def test_name_is_stripped(self, store: Store) -> None:
        store.set_strip_name("dev1", "  Back Wall  ")
        assert store.get_strip_names() == {"dev1": "Back Wall"}

    def test_empty_when_no_names_set(self, store: Store) -> None:
        assert store.get_strip_names() == {}

    def test_survives_reopen(self, tmp_path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        with Store(db_path) as s:
            s.set_strip_name("dev1", "Back Wall")
        with Store(db_path) as s:
            assert s.get_strip_names() == {"dev1": "Back Wall"}

    def test_clearing_name_preserves_sort_order(self, store: Store) -> None:
        # A strip with an order but no name keeps the (nameless) row + order.
        store.set_strip_orders(["dev1"])
        store.set_strip_name("dev1", "Back Wall")
        store.set_strip_name("dev1", "")
        assert store.get_strip_names() == {}
        assert store.get_strip_orders() == {"dev1": 0}


class TestStripOrders:
    def test_set_assigns_index_order(self, store: Store) -> None:
        store.set_strip_orders(["devA", "devB", "devC"])
        assert store.get_strip_orders() == {"devA": 0, "devB": 1, "devC": 2}

    def test_reorder_overwrites(self, store: Store) -> None:
        store.set_strip_orders(["devA", "devB", "devC"])
        store.set_strip_orders(["devC", "devA", "devB"])
        assert store.get_strip_orders() == {"devC": 0, "devA": 1, "devB": 2}

    def test_replace_drops_omitted_strips(self, store: Store) -> None:
        # The endpoint sends the full order, so a strip omitted from a later
        # call loses its position rather than keeping a stale one.
        store.set_strip_orders(["devA", "devB", "devC"])
        store.set_strip_orders(["devC", "devA"])
        assert store.get_strip_orders() == {"devC": 0, "devA": 1}

    def test_replace_preserves_named_strips(self, store: Store) -> None:
        # Dropping a strip's order must not delete its name row.
        store.set_strip_name("devB", "Back Wall")
        store.set_strip_orders(["devA", "devB"])
        store.set_strip_orders(["devA"])  # devB dropped from order
        assert store.get_strip_orders() == {"devA": 0}
        assert store.get_strip_names() == {"devB": "Back Wall"}

    def test_empty_when_unset(self, store: Store) -> None:
        assert store.get_strip_orders() == {}

    def test_coexists_with_name(self, store: Store) -> None:
        store.set_strip_name("devA", "Back Wall")
        store.set_strip_orders(["devA"])
        assert store.get_strip_names() == {"devA": "Back Wall"}
        assert store.get_strip_orders() == {"devA": 0}

    def test_survives_reopen(self, tmp_path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        with Store(db_path) as s:
            s.set_strip_orders(["devA", "devB"])
        with Store(db_path) as s:
            assert s.get_strip_orders() == {"devA": 0, "devB": 1}


class TestCircuitCRUD:
    def test_create_returns_id_and_row(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "coin-op ceiling drop", 20.0)
        assert isinstance(cid, int)
        c = store.get_circuit(cid)
        assert c == {
            "circuit_id": cid,
            "panel": "P1",
            "breaker": "B20",
            "description": "coin-op ceiling drop",
            "amps": pytest.approx(20.0),
        }

    def test_create_null_amps(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B22", "", None)
        assert store.get_circuit(cid)["amps"] is None

    def test_list_sorted_by_panel_breaker(self, store: Store) -> None:
        store.create_circuit("P2", "B1", "b", 15.0)
        store.create_circuit("P1", "B20", "a", 20.0)
        store.create_circuit("P1", "B2", "c", 20.0)
        got = [(c["panel"], c["breaker"]) for c in store.list_circuits()]
        assert got == [("P1", "B2"), ("P1", "B20"), ("P2", "B1")]

    def test_get_unknown_returns_none(self, store: Store) -> None:
        assert store.get_circuit(999) is None

    def test_duplicate_panel_breaker_rejected(self, store: Store) -> None:
        from juice.store import DuplicateCircuitError

        store.create_circuit("P1", "B20", "first", 20.0)
        with pytest.raises(DuplicateCircuitError):
            store.create_circuit("P1", "B20", "second", 15.0)

    def test_update_to_existing_panel_breaker_rejected(self, store: Store) -> None:
        from juice.store import DuplicateCircuitError

        store.create_circuit("P1", "B20", "a", 20.0)
        c2 = store.create_circuit("P1", "B22", "b", 20.0)
        with pytest.raises(DuplicateCircuitError):
            store.update_circuit(c2, "P1", "B20", "b", 20.0)

    def test_update_circuit(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "old", 20.0)
        store.update_circuit(cid, panel="P3", breaker="B5", description="new", amps=15.0)
        c = store.get_circuit(cid)
        assert (c["panel"], c["breaker"], c["description"], c["amps"]) == (
            "P3",
            "B5",
            "new",
            pytest.approx(15.0),
        )

    def test_delete_clears_devices_and_peaks(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        store.set_device_circuit("dev1", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store._conn.execute("INSERT INTO hourly_circuit_peak VALUES (?, ?, 100.0, 90.0)", [cid, h])
        store.delete_circuit(cid)
        assert store.get_circuit(cid) is None
        assert store.get_circuit_devices() == {}
        rows = store._conn.execute(
            "SELECT count(*) FROM hourly_circuit_peak WHERE circuit_id = ?", [cid]
        ).fetchone()[0]
        assert rows == 0


class TestCircuitDevices:
    def test_assign_and_get(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        store.set_device_circuit("dev1", cid)
        assert store.get_circuit_devices() == {"dev1": cid}

    def test_reassign_overwrites(self, store: Store) -> None:
        c1 = store.create_circuit("P1", "B20", "", 20.0)
        c2 = store.create_circuit("P1", "B22", "", 20.0)
        store.set_device_circuit("dev1", c1)
        store.set_device_circuit("dev1", c2)
        assert store.get_circuit_devices() == {"dev1": c2}

    def test_clear_with_none_removes_row(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        store.set_device_circuit("dev1", cid)
        store.set_device_circuit("dev1", None)
        assert store.get_circuit_devices() == {}

    def test_many_strips_one_circuit(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        store.set_device_circuit("dev1", cid)
        store.set_device_circuit("dev2", cid)
        assert store.get_circuit_devices() == {"dev1": cid, "dev2": cid}

    def test_assign_unknown_circuit_raises(self, store: Store) -> None:
        with pytest.raises(ValueError, match="Unknown circuit"):
            store.set_device_circuit("dev1", 999)


class TestSchemaMigration:
    def test_adds_peak_watts_and_backfills_from_readings(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a pre-migration DB: hourly_usage without peak_watts, with
        # an existing rollup row whose peak must backfill from readings.
        conn = duckdb.connect(db_path)
        conn.execute(
            """
            CREATE SEQUENCE plug_id_seq START 1;
            CREATE TABLE plugs (
                plug_id    SMALLINT PRIMARY KEY,
                device_id  VARCHAR NOT NULL,
                child_id   VARCHAR NOT NULL,
                alias      VARCHAR NOT NULL,
                has_emeter BOOLEAN NOT NULL DEFAULT TRUE,
                UNIQUE (device_id, child_id)
            );
            CREATE TABLE readings (
                ts        TIMESTAMP NOT NULL,
                plug_id   SMALLINT  NOT NULL,
                watts     FLOAT,
                voltage   FLOAT,
                amps      FLOAT,
                total_kwh FLOAT
            );
            CREATE TABLE hourly_usage (
                plug_id  SMALLINT  NOT NULL,
                hour_ts  TIMESTAMP NOT NULL,
                kwh      FLOAT     NOT NULL,
                samples  INTEGER   NOT NULL,
                PRIMARY KEY (plug_id, hour_ts)
            );
            """
        )
        conn.execute("INSERT INTO plugs VALUES (1, 'd1', 'c1', 'P', TRUE)")
        h = datetime(2026, 5, 25, 12, 0, 0)
        conn.execute("INSERT INTO readings VALUES (?, 1, 100.0, 120.0, 0.8, 0.0)", [h])
        conn.execute(
            "INSERT INTO readings VALUES (?, 1, 300.0, 120.0, 2.5, 0.0)",
            [h.replace(second=30)],
        )
        conn.execute("INSERT INTO hourly_usage VALUES (1, ?, 0.0025, 1)", [h])
        conn.close()

        with Store(db_path) as s:
            # A pre-#20 DB gains BOTH columns in order: peak_watts then p99.
            row = s._conn.execute(
                "SELECT peak_watts, peak_watts_p99 FROM hourly_usage WHERE plug_id = 1"
            ).fetchone()
            assert row[0] == pytest.approx(300.0)
            # Two on-readings {100, 300}; p99 interpolates near the top → > 100.
            assert row[1] is not None
            assert row[1] > 100.0

    def test_adds_peak_watts_p99_to_post_peak_db(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a post-#20 DB: both rollup tables WITH peak_watts but
        # WITHOUT peak_watts_p99.
        conn = duckdb.connect(db_path)
        conn.execute(
            """
            CREATE SEQUENCE plug_id_seq START 1;
            CREATE TABLE plugs (
                plug_id    SMALLINT PRIMARY KEY,
                device_id  VARCHAR NOT NULL,
                child_id   VARCHAR NOT NULL,
                alias      VARCHAR NOT NULL,
                has_emeter BOOLEAN NOT NULL DEFAULT TRUE,
                UNIQUE (device_id, child_id)
            );
            CREATE TABLE readings (
                ts        TIMESTAMP NOT NULL,
                plug_id   SMALLINT  NOT NULL,
                watts     FLOAT,
                voltage   FLOAT,
                amps      FLOAT,
                total_kwh FLOAT
            );
            CREATE TABLE hourly_usage (
                plug_id    SMALLINT  NOT NULL,
                hour_ts    TIMESTAMP NOT NULL,
                kwh        FLOAT     NOT NULL,
                samples    INTEGER   NOT NULL,
                peak_watts FLOAT,
                PRIMARY KEY (plug_id, hour_ts)
            );
            CREATE TABLE hourly_strip_peak (
                device_id  VARCHAR   NOT NULL,
                hour_ts    TIMESTAMP NOT NULL,
                peak_watts FLOAT     NOT NULL,
                PRIMARY KEY (device_id, hour_ts)
            );
            """
        )
        conn.execute("INSERT INTO plugs VALUES (1, 'd1', 'c1', 'P', TRUE)")
        h = datetime(2026, 5, 25, 12, 0, 0)
        # A dense hour with one inrush spike so p99 differs from peak.
        for i in range(120):
            conn.execute(
                "INSERT INTO readings VALUES (?, 1, 120.0, 120.0, 1.0, 0.0)",
                [h.replace(second=0) + timedelta(seconds=i * 7)],
            )
        conn.execute(
            "INSERT INTO readings VALUES (?, 1, 569.0, 120.0, 4.7, 0.0)",
            [h.replace(second=3)],
        )
        conn.execute("INSERT INTO hourly_usage VALUES (1, ?, 0.1, 121, 569.0)", [h])
        conn.execute("INSERT INTO hourly_strip_peak VALUES ('d1', ?, 569.0)", [h])
        conn.close()

        with Store(db_path) as s:
            usage = s._conn.execute(
                "SELECT peak_watts_p99 FROM hourly_usage WHERE plug_id = 1"
            ).fetchone()
            assert usage[0] == pytest.approx(120.0, abs=2.0)
            strip = s._conn.execute(
                "SELECT peak_watts_p99 FROM hourly_strip_peak WHERE device_id = 'd1'"
            ).fetchone()
            assert strip[0] == pytest.approx(120.0, abs=2.0)

    def test_adds_lock_columns_to_fully_legacy_machines_table(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a pre-migration DB: machines table without locked or lock_mode.
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
            # Both columns now exist; the legacy row is unlocked.
            row = s._conn.execute(
                "SELECT locked, lock_mode FROM machines WHERE machine_id = 1"
            ).fetchone()
            assert row[0] is False
            assert row[1] is None
            assert s.get_lock_modes() == {}

    def test_backfills_lock_mode_from_legacy_locked(self, tmp_path) -> None:
        import duckdb

        db_path = str(tmp_path / "legacy.duckdb")
        # Simulate a DB that has the old `locked` BOOLEAN but no lock_mode.
        conn = duckdb.connect(db_path)
        conn.execute(
            """
            CREATE SEQUENCE machine_id_seq START 1;
            CREATE TABLE machines (
                machine_id SMALLINT PRIMARY KEY,
                asset_id   VARCHAR NOT NULL UNIQUE,
                name       VARCHAR NOT NULL,
                locked     BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )
        conn.execute("INSERT INTO machines VALUES (1, 'M0013', 'Blackout', TRUE)")
        conn.execute("INSERT INTO machines VALUES (2, 'M0009', 'Star Trip', FALSE)")
        conn.close()

        with Store(db_path) as s:
            # A previously locked machine means locked-ON.
            assert s.get_lock_modes() == {"M0013": "on"}

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
        # Tuple shape: (plug_id, device_id, alias, is_drawing_latest)
        plug_id, device_id, alias, is_drawing_latest = rows[0]
        assert device_id == "ep10-a"
        assert alias == "Snack Machine"
        # No-emeter plug has no watt measurement → draw is unknown (None), not "on".
        assert is_drawing_latest is None

    def test_is_drawing_latest_reflects_most_recent_reading(self, store: Store) -> None:
        pid = store.ensure_plug("hs300", "c06", "Sign", has_emeter=True)
        now = datetime.now(UTC)
        t0 = now - timedelta(minutes=2)
        t1 = now - timedelta(minutes=1)
        # Drew power (qualifies for the recent-power window), then dropped to 0.
        store.insert_readings([(t0, pid, 200.0, 120.0, 1.6, 1.0)])
        store.insert_readings([(t1, pid, 0.0, 0.0, 0.0, 0.0)])

        rows = store.list_unassigned_outlets()
        assert len(rows) == 1
        assert rows[0][3] is False  # latest reading drew nothing

        # A newer drawing reading flips it back to True.
        store.insert_readings([(now, pid, 150.0, 120.0, 1.25, 1.0)])
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
            "SELECT plug_id, hour_ts, kwh, samples, peak_watts FROM hourly_usage"
        ).fetchall()
        store.refresh_hourly_usage()
        second = store._conn.execute(
            "SELECT plug_id, hour_ts, kwh, samples, peak_watts FROM hourly_usage"
        ).fetchall()
        assert first == second

    def test_records_peak_watts_per_hour(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # First row is the LAG anchor (prev_dt NULL, excluded from the hour).
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 250.0)
        _insert_reading(store, pid, h.replace(minute=1), 150.0)

        store.refresh_hourly_usage()

        rows = store._conn.execute("SELECT peak_watts FROM hourly_usage").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(250.0)

    def test_peak_watts_updates_when_higher_sample_arrives(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(second=30), 250.0)
        store.refresh_hourly_usage()

        _insert_reading(store, pid, h.replace(second=45), 400.0)
        store.refresh_hourly_usage()

        rows = store._conn.execute("SELECT peak_watts FROM hourly_usage").fetchall()
        assert rows[0][0] == pytest.approx(400.0)

    def test_records_peak_watts_p99_excluding_inrush(self, store: Store) -> None:
        # A dense hour of steady ~120W draw with one inrush spike: the raw
        # peak captures the spike, p99 excludes it. quantile_cont fully drops
        # a single top value only with >= 101 on-samples, so seed ~200.
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, base, 100.0)  # LAG anchor
        for i in range(200):
            ts = base + timedelta(seconds=2 + i * 7)
            _insert_reading(store, pid, ts, 120.0 + (i % 5))  # 120..124
        # One inrush spike near the start.
        _insert_reading(store, pid, base + timedelta(seconds=5), 569.0)

        store.refresh_hourly_usage()

        row = store._conn.execute(
            "SELECT peak_watts, peak_watts_p99 FROM hourly_usage WHERE hour_ts = ?",
            [base.replace(tzinfo=None)],
        ).fetchone()
        assert row[0] == pytest.approx(569.0)  # raw peak keeps the spike
        assert row[1] == pytest.approx(124.0, abs=2.0)  # p99 excludes it

    def test_peak_watts_p99_null_when_hour_all_off(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "P", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, pid, h, 0.0)
        _insert_reading(store, pid, h.replace(second=30), 0.0)
        store.refresh_hourly_usage()

        row = store._conn.execute("SELECT peak_watts, peak_watts_p99 FROM hourly_usage").fetchone()
        assert row[0] == pytest.approx(0.0)
        assert row[1] is None

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


class TestRefreshHourlyStripPeak:
    def test_sums_simultaneous_readings_per_ts(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d1", "c01", "B", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # ts1: 200+100=300; ts2: 50+400=450 (the peak); ts3: 250 alone.
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, b, h, 100.0)
        _insert_reading(store, a, h.replace(second=30), 50.0)
        _insert_reading(store, b, h.replace(second=30), 400.0)
        _insert_reading(store, a, h.replace(minute=1), 250.0)

        store.refresh_hourly_strip_peak()

        rows = store._conn.execute(
            "SELECT device_id, hour_ts, peak_watts FROM hourly_strip_peak"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "d1"
        assert rows[0][1] == h.replace(tzinfo=None)
        assert rows[0][2] == pytest.approx(450.0)

    def test_records_p99_of_per_ts_sums(self, store: Store) -> None:
        # Dense hour of steady ~120W strip draw with one inrush spike in the
        # per-ts sums: raw peak keeps it, p99 excludes it.
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for i in range(200):
            ts = base + timedelta(seconds=i * 7)
            _insert_reading(store, a, ts, 120.0 + (i % 5))
        _insert_reading(store, a, base + timedelta(seconds=3), 569.0)

        store.refresh_hourly_strip_peak()

        row = store._conn.execute(
            "SELECT peak_watts, peak_watts_p99 FROM hourly_strip_peak"
        ).fetchone()
        assert row[0] == pytest.approx(569.0)
        assert row[1] == pytest.approx(124.0, abs=2.0)

    def test_p99_null_when_all_sums_zero(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 0.0)
        _insert_reading(store, a, h.replace(second=30), 0.0)
        store.refresh_hourly_strip_peak()

        row = store._conn.execute(
            "SELECT peak_watts, peak_watts_p99 FROM hourly_strip_peak"
        ).fetchone()
        assert row[0] == pytest.approx(0.0)
        assert row[1] is None

    def test_separates_devices(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        other = store.ensure_plug("d2", "c00", "Other", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, other, h, 500.0)

        store.refresh_hourly_strip_peak()

        peaks = dict(
            store._conn.execute("SELECT device_id, peak_watts FROM hourly_strip_peak").fetchall()
        )
        assert peaks == {"d1": pytest.approx(200.0), "d2": pytest.approx(500.0)}

    def test_skips_no_emeter_plugs(self, store: Store) -> None:
        ep10 = store.ensure_plug("ep", "", "Snack", has_emeter=False)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, ep10, None, None, None, None)])

        store.refresh_hourly_strip_peak()
        assert store._conn.execute("SELECT * FROM hourly_strip_peak").fetchall() == []

    def test_null_watts_on_emeter_plug_counts_as_zero(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d1", "c01", "B", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        store.insert_readings([(h, b, None, None, None, None)])

        store.refresh_hourly_strip_peak()

        rows = store._conn.execute("SELECT peak_watts FROM hourly_strip_peak").fetchall()
        assert rows[0][0] == pytest.approx(200.0)

    def test_idempotent(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, a, h.replace(second=30), 300.0)

        store.refresh_hourly_strip_peak()
        first = store._conn.execute(
            "SELECT device_id, hour_ts, peak_watts FROM hourly_strip_peak"
        ).fetchall()
        store.refresh_hourly_strip_peak()
        second = store._conn.execute(
            "SELECT device_id, hour_ts, peak_watts FROM hourly_strip_peak"
        ).fetchall()
        assert first == second

    def test_backfills_full_history_when_table_empty(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h12 = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        h14 = datetime(2026, 5, 1, 14, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h12, 100.0)
        _insert_reading(store, a, h14, 300.0)

        store.refresh_hourly_strip_peak()

        rows = store._conn.execute(
            "SELECT hour_ts, peak_watts FROM hourly_strip_peak ORDER BY hour_ts"
        ).fetchall()
        assert [(r[0], r[1]) for r in rows] == [
            (h12.replace(tzinfo=None), pytest.approx(100.0)),
            (h14.replace(tzinfo=None), pytest.approx(300.0)),
        ]

    def test_repeat_call_updates_current_hour_with_new_max(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        store.refresh_hourly_strip_peak()

        _insert_reading(store, a, h.replace(second=30), 600.0)
        store.refresh_hourly_strip_peak()

        rows = store._conn.execute("SELECT peak_watts FROM hourly_strip_peak").fetchall()
        assert rows[0][0] == pytest.approx(600.0)


def _fill_hour(store: Store, plug_id: int, hour: datetime, watts: float, n: int = 4) -> None:
    """Seed n equal-watts readings across an hour so the p99 equals `watts`."""
    for i in range(n):
        _insert_reading(store, plug_id, hour + timedelta(seconds=i * 5), watts)


class TestPlugPeaks:
    def test_max_per_plug_within_window(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d1", "c01", "B", has_emeter=True)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        # Each hour filled with steady watts so the hourly p99 is exact; the
        # 30-day peak is the MAX across hours.
        _fill_hour(store, a, h12, 150.0)
        _fill_hour(store, a, h13, 250.0)
        _fill_hour(store, b, h12, 50.0)
        _fill_hour(store, b, h13, 80.0)
        store.refresh_hourly_usage()

        peaks = store.plug_peaks([a, b], h12, h13 + timedelta(hours=1))
        assert peaks == {a: pytest.approx(250.0), b: pytest.approx(80.0)}

    def test_uses_p99_not_raw_max(self, store: Store) -> None:
        # A dense hour of steady draw with one inrush spike — the robust peak
        # reflects sustained draw, not the spike.
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for i in range(200):
            _insert_reading(store, a, base + timedelta(seconds=i * 7), 120.0)
        _insert_reading(store, a, base + timedelta(seconds=3), 569.0)
        store.refresh_hourly_usage()

        peaks = store.plug_peaks([a], base, base + timedelta(hours=1))
        assert peaks[a] == pytest.approx(120.0, abs=2.0)

    def test_window_bounds_half_open(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h12, 500.0)
        _fill_hour(store, a, h13, 900.0)
        store.refresh_hourly_usage()

        # Window covers only hour 12 — hour 13's 900W is excluded.
        peaks = store.plug_peaks([a], h12, h13)
        assert peaks == {a: pytest.approx(500.0)}

    def test_empty_plug_ids_returns_empty_dict(self, store: Store) -> None:
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        assert store.plug_peaks([], h, h + timedelta(hours=1)) == {}

    def test_omits_plug_with_only_all_off_hours(self, store: Store) -> None:
        # An always-off plug has NULL p99 for every hour → omitted (the UI
        # then shows "—" rather than a misleading 0.0 W).
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h, 0.0)
        store.refresh_hourly_usage()

        assert store.plug_peaks([a], h, h + timedelta(hours=1)) == {}

    def test_skips_plugs_with_null_peaks(self, store: Store) -> None:
        # A hand-edited / pre-backfill row with NULL p99 must not crash or
        # surface as a peak.
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store._conn.execute(
            "INSERT INTO hourly_usage (plug_id, hour_ts, kwh, samples, peak_watts, peak_watts_p99) "
            "VALUES (?, ?, 0.1, 1, 100.0, NULL)",
            [a, h],
        )
        assert store.plug_peaks([a], h, h + timedelta(hours=1)) == {}


class TestStripPeaks:
    def test_max_per_device(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        other = store.ensure_plug("d2", "c00", "Other", has_emeter=True)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h12, 300.0)
        _fill_hour(store, a, h13, 200.0)
        _fill_hour(store, other, h12, 700.0)
        store.refresh_hourly_strip_peak()

        peaks = store.strip_peaks(h12, h13 + timedelta(hours=1))
        assert peaks == {"d1": pytest.approx(300.0), "d2": pytest.approx(700.0)}

    def test_window_filter_excludes_old_hours(self, store: Store) -> None:
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h_old = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
        h_new = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h_old, 900.0)
        _fill_hour(store, a, h_new, 200.0)
        store.refresh_hourly_strip_peak()

        peaks = store.strip_peaks(h_new, h_new + timedelta(hours=1))
        assert peaks == {"d1": pytest.approx(200.0)}

    def test_omits_device_with_only_null_p99(self, store: Store) -> None:
        # Guards float(None): an all-off device has NULL p99 and must be
        # omitted, not crash strip_peaks.
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h, 0.0)
        store.refresh_hourly_strip_peak()

        assert store.strip_peaks(h, h + timedelta(hours=1)) == {}


class TestRefreshHourlyCircuitPeak:
    def test_sums_simultaneous_across_strips_of_a_circuit(self, store: Store) -> None:
        # Two strips (d1, d2) on one circuit: the peak is the max over time of
        # the SUM across both strips at each instant — the breaker number.
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d2", "c00", "B", has_emeter=True)
        store.set_device_circuit("d1", cid)
        store.set_device_circuit("d2", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # ts1: 200+100=300; ts2: 50+400=450 (peak); ts3: 250 alone.
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, b, h, 100.0)
        _insert_reading(store, a, h.replace(second=30), 50.0)
        _insert_reading(store, b, h.replace(second=30), 400.0)
        _insert_reading(store, a, h.replace(minute=1), 250.0)

        store.refresh_hourly_circuit_peak()

        rows = store._conn.execute(
            "SELECT circuit_id, hour_ts, peak_watts FROM hourly_circuit_peak"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == cid
        assert rows[0][2] == pytest.approx(450.0)

    def test_records_p99_of_per_ts_sums(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        store.set_device_circuit("d1", cid)
        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for i in range(200):
            _insert_reading(store, a, base + timedelta(seconds=i * 7), 120.0 + (i % 5))
        _insert_reading(store, a, base + timedelta(seconds=3), 569.0)

        store.refresh_hourly_circuit_peak()

        row = store._conn.execute(
            "SELECT peak_watts, peak_watts_p99 FROM hourly_circuit_peak"
        ).fetchone()
        assert row[0] == pytest.approx(569.0)
        assert row[1] == pytest.approx(124.0, abs=2.0)

    def test_p99_null_when_all_sums_zero(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        store.set_device_circuit("d1", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 0.0)
        _insert_reading(store, a, h.replace(second=30), 0.0)
        store.refresh_hourly_circuit_peak()
        row = store._conn.execute(
            "SELECT peak_watts, peak_watts_p99 FROM hourly_circuit_peak"
        ).fetchone()
        assert row[0] == pytest.approx(0.0)
        assert row[1] is None

    def test_separates_circuits(self, store: Store) -> None:
        c1 = store.create_circuit("P1", "B20", "", 20.0)
        c2 = store.create_circuit("P1", "B22", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d2", "c00", "B", has_emeter=True)
        store.set_device_circuit("d1", c1)
        store.set_device_circuit("d2", c2)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 300.0)
        _insert_reading(store, b, h, 700.0)
        store.refresh_hourly_circuit_peak()
        peaks = dict(
            store._conn.execute("SELECT circuit_id, peak_watts FROM hourly_circuit_peak").fetchall()
        )
        assert peaks == {c1: pytest.approx(300.0), c2: pytest.approx(700.0)}

    def test_excludes_unassigned_devices(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        unassigned = store.ensure_plug("d2", "c00", "B", has_emeter=True)
        store.set_device_circuit("d1", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, unassigned, h, 999.0)
        store.refresh_hourly_circuit_peak()
        rows = store._conn.execute("SELECT peak_watts FROM hourly_circuit_peak").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(200.0)

    def test_skips_no_emeter_plugs(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        ep10 = store.ensure_plug("ep", "", "Snack", has_emeter=False)
        store.set_device_circuit("ep", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, ep10, None, None, None, None)])
        store.refresh_hourly_circuit_peak()
        assert store._conn.execute("SELECT * FROM hourly_circuit_peak").fetchall() == []

    def test_idempotent(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        store.set_device_circuit("d1", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _insert_reading(store, a, h, 200.0)
        _insert_reading(store, a, h.replace(second=30), 300.0)
        store.refresh_hourly_circuit_peak()
        first = store._conn.execute(
            "SELECT circuit_id, hour_ts, peak_watts FROM hourly_circuit_peak"
        ).fetchall()
        store.refresh_hourly_circuit_peak()
        second = store._conn.execute(
            "SELECT circuit_id, hour_ts, peak_watts FROM hourly_circuit_peak"
        ).fetchall()
        assert first == second

    def test_rebuild_reflects_membership_change(self, store: Store) -> None:
        c1 = store.create_circuit("P1", "B20", "", 20.0)
        c2 = store.create_circuit("P1", "B22", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        store.set_device_circuit("d1", c1)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h, 300.0)
        store.refresh_hourly_circuit_peak()
        assert store.circuit_peaks(h, h + timedelta(hours=1)) == {c1: pytest.approx(300.0)}

        # Move the strip to c2 and rebuild — history must follow membership.
        store.set_device_circuit("d1", c2)
        store.rebuild_hourly_circuit_peak()
        assert store.circuit_peaks(h, h + timedelta(hours=1)) == {c2: pytest.approx(300.0)}


class TestCircuitPeaks:
    def test_max_per_circuit(self, store: Store) -> None:
        c1 = store.create_circuit("P1", "B20", "", 20.0)
        c2 = store.create_circuit("P1", "B22", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        b = store.ensure_plug("d2", "c00", "B", has_emeter=True)
        store.set_device_circuit("d1", c1)
        store.set_device_circuit("d2", c2)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h12, 300.0)
        _fill_hour(store, a, h13, 200.0)
        _fill_hour(store, b, h12, 700.0)
        store.refresh_hourly_circuit_peak()
        peaks = store.circuit_peaks(h12, h13 + timedelta(hours=1))
        assert peaks == {c1: pytest.approx(300.0), c2: pytest.approx(700.0)}

    def test_omits_circuit_with_only_null_p99(self, store: Store) -> None:
        cid = store.create_circuit("P1", "B20", "", 20.0)
        a = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        store.set_device_circuit("d1", cid)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        _fill_hour(store, a, h, 0.0)
        store.refresh_hourly_circuit_peak()
        assert store.circuit_peaks(h, h + timedelta(hours=1)) == {}


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


class TestKwhByMachineAndLocalDay:
    def test_buckets_hours_into_local_central_days(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "Blackout - M0013", has_emeter=True)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(pid, mid, datetime(2026, 6, 1, 0, 0, 0, tzinfo=UTC))
        # 06-15 04:xx UTC = 06-14 23:xx Chicago (CDT) -> local day 06-14;
        # 06-15 06:xx UTC = 06-15 01:xx Chicago        -> local day 06-15.
        for h in (
            datetime(2026, 6, 15, 4, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 15, 6, 0, 0, tzinfo=UTC),
        ):
            _insert_reading(store, pid, h, 100.0)
            _insert_reading(store, pid, h.replace(minute=1), 100.0)  # +60s so kWh accrues
        store.refresh_hourly_usage()

        rows = store.kwh_by_machine_and_local_day(date(2026, 6, 14), date(2026, 6, 16))
        by_day = {r["day_local"]: r for r in rows}
        assert set(by_day) == {date(2026, 6, 14), date(2026, 6, 15)}
        assert all(r["machine_id"] == mid and r["machine_name"] == "Blackout" for r in rows)
        assert all(r["kwh"] > 0 for r in rows)

    def test_local_day_offset_differs_across_dst(self, store: Store) -> None:
        # UTC->Chicago is -5 (CDT) in summer, -6 (CST) in winter, so the same
        # wall-clock UTC hour maps to a different local day by season — proving the
        # bucketing uses the tz db, not a fixed offset.
        pid = store.ensure_plug("d1", "c1", "X - M1", has_emeter=True)
        mid = store.ensure_machine("M1", "X")
        store.update_assignment(pid, mid, datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC))
        # 05:30 UTC: winter -> 23:30 CST previous day; summer -> 00:30 CDT same day.
        for h in (
            datetime(2026, 1, 15, 5, 30, 0, tzinfo=UTC),  # -> 2026-01-14 local
            datetime(2026, 6, 15, 5, 30, 0, tzinfo=UTC),  # -> 2026-06-15 local
        ):
            _insert_reading(store, pid, h, 100.0)
            _insert_reading(store, pid, h.replace(minute=31), 100.0)
        store.refresh_hourly_usage()

        winter = store.kwh_by_machine_and_local_day(date(2026, 1, 14), date(2026, 1, 15))
        summer = store.kwh_by_machine_and_local_day(date(2026, 6, 15), date(2026, 6, 16))
        assert [r["day_local"] for r in winter] == [date(2026, 1, 14)]
        assert [r["day_local"] for r in summer] == [date(2026, 6, 15)]

    def test_unassigned_bucket_and_window_bounds(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "Spare", has_emeter=True)  # never assigned
        h = datetime(2026, 6, 15, 18, 0, 0, tzinfo=UTC)  # 13:00 Chicago -> 06-15
        _insert_reading(store, pid, h, 100.0)
        _insert_reading(store, pid, h.replace(minute=1), 100.0)
        store.refresh_hourly_usage()

        rows = store.kwh_by_machine_and_local_day(date(2026, 6, 15), date(2026, 6, 16))
        assert len(rows) == 1
        assert rows[0]["machine_id"] is None and rows[0]["machine_name"] == "Unassigned"
        # The half-open window excludes the day.
        assert store.kwh_by_machine_and_local_day(date(2026, 6, 16), date(2026, 6, 17)) == []


class TestUsageForPlugs:
    def test_sums_across_plugs_per_hour(self, store: Store) -> None:
        p1 = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        p2 = store.ensure_plug("d1", "c01", "B", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for pid in (p1, p2):
            _insert_reading(store, pid, h, 100.0)
            _insert_reading(store, pid, h.replace(second=30), 200.0)
        store.refresh_hourly_usage()

        rows = store.usage_for_plugs([p1, p2], h, h.replace(hour=13))
        assert len(rows) == 1
        hour_ts, kwh = rows[0]
        assert hour_ts == h.replace(tzinfo=None)
        # Each plug: 200W × 30s = 6000 Ws; two plugs = 12000 Ws.
        assert kwh == pytest.approx(12000 / 3600 / 1000, rel=1e-6)

    def test_excludes_other_plugs(self, store: Store) -> None:
        mine = store.ensure_plug("d1", "c00", "Mine", has_emeter=True)
        other = store.ensure_plug("d2", "c00", "Other", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for pid in (mine, other):
            _insert_reading(store, pid, h, 100.0)
            _insert_reading(store, pid, h.replace(second=30), 100.0)
        store.refresh_hourly_usage()

        rows = store.usage_for_plugs([mine], h, h.replace(hour=13))
        assert len(rows) == 1
        assert rows[0][1] == pytest.approx(100.0 * 30 / 3600 / 1000, rel=1e-6)

    def test_window_bounds_half_open(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        h12 = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        h13 = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        for h in (h12, h13):
            _insert_reading(store, pid, h.replace(second=10), 100.0)
            _insert_reading(store, pid, h.replace(second=40), 100.0)
        store.refresh_hourly_usage()

        rows = store.usage_for_plugs([pid], h12, h13)
        assert [r[0] for r in rows] == [h12.replace(tzinfo=None)]

    def test_empty_plug_ids_returns_empty(self, store: Store) -> None:
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        assert store.usage_for_plugs([], start, start.replace(hour=13)) == []

    def test_empty_window_returns_empty(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c00", "A", has_emeter=True)
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        assert store.usage_for_plugs([pid], start, start.replace(hour=13)) == []


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


class TestRefreshHourlyPlaySeconds:
    def test_records_play_and_on_seconds(self, store: Store) -> None:
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)  # 15:00 CDT
        _insert_series(store, pid, t0, _attract_watts(60))
        _insert_series(store, pid, t0 + timedelta(seconds=60), _playing_watts(120))

        store.refresh_hourly_play_seconds()

        rows = store._conn.execute(
            "SELECT machine_id, hour_local, play_seconds, on_seconds FROM hourly_play_seconds"
        ).fetchall()
        assert len(rows) == 1
        mid_r, hour_local, play, on = rows[0]
        assert mid_r == mid
        assert hour_local.hour == 15  # 20:00 UTC -> 15:00 CDT
        assert hour_local.date() == date(2026, 5, 25)
        # on-time spans attract + playing (~180s); play is the ~120s PLAYING part.
        assert on > play > 0
        assert play <= on

    def test_ignores_uncalibrated(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c1", "Uncalibrated - M9999", has_emeter=True)
        mid = store.ensure_machine("M9999", "Uncalibrated")
        store.update_assignment(pid, mid, datetime(2026, 5, 24, 0, 0, 0, tzinfo=UTC))
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))

        store.refresh_hourly_play_seconds()

        assert store._conn.execute("SELECT COUNT(*) FROM hourly_play_seconds").fetchone()[0] == 0

    def test_buckets_into_local_hours(self, store: Store) -> None:
        """A PLAYING burst crossing a local-hour boundary lands in two hours."""
        pid, mid = _setup_calibrated(store)
        warm = datetime(2026, 5, 25, 20, 54, 0, tzinfo=UTC)  # 15:54 CDT
        _insert_series(store, pid, warm, _attract_watts(60))
        play_start = warm + timedelta(seconds=60)  # 15:55 CDT
        _insert_series(store, pid, play_start, _playing_watts(10 * 60))  # crosses 16:00 CDT

        store.refresh_hourly_play_seconds()

        hours = {
            r[0].hour
            for r in store._conn.execute(
                "SELECT hour_local FROM hourly_play_seconds WHERE machine_id = ?", [mid]
            ).fetchall()
        }
        assert 15 in hours and 16 in hours

    def test_idempotent(self, store: Store) -> None:
        pid, _mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))
        store.refresh_hourly_play_seconds()
        first = store._conn.execute(
            "SELECT machine_id, hour_local, play_seconds, on_seconds FROM hourly_play_seconds"
        ).fetchall()
        store.refresh_hourly_play_seconds()
        second = store._conn.execute(
            "SELECT machine_id, hour_local, play_seconds, on_seconds FROM hourly_play_seconds"
        ).fetchall()
        assert first == second

    def test_incremental_keeps_full_day_not_just_evening(self, store: Store) -> None:
        """Regression for the daily rollup's UTC-window bug, which truncated each
        aged-out day to only its post-19:00-local (UTC-midnight) play. The hourly
        rollup's window aligns to local-hour boundaries, so morning play on a day
        that later falls outside the incremental window must survive."""
        pid, _mid = _setup_calibrated(store)
        # Morning play on 5/25: 10:00 CDT = 15:00 UTC — the part the old bug dropped.
        morning = datetime(2026, 5, 25, 15, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, morning, _attract_watts(60) + _playing_watts(600))
        store.refresh_hourly_play_seconds()
        before = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 26))
        assert before and before[0]["hours"] > 0
        base = before[0]["hours"]

        # Time moves on: readings 3 days later, so 5/25 is outside the 49h window.
        later = datetime(2026, 5, 28, 15, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, later, _attract_watts(60) + _playing_watts(120))
        store.refresh_hourly_play_seconds()  # incremental — must not truncate 5/25

        after = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 26))
        assert after and after[0]["hours"] == pytest.approx(base, abs=0.05)


class TestRebuildPlayHours:
    """Retroactive recompute: recalibration must re-classify ALL of a machine's
    history, not just a trailing window (the bug behind IJ's inflated Jul 6-12)."""

    def test_recalibration_is_retroactive(self, store: Store) -> None:
        # A lenient calibration classifies the wobbly ATTRACT stretch as PLAYING.
        pid, mid = _setup_calibrated(store, cal=Calibration(idle_max_rsd=None, play_min_rsd=0.01))
        t0 = datetime(2026, 7, 6, 20, 0, 0, tzinfo=UTC)  # 15:00 CDT, well outside any 49h window
        _insert_series(store, pid, t0, _attract_watts(600))
        store.refresh_hourly_play_seconds()

        win = (date(2026, 7, 6), date(2026, 7, 7))
        before = store.play_hours_by_machine(*win)
        assert before and before[0]["hours"] > 0.1  # inflated: ATTRACT counted as PLAYING

        # Recalibrate stricter so the same readings are ATTRACT, not PLAYING.
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=50.0))
        changed = store.rebuild_play_hours(mid)
        assert changed >= 0

        after = store.play_hours_by_machine(*win)
        after_hours = after[0]["hours"] if after else 0.0
        assert after_hours == pytest.approx(0.0, abs=0.01)  # history now reflects new calibration

    def test_rebuild_preserves_on_time(self, store: Store) -> None:
        """Rebuild recomputes play/on the same way the incremental refresh does,
        so a full rebuild over the same readings matches the incremental result."""
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60))
        _insert_series(store, pid, t0 + timedelta(seconds=60), _playing_watts(120))
        store.refresh_hourly_play_seconds()
        incremental = store._conn.execute(
            "SELECT machine_id, hour_local, play_seconds, on_seconds "
            "FROM hourly_play_seconds ORDER BY hour_local"
        ).fetchall()

        store.rebuild_play_hours(mid)
        rebuilt = store._conn.execute(
            "SELECT machine_id, hour_local, play_seconds, on_seconds "
            "FROM hourly_play_seconds ORDER BY hour_local"
        ).fetchall()
        assert rebuilt == incremental

    def test_no_calibration_or_assignment_is_noop(self, store: Store) -> None:
        mid = store.ensure_machine("M9", "Unassigned")  # no plug, no calibration
        assert store.rebuild_play_hours(mid) == 0

    def test_rebuild_spans_assignment_intervals(self, store: Store) -> None:
        """A machine moved between outlets keeps each plug's history for the time
        it was assigned there, and never picks up readings from before/after its
        tenure (which belong to another machine on that plug)."""
        cal = Calibration(idle_max_rsd=None, play_min_rsd=10.0)
        a = store.ensure_plug("dA", "c0", "A - M0013", has_emeter=True)
        b = store.ensure_plug("dB", "c0", "B - M0013", has_emeter=True)
        other = store.ensure_machine("M9", "Other")
        mid = store.ensure_machine("M0013", "Blackout")
        store.set_calibration(mid, cal)

        t0 = datetime(2026, 6, 1, 20, 0, 0, tzinfo=UTC)  # M joins plug A (15:00 CDT 6/1)
        t1 = datetime(2026, 6, 3, 20, 0, 0, tzinfo=UTC)  # M moves A -> B (15:00 CDT 6/3)

        # Plug B belonged to another machine before t1 — those readings aren't M's.
        store.update_assignment(b, other, datetime(2026, 5, 1, 20, 0, 0, tzinfo=UTC))
        _insert_series(store, b, datetime(2026, 5, 20, 20, 0, 0, tzinfo=UTC), _playing_watts(120))

        # M on plug A for [t0, t1).
        store.update_assignment(a, mid, t0)
        _insert_series(store, a, t0, _playing_watts(120))

        # Move M from A to B at t1; plug A keeps recording afterwards (on a later
        # day) but is no longer M's, so those readings must not count for M.
        store.update_assignment(a, None, t1)
        store.update_assignment(b, mid, t1)
        _insert_series(store, a, datetime(2026, 6, 5, 20, 0, 0, tzinfo=UTC), _playing_watts(120))
        _insert_series(store, b, t1, _playing_watts(120))

        store.rebuild_play_hours(mid)

        days = {
            d["day_local"]: d["hours"]
            for d in store.play_hours_by_machine(date(2026, 5, 1), date(2026, 7, 1))
            if d["machine_id"] == mid
        }
        assert days.get(date(2026, 6, 1), 0.0) > 0  # plug A interval counted
        assert days.get(date(2026, 6, 3), 0.0) > 0  # plug B interval counted
        assert date(2026, 5, 20) not in days  # pre-tenure readings on plug B excluded
        assert date(2026, 6, 5) not in days  # post-tenure readings on plug A excluded

    def test_calibrated_assigned_machine_ids(self, store: Store) -> None:
        _pid, mid = _setup_calibrated(store)
        store.ensure_machine("M9", "Uncalibrated")  # no calibration -> excluded
        assert store.calibrated_assigned_machine_ids() == [mid]


class TestAppliedMigrations:
    def test_marker_roundtrip(self, store: Store) -> None:
        assert store.has_migration("m1") is False
        store.mark_migration("m1")
        assert store.has_migration("m1") is True
        store.mark_migration("m1")  # idempotent, no error
        assert store.has_migration("m1") is True
        assert store.has_migration("other") is False


class TestPlayUtilizationGrid:
    def _cell(self, store: Store, mid: int, ts: datetime, play: float, on: float) -> None:
        store._conn.execute(
            "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)", [mid, ts, play, on]
        )

    def test_ratio_and_aggregation(self, store: Store) -> None:
        m1 = store.ensure_machine("M1", "A")
        m2 = store.ensure_machine("M2", "B")
        # 15:00 on 5/25: two machines, combined on=12h (clears the 10h gate).
        self._cell(store, m1, datetime(2026, 5, 25, 15, 0, 0), 3 * 3600.0, 6 * 3600.0)
        self._cell(store, m2, datetime(2026, 5, 25, 15, 0, 0), 1 * 3600.0, 6 * 3600.0)
        cells = store.play_utilization_grid(
            datetime(2026, 5, 25, 0, 0, 0), datetime(2026, 5, 26, 0, 0, 0)
        )
        assert len(cells) == 1
        c = cells[0]
        assert c["date_local"] == date(2026, 5, 25)
        assert c["hour"] == 15
        assert c["play_hours"] == pytest.approx(4.0)
        assert c["on_hours"] == pytest.approx(12.0)
        assert c["ratio"] == pytest.approx(4.0 / 12.0)

    def test_below_min_on_time_filtered(self, store: Store) -> None:
        mid = store.ensure_machine("M1", "A")
        # 5h on-time < 10h gate -> excluded; bump above and it appears.
        self._cell(store, mid, datetime(2026, 5, 25, 15, 0, 0), 2 * 3600.0, 5 * 3600.0)
        win = (datetime(2026, 5, 25, 0, 0, 0), datetime(2026, 5, 26, 0, 0, 0))
        assert store.play_utilization_grid(*win) == []
        # An explicit lower floor includes it.
        assert len(store.play_utilization_grid(*win, min_on_seconds=3600.0)) == 1

    def test_zero_play_open_hour_kept(self, store: Store) -> None:
        # Open (>=10h on) but nobody playing -> a real 0% cell, still shown.
        mid = store.ensure_machine("M1", "A")
        self._cell(store, mid, datetime(2026, 5, 25, 15, 0, 0), 0.0, 12 * 3600.0)
        cells = store.play_utilization_grid(
            datetime(2026, 5, 25, 0, 0, 0), datetime(2026, 5, 26, 0, 0, 0)
        )
        assert len(cells) == 1 and cells[0]["ratio"] == 0.0

    def test_empty_window(self, store: Store) -> None:
        assert (
            store.play_utilization_grid(
                datetime(2026, 1, 1, 0, 0, 0), datetime(2026, 1, 2, 0, 0, 0)
            )
            == []
        )


class TestPlayHoursByMachine:
    def test_basic_query(self, store: Store) -> None:
        # Derives from the hourly rollup now (single source of truth).
        pid, mid = _setup_calibrated(store)
        t0 = datetime(2026, 5, 25, 20, 0, 0, tzinfo=UTC)
        _insert_series(store, pid, t0, _attract_watts(60) + _playing_watts(120))
        store.refresh_hourly_play_seconds()

        rows = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 27))
        assert len(rows) == 1
        assert rows[0]["machine_id"] == mid
        assert rows[0]["machine_name"] == "Blackout"
        assert rows[0]["day_local"] == date(2026, 5, 25)
        assert rows[0]["hours"] > 0

    def test_sums_hours_across_the_day(self, store: Store) -> None:
        # Two play bursts in different hours of the same local day sum together
        # — and a from-scratch hourly rollup attributes the WHOLE day, not just
        # its evening (the regression the daily rollup's UTC-window bug caused).
        mid = store.ensure_machine("M1", "A")
        for hour, play_h, on_h in [(14, 1.0, 2.0), (15, 0.5, 2.0)]:
            store._conn.execute(
                "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)",
                [mid, datetime(2026, 5, 25, hour, 0, 0), play_h * 3600, on_h * 3600],
            )
        rows = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 26))
        assert len(rows) == 1
        assert rows[0]["hours"] == pytest.approx(1.5)

    def test_empty_window(self, store: Store) -> None:
        rows = store.play_hours_by_machine(date(2026, 5, 25), date(2026, 5, 27))
        assert rows == []


class TestAirSensors:
    def test_ensure_and_list(self, store: Store) -> None:
        seen = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        store.ensure_air_sensor("MAC1", "Main Floor", online=True, seen_ts=seen)
        store.ensure_air_sensor("MAC2", "Back Room", online=False, seen_ts=seen)
        sensors = {s["mac"]: s for s in store.list_air_sensors()}
        assert sensors["MAC1"]["name"] == "Main Floor"
        assert sensors["MAC1"]["online"] is True
        assert sensors["MAC2"]["online"] is False

    def test_ensure_upserts_name_and_status(self, store: Store) -> None:
        t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 6, 20, 12, 5, 0, tzinfo=UTC)
        store.ensure_air_sensor("MAC1", "Old Name", online=True, seen_ts=t0)
        store.ensure_air_sensor("MAC1", "New Name", online=False, seen_ts=t1)
        sensors = store.list_air_sensors()
        assert len(sensors) == 1
        assert sensors[0]["name"] == "New Name"
        assert sensors[0]["online"] is False
        # DB stores naive UTC (session tz pinned to UTC), like all juice timestamps.
        assert sensors[0]["first_seen"] == t0.replace(tzinfo=None)  # unchanged
        assert sensors[0]["last_seen"] == t1.replace(tzinfo=None)


class TestAirReadings:
    def _row(self, ts: datetime, mac: str = "MAC1", co2: float = 600.0) -> tuple:
        # (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
        return (ts, mac, 22.5, 45.0, co2, 8.0, 12.0, 130.0, None, 88.0)

    def test_insert_and_latest(self, store: Store) -> None:
        t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 6, 20, 12, 15, 0, tzinfo=UTC)
        store.insert_air_readings([self._row(t0, co2=600), self._row(t1, co2=700)])
        latest = store.air_latest()
        assert latest["MAC1"]["co2"] == 700.0  # most recent ts wins
        assert latest["MAC1"]["temperature"] == 22.5
        assert latest["MAC1"]["noise"] is None

    def test_insert_is_idempotent_on_ts_mac(self, store: Store) -> None:
        # Polling faster than the device's report cadence yields repeated
        # snapshots with the same timestamp — these must not duplicate rows.
        t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        store.insert_air_readings([self._row(t0)])
        store.insert_air_readings([self._row(t0)])
        count = store._conn.execute("SELECT COUNT(*) FROM air_readings").fetchone()[0]
        assert count == 1

    def test_history_window(self, store: Store) -> None:
        base = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        rows_in = [self._row(base + timedelta(minutes=15 * i), co2=600 + i) for i in range(4)]
        store.insert_air_readings(rows_in)
        hist = store.air_history("MAC1", base + timedelta(minutes=15), base + timedelta(minutes=46))
        assert [r["co2"] for r in hist] == [601.0, 602.0, 603.0]  # half-open window
        assert hist == sorted(hist, key=lambda r: r["ts"])
