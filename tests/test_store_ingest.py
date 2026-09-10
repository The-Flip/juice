"""The store side of tap ingest: schema, plug resolution, and batch commits.

What matters here is not that rows land. It is that a duplicate is impossible
rather than merely filtered, that an alias can never be blanked by a collector
that does not know aliases exist, and that a null meter reading stays null.
"""

from __future__ import annotations

import duckdb
import pytest

from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


class TestRelayOnColumn:
    """tap reports the relay directly. juice has only ever inferred it from
    watts, and `status_vocabulary.md` is explicit that those are different
    facts, so the column is the one genuinely new thing ingest records."""

    def test_the_column_exists(self, store: Store) -> None:
        cols = {r[1] for r in store._conn.execute("PRAGMA table_info('readings')").fetchall()}
        assert "relay_on" in cols

    def test_a_cloud_recorder_row_leaves_it_null(self, store: Store) -> None:
        """The recorder still writes 6-tuples. Null means "nobody told us",
        which is the truth for every row juice recorded before tap existed --
        and is why the column is nullable rather than defaulting to false."""
        plug_id = store.ensure_plug("DEV", "DEV00", "Some Machine - M0001")
        from datetime import UTC, datetime

        store.insert_readings([(datetime.now(UTC), plug_id, 1.0, 119.0, 0.1, 0.5)])
        assert store._conn.execute("SELECT relay_on FROM readings").fetchone()[0] is None

    def test_it_is_added_to_a_database_that_predates_it(self, tmp_path) -> None:
        """The migration path. Build a `readings` table with the old six
        columns, then let Store open it."""
        path = str(tmp_path / "old.duckdb")
        con = duckdb.connect(path)
        con.execute(
            """CREATE TABLE readings (ts TIMESTAMP NOT NULL, plug_id SMALLINT NOT NULL,
               watts FLOAT, voltage FLOAT, amps FLOAT, total_kwh FLOAT)"""
        )
        con.execute(
            "INSERT INTO readings VALUES (TIMESTAMP '2026-01-01 00:00:00', 1, 5.0, 119.0, 0.04, 1.5)"
        )
        con.close()

        with Store(path) as s:
            cols = {r[1] for r in s._conn.execute("PRAGMA table_info('readings')").fetchall()}
            assert "relay_on" in cols
            row = s._conn.execute("SELECT watts, voltage, relay_on FROM readings").fetchone()
            assert row == (5.0, 119.0, None), "the pre-existing row must survive untouched"

    def test_the_migration_is_idempotent(self, tmp_path) -> None:
        path = str(tmp_path / "twice.duckdb")
        with Store(path) as s:
            plug_id = s.ensure_plug("DEV", "DEV00", "alias")
            from datetime import UTC, datetime

            s.insert_readings([(datetime.now(UTC), plug_id, 1.0, 119.0, 0.1, 0.5)])
        with Store(path) as s:  # reopening runs _migrate again
            assert s._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1


class TestIngestCursors:
    """The cursor is the whole dedup story, so it needs to survive a restart
    and it must never move backwards."""

    def test_an_unseen_tap_has_no_cursor(self, store: Store) -> None:
        assert store.ingest_cursor("tap-1", "buf-1") is None

    def test_a_cursor_round_trips(self, store: Store) -> None:
        store.set_ingest_cursor("tap-1", "buf-1", "0" * 17 + "5")
        assert store.ingest_cursor("tap-1", "buf-1") == "0" * 17 + "5"

    def test_cursors_are_scoped_to_the_buffer(self, store: Store) -> None:
        """A new `buffer_id` means tap's storage was replaced and its sequence
        restarted, so the old cursor is meaningless (`tap/wire.py:57-62`)."""
        store.set_ingest_cursor("tap-1", "buf-1", "0" * 17 + "9")
        assert store.ingest_cursor("tap-1", "buf-2") is None

    def test_cursors_are_scoped_to_the_tap(self, store: Store) -> None:
        store.set_ingest_cursor("tap-1", "buf-1", "0" * 17 + "9")
        assert store.ingest_cursor("tap-2", "buf-1") is None

    def test_a_cursor_never_moves_backwards(self, store: Store) -> None:
        """Two live sockets for one tap -- a reconnect where the old TCP
        connection has not been reaped -- can deliver an older cursor after a
        newer one. Accepting it would hand the next `hello` a stale resume
        point and re-deliver everything in between."""
        store.set_ingest_cursor("tap-1", "buf-1", "0" * 16 + "20")
        store.set_ingest_cursor("tap-1", "buf-1", "0" * 17 + "5")
        assert store.ingest_cursor("tap-1", "buf-1") == "0" * 16 + "20"


def frame(rows, batch="b1", cursor=None) -> str:
    """A `readings` frame as it arrives off the socket -- raw text.

    Raw text on purpose: the rows never become Python objects. DuckDB parses,
    validates, converts units and resolves plug identity in one pass, which is
    both ~2x faster than doing any of it here and the reason `device_id` never
    reaches anything but a JSON parser.
    """
    import json

    return json.dumps(
        {"type": "readings", "batch": batch, "cursor": cursor or ("0" * 17 + "1"), "rows": rows}
    )


TS = 1788000000000  # 2026-08-29T10:40:00Z, comfortably inside the clock guard


def row(ts_ms=TS, device="DEV", child="DEV00", relay=1, mw=42000, mv=119000, ma=350, wh=12345):
    return [ts_ms, device, child, relay, mw, mv, ma, wh]


class TestCommitBatch:
    def test_a_good_batch_stores_its_rows_and_its_cursor(self, store: Store) -> None:
        r = store.commit_ingest_batch(
            "tap-1", "buf-1", "0" * 17 + "3", frame([row(), row(TS + 1000)])
        )
        assert (r.verdict, r.stored) == ("ok", 2)
        assert store.ingest_cursor("tap-1", "buf-1") == "0" * 17 + "3"

    def test_milli_units_become_units(self, store: Store) -> None:
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        got = store._conn.execute(
            "SELECT watts, voltage, amps, total_kwh, relay_on FROM readings"
        ).fetchone()
        assert got == pytest.approx((42.0, 119.0, 0.35, 12.345, True), rel=1e-6)

    def test_the_timestamp_survives_to_the_millisecond(self, store: Store) -> None:
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(ts_ms=TS + 123)]))
        ts = store._conn.execute("SELECT ts FROM readings").fetchone()[0]
        assert ts.microsecond == 123_000

    def test_null_means_unmeasured_not_zero(self, store: Store) -> None:
        """`tap/wire.py:78-80`. An outlet with no meter, or one whose read
        failed while the rest of the sweep succeeded, reports null -- and a
        null silently written as 0.0 is a machine that looks switched off."""
        store.commit_ingest_batch(
            "tap-1", "buf-1", "0" * 17 + "1", frame([row(mw=None, mv=None, ma=None, wh=None)])
        )
        got = store._conn.execute(
            "SELECT watts, voltage, amps, total_kwh, relay_on FROM readings"
        ).fetchone()
        assert got == (None, None, None, None, True)

    def test_relay_off_is_recorded_as_false_not_as_zero_watts(self, store: Store) -> None:
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(relay=0, mw=None)]))
        assert store._conn.execute("SELECT relay_on, watts FROM readings").fetchone() == (
            False,
            None,
        )

    def test_a_json_boolean_relay_is_tolerated(self, store: Store) -> None:
        """`tap/wire.py:78` says relay_on is 0/1 and never a JSON boolean, and
        tap honours that. Refusing `true` anyway would mean answering a
        cosmetic encoding difference with `bad_batch`, which discards real
        readings permanently. Accept both; be strict where it matters."""
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(relay=True)]))
        assert r.verdict == "ok"
        assert store._conn.execute("SELECT relay_on FROM readings").fetchone()[0] is True


class TestPoisonBatches:
    """`bad_batch` tells tap to skip those rows forever, so it is only ever for
    damage provable from the bytes. Nothing may be stored, and the cursor must
    not move -- otherwise the skip would take good rows with it."""

    def test_a_wrong_arity_row_poisons_the_batch(self, store: Store) -> None:
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(), [1, 2, 3]]))
        assert (r.verdict, r.bad) == ("bad_batch", 1)
        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0
        assert store.ingest_cursor("tap-1", "buf-1") is None

    def test_an_uncastable_relay_poisons_the_batch(self, store: Store) -> None:
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(relay="on")]))
        assert r.verdict == "bad_batch"
        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0

    def test_an_uncastable_meter_value_poisons_the_batch(self, store: Store) -> None:
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(mw="lots")]))
        assert r.verdict == "bad_batch"

    def test_a_non_numeric_timestamp_poisons_the_batch(self, store: Store) -> None:
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(ts_ms="now")]))
        assert r.verdict == "bad_batch"


class TestClockGuard:
    """A timestamp outside all reason is dropped per row, not per batch. Rows
    are ordered by cursor and not by time (`tap/wire.py:64-65`), so one device
    with a broken clock is spread across every batch -- poisoning batches for
    it would stall the entire stream rather than lose one device."""

    def test_a_1970_row_is_dropped_and_the_rest_are_kept(self, store: Store) -> None:
        r = store.commit_ingest_batch(
            "tap-1", "buf-1", "0" * 17 + "2", frame([row(), row(ts_ms=1000)])
        )
        assert (r.verdict, r.stored, r.dropped_ts) == ("ok", 1, 1)
        assert store.ingest_cursor("tap-1", "buf-1") == "0" * 17 + "2"

    def test_a_far_future_row_is_dropped(self, store: Store) -> None:
        far = int(TS + 86_400_000 * 365 * 50)
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "2", frame([row(ts_ms=far)]))
        assert (r.verdict, r.stored, r.dropped_ts) == ("ok", 0, 1)

    def test_a_modestly_fast_tap_is_not_punished(self, store: Store) -> None:
        """tap refuses anything more than 5 minutes past *its* clock. This is a
        different clock, and a tap a few minutes fast is a misconfiguration --
        dropping its readings would be permanent loss over a solvable problem."""
        from datetime import UTC, datetime

        soon = int(datetime.now(UTC).timestamp() * 1000) + 10 * 60 * 1000
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row(ts_ms=soon)]))
        assert (r.verdict, r.stored) == ("ok", 1)


class TestPlugResolution:
    def test_an_unseen_outlet_gets_a_plug_with_no_alias(self, store: Store) -> None:
        store.commit_ingest_batch(
            "tap-1", "buf-1", "0" * 17 + "1", frame([row(device="NEW", child="NEW00")])
        )
        assert store._conn.execute(
            "SELECT alias FROM plugs WHERE device_id = 'NEW'"
        ).fetchone() == ("",)

    def test_ingest_never_overwrites_an_existing_alias(self, store: Store) -> None:
        """The one that would have unassigned the museum. Machine assignment is
        driven entirely by the Kasa alias -- `refresh_metadata` pulls `M\\d+`
        out of it -- and tap does not know aliases exist. Resolving an outlet
        with `ensure_plug(..., "")` would blank every one of them."""
        plug_id = store.ensure_plug("DEV", "DEV00", "The Addams Family - M0017")
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        assert store._conn.execute(
            "SELECT alias FROM plugs WHERE plug_id = ?", [plug_id]
        ).fetchone() == ("The Addams Family - M0017",)
        assert store._plug_cache[("DEV", "DEV00")] == (plug_id, "The Addams Family - M0017")

    def test_rows_land_on_the_plug_the_recorder_already_knew(self, store: Store) -> None:
        """Same outlet, same plug_id -- otherwise cloud-era and tap-era history
        for one machine would fork into two plugs."""
        plug_id = store.ensure_plug("DEV", "DEV00", "Some Machine - M0001")
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        assert store._conn.execute("SELECT plug_id FROM readings").fetchone() == (plug_id,)

    def test_a_new_outlet_is_created_once_not_once_per_batch(self, store: Store) -> None:
        for i in range(3):
            store.commit_ingest_batch(
                "tap-1", "buf-1", "0" * 17 + str(i + 1), frame([row(device="NEW", child="NEW00")])
            )
        assert store._conn.execute(
            "SELECT count(*) FROM plugs WHERE device_id = 'NEW'"
        ).fetchone() == (1,)


class TestDuplicates:
    def test_a_replayed_batch_is_not_written_twice(self, store: Store) -> None:
        """tap resends on the *same* socket when an ack goes missing
        (`tap/uplink.py:61`, BATCH_ACK_TIMEOUT). The stored cursor is what
        makes that harmless."""
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        r = store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        assert r.verdict == "duplicate"
        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1

    def test_a_batch_from_a_replaced_buffer_is_not_treated_as_a_duplicate(
        self, store: Store
    ) -> None:
        """A new buffer_id restarts the sequence at zero, so a low cursor there
        is new data, not a replay."""
        store.commit_ingest_batch("tap-1", "buf-1", "0" * 16 + "50", frame([row()]))
        r = store.commit_ingest_batch(
            "tap-1", "buf-2", "0" * 17 + "1", frame([row(ts_ms=TS + 5000)])
        )
        assert (r.verdict, r.stored) == ("ok", 1)

    def test_rows_and_cursor_are_one_transaction(self, store: Store) -> None:
        """If these could disagree, every crash between them would be either a
        gap or a duplicate. Force the row insert to fail and assert neither
        moved."""
        store._conn.execute("DROP TABLE readings")
        with pytest.raises(duckdb.Error):
            store.commit_ingest_batch("tap-1", "buf-1", "0" * 17 + "1", frame([row()]))
        assert store.ingest_cursor("tap-1", "buf-1") is None
