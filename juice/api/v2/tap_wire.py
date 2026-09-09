"""juice's copy of the tap wire protocol.

**This is a deliberate duplicate of `tap/wire.py`, not an import.** That module
says why (`tap/wire.py:3-6`): neither codebase should be able to reach into the
other to "just check one field", because a shared module turns the wire contract
into a fiction — the two processes stop having to agree on anything and start
sharing an implementation. `tests/test_ingest_isolation.py` enforces both halves
of that rule: juice imports no `tap`, and the two copies must still agree.

Only the server half is spelled out here. juice never *sends* `readings`, so
there is no row encoder; and juice ignores `live`, `devices`, `command_result`
and `pong` for now, so there is no decoder for those either — the receiver drops
unknown and unhandled frames, which `tap/wire.py:97-99` explicitly permits.

Row decoding is not here either, and that is the surprising part. Rows never
become Python objects at all: the raw frame goes to DuckDB, which parses,
validates, converts units and resolves plug identity in one pass (see
`Store.stage_ingest_batch`). What this module keeps is the *layout* — the field
order the SQL indexes into, which is the one thing that must not drift.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

PROTOCOL_VERSION = 1

# tap -> server
HELLO = "hello"
READINGS = "readings"
LIVE = "live"
DEVICES = "devices"
COMMAND_RESULT = "command_result"
PONG = "pong"

# server -> tap
WELCOME = "welcome"
ACK = "ack"
NACK = "nack"
COMMAND = "command"
PING = "ping"

# `transient` means "try that batch again"; `bad_batch` means "this one will
# never work". The second is a data-loss verdict — tap logs an error and steps
# over those rows permanently — so it is only ever for facts provable from the
# bytes themselves, never for anything that depends on server state.
NACK_TRANSIENT = "transient"
NACK_BAD_BATCH = "bad_batch"

# Row layout. Changing this is a protocol break, and an *undetectable* one:
# version negotiation catches a bumped PROTOCOL_VERSION, but a reordering here
# at the same version means both sides agree they speak protocol 1 while every
# reading lands in the wrong column.
ROW_FIELDS = (
    "ts_ms",
    "device_id",
    "child_id",
    "relay_on",
    "power_mw",
    "voltage_mv",
    "current_ma",
    "energy_wh",
)

# 1-based positions, because the SQL indexes DuckDB lists and those are 1-based.
# Derived rather than written out so the SQL cannot drift from ROW_FIELDS.
IDX = {name: i + 1 for i, name in enumerate(ROW_FIELDS)}

DEFAULT_MAX_BATCH_ROWS = 5000
DEFAULT_WINDOW = 4
DEFAULT_LIVE_MAX_LAG_S = 300.0

# What this server actually advertises.
#
# `max_batch_rows` stays at tap's default: the per-statement cost of an ingest
# batch is largely fixed, so smaller batches cost *more* per row, not less
# (1000 rows measured at 7k rows/s against 5000 rows at 92k rows/s).
#
# `live_max_lag_s` stays at tap's default even though juice discards `live`
# frames today. Advertising 0 looks tidier -- "don't bother building them" --
# but it puts the suppression threshold exactly where a healthy collector's lag
# already sits, so tap logs a "live frames suppressed"/"resumed" pair every time
# lag crosses zero. A one-hour replay produced 30 such pairs: 59 of 78 log
# lines. Building and dropping a few KB/s of frames is the cheaper mistake than
# a production log that flaps.
SERVER_MAX_BATCH_ROWS = DEFAULT_MAX_BATCH_ROWS
SERVER_WINDOW = DEFAULT_WINDOW
SERVER_LIVE_MAX_LAG_S = DEFAULT_LIVE_MAX_LAG_S

# Clock guards. Mirrors `tap/buffer.py:56-61`, duplicated for the same reason as
# everything else in this file.
#
# The floor is identical to tap's. The ceiling deliberately is NOT: tap rejects
# anything more than 5 minutes past *its* clock, and this is a different clock.
# A tap a few minutes fast is a misconfiguration, not corruption, and dropping
# its readings would be a silent, permanent data loss over a solvable problem.
TS_FLOOR = datetime(2025, 1, 1, tzinfo=UTC)
TS_FLOOR_MS = int(TS_FLOOR.timestamp() * 1000)
TS_CEILING_SLACK_MS = 3600 * 1000


class BadFrameError(Exception):
    """The frame is malformed in a way no retry can fix."""


def welcome(resume_from: str | None, server_epoch: str) -> dict:
    """The answer to `hello`.

    `resume_from` is **exclusive** — tap sends rows strictly after it — and null
    means "everything you have". The server is the authority here, including
    when its cursor is *older* than tap's: that is how a juice restored from
    backup asks for its missing rows back (`tap/wire.py:70-74`).
    """
    return {
        "type": WELCOME,
        "protocol": PROTOCOL_VERSION,
        "server_epoch": server_epoch,
        "resume_from": resume_from,
        "max_batch_rows": SERVER_MAX_BATCH_ROWS,
        "window": SERVER_WINDOW,
        "live_max_lag_s": SERVER_LIVE_MAX_LAG_S,
    }


def ack(batch: str, cursor: str) -> dict:
    """A durability claim. Only ever sent after the rows are committed.

    `batch` must be echoed byte for byte: tap matches acks on `batch` alone and
    ignores the cursor we send back (`tap/uplink.py:419-425`), so a value that
    is merely equivalent — reformatted, re-cased, coerced from a non-string — is
    silently discarded and the stream stalls at the window limit for 120 s.
    """
    return {"type": ACK, "batch": batch, "cursor": cursor}


def nack(batch: str, code: str, message: str | None = None) -> dict:
    return {"type": NACK, "batch": batch, "code": code, "message": message}


def hello_identity(frame: dict) -> tuple[str, str]:
    """The `(tap_id, buffer_id)` a cursor is scoped to.

    `buffer_id` legitimately absent or empty: `tap.wire.hello` defaults it to
    `""`, and an old tap may not send it at all. `""` is a usable key — what
    matters is that a *different* value means a different sequence space.
    """
    if frame.get("type") != HELLO:
        raise BadFrameError(f"expected a {HELLO} frame, got {frame.get('type')!r}")
    tap_id = frame.get("tap_id")
    if not isinstance(tap_id, str) or not tap_id:
        raise BadFrameError(f"hello needs a non-empty string tap_id, got {tap_id!r}")
    buffer_id = frame.get("buffer_id") or ""
    if not isinstance(buffer_id, str):
        raise BadFrameError(f"buffer_id must be a string, got {buffer_id!r}")
    return tap_id, buffer_id


def hello_protocol(frame: dict) -> int:
    """tap's protocol version. Absent means 1 — the version that predates the
    field being mandatory."""
    protocol = frame.get("protocol", PROTOCOL_VERSION)
    if not isinstance(protocol, int) or isinstance(protocol, bool):
        raise BadFrameError(f"protocol must be an integer, got {protocol!r}")
    return protocol


def batch_id(frame: dict) -> str:
    """The batch id from a `readings` frame.

    Raises rather than returning None because a nack has to *name* a batch: a
    frame we cannot name is one we cannot refuse, so the caller has to drop it
    and let tap's ack timeout resend.
    """
    value = frame.get("batch")
    if not isinstance(value, str) or not value:
        raise BadFrameError(f"readings needs a non-empty string batch, got {value!r}")
    return value


def cursor_of(frame: dict) -> str:
    value = frame.get("cursor")
    if not isinstance(value, str) or not value:
        raise BadFrameError(f"readings needs a non-empty string cursor, got {value!r}")
    return value


def rows_of(frame: dict) -> Any:
    """The raw `rows` value, checked only for being a list.

    Deliberately not walked element by element — that is DuckDB's job, and doing
    it here would rebuild in Python exactly the per-row loop the SQL exists to
    avoid.
    """
    value = frame.get("rows")
    if not isinstance(value, list):
        raise BadFrameError(f"readings needs a list of rows, got {type(value).__name__}")
    return value
