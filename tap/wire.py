"""The message envelope between tap and its server.

This is the entire coupling surface between the two, and it is deliberately
duplicated on the server side rather than imported: neither codebase should be
able to reach into the other to "just check one field". If the two copies drift,
the version negotiation in `hello`/`welcome` is what catches it.

**Two upward message types, and the split is load-bearing.**

`readings` is the durable channel: cursor-ordered, acked, at-least-once. It is
the record of truth and it is allowed to be minutes or days behind.

`live` is a best-effort snapshot of what each outlet is doing *now*: unacked,
freely dropped, never replayed. It exists so that a collector three days behind
on backfill still shows the floor's current state, and — more importantly — so
that backfill can never drive a server's live state. Replaying three days of
history into a system with overload detection and a live event stream would fire
shutdowns for events that ended on Tuesday.

Reading rows are positional arrays. At five thousand rows a batch, the keys cost
more than the data.

Frames, tap -> server
---------------------
``hello``      ``{tap_id, version, protocol, buffer_oldest, buffer_newest}``.
               The two buffer fields are cursors, or null when the buffer is
               empty. Sent once, immediately, before anything else.
``readings``   ``{batch, cursor, rows[]}``. ``cursor`` is the cursor of the last
               row in ``rows``. Must be answered with an ``ack`` or a ``nack``
               naming the same ``batch``.
``live``       ``{rows[]}``. Unacked, droppable, never replayed.
``devices``    ``{devices: [{device_id, child_id, alias}]}``. The alias roster.
               Sent once per connection, right after ``welcome``.
``command_result`` ``{command_id, status, error}``; ``status`` is ``"ok"`` or
               ``"error"``, ``error`` is null when ok.
``pong``       ``{token}``, echoing a server ``ping``.

Frames, server -> tap
---------------------
``welcome``    ``{protocol?, server_epoch?, resume_from?, max_batch_rows?,
               window?, live_max_lag_s?}``. Every field but ``protocol`` is
               optional; omitted ones take the defaults below.
``ack``        ``{batch, cursor}``. **A durability claim**: send it only once the
               rows are stored. ``batch`` must match one tap sent, or it is
               ignored and the stream stalls at the window limit.
``nack``       ``{batch, code, message?}``. ``code`` is ``"transient"`` (tap
               resends that batch and everything after it) or ``"bad_batch"``
               (tap skips it permanently and logs an error).
``command``    ``{command_id, kind, device_id, child_id, expires_at?}``.
               ``kind`` is ``"turn_on"`` or ``"turn_off"``. ``child_id`` is
               ``""`` for a single-outlet device.
``ping``       ``{token}``; tap replies with ``pong`` carrying the same token.

Rules a server implementer needs and cannot infer
-------------------------------------------------
**Cursors are opaque.** Store and return the exact string. They are zero-padded
decimal today, but ordering is the only property promised.

**Rows are ordered by cursor, not by timestamp.** Two devices sweeping in the
same second land in commit order. Do not assume ``ts_ms`` is monotonic.

**Delivery is at-least-once.** A reconnect or a transient nack replays rows the
server may already hold. Deduplicate on ``(ts_ms, device_id, child_id)``.

**``resume_from`` is exclusive**: tap sends rows strictly after it. Null means
"from the start of tap's buffer". The server is the authority here — tap adopts
whatever it is told, including a cursor older than its own, which is how a
server restored from backup gets its missing rows back. A cursor tap no longer
holds is not an error; tap simply sends what it has.

**Units and nulls.** ``ts_ms`` is epoch milliseconds UTC. ``power_mw``,
``voltage_mv`` and ``current_ma`` are milli-units. ``relay_on`` is ``0`` or
``1``, never a JSON boolean. The four meter fields are nullable, and **null
means unmeasured, never zero** — an outlet with no energy meter, or one whose
read failed while the rest of the sweep succeeded. ``energy_wh`` is a lifetime
counter on IOT hardware and a period counter on SMART; do not build on it.

**Plug identity** is ``(device_id, child_id)``. ``child_id`` is ``""`` for a
single-outlet device. Aliases arrive only in ``devices``, never on a reading
row — putting them on every row would invalidate a server-side plug cache
thousands of times per batch.

**``live`` rows** use the same layout but always carry null ``current_ma`` and
``energy_wh``, and a synthesised "now" timestamp rather than an observation
time. They cover only devices tap can currently reach, and stop entirely while
tap is more than ``live_max_lag_s`` behind — a "live" frame from a collector
deep in backfill would be a lie.

**Timestamps in frames** (``expires_at``) are RFC 3339 **with an offset**. A
naive value is read as UTC and warned about.

**Unknown frame types are ignored**, in both directions, so either side can add
one without a flag day. A ``protocol`` mismatch, by contrast, is fatal: tap
refuses the welcome and reconnects, so bumping it is a breaking change.
"""

from __future__ import annotations

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

# Nack reasons. `transient` means "try that batch again"; `bad_batch` means "this
# one will never work" — a poison pill tap must step over rather than wedge on.
NACK_TRANSIENT = "transient"
NACK_BAD_BATCH = "bad_batch"

# Row layout for READINGS. Changing this is a protocol break.
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

DEFAULT_MAX_BATCH_ROWS = 5000
DEFAULT_WINDOW = 4
# Below this much lag, `live` is truthful. Above it, tap is deep in backfill and
# a "live" frame would be a lie, so it stops sending them.
DEFAULT_LIVE_MAX_LAG_S = 300.0


def hello(tap_id: str, version: str, oldest: str | None, newest: str | None) -> dict:
    return {
        "type": HELLO,
        "tap_id": tap_id,
        "version": version,
        "protocol": PROTOCOL_VERSION,
        "buffer_oldest": oldest,
        "buffer_newest": newest,
    }


def readings(batch_id: str, cursor: str, rows: list[list]) -> dict:
    return {"type": READINGS, "batch": batch_id, "cursor": cursor, "rows": rows}


def live(rows: list[list]) -> dict:
    return {"type": LIVE, "rows": rows}


def devices(entries: list[dict]) -> dict:
    """The alias roster. Aliases travel here, never on every reading row.

    The server resolves an outlet to a plug by `(device_id, child_id)` and
    caches it; an alias on every row would invalidate that cache thousands of
    times per batch.
    """
    return {"type": DEVICES, "devices": entries}


def command_result(command_id: str, status: str, error: str | None = None) -> dict:
    return {"type": COMMAND_RESULT, "command_id": command_id, "status": status, "error": error}


def pong(token: Any) -> dict:
    return {"type": PONG, "token": token}


class WelcomeError(Exception):
    """The server's welcome was unusable — a protocol mismatch or a malformed frame."""


def _cursor_or_none(value: Any) -> str | None:
    """Validate a server-supplied cursor.

    This is the one field that decides what data gets sent, and it was the one
    field with no validation. A malformed value used to surface as a bare
    `ValueError` deep in the sender, killing the session — and since the same
    welcome arrives on every reconnect, nothing was ever delivered again.
    """
    if value is None or value == "":
        return None
    # isascii() as well as isdigit(): the latter accepts non-ASCII digits like
    # "²", which would pass here and then raise ValueError in parse_cursor —
    # precisely the failure this function exists to prevent.
    if not isinstance(value, str) or not (value.isascii() and value.isdigit()):
        raise WelcomeError(f"resume_from must be a cursor string or null, got {value!r}")
    return value


def _int_or(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except TypeError, ValueError:
        raise WelcomeError(f"expected an integer, got {value!r}") from None


def _float_or(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except TypeError, ValueError:
        raise WelcomeError(f"expected a number, got {value!r}") from None


class Welcome:
    """The server's answer to `hello`, with the defaults it did not override."""

    __slots__ = ("resume_from", "max_batch_rows", "window", "live_max_lag_s", "server_epoch")

    def __init__(self, frame: dict) -> None:
        if frame.get("type") != WELCOME:
            raise WelcomeError(f"expected a {WELCOME} frame, got {frame.get('type')!r}")
        protocol = frame.get("protocol", PROTOCOL_VERSION)
        if protocol != PROTOCOL_VERSION:
            raise WelcomeError(f"server speaks protocol {protocol}, tap speaks {PROTOCOL_VERSION}")
        self.server_epoch = frame.get("server_epoch")
        # The server is the authority on what it has durably stored. tap sends
        # its own extent as a hint; this is the answer.
        self.resume_from = _cursor_or_none(frame.get("resume_from"))
        # Explicit None checks, not `or`: `or` would quietly turn a server's 0
        # into the default instead of refusing it, and 0 is exactly the value
        # that would wedge the sender in a silent no-progress loop.
        self.max_batch_rows = _int_or(frame.get("max_batch_rows"), DEFAULT_MAX_BATCH_ROWS)
        self.window = _int_or(frame.get("window"), DEFAULT_WINDOW)
        self.live_max_lag_s = _float_or(frame.get("live_max_lag_s"), DEFAULT_LIVE_MAX_LAG_S)
        if self.max_batch_rows < 1 or self.window < 1:
            raise WelcomeError(
                f"server sent a non-positive batch size ({self.max_batch_rows}) "
                f"or window ({self.window})"
            )
        if self.live_max_lag_s < 0:
            raise WelcomeError(f"server sent a negative live lag ({self.live_max_lag_s})")
