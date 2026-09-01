"""One window convention for every /api/v2 endpoint that takes a time range.

v1 has three, and sibling endpoints on the same page disagree:
`/api/machines/{id}/peak` is UTC hour-aligned, `/api/machines/{id}/cost` is
local calendar day, and `/api/air/{mac}/history` uses `from`/`to` with a third
parser. A client cannot learn the rule once.

Here there is exactly one input form:

    ?window=30d          (also 24h, 7d, 12w)
    ?from=YYYY-MM-DD&to=YYYY-MM-DD

Half-open [from, to), anchored on **local America/Chicago days**, because that is
what "Saturday" means to someone standing in the museum. Central is always a
whole-hour offset from UTC, so a local midnight is an exact UTC hour boundary and
the hourly rollups line up with local-day bounds without interpolation.

Two behaviours differ from v1 deliberately:

* **An oversized window is a 400, not a silent clamp.** v1 clamps to 365 days, so
  a client asking for two years receives one and cannot tell — a chart that lies
  about its own axis.
* **Every response echoes the resolved window back**, including its grain, so a
  client never has to reconstruct what it actually got.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from aiohttp import web

from juice.api.v2 import errors

LOCAL_TZ_NAME = "America/Chicago"
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

MAX_DAYS = 365
DEFAULT_WINDOW = "30d"

Grain = Literal["hour", "day"]

_SPEC = re.compile(r"^(\d+)([hdw])$")
_UNIT_DAYS = {"h": 1 / 24, "d": 1, "w": 7}


@dataclass(frozen=True)
class Window:
    """A resolved range, available in both grains callers need.

    The store exposes local-day methods (`kwh_by_machine_and_local_day`) and
    UTC-hour ones (`usage_by_machine`). Resolving both from one input is what
    lets every endpoint share a convention while still using the right query.
    """

    spec: str
    from_day: date
    to_day: date  # exclusive
    from_utc: datetime
    to_utc: datetime  # exclusive
    grain: Grain

    @property
    def days(self) -> int:
        return (self.to_day - self.from_day).days

    def echo(self) -> dict:
        """What the client actually got. Never omitted: a caller that asked for
        `30d` still needs the concrete dates to label an axis."""
        return {
            "spec": self.spec,
            "from": self.from_day.isoformat(),
            "to": self.to_day.isoformat(),
            "tz": LOCAL_TZ_NAME,
            "grain": self.grain,
            "days": self.days,
            # A local day is 23 or 25 hours twice a year, so a client computing
            # hours as days*24 is wrong on those days. Say it outright.
            "hours": int((self.to_utc - self.from_utc).total_seconds() // 3600),
        }


def _local_midnight_utc(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=LOCAL_TZ).astimezone(UTC)


def parse_window(
    request: web.Request, *, grain: Grain = "day", default: str = DEFAULT_WINDOW
) -> tuple[Window | None, web.Response | None]:
    """Resolve the window, or return the response explaining why not."""
    spec = request.query.get("window")
    raw_from = request.query.get("from")
    raw_to = request.query.get("to")

    if spec and (raw_from or raw_to):
        return None, errors.error(
            400, errors.BAD_REQUEST, "give either 'window' or 'from'/'to', not both"
        )

    today = datetime.now(LOCAL_TZ).date()

    if raw_from or raw_to:
        if not (raw_from and raw_to):
            return None, errors.error(
                400, errors.BAD_REQUEST, "'from' and 'to' must be given together"
            )
        try:
            from_day = date.fromisoformat(raw_from)
            to_day = date.fromisoformat(raw_to)
        except ValueError:
            return None, errors.error(
                400, errors.BAD_REQUEST, "'from' and 'to' must be YYYY-MM-DD dates"
            )
        if to_day <= from_day:
            return None, errors.error(
                400, errors.BAD_REQUEST, "'to' must be after 'from' (the range is half-open)"
            )
        resolved_spec = f"{raw_from}..{raw_to}"
    else:
        spec = spec or default
        match = _SPEC.match(spec)
        if not match:
            return None, errors.error(
                400,
                errors.BAD_REQUEST,
                f"'window' must look like 30d, 24h or 12w (got {spec!r})",
            )
        count, unit = int(match.group(1)), match.group(2)
        if count < 1:
            return None, errors.error(400, errors.BAD_REQUEST, "'window' must be at least 1")
        if unit == "h" and count % 24:
            # Every window here is anchored on local-day boundaries, so an hour
            # count that isn't a whole number of days cannot be honoured.
            # Rounding it up would return more data than was asked for — the
            # same class of lie as v1's silent clamp, in the other direction.
            return None, errors.error(
                400,
                errors.BAD_REQUEST,
                f"windows are whole local days; {count}h is not a multiple of 24",
                hours=count,
            )
        days = max(1, round(count * _UNIT_DAYS[unit]))
        # `to` is tomorrow so today's partial day is included — an operator
        # asking for 30d expects today's usage to be in it.
        to_day = today + timedelta(days=1)
        from_day = to_day - timedelta(days=days)
        resolved_spec = spec

    span = (to_day - from_day).days
    if span > MAX_DAYS:
        # Refuse rather than clamp. v1 silently truncates to 365, so a client
        # asking for two years gets one and cannot tell it was altered.
        return None, errors.error(
            400,
            errors.BAD_REQUEST,
            f"window is {span} days; the maximum is {MAX_DAYS}",
            requested_days=span,
            max_days=MAX_DAYS,
        )

    return (
        Window(
            spec=resolved_spec,
            from_day=from_day,
            to_day=to_day,
            from_utc=_local_midnight_utc(from_day),
            to_utc=_local_midnight_utc(to_day),
            grain=grain,
        ),
        None,
    )
