"""Every timezone an IOT device can name must be resolvable here.

Connecting to a legacy IOT device runs python-kasa's full `update()`, and its
Time module turns the firmware's timezone *index* into a `ZoneInfo` on every
connect (`kasa/iot/iottimezone.py`). The firmware table is full of the older
POSIX-style and `US/*` aliases — `CST6CDT`, `EST5EDT`, `US/Arizona` — which live
in tzdata's "backward" set. A trimmed system tz database (Debian slim, Alpine,
and this laptop) ships the `America/*` names and drops those aliases, so the
lookup raises `ZoneInfoNotFoundError`.

That is not a device problem and not a credentials problem, but it surfaces as
one: the exception escapes `connect`, the poller counts three failures, and the
device goes OFFLINE forever while `tap devices` — which never calls `update()` —
keeps listing it happily. Every HS300 at the museum is on index 13, so this took
the whole site down.

The fix is the `tzdata` wheel in tap's extra; this is what proves it is there.
"""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest

pytest.importorskip("kasa", reason="install with: uv sync --extra tap")

from kasa.iot.iottimezone import TIMEZONE_INDEX


def test_the_firmware_table_is_where_we_think_it_is():
    """Guards the test below against passing because it checked nothing."""
    assert len(TIMEZONE_INDEX) == 110
    # The index every museum HS300 reports. If python-kasa renames it to
    # America/Chicago the parametrised test would go green without ever
    # exercising a "backward" alias, which is the whole point.
    assert TIMEZONE_INDEX[13] == "CST6CDT"


@pytest.mark.parametrize("index", sorted(TIMEZONE_INDEX))
def test_every_firmware_timezone_resolves(index):
    key = TIMEZONE_INDEX[index]
    try:
        ZoneInfo(key)
    except ZoneInfoNotFoundError as e:
        pytest.fail(
            f"index {index} -> {key!r} is unresolvable: {e}. "
            "A device on this index will fail to connect and be reported OFFLINE."
        )
