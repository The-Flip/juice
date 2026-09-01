"""Tests for the single /api/v2 window convention.

v1 has three conventions and sibling endpoints on the same page disagree, so a
client cannot learn the rule once. These pin the one rule and the two places it
deliberately behaves differently from v1.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from juice.api.v2.window import MAX_DAYS, parse_window


class _Req:
    def __init__(self, **query: str) -> None:
        self.query = query


def _parse(**query: str):
    return parse_window(_Req(**query))  # type: ignore[arg-type]


class TestSpecForm:
    @pytest.mark.parametrize(
        ("spec", "days"), [("1d", 1), ("30d", 30), ("24h", 1), ("7d", 7), ("2w", 14)]
    )
    def test_relative_specs(self, spec: str, days: int) -> None:
        window, failure = _parse(window=spec)
        assert failure is None
        assert window is not None
        assert window.days == days

    def test_the_window_includes_today(self) -> None:
        """An operator asking for 30d expects today's usage in it."""
        window, _ = _parse(window="1d")
        assert window is not None
        assert window.to_day == date.today() + timedelta(days=1)

    def test_a_malformed_spec_is_rejected_with_the_value(self) -> None:
        window, failure = _parse(window="last-month")
        assert window is None
        assert failure is not None and failure.status == 400

    def test_defaults_when_absent(self) -> None:
        window, failure = _parse()
        assert failure is None
        assert window is not None and window.days == 30


class TestExplicitDates:
    def test_from_and_to(self) -> None:
        window, failure = _parse(**{"from": "2026-08-01", "to": "2026-08-31"})
        assert failure is None
        assert window is not None
        assert window.from_day == date(2026, 8, 1)
        assert window.to_day == date(2026, 8, 31)
        assert window.days == 30

    def test_half_open_is_enforced(self) -> None:
        _, failure = _parse(**{"from": "2026-08-01", "to": "2026-08-01"})
        assert failure is not None and failure.status == 400

    def test_both_are_required_together(self) -> None:
        _, failure = _parse(**{"from": "2026-08-01"})
        assert failure is not None and failure.status == 400

    def test_mixing_the_two_forms_is_rejected(self) -> None:
        """Silently preferring one would make the other's presence meaningless."""
        _, failure = _parse(window="30d", **{"from": "2026-08-01", "to": "2026-08-31"})
        assert failure is not None and failure.status == 400

    def test_a_bad_date_is_rejected(self) -> None:
        _, failure = _parse(**{"from": "August", "to": "2026-08-31"})
        assert failure is not None and failure.status == 400


class TestOversizedWindow:
    def test_rejected_rather_than_clamped(self) -> None:
        """v1 clamps to 365, so a client asking for two years receives one and
        cannot tell — a chart that lies about its own axis."""
        window, failure = _parse(**{"from": "2024-01-01", "to": "2026-01-01"})
        assert window is None
        assert failure is not None and failure.status == 400

    def test_the_refusal_says_what_was_asked_and_what_is_allowed(self) -> None:
        import json

        _, failure = _parse(window="500d")
        body = json.loads(failure.body)
        assert body["error"]["detail"]["requested_days"] == 500
        assert body["error"]["detail"]["max_days"] == MAX_DAYS

    def test_the_maximum_itself_is_allowed(self) -> None:
        window, failure = _parse(window=f"{MAX_DAYS}d")
        assert failure is None and window is not None


class TestEcho:
    def test_echoes_the_resolved_window(self) -> None:
        """A caller that asked for 30d still needs concrete dates to label an axis."""
        window, _ = _parse(window="7d")
        echoed = window.echo()

        assert echoed["spec"] == "7d"
        assert echoed["days"] == 7
        assert echoed["tz"] == "America/Chicago"
        assert echoed["grain"] == "day"
        assert date.fromisoformat(echoed["from"]) < date.fromisoformat(echoed["to"])

    def test_echoes_hours_because_a_local_day_is_not_always_24(self) -> None:
        """A local day is 23 or 25 hours twice a year, so a client computing
        days*24 is wrong on those days. The server says outright."""
        window, _ = _parse(**{"from": "2026-11-01", "to": "2026-11-02"})
        assert window.echo()["hours"] == 25  # DST ends; that day is 25 hours long

    def test_a_normal_day_is_24_hours(self) -> None:
        window, _ = _parse(**{"from": "2026-06-01", "to": "2026-06-02"})
        assert window.echo()["hours"] == 24


class TestBothGrains:
    def test_utc_bounds_are_local_midnights(self) -> None:
        """Central is a whole-hour offset from UTC, so local midnight is an exact
        UTC hour boundary and the hourly rollups line up without interpolation."""
        window, _ = _parse(**{"from": "2026-06-01", "to": "2026-06-02"})
        assert window.from_utc.minute == 0 and window.from_utc.second == 0
        assert window.to_utc.minute == 0
        assert (window.to_utc - window.from_utc) == timedelta(hours=24)
