"""Tests for juice.readings — the reading every collector produces."""

from __future__ import annotations

from juice.readings import outlet_number


class TestOutletNumber:
    def test_hs300_child_id_suffix_maps_one_based(self) -> None:
        base = "8006188258AD5449B36256BD70827E8C25536CB8"
        assert outlet_number(base + "00") == 1
        assert outlet_number(base + "03") == 4
        assert outlet_number(base + "05") == 6

    def test_empty_child_id_returns_none(self) -> None:
        # Single-outlet devices (EP10 _SelfPlug) use "" as their child_id.
        assert outlet_number("") is None

    def test_non_numeric_suffix_returns_none(self) -> None:
        assert outlet_number("8006188258AD5449B36256BD70827E8C25536CBXY") is None

    def test_short_child_id_returns_none(self) -> None:
        assert outlet_number("3") is None
