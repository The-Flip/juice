"""One outlet's reading, as every collector produces it."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PlugReading:
    child_id: str
    alias: str
    is_on: bool
    watts: float | None
    voltage: float | None
    amps: float | None
    total_kwh: float | None


def outlet_number(child_id: str) -> int | None:
    """1-based physical outlet position from an HS300 child_id.

    HS300 child IDs are the device_id plus a two-digit 0-based outlet index
    ("00".."05"). Single-outlet devices (EP10 _SelfPlug) use "" — no position.
    """
    if len(child_id) < 2 or not child_id[-2:].isdigit():
        return None
    return int(child_id[-2:]) + 1
