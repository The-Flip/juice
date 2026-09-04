"""Roster overlay rules: pinned wins, exclusions always win, and one dropped
broadcast never tears down a working poller."""

from __future__ import annotations

from tap.config import Config, DeviceSpec, ExcludeRule
from tap.device import Family
from tap.discovery import MISSING_ROUNDS_BEFORE_DROP, Roster


def _config(**kw) -> Config:
    return Config(**kw)


class TestOverlay:
    def test_pinned_devices_are_in_the_roster_before_any_discovery(self):
        roster = Roster(_config(devices=(DeviceSpec(host="10.0.0.1", pinned=True),)))
        assert set(roster.specs) == {"10.0.0.1"}

    def test_discovery_adds_new_hosts(self):
        roster = Roster(_config())
        added, removed = roster.apply_discovery({"10.0.0.2": Family.SMART})
        assert added == ["10.0.0.2"]
        assert removed == []
        assert roster.specs["10.0.0.2"].pinned is False

    def test_discovery_does_not_downgrade_a_pinned_device(self):
        roster = Roster(
            _config(devices=(DeviceSpec(host="10.0.0.1", family=Family.IOT, pinned=True),))
        )
        roster.apply_discovery({"10.0.0.1": Family.SMART})
        # The pin stays authoritative: config decides, discovery only refreshes.
        assert roster.specs["10.0.0.1"].family is Family.IOT
        assert roster.specs["10.0.0.1"].pinned is True

    def test_exclusion_beats_discovery(self):
        roster = Roster(_config(excludes=(ExcludeRule(host="10.0.0.9", reason="neighbour"),)))
        added, _ = roster.apply_discovery({"10.0.0.9": Family.SMART})
        assert added == []
        assert roster.specs == {}

    def test_exclusion_beats_a_pin_too(self):
        roster = Roster(
            _config(
                devices=(DeviceSpec(host="10.0.0.1", pinned=True),),
                excludes=(ExcludeRule(host="10.0.0.1"),),
            )
        )
        assert roster.specs == {}


class TestStability:
    def test_a_single_missed_round_does_not_drop_a_device(self):
        """One dropped UDP broadcast must never kill a healthy poller."""
        roster = Roster(_config())
        roster.apply_discovery({"10.0.0.2": Family.SMART})
        for _ in range(MISSING_ROUNDS_BEFORE_DROP - 1):
            added, removed = roster.apply_discovery({})
            assert removed == []
            assert "10.0.0.2" in roster.specs

    def test_a_persistently_missing_device_is_eventually_dropped(self):
        roster = Roster(_config())
        roster.apply_discovery({"10.0.0.2": Family.SMART})
        for _ in range(MISSING_ROUNDS_BEFORE_DROP - 1):
            roster.apply_discovery({})
        _added, removed = roster.apply_discovery({})
        assert removed == ["10.0.0.2"]
        assert roster.specs == {}

    def test_reappearing_resets_the_grace_counter(self):
        roster = Roster(_config())
        roster.apply_discovery({"10.0.0.2": Family.SMART})
        roster.apply_discovery({})
        roster.apply_discovery({"10.0.0.2": Family.SMART})  # it came back
        for _ in range(MISSING_ROUNDS_BEFORE_DROP - 1):
            _added, removed = roster.apply_discovery({})
            assert removed == []

    def test_a_pinned_device_is_never_dropped_however_long_it_is_missing(self):
        """Absent-and-pinned is information (it goes OFFLINE), not housekeeping."""
        roster = Roster(_config(devices=(DeviceSpec(host="10.0.0.1", pinned=True),)))
        for _ in range(MISSING_ROUNDS_BEFORE_DROP * 3):
            _added, removed = roster.apply_discovery({})
            assert removed == []
        assert "10.0.0.1" in roster.specs


class TestReload:
    def test_reload_keeps_discovered_hosts_and_applies_new_pins(self):
        roster = Roster(_config())
        roster.apply_discovery({"10.0.0.2": Family.SMART})
        roster.replace_config(_config(devices=(DeviceSpec(host="10.0.0.5", pinned=True),)))
        assert set(roster.specs) == {"10.0.0.2", "10.0.0.5"}

    def test_reload_applies_a_new_exclusion_to_a_discovered_host(self):
        roster = Roster(_config())
        roster.apply_discovery({"10.0.0.2": Family.SMART})
        roster.replace_config(_config(excludes=(ExcludeRule(host="10.0.0.2"),)))
        assert roster.specs == {}
