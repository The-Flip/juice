"""Building the device roster: LAN discovery overlaid on the config file.

The rules, stated once so they cannot drift:

1. Pinned `[[device]]` entries are always in the roster.
2. Discovery adds anything not already pinned by host.
3. `[[exclude]]` removes at any stage. Exclusion always wins.
4. **Discovery may add and refresh; only config may pin.** A pinned device that
   discovery stops seeing stays in the roster and goes OFFLINE, which is
   information. A discovered device that vanishes is eventually dropped, which
   is housekeeping.

Discovery is a UDP broadcast, so it does not survive Docker's default bridge
network — the pinned list is the supported answer there, not a workaround.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from tap.config import Config, DeviceSpec
from tap.device import Family

log = logging.getLogger(__name__)

# Rounds a discovered device may go unseen before its poller is torn down. One
# dropped UDP broadcast must never kill a healthy poller.
MISSING_ROUNDS_BEFORE_DROP = 3


@dataclass(frozen=True, slots=True)
class Discovered:
    """What one discovery round learned about a host.

    `device_id` is deliberately **not** filled in from the discovery response.
    Measured against a real P316M, the id a device announces during discovery
    (`d894c51b45688a64...`, 32 hex) is not the id it reports once connected
    (`80223EF61B73...`, 40 hex) — the latter is what the server keys plugs on.
    Matching an exclusion against the discovery-time value would look like it
    worked and silently never match, so a `device_id` exclusion is enforced by
    the poller instead, once the real id is known. The field stays here because
    a *pinned* device may carry an id from the config file.
    """

    host: str
    family: Family
    device_id: str = ""


class Roster:
    """The current device set, and the bookkeeping that keeps it stable."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._specs: dict[str, DeviceSpec] = {}
        self._missing: dict[str, int] = {}
        for spec in config.devices:
            if config.is_excluded(host=spec.host, device_id=spec.device_id) is None:
                self._specs[spec.host] = spec
        self._pinned = set(self._specs)

    def replace_config(self, config: Config) -> None:
        """Apply a reloaded config's pins and exclusions, keeping discovered hosts."""
        discovered = {host: spec for host, spec in self._specs.items() if host not in self._pinned}
        self._config = config
        self._specs = {}
        for spec in config.devices:
            if config.is_excluded(host=spec.host, device_id=spec.device_id) is None:
                self._specs[spec.host] = spec
        self._pinned = set(self._specs)
        for host, spec in discovered.items():
            if host in self._specs:
                continue
            if config.is_excluded(host=host, device_id=spec.device_id) is not None:
                continue
            self._specs[host] = spec

    @property
    def specs(self) -> dict[str, DeviceSpec]:
        return dict(self._specs)

    def apply_discovery(self, found: dict[str, Discovered]) -> tuple[list[str], list[str]]:
        """Merge a discovery round. Returns (added_hosts, removed_hosts)."""
        added: list[str] = []
        for host, entry in found.items():
            self._missing.pop(host, None)
            if host in self._specs:
                continue
            # The device id matters here: an exclusion written against an id
            # rather than a host would otherwise be ignored at admission and
            # the poller would already be running before anything noticed.
            rule = self._config.is_excluded(host=host, device_id=entry.device_id or None)
            if rule is not None:
                log.debug("discovery: ignoring %s (excluded: %s)", host, rule.reason or "no reason")
                continue
            self._specs[host] = DeviceSpec(
                host=host, family=entry.family, device_id=entry.device_id or None, pinned=False
            )
            added.append(host)

        removed: list[str] = []
        for host in list(self._specs):
            if host in found or host in self._pinned:
                continue
            self._missing[host] = self._missing.get(host, 0) + 1
            if self._missing[host] >= MISSING_ROUNDS_BEFORE_DROP:
                del self._specs[host]
                del self._missing[host]
                removed.append(host)
        return added, removed


async def discover(config: Config) -> dict[str, Discovered]:
    """One LAN discovery round: host -> what we learned about it.

    Returns an empty mapping rather than raising: a failed broadcast is a
    degraded round, not a reason to disturb pollers that are working.
    """
    from kasa import Credentials as KasaCredentials
    from kasa import Discover

    from tap.kasa_common import family_of

    creds = None
    if config.credentials is not None:
        creds = KasaCredentials(config.credentials.username, config.credentials.password)
    try:
        found = await Discover.discover(
            target=config.discovery.target,
            credentials=creds,
            # Not int(): a fractional timeout would truncate, and 0.5 -> 0
            # returns immediately with nothing discovered.
            discovery_timeout=config.discovery.timeout_seconds,
        )
    except Exception as e:  # noqa: BLE001 — a bad broadcast must not kill the loop
        log.warning("discovery failed: %s", e)
        return {}
    # No device_id: see the Discovered docstring. Discovery cannot learn the id
    # the server keys plugs on, for either family.
    return {host: Discovered(host=host, family=family_of(device)) for host, device in found.items()}
