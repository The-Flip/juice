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

from tap.config import Config, DeviceSpec
from tap.device import Family

log = logging.getLogger(__name__)

# Rounds a discovered device may go unseen before its poller is torn down. One
# dropped UDP broadcast must never kill a healthy poller.
MISSING_ROUNDS_BEFORE_DROP = 3


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

    def apply_discovery(self, found: dict[str, Family]) -> tuple[list[str], list[str]]:
        """Merge a discovery round. Returns (added_hosts, removed_hosts)."""
        added: list[str] = []
        for host, family in found.items():
            self._missing.pop(host, None)
            if host in self._specs:
                continue
            rule = self._config.is_excluded(host=host)
            if rule is not None:
                log.debug("discovery: ignoring %s (excluded: %s)", host, rule.reason or "no reason")
                continue
            self._specs[host] = DeviceSpec(host=host, family=family, pinned=False)
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


async def discover(config: Config) -> dict[str, Family]:
    """One LAN discovery round: host -> protocol family.

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
            discovery_timeout=int(config.discovery.timeout_seconds),
        )
    except Exception as e:  # noqa: BLE001 — a bad broadcast must not kill the loop
        log.warning("discovery failed: %s", e)
        return {}
    return {host: family_of(device) for host, device in found.items()}
