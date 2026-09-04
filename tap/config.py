"""Configuration: TOML file, environment, CLI flags.

This is the first config file in the repo — juice configures everything through
click options with `envvar=`, which works because juice has a dozen knobs and no
lists. `tap` has a device roster, so it needs a file.

**Precedence is CLI > env > TOML > compiled default**, and there is a test for
it, because precedence is the thing everyone guesses wrong.

Search path, first hit wins: `--config`, `$TAP_CONFIG`, `./tap.toml`,
`/etc/tap/tap.toml`. No file at all is legitimate: a discovery-only deployment
with credentials in the environment needs no TOML, and gets one INFO line
saying so rather than an error.

Secrets (`uplink.token`, device passwords) are read from the file if present but
belong in the environment — a secret in a config file is a secret in a backup.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from tap.device import Family
from tap.errors import EXIT_CONFIG, FatalError

CONFIG_ENV = "TAP_CONFIG"

# Every table and key the file may contain. A typo like `[[devices]]` or
# `retention_dayz` used to be accepted in silence, leaving tap running happily
# and collecting nothing — the worst possible outcome for a config mistake.
_KNOWN_TABLES = {
    "tap": {"id", "buffer_dir", "retention_days", "log_level"},
    "web": {"host", "port"},
    "uplink": {"url", "token", "enabled"},
    "discovery": {"enabled", "interval_seconds", "timeout_seconds", "target"},
    "polling": {"interval_seconds", "sweep_budget_seconds"},
    "credentials": {"username", "password"},
}
_KNOWN_ARRAYS = {
    "device": {"host", "family", "username", "password", "device_id"},
    "exclude": {"host", "device_id", "reason"},
}
_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
DEFAULT_CONFIG_PATHS = (Path("tap.toml"), Path("/etc/tap/tap.toml"))

DEFAULT_BUFFER_DIR = Path("/data/buffer")
DEFAULT_RETENTION_DAYS = 30
DEFAULT_WEB_PORT = 8010
# Bind loopback by default: the status page shows outlet aliases and device
# hosts. Not secret, but not something to expose to a whole LAN by accident.
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_DISCOVERY_INTERVAL = 300.0
DEFAULT_DISCOVERY_TIMEOUT = 5
DEFAULT_BROADCAST = "255.255.255.255"


@dataclass(frozen=True, slots=True)
class Credentials:
    username: str
    password: str


@dataclass(frozen=True, slots=True)
class DeviceSpec:
    """A device tap should poll.

    `pinned` marks one that came from the config file rather than discovery.
    Pinned devices are polled whether or not discovery ever sees them, and are
    never torn down — which is what makes tap work under Docker's default
    bridge network, where a UDP broadcast never reaches the LAN.
    """

    host: str
    family: Family = Family.AUTO
    credentials: Credentials | None = None
    device_id: str | None = None
    pinned: bool = False


@dataclass(frozen=True, slots=True)
class ExcludeRule:
    host: str | None = None
    device_id: str | None = None
    reason: str = ""

    def matches(self, *, host: str, device_id: str | None) -> bool:
        if self.host is not None and self.host == host:
            return True
        return self.device_id is not None and device_id is not None and self.device_id == device_id


@dataclass(frozen=True, slots=True)
class WebConfig:
    host: str = DEFAULT_WEB_HOST
    port: int = DEFAULT_WEB_PORT


@dataclass(frozen=True, slots=True)
class UplinkConfig:
    url: str | None = None
    token: str | None = None
    enabled: bool = True

    @property
    def active(self) -> bool:
        """True when there is somewhere to stream to. tap runs happily without one."""
        return self.enabled and bool(self.url)


@dataclass(frozen=True, slots=True)
class DiscoveryConfig:
    enabled: bool = True
    interval_seconds: float = DEFAULT_DISCOVERY_INTERVAL
    # Whole seconds: python-kasa types `discovery_timeout` as an int, and
    # quietly truncating 0.5 to 0 would return immediately having found nothing.
    # Rejecting a fractional value is better than silently ignoring it.
    timeout_seconds: int = DEFAULT_DISCOVERY_TIMEOUT
    target: str = DEFAULT_BROADCAST


@dataclass(frozen=True, slots=True)
class PollingConfig:
    """Cadence and the per-sweep deadline.

    Defaults match the hardware: the meter refreshes about once a second, and
    the budget sits under the interval so a hung sweep is cancelled before its
    successor is due. They are configurable because RF conditions are not
    universal — a strip far from the access point can legitimately need a longer
    budget, and raising it beats flapping the device OFFLINE.
    """

    interval_seconds: float = 1.0
    sweep_budget_seconds: float = 0.8


@dataclass(frozen=True, slots=True)
class Config:
    tap_id: str = "tap"
    buffer_dir: Path = DEFAULT_BUFFER_DIR
    retention_days: int = DEFAULT_RETENTION_DAYS
    log_level: str = "INFO"
    web: WebConfig = field(default_factory=WebConfig)
    uplink: UplinkConfig = field(default_factory=UplinkConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    credentials: Credentials | None = None
    devices: tuple[DeviceSpec, ...] = ()
    excludes: tuple[ExcludeRule, ...] = ()
    source_path: Path | None = None

    def credentials_for(self, spec: DeviceSpec) -> Credentials | None:
        """Per-device credentials, falling back to the global pair."""
        return spec.credentials or self.credentials

    def is_excluded(self, *, host: str, device_id: str | None = None) -> ExcludeRule | None:
        for rule in self.excludes:
            if rule.matches(host=host, device_id=device_id):
                return rule
        return None


# ---- loading ----------------------------------------------------------------


def find_config_path(
    explicit: str | os.PathLike[str] | None = None, *, environ=None
) -> Path | None:
    """First existing config path, or None. An explicit path that is missing is fatal."""
    environ = os.environ if environ is None else environ
    if explicit is not None:
        path = Path(explicit)
        if not path.is_file():
            raise FatalError(f"config file not found: {path}", EXIT_CONFIG)
        return path
    from_env = environ.get(CONFIG_ENV)
    if from_env:
        path = Path(from_env)
        if not path.is_file():
            raise FatalError(f"{CONFIG_ENV} points at a missing file: {path}", EXIT_CONFIG)
        return path
    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.is_file():
            return candidate
    return None


def _reject_unknown(data: dict[str, Any]) -> None:
    """Refuse a config with keys tap does not understand.

    Silently ignoring them is how a mistyped `[[devices]]` produces a tap that
    starts, reports healthy, and polls nothing at all.
    """
    for name, value in data.items():
        if name in _KNOWN_TABLES:
            if not isinstance(value, dict):
                raise FatalError(f"config: [{name}] must be a table", EXIT_CONFIG)
            unknown = set(value) - _KNOWN_TABLES[name]
            if unknown:
                allowed = ", ".join(sorted(_KNOWN_TABLES[name]))
                raise FatalError(
                    f"config: [{name}] has unknown key(s) {', '.join(sorted(unknown))}; "
                    f"allowed: {allowed}",
                    EXIT_CONFIG,
                )
        elif name in _KNOWN_ARRAYS:
            if not isinstance(value, list):
                raise FatalError(f"config: [[{name}]] must be an array of tables", EXIT_CONFIG)
            for i, entry in enumerate(value):
                if not isinstance(entry, dict):
                    raise FatalError(f"config: [[{name}]] #{i + 1} must be a table", EXIT_CONFIG)
                unknown = set(entry) - _KNOWN_ARRAYS[name]
                if unknown:
                    allowed = ", ".join(sorted(_KNOWN_ARRAYS[name]))
                    raise FatalError(
                        f"config: [[{name}]] #{i + 1} has unknown key(s) "
                        f"{', '.join(sorted(unknown))}; allowed: {allowed}",
                        EXIT_CONFIG,
                    )
        else:
            known = ", ".join(sorted([*_KNOWN_TABLES, *(f"[{k}]" for k in _KNOWN_ARRAYS)]))
            raise FatalError(
                f"config: unknown section {name!r}; known sections: {known}", EXIT_CONFIG
            )


def _table(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, dict):
        raise FatalError(f"config: [{name}] must be a table", EXIT_CONFIG)
    return value


def _typed(table: dict[str, Any], key: str, kind: type, where: str, default=None):
    if key not in table:
        return default
    value = table[key]
    # bool is a subclass of int; an int field must not silently accept `true`.
    if kind is int and isinstance(value, bool):
        raise FatalError(f"config: {where}.{key} must be an integer", EXIT_CONFIG)
    if kind is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, kind):
        raise FatalError(f"config: {where}.{key} must be {kind.__name__}", EXIT_CONFIG)
    return value


def _credentials(table: dict[str, Any], where: str) -> Credentials | None:
    username = _typed(table, "username", str, where)
    password = _typed(table, "password", str, where)
    if username and password:
        return Credentials(username, password)
    if username or password:
        raise FatalError(f"config: {where} needs both username and password", EXIT_CONFIG)
    return None


def _parse_devices(data: dict[str, Any]) -> tuple[DeviceSpec, ...]:
    raw = data.get("device", [])
    if not isinstance(raw, list):
        raise FatalError("config: [[device]] must be an array of tables", EXIT_CONFIG)
    specs: list[DeviceSpec] = []
    for i, entry in enumerate(raw):
        where = f"[[device]] #{i + 1}"
        if not isinstance(entry, dict):
            raise FatalError(f"config: {where} must be a table", EXIT_CONFIG)
        host = _typed(entry, "host", str, where)
        if not host:
            raise FatalError(f"config: {where} needs a host", EXIT_CONFIG)
        family_raw = _typed(entry, "family", str, where, "auto")
        try:
            family = Family(family_raw.lower())
        except ValueError:
            allowed = ", ".join(f.value for f in Family)
            raise FatalError(
                f"config: {where}.family must be one of {allowed} (got {family_raw!r})",
                EXIT_CONFIG,
            ) from None
        specs.append(
            DeviceSpec(
                host=host,
                family=family,
                credentials=_credentials(entry, where),
                device_id=_typed(entry, "device_id", str, where),
                pinned=True,
            )
        )
    return tuple(specs)


def _parse_excludes(data: dict[str, Any]) -> tuple[ExcludeRule, ...]:
    raw = data.get("exclude", [])
    if not isinstance(raw, list):
        raise FatalError("config: [[exclude]] must be an array of tables", EXIT_CONFIG)
    rules: list[ExcludeRule] = []
    for i, entry in enumerate(raw):
        where = f"[[exclude]] #{i + 1}"
        if not isinstance(entry, dict):
            raise FatalError(f"config: {where} must be a table", EXIT_CONFIG)
        host = _typed(entry, "host", str, where)
        device_id = _typed(entry, "device_id", str, where)
        if not host and not device_id:
            raise FatalError(f"config: {where} needs a host or a device_id", EXIT_CONFIG)
        rules.append(
            ExcludeRule(
                host=host, device_id=device_id, reason=_typed(entry, "reason", str, where, "")
            )
        )
    return tuple(rules)


def _from_toml(path: Path) -> Config:
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        raise FatalError(f"config: {path} is not valid TOML: {e}", EXIT_CONFIG) from None
    except OSError as e:
        raise FatalError(f"config: cannot read {path}: {e}", EXIT_CONFIG) from None

    _reject_unknown(data)
    tap_t = _table(data, "tap")
    web_t = _table(data, "web")
    up_t = _table(data, "uplink")
    disc_t = _table(data, "discovery")
    poll_t = _table(data, "polling")

    buffer_dir = _typed(tap_t, "buffer_dir", str, "[tap]")
    return Config(
        tap_id=_typed(tap_t, "id", str, "[tap]", "tap"),
        buffer_dir=Path(buffer_dir) if buffer_dir else DEFAULT_BUFFER_DIR,
        retention_days=_typed(tap_t, "retention_days", int, "[tap]", DEFAULT_RETENTION_DAYS),
        log_level=_typed(tap_t, "log_level", str, "[tap]", "INFO"),
        web=WebConfig(
            host=_typed(web_t, "host", str, "[web]", DEFAULT_WEB_HOST),
            port=_typed(web_t, "port", int, "[web]", DEFAULT_WEB_PORT),
        ),
        uplink=UplinkConfig(
            url=_typed(up_t, "url", str, "[uplink]"),
            token=_typed(up_t, "token", str, "[uplink]"),
            enabled=_typed(up_t, "enabled", bool, "[uplink]", True),
        ),
        discovery=DiscoveryConfig(
            enabled=_typed(disc_t, "enabled", bool, "[discovery]", True),
            interval_seconds=_typed(
                disc_t, "interval_seconds", float, "[discovery]", DEFAULT_DISCOVERY_INTERVAL
            ),
            timeout_seconds=_typed(
                disc_t, "timeout_seconds", int, "[discovery]", DEFAULT_DISCOVERY_TIMEOUT
            ),
            target=_typed(disc_t, "target", str, "[discovery]", DEFAULT_BROADCAST),
        ),
        polling=PollingConfig(
            interval_seconds=_typed(poll_t, "interval_seconds", float, "[polling]", 1.0),
            sweep_budget_seconds=_typed(poll_t, "sweep_budget_seconds", float, "[polling]", 0.8),
        ),
        credentials=_credentials(_table(data, "credentials"), "[credentials]"),
        devices=_parse_devices(data),
        excludes=_parse_excludes(data),
        source_path=path,
    )


def _int_env(environ, key: str, where: str) -> int | None:
    raw = environ.get(key)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        raise FatalError(f"{key} must be an integer (got {raw!r}) — {where}", EXIT_CONFIG) from None


def _apply_env(cfg: Config, environ) -> Config:
    """Overlay environment variables. Credentials prefer env over file."""
    web = cfg.web
    if host := environ.get("TAP_WEB_HOST"):
        web = replace(web, host=host)
    if (port := _int_env(environ, "TAP_WEB_PORT", "[web].port")) is not None:
        web = replace(web, port=port)

    uplink = cfg.uplink
    if url := environ.get("TAP_UPLINK_URL"):
        uplink = replace(uplink, url=url)
    if token := environ.get("TAP_UPLINK_TOKEN"):
        uplink = replace(uplink, token=token)

    credentials = cfg.credentials
    env_user = environ.get("KASA_USERNAME")
    env_pass = environ.get("KASA_PASSWORD")
    if env_user and env_pass:
        credentials = Credentials(env_user, env_pass)
    elif env_user or env_pass:
        # Half a pair is a typo, not a choice. Silently ignoring it surfaces
        # later as every device rejecting our credentials.
        raise FatalError(
            "KASA_USERNAME and KASA_PASSWORD must be set together "
            f"(only {'KASA_USERNAME' if env_user else 'KASA_PASSWORD'} is set)",
            EXIT_CONFIG,
        )

    retention = _int_env(environ, "TAP_RETENTION_DAYS", "[tap].retention_days")
    buffer_dir = environ.get("TAP_BUFFER_DIR")
    return replace(
        cfg,
        tap_id=environ.get("TAP_ID") or cfg.tap_id,
        buffer_dir=Path(buffer_dir) if buffer_dir else cfg.buffer_dir,
        retention_days=retention if retention is not None else cfg.retention_days,
        log_level=environ.get("TAP_LOG_LEVEL") or cfg.log_level,
        web=web,
        uplink=uplink,
        credentials=credentials,
    )


def _apply_overrides(cfg: Config, overrides: dict[str, Any]) -> Config:
    """Overlay CLI flags. A key whose value is None was not given."""
    given = {k: v for k, v in overrides.items() if v is not None}
    web = cfg.web
    if "web_host" in given:
        web = replace(web, host=given["web_host"])
    if "web_port" in given:
        web = replace(web, port=given["web_port"])
    uplink = cfg.uplink
    if "uplink_url" in given:
        uplink = replace(uplink, url=given["uplink_url"])
    if "uplink_token" in given:
        uplink = replace(uplink, token=given["uplink_token"])
    if "no_uplink" in given and given["no_uplink"]:
        uplink = replace(uplink, enabled=False)
    discovery = cfg.discovery
    if "no_discovery" in given and given["no_discovery"]:
        discovery = replace(discovery, enabled=False)
    return replace(
        cfg,
        tap_id=given.get("tap_id", cfg.tap_id),
        buffer_dir=Path(given["buffer_dir"]) if "buffer_dir" in given else cfg.buffer_dir,
        retention_days=given.get("retention_days", cfg.retention_days),
        log_level=given.get("log_level", cfg.log_level),
        web=web,
        uplink=uplink,
        discovery=discovery,
    )


def _validate(cfg: Config) -> None:
    if cfg.retention_days < 1:
        raise FatalError("config: [tap].retention_days must be at least 1", EXIT_CONFIG)
    if not 1 <= cfg.web.port <= 65535:
        raise FatalError("config: [web].port must be a valid TCP port", EXIT_CONFIG)
    if cfg.discovery.interval_seconds <= 0:
        raise FatalError("config: [discovery].interval_seconds must be positive", EXIT_CONFIG)
    if cfg.discovery.timeout_seconds <= 0:
        raise FatalError(
            "config: [discovery].timeout_seconds must be a positive whole number of seconds",
            EXIT_CONFIG,
        )
    if cfg.polling.interval_seconds <= 0:
        raise FatalError("config: [polling].interval_seconds must be positive", EXIT_CONFIG)
    if cfg.polling.sweep_budget_seconds <= 0:
        raise FatalError("config: [polling].sweep_budget_seconds must be positive", EXIT_CONFIG)
    if cfg.polling.sweep_budget_seconds >= cfg.polling.interval_seconds:
        # Otherwise a slow sweep is still running when the next one is due, and
        # sweeps pile up instead of being abandoned.
        raise FatalError(
            "config: [polling].sweep_budget_seconds must be less than interval_seconds "
            f"({cfg.polling.sweep_budget_seconds} >= {cfg.polling.interval_seconds})",
            EXIT_CONFIG,
        )
    if not cfg.tap_id:
        raise FatalError("config: [tap].id must not be empty", EXIT_CONFIG)
    if cfg.uplink.active and not cfg.uplink.url.startswith(
        ("ws://", "wss://", "http://", "https://")
    ):
        raise FatalError(
            "config: [uplink].url must be a ws://, wss://, http:// or https:// URL "
            f"(got {cfg.uplink.url!r})",
            EXIT_CONFIG,
        )
    if cfg.log_level.upper() not in _LOG_LEVELS:
        allowed = ", ".join(sorted(_LOG_LEVELS))
        raise FatalError(
            f"config: [tap].log_level must be one of {allowed} (got {cfg.log_level!r})",
            EXIT_CONFIG,
        )
    if not str(cfg.buffer_dir).strip():
        raise FatalError("config: [tap].buffer_dir must not be empty", EXIT_CONFIG)
    if not cfg.discovery.enabled and not cfg.devices:
        raise FatalError(
            "config: discovery is disabled and no [[device]] is pinned — tap would poll nothing",
            EXIT_CONFIG,
        )


def load_config(
    *,
    path: str | os.PathLike[str] | None = None,
    overrides: dict[str, Any] | None = None,
    environ=None,
) -> Config:
    """Build the effective config: defaults, then TOML, then env, then CLI."""
    environ = os.environ if environ is None else environ
    found = find_config_path(path, environ=environ)
    cfg = _from_toml(found) if found is not None else Config()
    cfg = _apply_env(cfg, environ)
    cfg = _apply_overrides(cfg, overrides or {})
    _validate(cfg)
    return cfg
