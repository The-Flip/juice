"""Shared plumbing for the two `python-kasa` adapters.

Everything that knows what `python-kasa` raises lives here or in the two
adapters; above this line the codebase sees only `tap.errors`.

The connection strategy is worth stating. A pinned device with an explicit
`family` gets a `DeviceConfig` built directly — no UDP, which is what makes tap
work under Docker's bridge network where broadcast never reaches the LAN.
Anything else is resolved with `Discover.discover_single`, a **unicast** probe
to one host that negotiates protocol and encryption for us rather than guessing.
"""

from __future__ import annotations

import base64
import binascii
import logging

from kasa import Credentials as KasaCredentials
from kasa import Device, DeviceConfig, Discover
from kasa.deviceconfig import (
    DeviceConnectionParameters,
    DeviceEncryptionType,
    DeviceFamily,
)
from kasa.exceptions import (
    AuthenticationError,
    KasaException,
    UnsupportedDeviceError,
    _ConnectionError,
)
from kasa.exceptions import (
    TimeoutError as KasaTimeoutError,
)

from tap.config import Credentials
from tap.device import Family
from tap.errors import DeviceAuthError, TransientError

log = logging.getLogger(__name__)

# Above SWEEP_BUDGET on purpose: `asyncio.timeout` around the sweep is the
# authority on how long a poll may take. This is the backstop for a socket the
# event loop cannot see, and for connect/discovery which are off the hot path.
TRANSPORT_TIMEOUT = 2

# python-kasa retries three times internally with a sleep between attempts
# (SmartProtocol._execute_query). Inside a sub-second sweep budget that turns
# one slow call into three; tap does its own retrying, at its own cadence.
NO_RETRY = 0


def translate(exc: BaseException) -> BaseException:
    """Map a python-kasa exception onto tap's taxonomy.

    Credential rejection is singled out because retrying it at poll cadence is
    how you get locked out of your own hardware.
    """
    if isinstance(exc, AuthenticationError):
        return DeviceAuthError(str(exc) or "device rejected our credentials")
    if isinstance(exc, KasaTimeoutError | _ConnectionError | TimeoutError | OSError):
        return TransientError(f"{type(exc).__name__}: {exc}")
    if isinstance(exc, UnsupportedDeviceError):
        return exc
    if isinstance(exc, KasaException):
        # Everything else from the library is a device- or protocol-level
        # complaint: worth another go next tick, not worth crashing over.
        return TransientError(f"{type(exc).__name__}: {exc}")
    return exc


def decode_alias(raw: str | None) -> str:
    """Decode a SMART device's base64 `nickname` (`VGFwbyBQMzE2TV8x` -> "Tapo P316M_1").

    **Only for SMART devices.** IOT devices send `alias` in the clear and must
    not be passed through here: the decision has to come from the device family,
    never from the shape of the string. Plenty of plain aliases are also valid
    base64 — every four-character `M###` asset tag is, and `M000` would silently
    decode to `3M4`. That is the exact field the server keys machine assignment
    on, so a wrong guess breaks it.

    An undecodable value is returned unchanged rather than mangled; that is a
    safety net for odd firmware, not a way to guess the encoding.
    """
    if not raw:
        return ""
    try:
        decoded = base64.b64decode(raw, validate=True)
    except binascii.Error, ValueError:
        return raw
    try:
        text = decoded.decode()
    except UnicodeDecodeError:
        return raw
    # Round-trip check: base64 tolerates some inputs it did not produce.
    if base64.b64encode(decoded).decode() != raw:
        return raw
    return text


_CONNECTION_PARAMS = {
    Family.SMART: DeviceConnectionParameters(
        DeviceFamily.SmartTapoPlug, DeviceEncryptionType.Klap, login_version=2, https=False
    ),
    Family.IOT: DeviceConnectionParameters(
        DeviceFamily.IotSmartPlugSwitch, DeviceEncryptionType.Xor
    ),
}


def _kasa_credentials(credentials: Credentials | None) -> KasaCredentials | None:
    if credentials is None:
        return None
    return KasaCredentials(credentials.username, credentials.password)


async def connect(
    host: str,
    *,
    family: Family,
    credentials: Credentials | None,
    timeout: float = TRANSPORT_TIMEOUT,
) -> Device:
    """Open a device connection, negotiating the protocol when family is AUTO."""
    creds = _kasa_credentials(credentials)
    try:
        if family is Family.AUTO:
            return await Discover.discover_single(host, credentials=creds, timeout=int(timeout))
        config = DeviceConfig(
            host=host,
            credentials=creds,
            timeout=int(timeout),
            connection_type=_CONNECTION_PARAMS[family],
        )
        return await Device.connect(config=config)
    except BaseException as e:
        raise translate(e) from e


async def probe_family(
    host: str, *, credentials: Credentials | None, timeout: float = TRANSPORT_TIMEOUT
) -> tuple[Family, str]:
    """Ask one host which protocol it speaks. Returns (family, model).

    A unicast discovery probe, so it works where a broadcast does not — which is
    every Docker bridge network. Used once per device, when the config says
    `family = "auto"`; after that the answer is cached for the device's life.
    """
    device = await connect(host, family=Family.AUTO, credentials=credentials, timeout=timeout)
    try:
        return family_of(device), device.model or ""
    finally:
        try:
            await device.disconnect()
        except Exception as e:  # noqa: BLE001 — probing must never raise
            log.debug("%s: probe disconnect failed: %s", host, e)


def family_of(device: Device) -> Family:
    """Which protocol family a connected device turned out to speak."""
    value = device.config.connection_type.device_family.value
    return Family.SMART if value.startswith("SMART.") else Family.IOT
