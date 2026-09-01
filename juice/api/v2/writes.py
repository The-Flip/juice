"""Power writes for /api/v2: 202 + a command the client can follow.

The contract, from status_vocabulary.md and the command registry:

    202 means "we have started talking to the device", NOT "we accepted the
    request".

That distinction is load-bearing. Anything checkable *without* a WAN round-trip
stays synchronous — an unknown machine, an ambiguous asset tag, a lock, a plug we
cannot control, a command already in flight. If those became a `failed` phase on
the stream, the operator would tap, get an optimistic pending state, and learn
300 ms later on a different channel that nothing was ever going to happen. That
is strictly worse than v1's synchronous 409.

Only the cloud call itself is asynchronous, because only it is genuinely slow
(4-30 s), and `confirmed` is decided by a corroborating relay reading rather than
by the call returning.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2 import errors
from juice.commands import Command, timeout_ms_for
from juice.identity import Resolution, resolve_asset


def _resolve(request: web.Request) -> tuple[Resolution | None, web.Response | None]:
    """Find the outlet an asset_id is on, or the response explaining why not."""
    state = request.app["recorder_state"]
    asset_id = request.match_info["asset_id"]
    resolution = resolve_asset(state, asset_id)

    if not resolution.found:
        return None, errors.error(
            404, errors.UNKNOWN_MACHINE, f"no machine with asset id {asset_id}"
        )
    if resolution.ambiguous:
        # Two live outlets carry the same tag — a Kasa label typo. Acting on
        # either would be a coin flip on someone's machine.
        return None, errors.error(
            409,
            errors.AMBIGUOUS_ASSIGNMENT,
            f"{asset_id} is claimed by more than one online outlet — fix the Kasa label",
            candidates=list(resolution.candidates),
        )
    return resolution, None


def _accepted(command: Command) -> web.Response:
    """The 202 envelope: what to watch, where, and for how long."""
    return web.json_response(
        {
            "command_id": command.id,
            "kind": command.kind,
            "expect": {"relay": "on" if command.expect_relay else "off"},
            "timeout_ms": timeout_ms_for(command.kind),
            "stream": "/api/v2/stream",
            "terminal_phases": ["confirmed", "failed", "timed_out", "refused", "superseded"],
        },
        status=202,
    )


def _precheck(state, plug_id: int, kind: str, asset_id: str) -> web.Response | None:
    """Everything refusable without touching the network.

    Returned synchronously so the UI can say why immediately, instead of showing
    a pending spinner for something that was never going to be attempted.
    """
    if state.plug_objects.get(plug_id) is None:
        return errors.error(
            409, errors.NOT_CONTROLLABLE, "this outlet cannot be controlled right now"
        )

    mode = state.lock_modes.get(asset_id)
    if mode is not None and (kind == "reboot" or (kind == "turn_on") != (mode == "on")):
        verb = "shutdown-locked" if mode == "on" else "startup-locked"
        return errors.error(
            409, errors.MACHINE_LOCKED, f"{asset_id} is {verb} — unlock it first", lock_mode=mode
        )

    conflict = state.commands.conflicts(plug_id, kind)
    if conflict is not None:
        # Two operators converging on one machine is the normal case, not the
        # edge case (user_needs J6). Say who holds it so the UI can offer to
        # watch rather than racing a second cloud call at the device.
        return errors.error(
            409,
            errors.COMMAND_IN_FLIGHT,
            f"a {conflict.kind} is already in flight for this machine",
            command={
                "command_id": conflict.id,
                "kind": conflict.kind,
                "phase": conflict.phase,
                "actor": conflict.actor,
                "attempt": conflict.attempt,
            },
        )
    return None


async def _body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


@access(Access.CONTROL)
async def handle_power(request: web.Request) -> web.Response:
    """POST /api/v2/machines/{asset_id}/power — {"on": bool}."""
    from juice.server import handle_power as v1_handle_power

    resolution, failure = _resolve(request)
    if failure is not None:
        return failure
    assert resolution is not None and resolution.plug_id is not None

    state = request.app["recorder_state"]
    body = await _body(request)
    on = body.get("on", True)
    if not isinstance(on, bool):
        return errors.error(400, errors.BAD_REQUEST, "'on' must be a boolean")

    kind = "turn_on" if on else "turn_off"
    refusal = _precheck(state, resolution.plug_id, kind, resolution.asset_id)
    if refusal is not None:
        return refusal

    return await _delegate(request, v1_handle_power, resolution.plug_id, kind, body={"on": on})


@access(Access.CONTROL)
async def handle_reboot(request: web.Request) -> web.Response:
    """POST /api/v2/machines/{asset_id}/reboot."""
    from juice.server import handle_reboot as v1_handle_reboot

    resolution, failure = _resolve(request)
    if failure is not None:
        return failure
    assert resolution is not None and resolution.plug_id is not None

    state = request.app["recorder_state"]
    refusal = _precheck(state, resolution.plug_id, "reboot", resolution.asset_id)
    if refusal is not None:
        return refusal

    return await _delegate(request, v1_handle_reboot, resolution.plug_id, "reboot")


async def _delegate(
    request: web.Request,
    v1_handler,
    plug_id: int,
    kind: str,
    body: dict | None = None,
) -> web.Response:
    """Run the v1 handler, then answer in v2's vocabulary.

    Deliberately reuses v1's actuation rather than copying it: the retry policy,
    audit rows, watch window and v1 event publishing are all one implementation,
    so the two APIs cannot drift on what a power action actually *does*. Only the
    response shape differs — v1 answers 200 with `{"ok": ...}`, v2 answers 202
    with a command to follow.
    """
    state = request.app["recorder_state"]
    proxied = _ProxyRequest(request, plug_id, body)
    response = await v1_handler(proxied)

    if response.status >= 400:
        # v1's prose error, re-coded. The prechecks above catch the refusals we
        # can anticipate; this is the device call genuinely failing.
        return errors.error(
            response.status if response.status != 500 else 502,
            errors.NOT_CONTROLLABLE,
            errors.message_from_v1(response, "device call failed"),
        )

    command = state.commands.in_flight_for_plug(plug_id)
    if command is None or command.kind != kind:
        # Already in the requested state, or the command settled synchronously.
        return web.json_response({"ok": True, "already": True}, status=200)
    return _accepted(command)


class _ProxyRequest:
    """Presents a v2 request to a v1 handler keyed on plug_id.

    v1 handlers read `match_info['plug_id']`, `.app`, `.json()` and the user bag;
    v2 addresses machines by asset_id. Rather than duplicate the handlers, this
    adapts the identity and leaves the behaviour alone.
    """

    def __init__(self, request: web.Request, plug_id: int, body: dict | None) -> None:
        self._request = request
        self._body = body or {}
        self.app = request.app
        self.match_info = {"plug_id": str(plug_id)}
        self.query = request.query

    def get(self, key, default=None):
        return self._request.get(key, default)

    def __getitem__(self, key):
        return self._request[key]

    def __contains__(self, key):
        return key in self._request

    async def json(self):
        return self._body
