"""`tap` — a standalone LAN collector for smart-plug power data.

`tap` polls Kasa/Tapo smart plugs over the local network, buffers every reading
to disk, and streams them to a server over a WebSocket. It replaced juice's
cloud recorder, which reached TP-Link's cloud for data that never leaves the
building; since the 2026-09-16 cutover it is the only collector.

**`tap` imports nothing from `juice.*`, and must not.** It knows about hosts,
devices, outlets, watts, volts and amps, and how to hand them to a server. It
does not know what a machine, an asset tag, a circuit or a pinball table is; an
outlet alias is an opaque string it copies verbatim. If a change here needs any
of those concepts, the change belongs in juice instead.

The coupling surface is `tap.wire` — the message envelope — which is duplicated
on the server side rather than shared, so neither package can reach into the
other's internals to "just check one field". `tests/tap/test_isolation.py`
enforces the import rule; it is an invariant, not a convention.
"""

__version__ = "0.1.0"
