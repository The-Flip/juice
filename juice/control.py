"""What a power handler needs from a plug object, and nothing else.

`RecorderState.plug_objects` holds one of these per outlet. The cloud collector
puts a `juice.collector.Plug` (or `_SelfPlug`) there; the tap collector a
`juice.collector_tap.TapPlug`; the e2e fixture a fake. The handlers -- power,
reboot, all-on/all-off, the overload shutdown -- only ever call `turn_on()` /
`turn_off()` through `call_with_retry` and read `.alias` for the log line, so
that is the whole contract. Kept as a protocol in a module that names no
collector, because the handlers must not care which one is on duty.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Controllable(Protocol):
    alias: str

    async def turn_on(self) -> None: ...

    async def turn_off(self) -> None: ...
