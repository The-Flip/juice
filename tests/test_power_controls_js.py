"""Unit tests for the detail-page power-control state machine.

The flaky bit of the UI is the client-side decision of *what the power button
should show* as relay readings stream in during turn-on / turn-off / reboot.
Rather than mock a browser, we extract the two pure deciders that ship in
``DETAIL_HTML`` (``pcReduceReading`` + ``pcPowerButton``, delimited by sentinel
comments) and exercise the exact source under Node. This pins the behaviour that
caused the reboot "flicker" (button flipping Turn Off → Turn On → Turn Off after
a reboot) so it can't regress.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

_SERVER = Path(__file__).resolve().parent.parent / "juice" / "server.py"
_NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(_NODE is None, reason="node not available to run the JS deciders")


def _state_machine_js() -> str:
    src = _SERVER.read_text()
    m = re.search(r"// __PC_STATE_MACHINE_START__(.*?)// __PC_STATE_MACHINE_END__", src, re.S)
    assert m, "power-control state-machine sentinels not found in server.py"
    return m.group(1)


# A faithful, timer-free replay of the client flow: a click seeds `pending`, then
# each relay reading runs through pcReduceReading; we record the button after each
# step. `reboot_start` mirrors the SSE event that puts non-clicking viewers into
# the reboot pending state.
_HARNESS = """
function simulate(initialRelayOn, steps) {
  let pending = null;
  let relayOn = initialRelayOn;
  const out = [];
  function render() {
    const pb = pcPowerButton(relayOn, false, null, pending);
    out.push({ label: pb.label, disabled: pb.disabled });
  }
  for (const s of steps) {
    if (s.kind === 'click') pending = { action: s.action, sawOff: false };
    else if (s.kind === 'reboot_start') { if (!pending) pending = { action: 'reboot', sawOff: false }; }
    else if (s.kind === 'abort') pending = null;
    else if (s.kind === 'reading') { relayOn = s.relayOn; pending = pcReduceReading(pending, relayOn); }
    render();
  }
  return out;
}
const SCENARIOS = JSON.parse(process.argv[1]);
const result = {};
for (const [name, spec] of Object.entries(SCENARIOS)) {
  result[name] = simulate(spec.initialRelayOn, spec.steps);
}
process.stdout.write(JSON.stringify(result));
"""


def _run(scenarios: dict) -> dict:
    script = _state_machine_js() + "\n" + _HARNESS
    # Trusted input: `node` resolved from PATH, script built from our own source.
    proc = subprocess.run(  # noqa: S603
        [_NODE, "-e", script, json.dumps(scenarios)],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,  # a regressed harness/infinite loop fails fast instead of hanging CI
    )
    return json.loads(proc.stdout)


def _labels(steps: list[dict]) -> list[tuple[str, bool]]:
    """Collapse consecutive identical (label, disabled) renders into a sequence
    of distinct visible states — that's what the user actually perceives."""
    seq: list[tuple[str, bool]] = []
    for s in steps:
        cur = (s["label"], s["disabled"])
        if not seq or seq[-1] != cur:
            seq.append(cur)
    return seq


# Reboot relay timeline: ON (still on at start) → OFF (hold) → OFF → ON (back).
_REBOOT_STEPS = [
    {"kind": "click", "action": "reboot"},
    {"kind": "reboot_start"},
    {"kind": "reading", "relayOn": True},
    {"kind": "reading", "relayOn": False},
    {"kind": "reading", "relayOn": False},
    {"kind": "reading", "relayOn": True},
    {"kind": "reading", "relayOn": True},
]


def test_reboot_holds_disabled_then_settles_on_turn_off() -> None:
    """The reported bug: button must go Rebooting… (disabled) → Turn Off, with NO
    intermediate flip to an enabled Turn Off/Turn On while the relay cycles."""
    out = _run({"reboot": {"initialRelayOn": True, "steps": _REBOOT_STEPS}})["reboot"]
    seq = _labels(out)
    # Exactly two visible states, in order — the precise behaviour the user asked for.
    assert seq == [("Rebooting…", True), ("Turn Off", False)], seq
    # The bad sequence the user saw (…, Turn Off, Turn On, Turn Off) is impossible:
    enabled = [lbl for lbl, dis in seq if not dis]
    assert enabled == ["Turn Off"]


def test_reboot_does_not_settle_while_relay_never_drops() -> None:
    """If the relay is never observed to go off, reboot stays pending (guards the
    premature-settle bug where the pre-off 'on' reading settled it instantly)."""
    steps = [{"kind": "click", "action": "reboot"}] + [
        {"kind": "reading", "relayOn": True} for _ in range(5)
    ]
    out = _run({"r": {"initialRelayOn": True, "steps": steps}})["r"]
    assert all(s["disabled"] and s["label"] == "Rebooting…" for s in out), out


def test_turn_on_disabled_until_relay_on() -> None:
    steps = [
        {"kind": "click", "action": "turn_on"},
        {"kind": "reading", "relayOn": False},  # stale pre-relay tick
        {"kind": "reading", "relayOn": True},  # settles
    ]
    out = _run({"on": {"initialRelayOn": False, "steps": steps}})["on"]
    assert _labels(out) == [("Turning on…", True), ("Turn Off", False)]


def test_turn_off_disabled_until_relay_off_and_no_post_settle_flip() -> None:
    steps = [
        {"kind": "click", "action": "turn_off"},
        {"kind": "reading", "relayOn": True},  # stale pre-relay tick
        {"kind": "reading", "relayOn": False},  # settles
        {"kind": "reading", "relayOn": False},  # later tick must not flip it
    ]
    out = _run({"off": {"initialRelayOn": True, "steps": steps}})["off"]
    assert _labels(out) == [("Turning off…", True), ("Turn On", False)]
