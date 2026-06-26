import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pcReduceReading, pcPowerButton } from './power.js';

// Faithful, timer-free replay of the client flow: a click seeds `pending`, then
// each relay reading runs through pcReduceReading; we record the button after
// each step. `reboot_start` mirrors the SSE event that puts non-clicking viewers
// into the reboot pending state.
function simulate(initialRelayOn, steps) {
  let pending = null;
  let relayOn = initialRelayOn;
  const out = [];
  const render = () => {
    const pb = pcPowerButton(relayOn, false, null, pending);
    out.push({ label: pb.label, disabled: pb.disabled });
  };
  for (const s of steps) {
    if (s.kind === 'click') pending = { action: s.action, sawOff: false };
    else if (s.kind === 'reboot_start') { if (!pending) pending = { action: 'reboot', sawOff: false }; }
    else if (s.kind === 'abort') pending = null;
    else if (s.kind === 'reading') { relayOn = s.relayOn; pending = pcReduceReading(pending, relayOn); }
    render();
  }
  return out;
}

// Collapse consecutive identical renders into the sequence of distinct visible
// states — that's what the user actually perceives.
function visibleStates(out) {
  const seq = [];
  for (const s of out) {
    const cur = [s.label, s.disabled];
    if (!seq.length || seq[seq.length - 1][0] !== cur[0] || seq[seq.length - 1][1] !== cur[1]) {
      seq.push(cur);
    }
  }
  return seq;
}

// Reboot relay timeline: ON (still on at start) → OFF (hold) → OFF → ON (back).
const REBOOT_STEPS = [
  { kind: 'click', action: 'reboot' },
  { kind: 'reboot_start' },
  { kind: 'reading', relayOn: true },
  { kind: 'reading', relayOn: false },
  { kind: 'reading', relayOn: false },
  { kind: 'reading', relayOn: true },
  { kind: 'reading', relayOn: true },
];

test('reboot holds disabled, then settles on Turn Off with no flicker', () => {
  // The reported bug: button must go Rebooting… (disabled) → Turn Off, never
  // flipping through an enabled Turn Off → Turn On → Turn Off while it cycles.
  const seq = visibleStates(simulate(true, REBOOT_STEPS));
  assert.deepEqual(seq, [['Rebooting…', true], ['Turn Off', false]]);
  const enabled = seq.filter(([, dis]) => !dis).map(([lbl]) => lbl);
  assert.deepEqual(enabled, ['Turn Off']);
});

test('reboot does not settle while the relay never drops', () => {
  // Guards the premature-settle bug where the pre-off "on" reading settled it.
  const steps = [{ kind: 'click', action: 'reboot' }];
  for (let i = 0; i < 5; i++) steps.push({ kind: 'reading', relayOn: true });
  const out = simulate(true, steps);
  assert.ok(out.every((s) => s.disabled && s.label === 'Rebooting…'));
});

test('turn on: disabled until the relay reads on', () => {
  const seq = visibleStates(simulate(false, [
    { kind: 'click', action: 'turn_on' },
    { kind: 'reading', relayOn: false }, // stale pre-relay tick
    { kind: 'reading', relayOn: true },  // settles
  ]));
  assert.deepEqual(seq, [['Turning on…', true], ['Turn Off', false]]);
});

test('turn off: disabled until relay off, no post-settle flip', () => {
  const seq = visibleStates(simulate(true, [
    { kind: 'click', action: 'turn_off' },
    { kind: 'reading', relayOn: true },  // stale pre-relay tick
    { kind: 'reading', relayOn: false }, // settles
    { kind: 'reading', relayOn: false }, // later tick must not flip it
  ]));
  assert.deepEqual(seq, [['Turning off…', true], ['Turn On', false]]);
});

test('pcPowerButton: offline and lock states are disabled and inert', () => {
  // server.py keys the click handler off `disabled`/`action`, so pin both —
  // a regression that re-enabled an offline/locked button must fail here.
  const offline = pcPowerButton(true, true, null, null);
  assert.deepEqual([offline.label, offline.disabled, offline.action], ['Offline', true, null]);
  const lockedOn = pcPowerButton(true, false, 'on', null);
  assert.deepEqual([lockedOn.label, lockedOn.disabled, lockedOn.action], ['Locked', true, null]);
  const lockedOff = pcPowerButton(false, false, 'off', null);
  assert.deepEqual([lockedOff.label, lockedOff.disabled, lockedOff.action], ['Locked', true, null]);
  const on = pcPowerButton(false, false, null, null);
  assert.deepEqual([on.label, on.disabled, on.action], ['Turn On', false, 'turn_on']);
});
