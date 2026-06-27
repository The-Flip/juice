// Power-control action state machine (pure logic, shared by the machine-detail
// page). `pending` is null when idle; while a turn-on / turn-off / reboot is in
// flight it's { action, sawOff } and every control renders disabled with a
// neutral in-progress label.
//
// The settle rule is the whole point: the authoritative `readings` relay stream
// is what CLEARS a pending action — never the server's transient `reboot`/
// `power_change` events. So the render that drops `pending` always uses a real,
// current relay reading and can't flicker to a stale value afterwards. Reboot
// settles only after the relay is observed to go off and THEN back on (sawOff);
// settling on a plain relay-match would fire prematurely on the pre-off "on".
//
// These are pure + dependency-free; unit-tested in power.test.js and inlined into
// DETAIL_HTML at serve time (the `export ` is stripped — see juice/web/README.md).

export function pcReduceReading(pending, relayOn) {
  if (!pending) return null;
  if (pending.action === 'turn_on') return relayOn ? null : pending;
  if (pending.action === 'turn_off') return relayOn ? pending : null;
  // reboot: settle once the relay reads on, but only after the off→on cycle has
  // actually run — observed (sawOff) or confirmed by the server's authoritative
  // reboot `on` event (onConfirmed, set via pcConfirmRebootOn). Without that gate
  // the pre-off "on" reading settles prematurely; with sawOff alone, a missed or
  // late OFF in the ~1Hz cloud-sysinfo relay stream hangs the button until the
  // pending timeout fires.
  if (pending.sawOff || pending.onConfirmed) return relayOn ? null : pending;
  return relayOn ? pending : { ...pending, sawOff: true };
}

// Mark a pending reboot as server-confirmed-on. The server opens a watch window
// for a fresh relay reading and emits a reboot `on` event once the power-on
// lands; this only
// *enables* the settle — the actual clear still needs a real relayOn reading, so
// the button can't flicker to a stale value (see pcReduceReading).
export function pcConfirmRebootOn(pending) {
  if (pending && pending.action === 'reboot') return { ...pending, onConfirmed: true };
  return pending;
}

export function pcPowerButton(relayOn, offline, lockMode, pending) {
  if (pending) {
    const label = pending.action === 'turn_on' ? 'Turning on…'
      : pending.action === 'turn_off' ? 'Turning off…' : 'Rebooting…';
    const cls = pending.action === 'turn_on' ? 'btn-power-on'
      : pending.action === 'turn_off' ? 'btn-power-off' : 'btn-reboot';
    return { label: label, cls: cls, disabled: true, action: null };
  }
  if (offline) return { label: 'Offline', cls: 'btn-power-off', disabled: true, action: null };
  const blocked = (relayOn && lockMode === 'on') || (!relayOn && lockMode === 'off');
  if (blocked) {
    return { label: 'Locked', cls: relayOn ? 'btn-power-off' : 'btn-power-on',
             disabled: true, action: null };
  }
  return { label: relayOn ? 'Turn Off' : 'Turn On',
           cls: relayOn ? 'btn-power-off' : 'btn-power-on',
           disabled: false, action: relayOn ? 'turn_off' : 'turn_on' };
}
