import { escapeHtml } from './format.js';
import { pcPowerButton } from './power.js';

// Build the machine-detail meta-bar inner HTML (state badge, numeric readouts,
// plug/strip rows, and the action buttons). Pure: page state is passed in
// (publicMode, pending, peakWatts) and the result is a string the caller sets as
// #meta-bar innerHTML. The onclick handlers reference page-global functions
// (togglePower/rebootMachine/toggleLock/calibrate) resolved at click time.
export function buildMeta(m, { publicMode, pending, peakWatts }) {
  const noEmeter = m.has_emeter === false;
  const offline = m.power_status === 'offline';
  const noDraw = m.power_status === 'no_draw';
  // For control, "on" means the outlet relay is energized (incl. the no-draw
  // case). Use the explicit relay field (m.is_on) rather than re-deriving it from
  // power_status; gate on !offline since a stale relay flag shouldn't imply control
  // of an unreachable plug. Equivalent to the old `power_status === 'on' || noDraw`.
  const relayOn = !!m.is_on && !offline;
  const badgeState = offline ? 'OFFLINE'
    : noDraw ? 'NO_DRAW'
    : (noEmeter ? (relayOn ? 'PLAYING' : 'OFF') : (m.state || 'OFF'));
  const badgeLabel = offline ? 'OFFLINE'
    : noDraw ? 'No draw'
    : (noEmeter ? (relayOn ? 'ON' : 'OFF') : (m.state || 'OFF'));
  const watts = m.power ? m.power.watts.toFixed(1) + ' W' : (noEmeter ? 'no data' : '--');
  const volts = m.power ? m.power.voltage.toFixed(1) + ' V' : (noEmeter ? '--' : '--');
  const amps = m.power ? m.power.amps.toFixed(3) + ' A' : (noEmeter ? '--' : '--');
  const kwh = m.power ? m.power.total_kwh.toFixed(1) + ' kWh' : (noEmeter ? '--' : '--');

  // Public viewers don't see plug/strip names or any controls.
  const plugNum = m.plug && m.plug.outlet_number != null ? m.plug.outlet_number : null;
  const plugLabel = m.plug
    ? (plugNum != null ? `#${plugNum} — ${escapeHtml(m.plug.alias)}` : escapeHtml(m.plug.alias))
    : '--';
  const stripLabel = m.plug && m.plug.device_id
    ? `<a href="/strip/${encodeURIComponent(m.plug.device_id)}">${escapeHtml(m.strip_alias || '--')}</a>`
    : escapeHtml(m.strip_alias || '--');
  const plugStripRows = publicMode ? '' :
    `<div class="meta-item">Plug <span class="val">${plugLabel}</span></div>
     <div class="meta-item">Strip <span class="val">${stripLabel}</span></div>`;
  // Every control is disabled while an action is pending. The power button's
  // label/colour/disabled come straight from the pure pcPowerButton decider so
  // the shipped logic is exactly what the unit tests exercise.
  const isPending = !publicMode && pending !== null;
  const pb = pcPowerButton(relayOn, offline, m.lock_mode, isPending ? pending : null);

  const calButton = (publicMode || noEmeter)
    ? ''
    : `<button class="btn btn-calibrate" id="cal-btn"${isPending ? ' disabled' : ' onclick="calibrate()"'}>${m.calibrated ? 'Recalibrate' : 'Calibrate'}</button>`;
  const pbTitle = pb.disabled
    ? (isPending ? pb.label : offline ? 'Device offline' : 'Unlock to change power')
    : ('Turn the machine ' + (pb.action === 'turn_on' ? 'on' : 'off'));
  const powerButton = publicMode
    ? ''
    : `<button class="btn ${pb.cls}" id="power-btn"${pb.disabled ? ' disabled' : ` onclick="togglePower(${pb.action === 'turn_on'})"`} title="${pbTitle}">${pb.label}</button>`;
  const lockButton = publicMode
    ? ''
    : `<button class="btn btn-lock${m.locked ? ' locked' : ''}" id="lock-btn"${isPending ? ' disabled' : ` onclick="toggleLock(${m.locked ? 'false' : 'true'})"`}>${m.locked ? '&#128275; Unlock' : '&#128274; Lock'}</button>`;
  // Reboot (power-cycle) is ALWAYS rendered so the row never reflows; it's just
  // disabled unless the machine is reachable, on, unlocked, and idle.
  const rebootDisabled = isPending || offline || !relayOn || !!m.lock_mode;
  const rebootTitle = isPending ? 'Action in progress'
    : offline ? 'Device offline'
    : !relayOn ? 'Turn on before rebooting'
    : m.lock_mode ? 'Unlock to reboot'
    : 'Power-cycle this machine';
  const rebootButton = publicMode
    ? ''
    : `<button class="btn btn-reboot" id="reboot-btn"${rebootDisabled ? ' disabled' : ' onclick="rebootMachine()"'} title="${rebootTitle}">Reboot</button>`;
  const actions = (powerButton || rebootButton || lockButton || calButton)
    ? `<div class="actions">${powerButton}${rebootButton}${lockButton}${calButton}</div>`
    : '';
  const lockBadge = m.lock_mode === 'on'
    ? '<div class="lock-badge" title="Locked on">&#128274; Locked on</div>'
    : m.lock_mode === 'off'
      ? '<div class="lock-badge" title="Locked off">&#128274; Locked off</div>'
      : '';
  return `
    <div class="state-badge state-${badgeState}"><div class="dot"></div>${badgeLabel}</div>
    ${noDraw ? '<span class="no-draw-hint">Outlet on — machine off, unplugged, or faulted</span>' : ''}
    ${lockBadge}
    <div class="meta-item num"><span class="val">${watts}</span></div>
    <div class="meta-item num"><span class="val">${volts}</span></div>
    <div class="meta-item num"><span class="val">${amps}</span></div>
    <div class="meta-item">Total <span class="val">${kwh}</span></div>
    <div class="meta-item num">Peak <span class="val">${peakWatts != null ? peakWatts.toFixed(1) + ' W' : '&mdash;'}</span></div>
    <div class="meta-item">Asset <span class="val">${escapeHtml(m.asset_id)}</span></div>
    ${plugStripRows}
    ${actions}
  `;
}

// The detail page's strip-outlet map. Same shape as the strip page's outlet rows
// (juice/web/strip.js) but highlights the current machine's row: its name is a
// plain <span> (not a self-link) and it gets a `current` class + a "this machine"
// tag. plugId (the page's current plug) is threaded in. Pure: string out.

// The "Plug N of M on <strip link>" caption above the rows.
export function buildOutletMapHeader(strip, plugId) {
  const mine = strip.outlets.find(o => o.plug_id === plugId);
  const n = mine && mine.outlet_number != null ? mine.outlet_number : '?';
  return `Plug ${n} of ${strip.outlets.length} on ` +
    `<a href="/strip/${encodeURIComponent(strip.device_id)}">${escapeHtml(strip.display_name || strip.device_id)}</a>`;
}

export function buildDetailOutletRows(strip, plugId) {
  return strip.outlets.map(o => {
    const dot = strip.offline ? 'offline' : (o.power_status || (o.is_on ? 'on' : 'off'));
    const watts = o.watts != null ? o.watts.toFixed(1) + ' W' : '—';
    const what = o.machine
      ? (o.plug_id === plugId
          ? `<span>${escapeHtml(o.machine.name)}</span>`
          : `<a href="/machine/${o.plug_id}">${escapeHtml(o.machine.name)}</a>`)
      : `<span class="outlet-empty">${escapeHtml(o.alias) || '—'}</span>`;
    const current = o.plug_id === plugId;
    return `
      <div class="outlet-row${current ? ' current' : ''}">
        <div class="outlet-num">${o.outlet_number ?? '·'}</div>
        <div class="outlet-dot ${dot}" title="${dot === 'no_draw' ? 'Outlet on — machine off, unplugged, or faulted' : ''}"></div>
        <div class="outlet-watts">${strip.offline ? 'OFFLINE' : watts}</div>
        <div class="outlet-machine">${what}</div>
        ${current ? '<span class="outlet-this">this machine</span>' : ''}
      </div>`;
  }).join('');
}
