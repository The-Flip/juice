import { escapeHtml } from './format.js';
import { pcPowerButton } from './power.js';

// Build the machine-detail meta-bar inner HTML: the live state badge, the
// no-draw hint, the lock badge, and the action buttons. The numeric readouts and
// the plug/strip/calibration/cost rows live in a separate Details table below the
// outlet map (buildDetailStats). Pure: page state is passed in (publicMode,
// pending) and the result is a string the caller sets as #meta-bar innerHTML. The
// onclick handlers reference page-global functions (togglePower/rebootMachine/
// toggleLock/calibrate) resolved at click time.
export function buildMeta(m, { publicMode, pending }) {
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
    ${actions}
  `;
}

// Build the machine-detail "Details" table (below the outlet map): the numeric
// electrical readouts + peak + asset for everyone, and plug/strip, calibration
// thresholds, and 30-day average daily cost for operators. Pure: page state is
// passed in (publicMode, peakWatts, avgDailyCost) so the live cells re-render on
// each SSE tick, and the aggregates (peak, cost) fill in when their fetches land.
export function buildDetailStats(m, { publicMode, peakWatts, avgDailyCost }) {
  const noEmeter = m.has_emeter === false;
  const watts = m.power ? m.power.watts.toFixed(1) + ' W' : (noEmeter ? 'no data' : '—');
  const volts = m.power ? m.power.voltage.toFixed(1) + ' V' : '—';
  const amps = m.power ? m.power.amps.toFixed(3) + ' A' : '—';
  const kwh = m.power ? m.power.total_kwh.toFixed(1) + ' kWh' : '—';
  const peak = peakWatts != null ? peakWatts.toFixed(1) + ' W' : '—';

  const rows = [
    ['Watts', watts],
    ['Voltage', volts],
    ['Amps', amps],
    ['Total energy', kwh],
    ['Peak (30d)', peak],
    ['Asset', escapeHtml(m.asset_id)],
  ];

  if (!publicMode) {
    const plugNum = m.plug && m.plug.outlet_number != null ? m.plug.outlet_number : null;
    const plugLabel = m.plug
      ? (plugNum != null ? `#${plugNum} — ${escapeHtml(m.plug.alias)}` : escapeHtml(m.plug.alias))
      : '—';
    const stripLabel = m.plug && m.plug.device_id
      ? `<a href="/strip/${encodeURIComponent(m.plug.device_id)}">${escapeHtml(m.strip_alias || '—')}</a>`
      : escapeHtml(m.strip_alias || '—');
    // m.calibration is {idle_max_rsd, play_min_rsd} or null (uncalibrated). Show
    // the play threshold and, when set, the idle one; else say "uncalibrated".
    const cal = m.calibration;
    const calLabel = cal
      ? `play &ge; ${cal.play_min_rsd.toFixed(1)}%`
        + (cal.idle_max_rsd != null ? `, idle &le; ${cal.idle_max_rsd.toFixed(1)}%` : '')
      : 'uncalibrated';
    const costLabel = avgDailyCost != null ? '$' + avgDailyCost.toFixed(2) + '/day' : '—';
    rows.push(['Plug', plugLabel]);
    rows.push(['Strip', stripLabel]);
    rows.push(['Calibration', calLabel]);
    rows.push(['Avg daily cost (30d)', costLabel]);
  }

  const body = rows
    .map(([k, v]) => `<tr><th>${k}</th><td>${v}</td></tr>`)
    .join('');
  return `<div class="detail-stats-header">Details</div>`
    + `<table class="detail-stats-table"><tbody>${body}</tbody></table>`;
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
