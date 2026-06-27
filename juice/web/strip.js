import { escapeHtml } from './format.js';
import { circuitLabel } from './circuit.js';

// Pure HTML builders for the strip page. Each takes its inputs as parameters
// (page state threaded in, not read from globals) and returns a string; the thin
// DOM glue in the template sets innerHTML and wires listeners. See web/README.md.

// The strip-title contents: display name (falling back to device_id), an optional
// "(alias: …)" hint when a custom name shadows a different Kasa alias, and the
// rename pencil (operator-only, hidden by .private-only in public mode).
export function buildStripHeader(strip) {
  const display = strip.display_name || strip.device_id;
  const aliasHint = (strip.name && strip.alias && strip.alias !== strip.name)
    ? `<span class="alias-hint">(alias: ${escapeHtml(strip.alias)})</span>` : '';
  return `
    <span id="strip-name">${escapeHtml(display)}</span>
    ${aliasHint}
    <button class="edit-name-btn private-only" title="Rename strip"
      onclick="startEditName()">&#9998;</button>`;
}

// The per-outlet power button (operator-only), mirroring the dashboard tile
// toggle (tiles.js) and the pcPowerButton blocked rule. Returns '' (no button)
// in public mode, when the strip or outlet is offline, or when the row has no
// controllable plug. A pending toggle renders a disabled neutral label so a
// stale readings/power_change tick can't flip it back; a lock that forbids the
// next transition renders a disabled "Locked".
function outletButton(strip, o, publicMode, pendingPlugs) {
  if (publicMode || strip.offline || o.power_status === 'offline' || o.plug_id == null) {
    return '';
  }
  const pend = pendingPlugs.get(o.plug_id);
  if (pend) {
    return `<button class="tile-toggle ${pend === 'turn_on' ? 'on' : 'off'}" disabled>`
      + `${pend === 'turn_on' ? 'Turning on&hellip;' : 'Turning off&hellip;'}</button>`;
  }
  const isOn = !!o.is_on;
  const blocked = (isOn && o.lock_mode === 'on') || (!isOn && o.lock_mode === 'off');
  if (blocked) {
    return `<button class="tile-toggle ${isOn ? 'off' : 'on'}" disabled>Locked</button>`;
  }
  return `<button class="tile-toggle ${isOn ? 'off' : 'on'}"
            onclick="togglePlug(event, ${o.plug_id}, ${isOn ? 'false' : 'true'})">
            ${isOn ? 'Turn Off' : 'Turn On'}
          </button>`;
}

// The outlet map rows. Empty → a no-data placeholder. Each row shows the outlet
// number, a status dot (offline / power_status / on-off), watts (or OFFLINE), the
// assigned machine link or the bare alias, and (operator-only) a power button.
// `pendingPlugs` is the page's plug_id → 'turn_on'|'turn_off' map; `publicMode`
// suppresses the controls. Both default so existing single-arg callers/tests work.
export function buildOutletRows(strip, { publicMode = false, pendingPlugs = new Map() } = {}) {
  if (!strip.outlets.length) {
    return '<div class="no-data">No outlets discovered</div>';
  }
  return strip.outlets.map(o => {
    const dot = strip.offline ? 'offline' : (o.power_status || (o.is_on ? 'on' : 'off'));
    const watts = o.watts != null ? o.watts.toFixed(1) + ' W' : '—';
    const what = o.machine
      ? `<a href="/machine/${o.plug_id}">${escapeHtml(o.machine.name)}</a>
         <span class="outlet-empty">(${escapeHtml(o.machine.asset_id)})</span>`
      : `<span class="outlet-empty">${escapeHtml(o.alias) || '—'}</span>`;
    return `
      <div class="outlet-row">
        <div class="outlet-num">${o.outlet_number ?? '·'}</div>
        <div class="outlet-dot ${dot}" title="${dot === 'no_draw' ? 'Outlet on — machine off, unplugged, or faulted' : ''}"></div>
        <div class="outlet-watts">${strip.offline ? 'OFFLINE' : watts}</div>
        <div class="outlet-machine">${what}</div>
        <div class="outlet-ctl private-only">${outletButton(strip, o, publicMode, pendingPlugs)}</div>
      </div>`;
  }).join('');
}

// The "Circuit: <link> <select>" assignment line. `mine` is the circuit this strip
// belongs to (if any); the select offers every circuit plus unassign / new options.
export function buildCircuitLine(allCircuits, deviceId) {
  const mine = allCircuits.find(c => (c.device_ids || []).includes(deviceId));
  const link = mine
    ? `<a href="/circuit/${mine.circuit_id}">${escapeHtml(circuitLabel(mine))}</a>`
    : '<span>none</span>';
  const opts = ['<option value="">— change circuit —</option>']
    .concat(allCircuits.map(c =>
      `<option value="${c.circuit_id}"${mine && c.circuit_id === mine.circuit_id ? ' selected' : ''}>`
      + `${escapeHtml(circuitLabel(c))}</option>`))
    .concat(['<option value="none">— unassigned —</option>',
             '<option value="new">+ New circuit…</option>']);
  return `Circuit: ${link} <select id="circuit-select">${opts.join('')}</select>`;
}
