import { escapeHtml } from './format.js';

// Group the flat machines list into per-strip rows, preserving first-seen order.
// Shared with the caller's sparkline-draw loop so the `spark-<idx>` canvas ids
// line up (idx is the position in this same strip-grouped, flattened order).
export function groupByStrip(machines) {
  const strips = [];
  const stripMap = new Map();
  for (const m of machines) {
    const key = m.strip_device_id || '';
    if (!stripMap.has(key)) {
      const group = { deviceId: key, alias: m.strip_alias || 'Unknown Strip', machines: [] };
      stripMap.set(key, group);
      strips.push(group);
    }
    stripMap.get(key).machines.push(m);
  }
  return strips;
}

// Build the dashboard tile grid HTML. Pure: page state passed in (publicMode,
// pendingPlugs Map). The caller sets #content innerHTML, then draws sparklines
// into the `spark-<idx>` canvases this emits (idx = flattened machine position).
export function buildTiles(strips, outlets, { publicMode, pendingPlugs }) {
  let html = '';
  let idx = 0;
  for (const strip of strips) {
    // Public viewers don't see strip names — render the tiles without a
    // group label so we don't leak "Strip 1 / Strip 2" or fall back to a
    // placeholder "Unknown Strip".
    const stripLabel = publicMode
      ? ''
      : (strip.deviceId
          ? `<a class="strip-label" href="/strip/${encodeURIComponent(strip.deviceId)}">${escapeHtml(strip.alias)}</a>`
          : `<div class="strip-label">${escapeHtml(strip.alias)}</div>`);
    html += `<div class="strip-row">${stripLabel}<div class="tiles">`;
    for (const m of strip.machines) {
      const plugId = m.plug ? m.plug.plug_id : 0;
      const offline = !!m.offline;
      if (m.has_emeter === false) {
        // Simplified tile for no-emeter machines (e.g. EP10-backed).
        const isOn = !!m.is_on;
        const dotState = offline ? 'OFFLINE' : (isOn ? 'PLAYING' : 'OFF');
        // No control over an unreachable plug; hide the toggle when offline.
        // While an action is pending, show a disabled neutral label so a stale
        // readings tick can't flip it back.
        const pend = pendingPlugs.get(plugId);
        const toggleBtn = (publicMode || offline) ? ''
          : pend
            ? `<button class="tile-toggle ${pend === 'turn_on' ? 'on' : 'off'}" disabled>${pend === 'turn_on' ? 'Turning on&hellip;' : 'Turning off&hellip;'}</button>`
            : `<button class="tile-toggle ${isOn ? 'off' : 'on'}"
                 onclick="togglePlug(event, ${plugId}, ${isOn ? 'false' : 'true'})">
                 ${isOn ? 'Turn Off' : 'Turn On'}
               </button>`;
        const body = offline
          ? `<div class="tile-offline">OFFLINE</div>`
          : `<div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>${toggleBtn}`;
        html += `
          <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${dotState}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
              ${m.lock_mode ? `<span class="tile-lock" title="Locked ${m.lock_mode}">&#128274;</span>` : ''}
            </div>
            ${body}
          </a>`;
      } else {
        const noDraw = m.power_status === 'no_draw';
        // Dot reflects power_status: energized-but-idle ('no_draw') is amber and
        // distinct from a plain OFF outlet; a drawing machine keeps its classifier
        // substate (PLAYING/ATTRACT/IDLE).
        const st = offline ? 'OFFLINE'
          : noDraw ? 'NO_DRAW'
          : (m.power_status === 'off' ? 'OFF' : (m.state || 'null'));
        const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
        const body = offline
          ? `<div class="tile-offline">OFFLINE</div>`
          : `<div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
             <div class="tile-watts">${watts}</div>
             ${noDraw ? '<div class="tile-note" title="Outlet on — machine off, unplugged, or faulted">outlet on · no draw</div>' : ''}`;
        html += `
          <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${st}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
              ${m.lock_mode ? `<span class="tile-lock" title="Locked ${m.lock_mode}">&#128274;</span>` : ''}
            </div>
            ${body}
          </a>`;
      }
      idx++;
    }
    html += '</div></div>';
  }

  // Outlets section: unassigned no-emeter outlets (e.g. snack machine).
  if (outlets && outlets.length) {
    html += '<div class="strip-row outlets-section"><div class="strip-label">Outlets</div><div class="tiles">';
    for (const o of outlets) {
      const isOn = !!o.is_on;
      const pend = pendingPlugs.get(o.plug_id);
      const toggleBtn = pend
        ? `<button class="tile-toggle ${pend === 'turn_on' ? 'on' : 'off'}" disabled>${pend === 'turn_on' ? 'Turning on&hellip;' : 'Turning off&hellip;'}</button>`
        : `<button class="tile-toggle ${isOn ? 'off' : 'on'}"
            onclick="togglePlug(event, ${o.plug_id}, ${isOn ? 'false' : 'true'})">
            ${isOn ? 'Turn Off' : 'Turn On'}
          </button>`;
      html += `
        <div class="tile outlet-tile">
          <div class="outlet-alias">${escapeHtml(o.alias)}</div>
          <div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>
          ${toggleBtn}
        </div>`;
    }
    html += '</div></div>';
  }
  return html;
}
