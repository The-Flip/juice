import { escapeHtml } from './format.js';

// Pure helpers for the /air page: sensor ordering, the museum closed-hours
// backdrop, and the chip/legend HTML builders. Inlined via the JS_AIR marker —
// see juice/web/README.md. Page state (selected sets, the METRICS/RANGES config,
// colorFor) is threaded in as parameters so these stay pure + testable. (colorFor,
// sensorName, bandClass themselves stay inline: they read page state / METRICS.)

// Canonical sensor display order: front, back, workshop, then anything else.
const SENSOR_ORDER = ['front', 'back', 'workshop'];

export function sensorRank(s) {
  const n = ((s && s.name) || '').toLowerCase();
  const i = SENSOR_ORDER.findIndex((k) => n.includes(k));
  return i < 0 ? SENSOR_ORDER.length : i;
}

export function roleOf(s) {
  const n = ((s && s.name) || '').toLowerCase();
  return SENSOR_ORDER.find((k) => n.includes(k)) || null;  // 'front'|'back'|'workshop'|null
}

export function orderSensors(list) {
  return list.slice().sort((a, b) =>
    sensorRank(a) - sensorRank(b) || (a.name || '').localeCompare(b.name || ''));
}

// Local-time hours The Flip is open; closed spans get a light backdrop on charts.
const OPEN_HOURS = { 0: [11, 18] };  // Sunday
const DEFAULT_OPEN = [10, 20];       // Mon–Sat

// The [closed-start, closed-end) spans within [t0, t1), clamped to that window.
// Each day contributes the before-open and after-close spans (in local time).
export function closedIntervals(t0, t1) {
  const out = [];
  const d = new Date(t0); d.setHours(0, 0, 0, 0);
  while (d < t1) {
    const [oh, ch] = OPEN_HOURS[d.getDay()] || DEFAULT_OPEN;
    const dayStart = new Date(d);
    const openStart = new Date(d); openStart.setHours(oh, 0, 0, 0);
    const openEnd = new Date(d); openEnd.setHours(ch, 0, 0, 0);
    const nextDay = new Date(d); nextDay.setDate(nextDay.getDate() + 1);
    out.push([dayStart, openStart], [openEnd, nextDay]);
    d.setTime(nextDay.getTime());
  }
  return out
    .map(([a, b]) => [new Date(Math.max(+a, +t0)), new Date(Math.min(+b, +t1))])
    .filter(([a, b]) => b > a);
}

// Metric/range selector chips and the chart legend. Caller wires the click
// listeners after setting innerHTML.
export function buildMetricChips(primary, selectedMetrics, metrics) {
  return primary.map((k) => {
    const on = selectedMetrics.has(k);
    return `<button class="chip ${on ? 'active' : ''}" role="button" aria-pressed="${on}"`
      + ` data-metric="${k}">${metrics[k].label}</button>`;
  }).join('');
}

export function buildRangeChips(ranges, rangeDays) {
  return ranges.map((r) => {
    const on = r.days === rangeDays;
    return `<button class="chip ${on ? 'active' : ''}" role="button" aria-pressed="${on}"`
      + ` data-days="${r.days}">${r.label}</button>`;
  }).join('');
}

export function buildLegend(devices, colorFor) {
  return devices.map((s) =>
    `<span class="item"><span class="swatch" style="background:${colorFor(s.mac)}"></span>`
    + `${escapeHtml(s.name || s.mac)}</span>`).join('');
}

// Format a reading value (em-dash for missing); whole number unless decimals.
export function fmt(v, decimals) {
  if (v === null || v === undefined) return '—';
  return decimals ? v.toFixed(decimals) : Math.round(v).toString();
}

// "x min/h ago" once a sensor is >45 min stale (~3 missed 15-min reports), else null.
export function staleLabel(ts) {
  if (!ts) return null;
  const ageMin = (Date.now() - new Date(ts).getTime()) / 60000;
  if (ageMin < 45) return null;
  if (ageMin < 120) return Math.round(ageMin) + ' min ago';
  return Math.round(ageMin / 60) + ' h ago';
}

// Build the sensor cards. Page state threaded in: the selectedDevices Set, the
// metrics config + primary order, and colorFor/bandClass (which read page state).
export function buildSensorCards(sensors, { primary, metrics, selectedDevices, colorFor, bandClass }) {
  return sensors.map((s) => {
    const primaries = primary.map((k) => {
      const m = metrics[k];
      const cls = bandClass(k, s[k]);
      return `<div class="metric"><div class="label">${m.label}</div>` +
        `<div class="value ${cls}">${fmt(s[k], m.decimals)}` +
        `<span class="unit">${m.unit}</span></div></div>`;
    }).join('');
    const secondary = ['tvoc', 'battery']
      .filter((k) => s[k] !== null && s[k] !== undefined)
      .map((k) => `<span>${metrics[k].label}: ${fmt(s[k], metrics[k].decimals)} ${metrics[k].unit}</span>`)
      .join('');
    const stale = staleLabel(s.ts);
    const badge = s.online
      ? '<span class="badge online">online</span>'
      : '<span class="badge offline">offline</span>';
    const inc = selectedDevices.has(s.mac);
    return `<div class="card ${inc ? '' : 'excluded'}" role="button" tabindex="0" aria-pressed="${inc}" data-mac="${escapeHtml(s.mac)}">
        <div class="card-head">
          <span class="card-swatch" style="background:${colorFor(s.mac)}"></span>
          <span class="card-name">${escapeHtml(s.name || s.mac)}</span>${badge}
        </div>
        <div class="metrics">${primaries}</div>
        ${secondary ? `<div class="secondary">${secondary}</div>` : ''}
        ${stale ? `<div class="stale">Last reading ${stale}</div>` : ''}
      </div>`;
  }).join('');
}
