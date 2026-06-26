// Pure helpers for the /air page: sensor ordering and the museum closed-hours
// backdrop. Inlined via the JS_AIR marker — see juice/web/README.md. (colorFor,
// sensorName, bandClass stay inline: they read page state / the METRICS table.)

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
