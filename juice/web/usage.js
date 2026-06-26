// Pure (d3-free) data helpers for the /usage page charts — busy-grid pooling and
// x-axis tick thinning. Inlined via the JS_USAGE marker — see juice/web/README.md.
// (The d3-formatted view/labels and the draw calls stay inline; these are the
// shaping the chart code delegates to.)

// Date (YYYY-MM-DD) -> Mon=0 … Sun=6 (JS getDay is Sun=0, so shift).
export function busyWeekdayIdx(iso) {
  return (new Date(iso + 'T00:00:00').getDay() + 6) % 7;
}

// "Avg week" pooling: collapse per-date cells into one cell per (weekday, hour),
// summing play/on hours across each weekday's occurrences and recomputing the
// ratio. Returns { cells, hours } with hours ascending. d3-free so it's testable.
export function busyWeekAggregate(cells) {
  const agg = new Map();
  for (const c of cells) {
    const wd = busyWeekdayIdx(c.date);
    const k = wd + '|' + c.hour;
    const a = agg.get(k) || { col: wd, hour: c.hour, play: 0, on: 0 };
    a.play += c.play_hours;
    a.on += c.on_hours;
    agg.set(k, a);
  }
  const out = [...agg.values()].map((a) => ({
    col: a.col, hour: a.hour, play_hours: a.play, on_hours: a.on,
    ratio: a.on > 0 ? a.play / a.on : 0,
  }));
  const hours = [...new Set(out.map((c) => c.hour))].sort((a, b) => a - b);
  return { cells: out, hours };
}

// Thin a categorical x-axis to a legible number of labels: aim for at most
// ~innerW/pxPerTick ticks (clamped to [minTicks, maxTicks]), then keep every Nth
// item so the labels are evenly spaced and the first is always shown. Pure
// arithmetic + filter (d3-free) — the chart draw code feeds the result to
// d3.axisBottom(...).tickValues(...). Returns a subset of `items` in order.
export function pickEveryNthTicks(items, innerW, { maxTicks, pxPerTick, minTicks = 3 }) {
  const target = Math.max(minTicks, Math.min(maxTicks, Math.floor(innerW / pxPerTick)));
  const every = Math.max(1, Math.ceil(items.length / target));
  return items.filter((_, i) => i % every === 0);
}
