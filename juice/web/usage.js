// Pure aggregation for the /usage "When we're busy" grid. Inlined via the
// JS_USAGE marker — see juice/web/README.md. (The d3-formatted view/labels stay
// inline in busyView; this is the data pooling it delegates to.)

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
