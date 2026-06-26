// Shared pure formatters, de-duplicated out of the page templates (escapeHtml was
// copy-pasted into all 7, in three slightly different variants; fmtTimeShort into
// two). Inlined via the JS_FORMAT marker — see juice/web/README.md.

// HTML-escape for interpolation into innerHTML. Null/undefined → '' (so a missing
// field renders blank, not the string "null"); escapes & < > " and ' (the single
// quote matters when the value lands inside a single-quoted attribute).
export function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}

// Compact event-log timestamp: time-of-day for today, prefixed with a short date
// for older days (and the year when it differs). DB times are UTC ISO strings;
// rendered in the browser's local zone.
export function fmtTimeShort(iso) {
  const d = new Date(iso);
  const time = d.toLocaleTimeString([], {hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'});
  const now = new Date();
  if (d.getFullYear() === now.getFullYear()
      && d.getMonth() === now.getMonth()
      && d.getDate() === now.getDate()) {
    return time;  // today: time only, stays compact
  }
  // older: include date; add year only if it differs from this year
  const opts = {month: 'short', day: 'numeric'};
  if (d.getFullYear() !== now.getFullYear()) opts.year = 'numeric';
  return d.toLocaleDateString([], opts) + ' ' + time;
}
