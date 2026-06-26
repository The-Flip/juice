import { escapeHtml, fmtTimeShort } from './format.js';

// Full local timestamp for the events table (distinct from format.js's compact
// fmtTimeShort). EVENTS-only, so it stays private to this module.
function fmtTs(iso) {
  return new Date(iso).toLocaleString();  // DB stores UTC; render in local time
}

// Build one <tr> for the power-events table from an event row.
export function buildEventRow(e) {
  const isOn = e.action === 'turn_on';
  const actionLabel = isOn ? 'ON' : 'OFF';
  const actionCls = isOn ? 'action-on' : 'action-off';
  const target = e.machine_name || e.plug_alias || ('Plug ' + e.plug_id);
  const sourceCls = 'source-' + e.source;
  const sourceLabel = e.source === 'individual' ? 'individual' : e.source.replace('_', ' ');
  const result = e.result === 'ok' ? 'ok' : 'error';
  const resultCls = e.result === 'error' ? 'result-error' : '';
  const detail = e.error ? ' — ' + escapeHtml(e.error) : '';
  return (
    '<tr>'
    + '<td>' + escapeHtml(fmtTs(e.ts)) + '</td>'
    + '<td>' + escapeHtml(e.actor) + '</td>'
    + '<td>' + escapeHtml(target) + '</td>'
    + '<td class="' + actionCls + '">' + actionLabel + '</td>'
    + '<td><span class="' + sourceCls + '">' + escapeHtml(sourceLabel) + '</span></td>'
    + '<td class="' + resultCls + '">' + result + detail + '</td>'
    + '</tr>'
  );
}

// Inner HTML for one row of the compact "recent power events" preview list (the
// dashboard + machine-detail pages). The caller creates the <li> wrapper. Shared
// so the two pages don't drift (they were byte-identical copies).
export function buildRecentEventRow(e) {
  const target = e.machine_name || e.plug_alias || ('Plug ' + e.plug_id);
  const time = '<span class="evt-time">' + escapeHtml(fmtTimeShort(e.ts)) + '</span>';
  // FlipFix report outcomes: show the note; failures/skips in red.
  if (e.source === 'flipfix') {
    const cls = e.result === 'error' ? 'evt-error' : 'evt-source';
    return time
      + '<span>' + escapeHtml(target) + '</span>'
      + '<span class="' + cls + '">' + escapeHtml(e.error || 'FlipFix') + '</span>';
  }
  const src = e.source === 'individual' ? ''
    : e.source === 'all_on' ? '(all on)'
    : e.source === 'all_off' ? '(all off)'
    : e.source ? '(' + e.source.replace(/_/g, ' ') + ')' : '';
  const err = e.result === 'error' ? ' — ' + (e.error || 'error') : '';
  const srcSpan = src ? '<span class="evt-source">' + escapeHtml(src) + '</span>' : '';
  const errSpan = err ? '<span class="evt-error">' + escapeHtml(err) + '</span>' : '';
  // A reboot is a power-cycle, not an on/off — don't shoehorn it into the OFF label.
  if (e.action === 'reboot') {
    return time
      + '<span>' + escapeHtml(e.actor) + ' rebooted</span>'
      + '<span>' + escapeHtml(target) + '</span>'
      + srcSpan + errSpan;
  }
  const isOn = e.action === 'turn_on';
  const onCls = isOn ? 'on' : 'off';
  const onLbl = isOn ? 'ON' : 'OFF';
  return time
    + '<span>' + escapeHtml(e.actor) + ' turned</span>'
    + '<span class="evt-action ' + onCls + '">' + onLbl + '</span>'
    + '<span>' + escapeHtml(target) + '</span>'
    + srcSpan + errSpan;
}
