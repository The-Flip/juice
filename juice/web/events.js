import { escapeHtml } from './format.js';

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
