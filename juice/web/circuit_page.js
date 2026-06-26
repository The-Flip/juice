import { escapeHtml } from './format.js';
import { circuitLabel } from './circuit.js';

// Pure HTML builders for the circuit page (the /circuit/<id> view). Each takes
// its inputs as parameters — page state threaded in, not read from globals — and
// returns a string; the thin DOM glue in the template sets innerHTML and wires
// listeners. (circuitLabel itself is the shared helper in circuit.js.)

// The circuit-title contents: the human label plus the edit pencil (operator-only).
export function buildCircuitHeader(circuit) {
  return `<span id="circuit-name">${escapeHtml(circuitLabel(circuit))}</span>` +
    `<button class="edit-name-btn private-only" title="Edit circuit" onclick="startEdit()">&#9998;</button>`;
}

// The assigned-strips list. Empty → a no-data placeholder. Each row links the
// strip page and (operator-only) a Remove button that unassigns it.
export function buildMemberRows(members) {
  if (!members.length) {
    return '<div class="no-data">No strips assigned yet.</div>';
  }
  return members.map(m => `
      <div class="member-row">
        <a href="/strip/${encodeURIComponent(m.device_id)}">${escapeHtml(m.display_name || m.device_id)}</a>
        <span class="spacer"></span>
        <button class="btn btn-cancel private-only"
          onclick="assignStrip('${encodeURIComponent(m.device_id)}', null)">Remove</button>
      </div>`).join('');
}

// The "Add a strip…" dropdown: every strip not already on this circuit.
export function buildAddStripOptions(allStrips, members) {
  const taken = new Set(members.map(m => m.device_id));
  return ['<option value="">Add a strip…</option>'].concat(
    allStrips.filter(s => !taken.has(s.device_id)).map(s =>
      `<option value="${escapeHtml(s.device_id)}">${escapeHtml(s.display_name || s.device_id)}</option>`)).join('');
}
