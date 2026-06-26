import { escapeHtml } from './format.js';

// Usage-page peak tables (operators only): per-strip and per-circuit current /
// 30-day-actual / 30-day-theoretical watts, with a shared 3-layer bar. Pure: data
// in, table HTML out ('' when empty). The caller toggles the empty-state element.

const fmtW = (v) => (v != null ? v.toFixed(1) + ' W' : '—');

export function buildStripPeaks(strips) {
  const shown = (strips || []).filter((s) => s.peak_watts_theoretical != null
    || s.peak_watts_actual != null || s.current_watts != null);
  if (!shown.length) return '';
  // All bars share one scale: the largest theoretical peak (worst case).
  const maxW = Math.max(...shown.map((s) =>
    Math.max(s.peak_watts_theoretical || 0, s.peak_watts_actual || 0, s.current_watts || 0)));
  const pct = (v) => maxW > 0 ? Math.min(100, (v || 0) / maxW * 100) : 0;
  const body = shown.map((s) => `
    <tr>
      <td class="peak-name">
        <a href="/strip/${encodeURIComponent(s.device_id)}">${escapeHtml(s.display_name || s.device_id)}</a>
      </td>
      <td class="bar-cell">
        <div class="peak-track">
          <div class="peak-bar-theoretical" style="width:${pct(s.peak_watts_theoretical)}%"
            title="Theoretical peak ${fmtW(s.peak_watts_theoretical)}"></div>
          <div class="peak-bar-actual" style="width:${pct(s.peak_watts_actual)}%"
            title="Actual peak ${fmtW(s.peak_watts_actual)}"></div>
          <div class="peak-bar-current" style="width:${pct(s.current_watts)}%"
            title="Current ${fmtW(s.current_watts)}"></div>
        </div>
      </td>
      <td class="peak-num now">${fmtW(s.current_watts)}</td>
      <td class="peak-num">${fmtW(s.peak_watts_actual)}</td>
      <td class="peak-num">${fmtW(s.peak_watts_theoretical)}</td>
    </tr>`).join('');
  return `
    <div class="peak-table-wrap">
      <table class="peak-table">
        <thead><tr>
          <th>Strip</th>
          <th class="bar-col"></th>
          <th>Current</th>
          <th>Peak (30d)</th>
          <th>Max possible (30d)</th>
        </tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}

export function buildCircuitPeaks(circuits) {
  const list = circuits || [];
  if (!list.length) return '';
  const maxW = Math.max(1, ...list.map((c) =>
    Math.max(c.peak_watts_theoretical || 0, c.peak_watts_actual || 0, c.current_watts || 0)));
  const pct = (v) => Math.min(100, (v || 0) / maxW * 100);
  const body = list.map((c) => {
    const p = c.pct_of_capacity;
    const capCls = p == null ? '' : (p >= 80 ? ' over' : (p >= 60 ? ' warn' : ''));
    const capTxt = p != null ? p.toFixed(0) + '%' : '—';
    return `
    <tr>
      <td class="peak-name">
        <a href="/circuit/${c.circuit_id}">${escapeHtml(c.label)}</a>
      </td>
      <td class="bar-cell">
        <div class="peak-track">
          <div class="peak-bar-theoretical" style="width:${pct(c.peak_watts_theoretical)}%"
            title="Theoretical peak ${fmtW(c.peak_watts_theoretical)}"></div>
          <div class="peak-bar-actual" style="width:${pct(c.peak_watts_actual)}%"
            title="Actual peak ${fmtW(c.peak_watts_actual)}"></div>
          <div class="peak-bar-current" style="width:${pct(c.current_watts)}%"
            title="Current ${fmtW(c.current_watts)}"></div>
        </div>
      </td>
      <td class="peak-num now">${fmtW(c.current_watts)}</td>
      <td class="peak-num">${fmtW(c.peak_watts_actual)}</td>
      <td class="peak-num">${fmtW(c.peak_watts_theoretical)}</td>
      <td class="peak-num${capCls}">${capTxt}</td>
    </tr>`;
  }).join('');
  return `
    <div class="peak-table-wrap">
      <table class="peak-table">
        <thead><tr>
          <th>Circuit</th>
          <th class="bar-col"></th>
          <th>Current</th>
          <th>Peak (30d)</th>
          <th>Max possible (30d)</th>
          <th>% of capacity</th>
        </tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}
