import { escapeHtml } from './format.js';

// Per-machine energy-cost table for the /usage page (operators only). Pure: the
// /api/cost payload in, table HTML out ('' when there are no machines). Reuses
// the shared .peak-table styling. Two cost columns — a "normal" day (the server's
// 3rd-highest-cost day) and the 30-day month total — plus a month total row.

const fmtCost = (v) => '$' + (v == null ? 0 : v).toFixed(2);

export function buildCostTable(data) {
  const machines = (data && data.machines) || [];
  if (!machines.length) return '';
  const body = machines
    .map(
      (m) => `
    <tr>
      <td class="peak-name">${escapeHtml(m.name)}</td>
      <td class="peak-num">${fmtCost(m.normal_day_cost)}</td>
      <td class="peak-num now">${fmtCost(m.month_cost)}</td>
    </tr>`,
    )
    .join('');
  const total = data.month_total_cost != null ? data.month_total_cost : 0;
  return `
    <div class="peak-table-wrap">
      <table class="peak-table">
        <thead><tr>
          <th>Machine</th>
          <th>Normal day</th>
          <th>Month (30 days)</th>
        </tr></thead>
        <tbody>${body}</tbody>
        <tfoot><tr>
          <td class="peak-name">Total</td>
          <td class="peak-num"></td>
          <td class="peak-num now">${fmtCost(total)}</td>
        </tr></tfoot>
      </table>
    </div>`;
}
