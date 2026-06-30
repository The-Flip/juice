import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildCostTable } from './cost.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

test('buildCostTable: empty / no machines returns ""', () => {
  assert.equal(buildCostTable({ machines: [] }), '');
  assert.equal(buildCostTable({}), '');
  assert.equal(buildCostTable(undefined), '');
});

test('buildCostTable: a row per machine with normal-day + month cost, and a total', () => {
  const el = parse(
    buildCostTable({
      machines: [
        { name: 'Blackout', normal_day_cost: 0.25, month_cost: 7.4 },
        { name: 'Cyclone', normal_day_cost: 0.1, month_cost: 3.0 },
      ],
      month_total_cost: 10.4,
    }),
  );
  const rows = el.querySelectorAll('tbody tr');
  assert.equal(rows.length, 2);
  const first = rows[0].querySelectorAll('td');
  assert.match(first[0].textContent, /Blackout/);
  assert.equal(first[1].textContent, '$0.25'); // normal day
  assert.equal(first[2].textContent, '$7.40'); // month (2-dp formatting)
  // Total row in the footer.
  assert.equal(el.querySelector('tfoot .peak-num.now').textContent, '$10.40');
});

test('buildCostTable: escapes the machine name', () => {
  const el = parse(buildCostTable({ machines: [{ name: '<img src=x>', month_cost: 1 }] }));
  assert.equal(el.querySelector('.peak-name img'), null);
  assert.match(el.querySelector('.peak-name').textContent, /<img src=x>/);
});
