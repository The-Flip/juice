// d3 charts draw with real (non-empty) axes for the seeded fixture. Covers the
// un-unit-testable surface: actual SVG render + scales/axes, and that the
// operator-only strip/circuit pages reach their charts after login.
import { test, expect } from '@playwright/test';
import { discoverIds } from './helpers.js';

let ids;
test.beforeEach(async ({ page }) => {
  ids = await discoverIds(page); // logs in + finds an emeter plug/strip/circuit
});

// The chart elements ARE the <svg> (id on the svg itself). Axis ticks are a
// cheap, render-type-agnostic proof that scales/axes were built from data. We
// assert the svg is visible and the ticks are attached + plural — not that each
// tick <g> is "visible" (Playwright reports degenerate-bbox <g>s as hidden).
async function expectChartDrawn(page, svgSelector) {
  await expect(page.locator(svgSelector)).toBeVisible();
  await expect(page.locator(`${svgSelector} .tick`).first()).toBeAttached();
  expect(await page.locator(`${svgSelector} .tick`).count()).toBeGreaterThan(1);
}

async function gotoDrawn(page, path, svgSelector) {
  const resp = await page.goto(path);
  expect(resp.status()).toBe(200);
  await expectChartDrawn(page, svgSelector);
}

test('usage page: energy, play-hours, and busy-grid charts draw', async ({ page }) => {
  await page.goto('/usage');
  for (const sel of ['#chart', '#play-chart', '#busy-chart']) {
    await expectChartDrawn(page, sel);
  }
});

test('usage page (operator): energy-cost chart + per-machine cost table populate', async ({
  page,
}) => {
  // beforeEach logged in, so the operator-only cost section loads.
  await page.goto('/usage');
  await expectChartDrawn(page, '#cost-chart'); // the cost-per-day bar chart drew
  await expect(page.locator('#machine-costs-rows tr').first()).toBeVisible();
});

test('machine detail: power chart draws', async ({ page }) => {
  await gotoDrawn(page, `/machine/${ids.plugId}`, '#chart');
});

test('machine detail (operator): Details table renders below the outlet map', async ({
  page,
}) => {
  // beforeEach logged in, so the operator rows (Plug/Strip/Calibration/cost) show.
  await page.goto(`/machine/${ids.plugId}`);
  const stats = page.locator('#detail-stats');
  await expect(stats.locator('.detail-stats-header')).toHaveText(/Details/);
  // The avg-daily-cost row is operator-only; it fills in from a separate fetch.
  await expect(stats.getByText('Avg daily cost (30d)')).toBeVisible();
  await expect(stats.getByText('Calibration')).toBeVisible();
});

test('strip page: usage chart draws', async ({ page }) => {
  await gotoDrawn(page, `/strip/${ids.stripId}`, '#usage-chart');
});

test('circuit page: usage chart draws', async ({ page }) => {
  await gotoDrawn(page, `/circuit/${ids.circuitId}`, '#usage-chart');
});

test('air page: a per-metric sensor chart draws', async ({ page }) => {
  // The air page renders one small chart per metric in #air-<metric>.
  await gotoDrawn(page, '/air', '#air-temperature');
});
