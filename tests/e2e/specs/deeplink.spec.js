// Deep links to a section hash must land on that section even though the charts
// above it render asynchronously (and push it down) after the browser's initial
// scroll. Regression for `/usage#busy` staying stuck at the top.
import { test, expect } from '@playwright/test';

test('deep link to #busy scrolls to the busy section once charts render', async ({ page }) => {
  await page.goto('/usage#busy');

  // The charts above #busy (energy, play-hours) render async and grow, pushing the
  // busy section down — wait for them (and the busy chart) to draw.
  await expect(page.locator('#chart .tick').first()).toBeAttached();
  await expect(page.locator('#play-chart .tick').first()).toBeAttached();
  await expect(page.locator('#busy-chart .tick').first()).toBeAttached();

  // The busy section heading should end up scrolled into view (not stuck at top).
  await expect(page.locator('#busy')).toBeInViewport();
  expect(await page.evaluate(() => window.scrollY)).toBeGreaterThan(0);
});

test('deep link to an air metric panel lands on it once charts render', async ({ page }) => {
  // Integration check that an /air#air-<metric> deep link ends on the target.
  // The panels are built dynamically after the data fetch (delayed here to the
  // realistic late-render timing); chromium scrolls to a fragment when its target
  // is inserted, and honorHashScroll re-queries to cover non-chromium browsers —
  // either way the below-the-fold panel must end up in view. (The helper's own
  // dynamic-target logic is unit-tested in juice/web/scroll.test.js.)
  await page.route('**/api/air/**', async (route) => {
    await new Promise((r) => setTimeout(r, 1500));
    await route.continue();
  });

  // First discover the last metric panel (the most-below-the-fold target).
  await page.goto('/air');
  await page.locator('.panel[id^="air-"] svg .tick').first().waitFor({ state: 'attached' });
  const lastId = await page.evaluate(() => {
    const panels = [...document.querySelectorAll('.panel[id^="air-"]')];
    return panels[panels.length - 1]?.id;
  });
  expect(lastId, 'air page should render metric panels').toBeTruthy();

  await page.goto(`/air#${lastId}`);
  await expect(page.locator(`#${lastId} svg .tick`).first()).toBeAttached();
  await expect(page.locator(`#${lastId}`)).toBeInViewport();
  expect(await page.evaluate(() => window.scrollY)).toBeGreaterThan(0);
});
