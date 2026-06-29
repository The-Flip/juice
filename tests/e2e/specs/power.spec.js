// Power-control flows (interactive mode: fake plug objects + a live readings
// tick). Covers a single-machine toggle settling via SSE and the strip All-Off
// operation banner progressing to complete.
import { test, expect } from '@playwright/test';
import { login } from './helpers.js';

const asList = (d) => (Array.isArray(d) ? d : d.machines);

test('machine detail: toggle off settles to "Turn On" via the readings tick', async ({ page }) => {
  await login(page);
  const machines = asList(await (await page.request.get('/api/machines')).json());
  const on = machines.find((m) => m.has_emeter && m.is_on && m.plug?.plug_id);

  await page.goto(`/machine/${on.plug.plug_id}`);
  const btn = page.locator('#power-btn');
  await expect(btn).toHaveText(/Turn Off/); // machine is on

  await btn.click(); // request power off
  // The fake relay flips immediately and the ~1s readings tick reconciles the
  // pending state, so the button settles to the "Turn On" affordance.
  await expect(btn).toHaveText(/Turn On/, { timeout: 12_000 });

  // And turning it back on settles to "Turn Off".
  await btn.click();
  await expect(btn).toHaveText(/Turn Off/, { timeout: 12_000 });
});

test('strip page: All-Off runs an operation and the banner reaches complete', async ({ page }) => {
  await login(page);
  const machines = asList(await (await page.request.get('/api/machines')).json());
  const stripId = machines.find((m) => m.has_emeter && m.strip_device_id)?.strip_device_id;

  const onBefore = async () =>
    (await (await page.request.get(`/api/strips/${stripId}`)).json()).outlets.filter(
      (o) => o.is_on,
    ).length;
  const before = await onBefore();
  expect(before, 'strip should have outlets on to turn off').toBeGreaterThan(0);

  await page.goto(`/strip/${stripId}`);
  page.on('dialog', (d) => d.accept()); // "Turn off every outlet…?" confirm

  await page.locator('#btn-all-off').click();
  const banner = page.locator('#op-banner');
  await expect(banner).toBeVisible();
  // The op staggers through the strip's outlets; the banner ends at "… complete".
  await expect(banner).toContainText(/complete/i, { timeout: 30_000 });

  // And it actually flipped relays (via the fakes), not just ran the banner — the
  // on-count drops. (Not all reach 0: all-off intentionally skips PLAYING machines.)
  await expect.poll(onBefore, { timeout: 5_000 }).toBeLessThan(before);
});
