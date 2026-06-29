// Circuit assignment from the strip page (operator write; a pure DB op). Change
// the strip's circuit and confirm it persists across a reload.
import { test, expect } from '@playwright/test';
import { login } from './helpers.js';

const asList = (d) => (Array.isArray(d) ? d : d.machines);

test('strip page: assigning a circuit persists', async ({ page }) => {
  await login(page);
  const machines = asList(await (await page.request.get('/api/machines')).json());
  const stripId = machines.find((m) => m.has_emeter && m.strip_device_id)?.strip_device_id;

  await page.goto(`/strip/${stripId}`);
  const select = page.locator('#circuit-select');
  await expect(select).toBeVisible();

  // Pick a real circuit option different from the current assignment.
  const target = await select.evaluate((el) => {
    const cur = el.value;
    return [...el.options].map((o) => o.value).find((v) => v !== cur && v !== '');
  });
  expect(target, 'fixture should offer another circuit to assign').toBeTruthy();

  // Changing the select POSTs to /api/strips/{id}/circuit; wait for it to land.
  const [resp] = await Promise.all([
    page.waitForResponse(
      (r) => r.url().includes('/strips/') && r.url().endsWith('/circuit') && r.request().method() === 'POST',
    ),
    select.selectOption(target),
  ]);
  expect(resp.ok()).toBeTruthy();

  // The assignment is committed to the DB + in-memory state — it survives a reload.
  await page.reload();
  await expect(page.locator('#circuit-select')).toHaveValue(target);
});
