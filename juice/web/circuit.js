// Shared circuit-identity helper, used on both the strip page (the circuit-
// assignment line) and the circuit page (the title) — one definition instead of
// two copies. Renders a circuit row as a human label ("P1 B20 — Backline", or
// just "P1 B20" with no description). Pure: row in, plain (unescaped) string out;
// callers escapeHtml() the result where they interpolate it.

export function circuitLabel(c) {
  const loc = `${c.panel} ${c.breaker}`.trim();
  return c.description ? `${loc} — ${c.description}` : loc;
}
