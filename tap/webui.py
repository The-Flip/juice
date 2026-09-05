"""A small read-only status page, served on its own port.

Everything here is rendered from the in-memory `Health` record. No database read
happens on the request path, deliberately: the page has to render when the disk
is wedged, because that is exactly when somebody is looking at it.

This is a separate process from juice with no shared session or cookie setup, so
it does not reuse juice's page machinery. One HTML string with inline CSS and a
little vanilla JS is the whole of it.

It binds loopback by default. The page shows outlet aliases and device hosts —
not secret, but not something to put on a LAN by accident either.
"""

from __future__ import annotations

import logging

from aiohttp import web

from tap.health import Health

# A typed key rather than a bare string: aiohttp asks for these, and it means
# the handlers below get a real type instead of Any.
HEALTH = web.AppKey("health", Health)

log = logging.getLogger(__name__)

# The watchdog's opinion, in HTTP form: 503 while unhealthy so a container
# healthcheck or a load balancer can see it without parsing anything.
HEALTH_OK = 200
HEALTH_UNHEALTHY = 503

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tap</title>
<style>
  :root {
    --bg: #f6f7f9; --panel: #fff; --ink: #1a1d21; --muted: #5f6672;
    --line: #e3e6ea; --ok: #1a7f4b; --warn: #9a6400; --bad: #b3261e; --accent: #2a5db0;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #14171a; --panel: #1c2024; --ink: #e8eaed; --muted: #9aa2ad;
      --line: #2c3239; --ok: #4ac585; --warn: #e0a437; --bad: #ef6b62; --accent: #7aa5f0;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
         font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
  header { padding: 16px 20px; border-bottom: 1px solid var(--line); background: var(--panel);
           display: flex; flex-wrap: wrap; gap: 8px 24px; align-items: baseline; }
  h1 { font-size: 16px; margin: 0; font-weight: 650; letter-spacing: -0.01em; }
  h2 { font-size: 13px; margin: 0 0 10px; font-weight: 650; text-transform: uppercase;
       letter-spacing: 0.06em; color: var(--muted); }
  .meta { color: var(--muted); font-size: 12px; }
  main { padding: 20px; display: grid; gap: 16px; max-width: 1400px; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
           padding: 16px; }
  .cards { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); }
  table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
  th, td { text-align: left; padding: 6px 10px 6px 0; border-bottom: 1px solid var(--line);
           white-space: nowrap; }
  th { font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted);
       font-weight: 600; }
  tbody tr:last-child td { border-bottom: 0; }
  .scroll { overflow-x: auto; }
  .num { text-align: right; }
  .badge { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 11px;
           font-weight: 650; letter-spacing: 0.02em; }
  .online { background: color-mix(in srgb, var(--ok) 16%, transparent); color: var(--ok); }
  .degraded, .starting { background: color-mix(in srgb, var(--warn) 18%, transparent);
                         color: var(--warn); }
  .offline, .unauthorized { background: color-mix(in srgb, var(--bad) 16%, transparent);
                            color: var(--bad); }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: 3px 14px; font-size: 13px; }
  .kv dt { color: var(--muted); }
  .kv dd { margin: 0; font-variant-numeric: tabular-nums; }
  .bad { color: var(--bad); font-weight: 650; }
  .ok { color: var(--ok); }
  .dim { color: var(--muted); }
  .outlets { font-size: 12px; color: var(--muted); }
  .outlet { display: inline-block; margin-right: 12px; }
  .on { color: var(--ok); } .off { color: var(--muted); }
  .warnbar { background: color-mix(in srgb, var(--bad) 12%, transparent); color: var(--bad);
             padding: 8px 20px; font-size: 13px; border-bottom: 1px solid var(--line); }
</style>
</head>
<body>
<header>
  <h1>tap <span class="dim" id="tap-id"></span></h1>
  <span class="meta" id="summary">connecting…</span>
</header>
<div class="warnbar" id="warnings" hidden></div>
<main>
  <section class="panel">
    <h2>Collection</h2>
    <div class="scroll"><table>
      <thead><tr>
        <th>Host</th><th>Model</th><th>State</th><th class="num">Last OK</th>
        <th class="num">p50</th><th class="num">p95</th><th class="num">Sweeps</th>
        <th class="num">Failed</th><th>Outlets</th>
      </tr></thead>
      <tbody id="devices"><tr><td colspan="9" class="dim">no devices yet</td></tr></tbody>
    </table></div>
  </section>
  <div class="cards">
    <section class="panel"><h2>Uploading</h2><dl class="kv" id="uplink"></dl></section>
    <section class="panel"><h2>Buffer</h2><dl class="kv" id="buffer"></dl></section>
  </div>
  <section class="panel">
    <h2>Buffer days</h2>
    <div class="scroll"><table>
      <thead><tr><th>Day</th><th class="num">Rows</th><th class="num">Size</th></tr></thead>
      <tbody id="days"><tr><td colspan="3" class="dim">empty</td></tr></tbody>
    </table></div>
  </section>
</main>
<script>
const REFRESH_MS = 1000;
const fmtBytes = n => {
  if (!n) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(u.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return (n / 1024 ** i).toFixed(i ? 1 : 0) + " " + u[i];
};
const fmtDur = s => {
  if (s === null || s === undefined) return "—";
  if (s < 60) return s.toFixed(0) + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + Math.floor(s % 60) + "s";
  if (s < 86400) return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
  return Math.floor(s / 86400) + "d " + Math.floor((s % 86400) / 3600) + "h";
};
const num = n => (n === null || n === undefined) ? "—" : n.toLocaleString();
const el = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
};
const dl = (parent, pairs) => {
  parent.replaceChildren();
  for (const [k, v, cls] of pairs) {
    parent.append(el("dt", null, k), el("dd", cls || null, v));
  }
};

function renderDevices(devices) {
  const body = document.getElementById("devices");
  body.replaceChildren();
  if (!devices.length) {
    const tr = el("tr");
    tr.append(el("td", "dim", "no devices yet"));
    tr.firstChild.colSpan = 9;
    body.append(tr);
    return;
  }
  for (const d of devices) {
    const tr = el("tr");
    tr.append(el("td", null, d.host));
    tr.append(el("td", "dim", d.model || "—"));
    const state = el("td");
    state.append(el("span", "badge " + d.state, d.state));
    tr.append(state);
    tr.append(el("td", "num", fmtDur(d.last_ok_age_s)));
    tr.append(el("td", "num", d.sweep_p50_ms === null ? "—" : d.sweep_p50_ms + "ms"));
    tr.append(el("td", "num", d.sweep_p95_ms === null ? "—" : d.sweep_p95_ms + "ms"));
    tr.append(el("td", "num", num(d.sweeps_ok)));
    tr.append(el("td", "num" + (d.sweeps_failed ? " bad" : ""), num(d.sweeps_failed)));
    const outlets = el("td", "outlets");
    for (const o of d.outlets) {
      const span = el("span", "outlet");
      span.append(el("span", o.relay_on ? "on" : "off", o.relay_on ? "\\u25cf" : "\\u25cb"));
      span.append(document.createTextNode(
        " " + (o.alias || o.child_id || "outlet") +
        (o.watts === null ? "" : " " + o.watts.toFixed(1) + "W")));
      if (o.overcurrent) span.append(el("span", "bad", " OVERCURRENT"));
      outlets.append(span);
    }
    tr.append(outlets);
    body.append(tr);
    if (d.last_error) {
      // A device that fails intermittently never reaches OFFLINE, so the table
      // row alone said only "online" and a count. This is the line that says
      // what actually went wrong and which round trip it went wrong on.
      const sub = el("tr");
      const td = el("td", "dim");
      td.colSpan = 9;
      const kinds = Object.entries(d.failures_by_kind || {})
        .sort((a, b) => b[1] - a[1])
        .map(([k, n]) => k + " \u00d7" + n)
        .join(", ");
      const bits = ["\u21b3 " + d.last_error];
      if (kinds) bits.push(kinds);
      if (d.sweep_fail_p95_ms !== null && d.sweep_fail_p95_ms !== undefined) {
        bits.push("failed-attempt p95 " + d.sweep_fail_p95_ms + "ms");
      }
      if (d.last_error_at) bits.push("at " + d.last_error_at.slice(11, 19) + "Z");
      td.textContent = bits.join("  \u00b7  ");
      sub.append(td);
      body.append(sub);
    }
  }
}

function render(s) {
  document.getElementById("tap-id").textContent = s.tap_id;
  document.getElementById("summary").textContent =
    `v${s.version} · up ${fmtDur(s.uptime_seconds)} · ${s.devices.length} device(s)` +
    (s.config_path ? ` · ${s.config_path}` : " · no config file");
  const warn = document.getElementById("warnings");
  warn.hidden = !s.warnings.length;
  warn.textContent = s.warnings.join(" · ");

  const u = s.uplink;
  dl(document.getElementById("uplink"), !u.enabled
    ? [["Status", "standalone — no server configured", "dim"]]
    : [
      ["Status", u.connected ? "connected" : "disconnected", u.connected ? "ok" : "bad"],
      ["URL", u.url],
      [u.connected ? "Connected for" : "Retry in",
       u.connected ? fmtDur((Date.now() - Date.parse(u.since)) / 1000) : fmtDur(u.backoff_s)],
      ["Lag", num(u.lag_rows) + " rows / " + fmtDur(u.lag_seconds)],
      ["Acked cursor", u.acked_cursor || "—"],
      ["Batches", `${num(u.batches_sent)} sent · ${num(u.batches_acked)} acked`],
      ["Nacked", num(u.batches_nacked), u.batches_nacked ? "bad" : null],
      ["Poisoned", num(u.batches_poisoned), u.batches_poisoned ? "bad" : null],
      ["Reconnects", num(u.reconnects)],
      ["Commands", `${num(u.commands_received)} received · ${num(u.commands_failed)} failed`],
      ["Live frames", u.live_suppressed ? "suppressed (backfilling)" : "flowing"],
    ]);

  const b = s.buffer;
  dl(document.getElementById("buffer"), [
    ["Rows written", num(b.rows_written)],
    ["Rows dropped", num(b.rows_dropped), b.rows_dropped ? "bad" : "ok"],
    ["On disk", fmtBytes(b.total_bytes)],
    ["Oldest", b.oldest_ts ? b.oldest_ts.replace("T", " ").slice(0, 19) : "—"],
    ["Newest", b.newest_ts ? b.newest_ts.replace("T", " ").slice(0, 19) : "—"],
    ["Retention", b.retention_days + " days"],
    ["Queue depth", num(b.queue_depth)],
    ["Last commit", b.last_commit_ms === null ? "—" : b.last_commit_ms + " ms"],
  ]);

  const days = document.getElementById("days");
  days.replaceChildren();
  if (!b.days.length) {
    const tr = el("tr");
    tr.append(el("td", "dim", "empty"));
    tr.firstChild.colSpan = 3;
    days.append(tr);
  } else {
    for (const d of b.days) {
      const tr = el("tr");
      tr.append(el("td", null, d.day));
      tr.append(el("td", "num", num(d.rows)));
      tr.append(el("td", "num", fmtBytes(d.bytes)));
      days.append(tr);
    }
  }
  renderDevices(s.devices);
}

async function tick() {
  try {
    const r = await fetch("api/status", { cache: "no-store" });
    render(await r.json());
  } catch (e) {
    document.getElementById("summary").textContent = "cannot reach tap: " + e;
  }
}
tick();
setInterval(tick, REFRESH_MS);
</script>
</body>
</html>
"""


async def handle_page(request: web.Request) -> web.Response:
    return web.Response(text=PAGE, content_type="text/html")


async def handle_status(request: web.Request) -> web.Response:
    return web.json_response(request.app[HEALTH].snapshot())


async def handle_health(request: web.Request) -> web.Response:
    """Liveness for a container healthcheck. 503 while the watchdog is unhappy."""
    health = request.app[HEALTH]
    reasons = list(health.warnings)
    ok = not reasons
    return web.json_response(
        {"ok": ok, "reasons": reasons, "uptime_seconds": round(health.uptime_seconds, 1)},
        status=HEALTH_OK if ok else HEALTH_UNHEALTHY,
    )


def create_app(health: Health) -> web.Application:
    app = web.Application()
    app[HEALTH] = health
    app.router.add_get("/", handle_page)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/health", handle_health)
    return app


async def serve(health: Health, host: str, port: int) -> web.AppRunner:
    """Start the status page, or fail with a message instead of a traceback."""
    from tap.errors import EXIT_CONFIG, FatalError

    runner = web.AppRunner(create_app(health), access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, host, port).start()
    except OSError as e:
        await runner.cleanup()
        raise FatalError(
            f"cannot serve the status page on {host}:{port}: {e.strerror or e} — "
            "is another tap already running?",
            EXIT_CONFIG,
        ) from None
    log.info("status page at http://%s:%d/", host, port)
    return runner
