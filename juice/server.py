"""HTTP server with API and web dashboard for juice."""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field

from aiohttp import web

from juice.collector import Plug, PlugReading
from juice.state import Calibration, CalibrationError, auto_calibrate, classify
from juice.store import Store

log = logging.getLogger(__name__)

BUFFER_SIZE = 3600  # ~60 minutes at 1s polling

# Seed calibrations for known machines (keyed by machine name)
SEED_CALIBRATIONS: dict[str, Calibration] = {
    "Eight Ball Deluxe Limited Edition": Calibration(idle_max_rsd=1.0, play_min_rsd=8.0),
    "Godzilla (Premium)": Calibration(idle_max_rsd=2.0, play_min_rsd=12.0),
    "Hyperball": Calibration(idle_max_rsd=None, play_min_rsd=13.0),
    "Revenge From Mars": Calibration(idle_max_rsd=None, play_min_rsd=5.0),
    "The Addams Family": Calibration(idle_max_rsd=2.1, play_min_rsd=7.0),
}


@dataclass
class RecorderState:
    """Shared state between the recorder loop and the HTTP API."""

    plug_readings: dict[int, PlugReading] = field(default_factory=dict)
    watt_buffers: dict[int, deque] = field(default_factory=dict)
    assignments: dict[int, tuple[str, str]] = field(
        default_factory=dict
    )  # plug_id -> (name, asset_id)
    plugs: dict[int, tuple[str, str, str]] = field(
        default_factory=dict
    )  # plug_id -> (device_id, child_id, alias)
    calibrations: dict[int, Calibration] = field(default_factory=dict)  # plug_id -> Calibration
    strip_aliases: dict[str, str] = field(default_factory=dict)  # device_id -> strip alias
    plug_objects: dict[int, Plug] = field(default_factory=dict)  # plug_id -> Plug (for control)


def seed_buffers(state: RecorderState, store: Store) -> None:
    """Pre-fill watt_buffers from DB so sparklines are available immediately."""
    from collections import deque

    for plug_id in state.assignments:
        watts = store.get_recent_watts(plug_id, seconds=BUFFER_SIZE)
        if watts:
            state.watt_buffers[plug_id] = deque(watts, maxlen=BUFFER_SIZE)


async def handle_machines(request: web.Request) -> web.Response:
    state: RecorderState = request.app["recorder_state"]

    machines = []
    for plug_id, (name, asset_id) in state.assignments.items():
        reading = state.plug_readings.get(plug_id)
        plug_info = state.plugs.get(plug_id)

        power = None
        if reading:
            power = {
                "watts": round(reading.watts, 1),
                "voltage": round(reading.voltage, 1),
                "amps": round(reading.amps, 3),
                "total_kwh": round(reading.total_kwh, 1),
            }

        machine_state = None
        sparkline: list[float] = []
        sparkline_states: list[str] = []
        buf = state.watt_buffers.get(plug_id)
        if buf:
            watts_list = list(buf)
            sparkline = watts_list
            cal = state.calibrations.get(plug_id)
            if cal:
                classified = classify(watts_list, cal)
                sparkline_states = [s.value for s in classified]
                if classified:
                    machine_state = classified[-1].value

        plug_data = None
        if plug_info:
            device_id, child_id, alias = plug_info
            plug_data = {
                "plug_id": plug_id,
                "device_id": device_id,
                "child_id": child_id,
                "alias": alias,
            }

        strip_device_id = plug_info[0] if plug_info else ""
        strip_alias = state.strip_aliases.get(strip_device_id, "")

        machines.append(
            {
                "name": name,
                "asset_id": asset_id,
                "plug": plug_data,
                "power": power,
                "state": machine_state,
                "sparkline": sparkline,
                "sparkline_states": sparkline_states,
                "strip_device_id": strip_device_id,
                "strip_alias": strip_alias,
                "calibrated": plug_id in state.calibrations,
            }
        )

    machines.sort(key=lambda m: (m["strip_device_id"], m["plug"]["plug_id"] if m["plug"] else 0))
    return web.json_response({"machines": machines})


async def handle_calibrate(request: web.Request) -> web.Response:
    plug_id = int(request.match_info["plug_id"])
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    assignment = state.assignments.get(plug_id)
    if not assignment:
        return web.json_response({"error": "Plug not assigned to a machine"}, status=400)

    name, asset_id = assignment
    machine_id = store.ensure_machine(asset_id, name)

    watts = store.get_recent_watts(plug_id, seconds=3600)
    try:
        calibration = auto_calibrate(watts)
    except CalibrationError as e:
        log.warning("Calibration failed for %s: %s", name, e)
        return web.json_response({"error": str(e)}, status=400)

    store.set_calibration(machine_id, calibration)
    state.calibrations[plug_id] = calibration
    log.info(
        "Calibrated %s: idle_max_rsd=%s, play_min_rsd=%.1f",
        name,
        calibration.idle_max_rsd,
        calibration.play_min_rsd,
    )

    return web.json_response(
        {
            "machine": name,
            "calibration": {
                "idle_max_rsd": calibration.idle_max_rsd,
                "play_min_rsd": calibration.play_min_rsd,
            },
        }
    )


async def handle_readings(request: web.Request) -> web.Response:
    plug_id = int(request.match_info["plug_id"])
    hours = int(request.query.get("hours", "24"))
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    from datetime import UTC, datetime, timedelta

    since = datetime.now(UTC) - timedelta(hours=hours)
    rows = store.get_readings_since(plug_id, since)

    watts = [r[1] for r in rows]
    states: list[str] = []
    cal = state.calibrations.get(plug_id)
    if cal and watts:
        states = [s.value for s in classify(watts, cal)]

    return web.json_response(
        {
            "timestamps": [r[0] for r in rows],
            "watts": watts,
            "states": states,
        }
    )


async def handle_power(request: web.Request) -> web.Response:
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    plug_id = int(request.match_info["plug_id"])
    state: RecorderState = request.app["recorder_state"]

    plug = state.plug_objects.get(plug_id)
    if plug is None:
        return web.json_response({"error": "Plug not available"}, status=400)

    body = await request.json()
    on = body.get("on", True)

    try:
        if on:
            await plug.turn_on()
        else:
            await plug.turn_off()
    except Exception as e:
        log.warning("Power control failed for plug %d: %s", plug_id, e)
        return web.json_response({"error": str(e)}, status=500)

    log.info("Plug %d (%s) turned %s", plug_id, plug.alias, "ON" if on else "OFF")
    return web.json_response({"ok": True, "on": on})


async def handle_dashboard(request: web.Request) -> web.Response:
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_machine_detail(request: web.Request) -> web.Response:
    return web.Response(text=DETAIL_HTML, content_type="text/html")


def create_app(
    recorder_state: RecorderState,
    store: Store,
    oauth_config: dict | None = None,
) -> web.Application:
    app = web.Application()
    app["recorder_state"] = recorder_state
    app["store"] = store

    if oauth_config:
        from juice.auth import setup_auth

        setup_auth(app, oauth_config)

    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/machine/{plug_id}", handle_machine_detail)
    app.router.add_get("/api/machines", handle_machines)
    app.router.add_get("/api/machines/{plug_id}/readings", handle_readings)
    app.router.add_post("/api/machines/{plug_id}/calibrate", handle_calibrate)
    app.router.add_post("/api/machines/{plug_id}/power", handle_power)
    return app


async def start_server(
    recorder_state: RecorderState,
    store: Store,
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 8000,
    oauth_config: dict | None = None,
) -> web.AppRunner:
    app = create_app(recorder_state, store, oauth_config=oauth_config)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7;
    color: #1d1d1f;
    min-height: 100vh;
  }
  header {
    padding: 20px 28px 14px;
    border-bottom: 1px solid #d2d2d7;
    background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header h1 {
    font-size: 17px;
    font-weight: 600;
    color: #86868b;
    flex: 1;
  }
  header h1 span { color: #1d1d1f; }
  .power-btns { display: flex; gap: 8px; }
  .power-btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; color: #fff; transition: opacity 0.15s;
  }
  .power-btn:hover { opacity: 0.85; }
  .power-btn:disabled { opacity: 0.5; cursor: default; }
  .power-btn-on { background: #34c759; }
  .power-btn-off { background: #ff3b30; }
  #content { padding: 20px 28px; }
  .strip-row {
    margin-bottom: 20px;
  }
  .strip-label {
    font-size: 12px;
    font-weight: 600;
    color: #86868b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }
  .tiles {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }
  .tile {
    width: 140px;
    height: 140px;
    background: #fff;
    border: 1px solid #d2d2d7;
    border-radius: 10px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    position: relative;
    cursor: pointer;
    transition: box-shadow 0.15s;
    text-decoration: none;
    color: inherit;
  }
  .tile:hover {
    box-shadow: 0 2px 12px rgba(0,0,0,0.1);
  }
  .tile-top {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 6px;
  }
  .state-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .state-OFF { background: #aeaeb2; }
  .state-ATTRACT { background: #007aff; }
  .state-PLAYING { background: #ff3b30; }
  .state-IDLE { background: #ff9500; }
  .state-null { background: #aeaeb2; border: 1px dashed #c7c7cc; }
  .machine-name {
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    color: #1d1d1f;
  }
  .sparkline-wrap {
    flex: 1;
    min-height: 0;
    border-radius: 4px;
    overflow: hidden;
  }
  .sparkline-wrap canvas {
    width: 100%;
    height: 100%;
  }
  .tile-watts {
    font-size: 11px;
    font-weight: 500;
    color: #86868b;
    text-align: right;
    margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }
  .no-data {
    text-align: center;
    padding: 60px 20px;
    color: #86868b;
    font-size: 14px;
  }
</style>
</head>
<body>
<header>
  <h1><span>juice</span> &mdash; machine status</h1>
  <div class="power-btns">
    <button class="power-btn power-btn-on" id="btn-all-on" onclick="allPower(true)">All On</button>
    <button class="power-btn power-btn-off" id="btn-all-off" onclick="allPower(false)">All Off</button>
  </div>
</header>
<div id="content">
  <div class="no-data">Connecting...</div>
</div>
<script>
const STATE_COLORS = {
  OFF: '#aeaeb2', ATTRACT: '#007aff', PLAYING: '#ff3b30', IDLE: '#ff9500'
};

function drawSparkline(canvas, data, states) {
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth * dpr;
  const h = canvas.clientHeight * dpr;
  canvas.width = w;
  canvas.height = h;
  ctx.clearRect(0, 0, w, h);
  if (!data || data.length < 2) return;
  const max = 300;
  const step = w / (data.length - 1);
  const pad = 2 * dpr;
  // Draw state backdrop bands
  if (states && states.length === data.length) {
    let i = 0;
    while (i < states.length) {
      const st = states[i];
      let j = i;
      while (j < states.length && states[j] === st) j++;
      const x0 = i === 0 ? 0 : (i - 0.5) * step;
      const x1 = j >= states.length ? w : (j - 0.5) * step;
      const c = STATE_COLORS[st];
      if (c) { ctx.fillStyle = c + '30'; ctx.fillRect(x0, 0, x1 - x0, h); }
      i = j;
    }
  }
  // Line + fill
  const lastState = states && states.length ? states[states.length - 1] : null;
  const color = STATE_COLORS[lastState] || '#aeaeb2';
  ctx.beginPath();
  ctx.moveTo(0, h);
  for (let i = 0; i < data.length; i++) {
    ctx.lineTo(i * step, h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad));
  }
  ctx.lineTo(w, h);
  ctx.closePath();
  ctx.fillStyle = color + '18';
  ctx.fill();
  ctx.beginPath();
  for (let i = 0; i < data.length; i++) {
    const x = i * step;
    const y = h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * dpr;
  ctx.stroke();
}

function renderMachines(machines) {
  const el = document.getElementById('content');
  if (!machines.length) {
    el.innerHTML = '<div class="no-data">No machines assigned</div>';
    return;
  }

  // Group by strip
  const strips = [];
  const stripMap = new Map();
  for (const m of machines) {
    const key = m.strip_device_id || '';
    if (!stripMap.has(key)) {
      const group = { alias: m.strip_alias || 'Unknown Strip', machines: [] };
      stripMap.set(key, group);
      strips.push(group);
    }
    stripMap.get(key).machines.push(m);
  }

  let html = '';
  let idx = 0;
  for (const strip of strips) {
    html += `<div class="strip-row"><div class="strip-label">${strip.alias}</div><div class="tiles">`;
    for (const m of strip.machines) {
      const st = m.state || 'null';
      const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
      const volts = m.power ? m.power.voltage.toFixed(1) + 'V' : '--';
      const amps = m.power ? m.power.amps.toFixed(3) + 'A' : '--';
      const kwh = m.power ? m.power.total_kwh.toFixed(1) + ' kWh' : '--';
      const stLabel = st === 'null' ? 'UNCALIBRATED' : st;
      const plugId = m.plug ? m.plug.plug_id : 0;
      html += `
        <a class="tile" href="/machine/${plugId}">
          <div class="tile-top">
            <div class="state-dot state-${st}"></div>
            <div class="machine-name">${m.name}</div>
          </div>
          <div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
          <div class="tile-watts">${watts}</div>
        </a>`;
      idx++;
    }
    html += '</div></div>';
  }
  el.innerHTML = html;

  idx = 0;
  for (const strip of strips) {
    for (const m of strip.machines) {
      const canvas = document.getElementById('spark-' + idx);
      if (canvas && m.sparkline && m.sparkline.length > 1) {
        drawSparkline(canvas, m.sparkline, m.sparkline_states);
      }
      idx++;
    }
  }

}

let lastMachines = [];

async function poll() {
  try {
    const resp = await fetch('/api/machines');
    const data = await resp.json();
    lastMachines = data.machines;
    renderMachines(data.machines);
  } catch (e) {}
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function allPower(on) {
  if (!on && !confirm('Turn off all machines?')) return;

  const btnOn = document.getElementById('btn-all-on');
  const btnOff = document.getElementById('btn-all-off');
  btnOn.disabled = true;
  btnOff.disabled = true;

  // Filter machines: already sorted in outlet order by the API
  const targets = lastMachines.filter(m => {
    if (!m.plug) return false;
    const isOn = m.power && m.power.watts > 0;
    if (on && isOn) return false;   // already on
    if (!on && !isOn) return false; // already off
    if (!on && m.state === 'PLAYING') return false; // don't turn off while playing
    return true;
  });

  const label = on ? 'Turning on' : 'Turning off';
  const btn = on ? btnOn : btnOff;

  for (let i = 0; i < targets.length; i++) {
    btn.textContent = label + ' ' + (i + 1) + '/' + targets.length + '...';
    try {
      await fetch('/api/machines/' + targets[i].plug.plug_id + '/power', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({on})
      });
    } catch (e) {}
    if (i < targets.length - 1) await sleep(on ? 2000 : 1000);
  }

  btnOn.textContent = 'All On';
  btnOff.textContent = 'All Off';
  btnOn.disabled = false;
  btnOff.disabled = false;
  poll();
}

poll();
setInterval(poll, 2000);
</script>
</body>
</html>
"""


DETAIL_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice — machine detail</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7; color: #1d1d1f; min-height: 100vh;
  }
  header {
    padding: 16px 28px; border-bottom: 1px solid #d2d2d7; background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header a { color: #007aff; text-decoration: none; font-size: 14px; font-weight: 500; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .meta-bar {
    display: flex; gap: 24px; padding: 16px 28px; background: #fff;
    border-bottom: 1px solid #d2d2d7; flex-wrap: wrap; align-items: center;
  }
  .meta-item { font-size: 13px; color: #86868b; }
  .meta-item .val { color: #1d1d1f; font-weight: 600; font-variant-numeric: tabular-nums; }
  .state-badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600;
  }
  .state-badge .dot { width: 8px; height: 8px; border-radius: 50%; }
  .state-OFF { background: #f2f2f7; color: #8e8e93; }
  .state-OFF .dot { background: #aeaeb2; }
  .state-ATTRACT { background: #e3f2fd; color: #1565c0; }
  .state-ATTRACT .dot { background: #007aff; }
  .state-PLAYING { background: #fce4ec; color: #c62828; }
  .state-PLAYING .dot { background: #ff3b30; }
  .state-IDLE { background: #fff8e1; color: #f57f17; }
  .state-IDLE .dot { background: #ff9500; }
  .actions { display: flex; gap: 8px; margin-left: auto; }
  .btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: default; }
  .btn-power-on { background: #34c759; color: #fff; }
  .btn-power-off { background: #ff3b30; color: #fff; }
  .btn-calibrate { background: #007aff; color: #fff; }
  .chart-wrap { padding: 20px 28px; }
  .chart-area {
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 16px; overflow: hidden;
  }
  svg { display: block; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  .chart-tooltip {
    position: absolute; pointer-events: none; background: rgba(255,255,255,0.95);
    border: 1px solid #d2d2d7; border-radius: 6px; padding: 8px 12px;
    font-size: 12px; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  .chart-tooltip .tt-time { color: #86868b; }
  .chart-tooltip .tt-watts { font-weight: 600; font-size: 14px; }
  .toast {
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 500;
    z-index: 100; transition: opacity 0.3s; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
  }
  .toast-success { background: #34c759; color: #fff; }
  .toast-error { background: #ff3b30; color: #fff; }
  .cal-info { font-size: 11px; color: #86868b; margin-top: 2px; }
</style>
</head>
<body>

<header>
  <a href="/">&larr; Dashboard</a>
  <h1 id="machine-name">Loading...</h1>
</header>

<div class="meta-bar" id="meta-bar">
  <div class="meta-item">Loading...</div>
</div>

<div class="chart-wrap">
  <div class="chart-area">
    <svg id="chart"></svg>
  </div>
</div>
<div class="chart-tooltip" id="chart-tooltip"></div>

<script>
const STATE_COLORS = { OFF: '#aeaeb2', ATTRACT: '#007aff', PLAYING: '#ff3b30', IDLE: '#ff9500' };
const plugId = parseInt(location.pathname.split('/').pop());

let machineData = null;

async function fetchMachineInfo() {
  const resp = await fetch('/api/machines');
  const data = await resp.json();
  return data.machines.find(m => m.plug && m.plug.plug_id === plugId);
}

function showToast(msg, type) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 300); }, 4000);
}

function renderMeta(m) {
  if (!m) return;
  machineData = m;
  document.getElementById('machine-name').textContent = m.name;
  document.title = 'juice — ' + m.name;

  const st = m.state || 'OFF';
  const isOn = m.power && m.power.watts > 0;
  const watts = m.power ? m.power.watts.toFixed(1) + ' W' : '--';
  const volts = m.power ? m.power.voltage.toFixed(1) + ' V' : '--';
  const amps = m.power ? m.power.amps.toFixed(3) + ' A' : '--';
  const kwh = m.power ? m.power.total_kwh.toFixed(1) + ' kWh' : '--';

  const bar = document.getElementById('meta-bar');
  bar.innerHTML = `
    <div class="state-badge state-${st}"><div class="dot"></div>${st}</div>
    <div class="meta-item"><span class="val">${watts}</span></div>
    <div class="meta-item"><span class="val">${volts}</span></div>
    <div class="meta-item"><span class="val">${amps}</span></div>
    <div class="meta-item">Total <span class="val">${kwh}</span></div>
    <div class="meta-item">Asset <span class="val">${m.asset_id}</span></div>
    <div class="meta-item">Plug <span class="val">${m.plug ? m.plug.alias : '--'}</span></div>
    <div class="meta-item">Strip <span class="val">${m.strip_alias || '--'}</span></div>
    <div class="actions">
      <button class="btn ${isOn ? 'btn-power-off' : 'btn-power-on'}" id="power-btn"
        onclick="togglePower(${isOn ? 'false' : 'true'})">${isOn ? 'Turn Off' : 'Turn On'}</button>
      <button class="btn btn-calibrate" id="cal-btn" onclick="calibrate()">
        ${m.calibrated ? 'Recalibrate' : 'Calibrate'}</button>
    </div>
  `;
}

async function togglePower(on) {
  const btn = document.getElementById('power-btn');
  btn.disabled = true;
  btn.textContent = on ? 'Turning on...' : 'Turning off...';
  try {
    const resp = await fetch('/api/machines/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error, 'error'); }
    else {
      showToast('Turned ' + (on ? 'on' : 'off'), 'success');
      // Optimistic update — flip button immediately
      if (machineData) {
        if (!on) { machineData.power = null; machineData.state = 'OFF'; }
        renderMeta(machineData);
        return;
      }
    }
  } catch (e) { showToast('Failed', 'error'); }
  btn.disabled = false;
  refreshMeta();
}

async function calibrate() {
  const btn = document.getElementById('cal-btn');
  btn.disabled = true;
  btn.textContent = 'Calibrating...';
  try {
    const resp = await fetch('/api/machines/' + plugId + '/calibrate', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error, 'error'); }
    else {
      const c = data.calibration;
      const idle = c.idle_max_rsd !== null ? c.idle_max_rsd.toFixed(1) : 'N/A';
      showToast(data.machine + ': idle=' + idle + ', play=' + c.play_min_rsd.toFixed(1), 'success');
    }
  } catch (e) { showToast('Calibration failed', 'error'); }
  btn.disabled = false;
  btn.textContent = 'Recalibrate';
}

async function refreshMeta() {
  const m = await fetchMachineInfo();
  if (m) renderMeta(m);
}

// -- Chart -------------------------------------------------------------------

const margin = { top: 12, right: 16, bottom: 36, left: 52 };
const width = Math.min(window.innerWidth - 88, 1200);
const height = 300;
const innerW = width - margin.left - margin.right;
const innerH = height - margin.top - margin.bottom;

const svg = d3.select('#chart').attr('width', width).attr('height', height);
const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

const clipId = 'clip-detail';
svg.append('defs').append('clipPath').attr('id', clipId)
  .append('rect').attr('width', innerW).attr('height', innerH);

const xScale = d3.scaleTime().range([0, innerW]);
const yScale = d3.scaleLinear().range([innerH, 0]);

const xAxisG = g.append('g').attr('class', 'axis').attr('transform', `translate(0,${innerH})`);
const yAxisG = g.append('g').attr('class', 'axis');
const gridG = g.append('g').attr('class', 'grid');
const chartG = g.append('g').attr('clip-path', `url(#${clipId})`);

const areaPath = chartG.append('path').attr('opacity', 0.15);
const linePath = chartG.append('path').attr('fill', 'none').attr('stroke-width', 1);
const hoverLine = chartG.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3')
  .attr('y1', 0).attr('y2', innerH).style('display', 'none');
const hoverDot = chartG.append('circle').attr('r', 4).style('display', 'none')
  .attr('fill', '#007aff').attr('stroke', '#fff').attr('stroke-width', 2);

const tooltip = d3.select('#chart-tooltip');

async function loadChart() {
  const resp = await fetch('/api/machines/' + plugId + '/readings?hours=24');
  const data = await resp.json();
  if (!data.timestamps.length) return;

  const points = data.timestamps.map((t, i) => ({ ts: new Date(t), watts: data.watts[i], state: data.states[i] || null }));

  xScale.domain(d3.extent(points, d => d.ts));
  yScale.domain([0, d3.max(points, d => d.watts) * 1.1 || 100]).nice();

  xAxisG.call(d3.axisBottom(xScale).ticks(8).tickFormat(d3.timeFormat('%-I:%M %p')));
  yAxisG.call(d3.axisLeft(yScale).ticks(6).tickFormat(d => d + ' W'));
  gridG.call(d3.axisLeft(yScale).ticks(6).tickSize(-innerW).tickFormat(''));

  // State backdrop bands
  chartG.selectAll('.state-band').remove();
  if (data.states && data.states.length) {
    const bands = [];
    let ci = 0;
    while (ci < points.length) {
      const st = points[ci].state;
      let cj = ci;
      while (cj < points.length && points[cj].state === st) cj++;
      bands.push({ state: st, start: points[ci].ts, end: points[cj - 1].ts });
      ci = cj;
    }
    chartG.selectAll('.state-band').data(bands).enter()
      .insert('rect', ':first-child').attr('class', 'state-band')
      .attr('x', d => xScale(d.start))
      .attr('width', d => Math.max(1, xScale(d.end) - xScale(d.start)))
      .attr('y', 0).attr('height', innerH)
      .attr('fill', d => STATE_COLORS[d.state] || '#aeaeb2')
      .attr('opacity', 0.18);
  }

  const line = d3.line().x(d => xScale(d.ts)).y(d => yScale(d.watts));
  const area = d3.area().x(d => xScale(d.ts)).y0(innerH).y1(d => yScale(d.watts));

  linePath.datum(points).attr('d', line).attr('stroke', '#007aff');
  areaPath.datum(points).attr('d', area).attr('fill', '#007aff');

  // Hover
  const bisect = d3.bisector(d => d.ts).left;
  svg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, g.node());
    if (mx < 0 || mx > innerW) { hoverLine.style('display','none'); hoverDot.style('display','none'); tooltip.style('display','none'); return; }
    const ts = xScale.invert(mx);
    let i = bisect(points, ts, 1);
    if (i >= points.length) i = points.length - 1;
    if (i > 0 && (ts - points[i-1].ts) < (points[i].ts - ts)) i--;
    const d = points[i];
    hoverLine.attr('x1', xScale(d.ts)).attr('x2', xScale(d.ts)).style('display', null);
    hoverDot.attr('cx', xScale(d.ts)).attr('cy', yScale(d.watts)).style('display', null);

    const fmt = d3.timeFormat('%-I:%M:%S %p');
    tooltip.html(`<div class="tt-time">${fmt(d.ts)}</div><div class="tt-watts">${d.watts.toFixed(1)} W</div>`)
      .style('display', 'block');
    const rect = document.getElementById('chart').getBoundingClientRect();
    let left = rect.left + margin.left + xScale(d.ts) + 14;
    let top = rect.top + margin.top + yScale(d.watts) - 20 + window.scrollY;
    if (left + 140 > window.innerWidth) left -= 170;
    tooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    hoverLine.style('display','none'); hoverDot.style('display','none'); tooltip.style('display','none');
  });
}

// -- Init --------------------------------------------------------------------

(async () => {
  const m = await fetchMachineInfo();
  if (m) renderMeta(m);
  else document.getElementById('machine-name').textContent = 'Machine not found';
  await loadChart();
})();

setInterval(refreshMeta, 5000);
</script>
</body>
</html>
"""
