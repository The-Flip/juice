"""HTTP server with API and web dashboard for juice."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from aiohttp import web

from juice.collector import PlugReading
from juice.state import Calibration, classify
from juice.store import Store

BUFFER_SIZE = 300  # ~5 minutes at 1s polling

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
    assignments: dict[int, tuple[str, str]] = field(default_factory=dict)  # plug_id -> (name, asset_id)
    plugs: dict[int, tuple[str, str, str]] = field(default_factory=dict)  # plug_id -> (device_id, child_id, alias)
    calibrations: dict[int, Calibration] = field(default_factory=dict)  # plug_id -> Calibration
    strip_aliases: dict[str, str] = field(default_factory=dict)  # device_id -> strip alias


async def handle_machines(request: web.Request) -> web.Response:
    state: RecorderState = request.app["recorder_state"]

    machines = []
    for plug_id, (name, asset_id) in state.assignments.items():
        reading = state.plug_readings.get(plug_id)
        buf = state.watt_buffers.get(plug_id)
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
        if buf:
            watts_list = list(buf)
            sparkline = watts_list
            cal = state.calibrations.get(plug_id)
            if cal:
                states = classify(watts_list, cal)
                if states:
                    machine_state = states[-1].value

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

        machines.append({
            "name": name,
            "asset_id": asset_id,
            "plug": plug_data,
            "power": power,
            "state": machine_state,
            "sparkline": sparkline,
            "strip_device_id": strip_device_id,
            "strip_alias": strip_alias,
        })

    machines.sort(key=lambda m: (m["strip_device_id"], m["plug"]["plug_id"] if m["plug"] else 0))
    return web.json_response({"machines": machines})


async def handle_dashboard(request: web.Request) -> web.Response:
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


def create_app(recorder_state: RecorderState) -> web.Application:
    app = web.Application()
    app["recorder_state"] = recorder_state
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/api/machines", handle_machines)
    return app


async def start_server(
    recorder_state: RecorderState,
    host: str = "0.0.0.0",
    port: int = 8000,
) -> web.AppRunner:
    app = create_app(recorder_state)
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
  }
  header h1 {
    font-size: 17px;
    font-weight: 600;
    color: #86868b;
  }
  header h1 span { color: #1d1d1f; }
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
    cursor: default;
    transition: box-shadow 0.15s;
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
  .tooltip {
    display: none;
    position: fixed;
    background: #fff;
    border: 1px solid #d2d2d7;
    border-radius: 8px;
    padding: 10px 12px;
    box-shadow: 0 4px 16px rgba(0,0,0,0.12);
    white-space: nowrap;
    z-index: 10;
    font-size: 12px;
    color: #1d1d1f;
    pointer-events: none;
  }
  .tooltip table { border-collapse: collapse; }
  .tooltip td {
    padding: 1px 0;
  }
  .tooltip .tl { color: #86868b; padding-right: 10px; }
  .tooltip .tv { font-variant-numeric: tabular-nums; font-weight: 500; }
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
</header>
<div id="content">
  <div class="no-data">Connecting...</div>
</div>
<script>
const STATE_COLORS = {
  OFF: '#aeaeb2', ATTRACT: '#007aff', PLAYING: '#ff3b30', IDLE: '#ff9500'
};

function drawSparkline(canvas, data, state) {
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth * dpr;
  const h = canvas.clientHeight * dpr;
  canvas.width = w;
  canvas.height = h;
  ctx.clearRect(0, 0, w, h);
  if (!data || data.length < 2) return;
  const max = Math.max(...data, 1);
  const step = w / (data.length - 1);
  const pad = 2 * dpr;
  const color = STATE_COLORS[state] || '#aeaeb2';
  ctx.beginPath();
  ctx.moveTo(0, h);
  for (let i = 0; i < data.length; i++) {
    ctx.lineTo(i * step, h - pad - (data[i] / max) * (h - 2 * pad));
  }
  ctx.lineTo(w, h);
  ctx.closePath();
  ctx.fillStyle = color + '18';
  ctx.fill();
  ctx.beginPath();
  for (let i = 0; i < data.length; i++) {
    const x = i * step;
    const y = h - pad - (data[i] / max) * (h - 2 * pad);
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
      html += `
        <div class="tile">
          <div class="tile-top">
            <div class="state-dot state-${st}"></div>
            <div class="machine-name">${m.name}</div>
          </div>
          <div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
          <div class="tile-watts">${watts}</div>
          <div class="tooltip"><table>
            <tr><td class="tl">State</td><td class="tv">${stLabel}</td></tr>
            <tr><td class="tl">Power</td><td class="tv">${watts}</td></tr>
            <tr><td class="tl">Voltage</td><td class="tv">${volts}</td></tr>
            <tr><td class="tl">Current</td><td class="tv">${amps}</td></tr>
            <tr><td class="tl">Total</td><td class="tv">${kwh}</td></tr>
            <tr><td class="tl">Asset</td><td class="tv">${m.asset_id}</td></tr>
            <tr><td class="tl">Plug</td><td class="tv">${m.plug ? m.plug.alias : '--'}</td></tr>
          </table></div>
        </div>`;
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
        drawSparkline(canvas, m.sparkline, m.state);
      }
      idx++;
    }
  }

  // Position tooltips on hover using fixed positioning
  for (const tile of el.querySelectorAll('.tile')) {
    const tip = tile.querySelector('.tooltip');
    if (!tip) continue;
    tile.addEventListener('mouseenter', () => {
      tip.style.display = 'block';
      const tr = tile.getBoundingClientRect();
      const th = tip.offsetHeight;
      const tw = tip.offsetWidth;
      const gap = 6;
      // Center horizontally on the tile, clamp to viewport
      let left = tr.left + tr.width / 2 - tw / 2;
      left = Math.max(4, Math.min(left, window.innerWidth - tw - 4));
      // Prefer above; flip below if it would clip the top
      let top;
      if (tr.top - th - gap >= 0) {
        top = tr.top - th - gap;
      } else {
        top = tr.bottom + gap;
      }
      tip.style.left = left + 'px';
      tip.style.top = top + 'px';
    });
    tile.addEventListener('mouseleave', () => {
      tip.style.display = 'none';
    });
  }
}

async function poll() {
  try {
    const resp = await fetch('/api/machines');
    const data = await resp.json();
    renderMachines(data.machines);
  } catch (e) {}
}

poll();
setInterval(poll, 2000);
</script>
</body>
</html>
"""
