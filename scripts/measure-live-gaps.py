"""Measure the live-frame gap distribution from a real tap.

Stage 4 needs a number: how long may pass between consecutive `live` frames for
one outlet before `OverloadWindow` should refuse to fire? Picking that from the
configured poll interval would be guessing -- the whole point of the coverage gate
is the gaps nobody predicted.

So this is a minimal server that speaks the `tap/wire.py` protocol well enough to
keep a real tap streaming (hello -> welcome, readings -> ack), and records the
arrival time of every `live` frame and the outlets in it. It stores no readings: a
measurement instrument should not be able to be mistaken for a juice.

Usage:
    uv run python scripts/measure-live-gaps.py --port 8123 --out gaps.json [--seconds N]
    uv run tap run --uplink-url ws://127.0.0.1:8123/ingest --uplink-token x ...

Induce the failures that produce the large gaps (they do not occur in a quiet
run): `--stall-after S --stall-for T` withholds acks, `--drop-after S` closes the
socket once. Note tap stops *sending* once its window fills, so a stall ends on
tap's own `BATCH_ACK_TIMEOUT` (120s) regardless of T. Note also that tap's
`lag_seconds` freezes during an ack outage (see todo.md), so `--live-max-lag`
cannot currently provoke live suppression this way.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time
from collections import defaultdict

from aiohttp import WSMsgType, web

PROTOCOL_VERSION = 1


class Recorder:
    def __init__(
        self,
        live_max_lag_s: float = 86_400.0,
        stall_after: float = 0.0,
        stall_for: float = 0.0,
        drop_after: float = 0.0,
    ) -> None:
        self.live_max_lag_s = live_max_lag_s
        # Stop acking for a while: the buffer backs up, tap's reported lag grows
        # past `live_max_lag_s`, and it suppresses live frames. That suppression
        # gap is the one `OverloadWindow`'s coverage bound has to survive, and it
        # will not happen by itself in a quiet eleven minutes.
        self.stall_after = stall_after
        self.stall_for = stall_for
        # Close the socket once, to measure the reconnect gap.
        self.drop_after = drop_after
        self.dropped = False
        self.events: list[tuple[float, str]] = []
        # (device_id, child_id) -> list of monotonic arrival times
        self.arrivals: dict[tuple[str, str], list[float]] = defaultdict(list)
        self.frames: list[tuple[float, int]] = []  # (t, row count)
        self.batches = 0
        self.rows = 0
        self.started = time.monotonic()
        self.hello: dict | None = None
        self.roster: list[dict] = []
        self.roster_frames = 0

    async def handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)
        print("[recorder] tap connected")
        async for message in ws:
            if message.type is not WSMsgType.TEXT:
                continue
            frame = json.loads(message.data)
            kind = frame.get("type")
            if kind == "hello":
                self.hello = frame
                print(f"[recorder] hello from {frame.get('tap_id')} v{frame.get('version')}")
                await ws.send_json(
                    {
                        "type": "welcome",
                        "protocol": PROTOCOL_VERSION,
                        "server_epoch": "gap-recorder",
                        "resume_from": None,
                        "live_max_lag_s": self.live_max_lag_s,
                    }
                )
            elif kind == "devices":
                self.roster = frame.get("devices") or []
                self.roster_frames += 1
                print(f"[recorder] roster: {len(self.roster)} outlets (frame {self.roster_frames})")
            elif kind == "readings":
                self.batches += 1
                self.rows += len(frame.get("rows") or [])
                elapsed = time.monotonic() - self.started
                stalling = (
                    self.stall_for
                    and self.stall_after <= elapsed < self.stall_after + self.stall_for
                )
                if stalling:
                    if not self.events or self.events[-1][1] != "stall-start":
                        self.events.append((elapsed, "stall-start"))
                        print(f"[recorder] {elapsed:.1f}s: withholding acks")
                    continue
                if self.events and self.events[-1][1] == "stall-start":
                    self.events.append((elapsed, "stall-end"))
                    print(f"[recorder] {elapsed:.1f}s: acking again")
                await ws.send_json(
                    {"type": "ack", "batch": frame.get("batch"), "cursor": frame.get("cursor")}
                )
                if self.drop_after and not self.dropped and elapsed >= self.drop_after:
                    self.dropped = True
                    self.events.append((elapsed, "socket-dropped"))
                    print(f"[recorder] {elapsed:.1f}s: dropping the socket")
                    await ws.close()
                    break
            elif kind == "live":
                now = time.monotonic()
                rows = frame.get("rows") or []
                self.frames.append((now, len(rows)))
                for row in rows:
                    if len(row) >= 3:
                        self.arrivals[(row[1], row[2])].append(now)
            elif kind == "ping":
                await ws.send_json({"type": "pong", "token": frame.get("token")})
        print("[recorder] tap disconnected")
        return ws

    def report(self) -> dict:
        """Per-outlet inter-arrival gaps, plus the whole-fleet distribution."""

        def stats(values: list[float]) -> dict:
            if not values:
                return {}
            ordered = sorted(values)

            def pct(p: float) -> float:
                if len(ordered) == 1:
                    return round(ordered[0], 3)
                idx = min(int(p * (len(ordered) - 1)), len(ordered) - 1)
                return round(ordered[idx], 3)

            return {
                "n": len(ordered),
                "min": round(ordered[0], 3),
                "p50": pct(0.50),
                "p95": pct(0.95),
                "p99": pct(0.99),
                "max": round(ordered[-1], 3),
                "mean": round(sum(ordered) / len(ordered), 3),
            }

        per_outlet = {}
        all_gaps: list[float] = []
        for (device_id, child_id), times in sorted(self.arrivals.items()):
            gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
            all_gaps.extend(gaps)
            per_outlet[f"{device_id[:12]}/{child_id[-2:] or '--'}"] = {
                "frames": len(times),
                "gaps": stats(gaps),
            }
        frame_gaps = [b - a for (a, _), (b, _) in zip(self.frames, self.frames[1:], strict=False)]
        return {
            "duration_s": round(time.monotonic() - self.started, 1),
            "live_frames": len(self.frames),
            "outlets_seen": len(self.arrivals),
            "readings_batches": self.batches,
            "readings_rows": self.rows,
            "roster_frames": self.roster_frames,
            "roster_has_emeter_present": any("has_emeter" in e for e in self.roster),
            "roster_device_alias_present": any(e.get("device_alias") for e in self.roster),
            "events": [(round(t, 1), what) for t, what in self.events],
            "frame_interval": stats(frame_gaps),
            "per_outlet_gap": stats(all_gaps),
            "outlets": per_outlet,
        }


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8123)
    ap.add_argument("--out", default="gaps.json")
    ap.add_argument("--seconds", type=float, default=0.0, help="stop after this long")
    ap.add_argument("--live-max-lag", type=float, default=86_400.0)
    ap.add_argument("--stall-after", type=float, default=0.0)
    ap.add_argument("--stall-for", type=float, default=0.0)
    ap.add_argument("--drop-after", type=float, default=0.0)
    args = ap.parse_args()

    rec = Recorder(
        live_max_lag_s=args.live_max_lag,
        stall_after=args.stall_after,
        stall_for=args.stall_for,
        drop_after=args.drop_after,
    )
    app = web.Application()
    app.router.add_get("/ingest", rec.handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", args.port)
    await site.start()
    print(f"[recorder] listening on ws://127.0.0.1:{args.port}/ingest")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    if args.seconds:
        loop.call_later(args.seconds, stop.set)
    await stop.wait()

    report = rec.report()
    with open(args.out, "w") as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps(report, indent=2))
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
