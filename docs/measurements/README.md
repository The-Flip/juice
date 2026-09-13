# Live-frame gap measurements, 2026-09-12

Taken against the museum's real fleet from a dev machine on the same LAN, with
`scripts/measure-live-gaps.py` as the receiver and a real `tap run` pointed at it.
Machines were drawing power throughout. Two reports:

| file | what | duration |
|---|---|---|
| `live-gaps-steady-20260912.json` | steady state, nothing induced | 660s |
| `live-gaps-induced-20260912.json` | acks withheld, then socket dropped | 300s |

## What the artifacts support

- **Per-outlet live-frame gaps in steady state**: n=28,244, p50 1.001s, p99 1.003s,
  max **1.006s**. A metronome. This is the number Stage 4's coverage bound for tap
  keys off, and ~5s clears it with room.
- **Under induced failure** the worst per-outlet gap was **2.382s**, and every
  outlet's max is exactly that value — consistent with one anomalous interval per
  outlet, i.e. one event.
- **The live channel kept its 1.001s cadence throughout the ack stall** (p99
  1.003s excluding that one gap). So an ack outage does not itself gap the live
  channel.
- **The roster fields from the Stage 5 work arrived**: `has_emeter` and
  `device_alias` are present in the `devices` frame received from real HS300s.

## What they do *not* support, despite earlier wording

These corrections come from an adversarial review of the commit that added the
files. The commit message (`44182be`) is not amended; this file is the record.

- **"A 120-second ack outage"** was not a configured 120s. The recorder was asked
  for `--stall-for 30`; the stall shows as 45.0s→165.3s because tap stops
  *sending* once its 4-batch window fills, so nothing arrives to end the stall
  until tap's own `BATCH_ACK_TIMEOUT` (120s) fires. The number is tap's timeout,
  not the experiment's parameter.
- **The 2.382s gap cannot be attributed to the socket drop alone.** `stall-end`
  and `socket-dropped` share the timestamp 165.3s — the first post-stall batch
  triggers both — so the resend and the drop are confounded in this run.
- **Live suppression was never exercised.** The induced run set `--live-max-lag 5`
  intending to trip it, but tap's `lag_seconds` freezes during an ack outage (a
  tap defect recorded in `todo.md`), so the threshold could not be crossed. The
  5s bound therefore has **no measurement behind it for the suppression gap**.
  That gap is bounded by construction — tap resumes live frames the moment lag
  drops under the threshold — but it has not been observed.
- **`roster_frames: 3` in the steady report is not evidence the heartbeat
  works.** The recorder counted neither connections nor hellos, and 1–2 reconnects
  fit in the ~33s before the first live frame, each of which sends a roster. The
  heartbeat is proven by `tests/tap/test_uplink.py::TestTheRosterHeartbeat`, not
  by this artifact.
- **The readings-channel figures quoted elsewhere** (7,325 gaps across 43 plugs,
  p50 1.001s, p99 1.11s, max 1.25s) and the **sweep timings** (p50 ~660ms, p95
  ~1040ms per HS300 with six metered outlets drawing) came from reading tap's
  buffer directly and from `/api/status` during the run. Neither is in these JSON
  files, and they were taken when 43 outlets were online rather than the 49 the
  JSON reports see. Treat them as a one-off observation, not an artifact.
- **The steady report predates the script's `events` field** — it was produced by
  an earlier revision of the recorder, which is why it has no `events` key.

## Reproducing

    uv run python scripts/measure-live-gaps.py --port 8123 --out /tmp/gaps.json --seconds 660
    uv run tap run --buffer-dir /tmp/tapbuf --uplink-url ws://127.0.0.1:8123/ingest \
        --uplink-token x --tap-id measure --web-port 8011

`tap probe` and `tap run` only read. `tap relay` switches a real outlet on a real
machine — it is not part of any measurement.
