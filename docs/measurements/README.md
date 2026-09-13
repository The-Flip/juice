# Live-frame gap measurements, 2026-09-12

Taken against the museum's real fleet from a dev machine on the same LAN, with
`scripts/measure-live-gaps.py` as the receiver and a real `tap run` pointed at it.
Machines were drawing power throughout. Two reports:

| file | what | duration |
|---|---|---|
| `live-gaps-steady-20260912.json` | steady state, nothing induced | 660s |
| `live-gaps-induced-20260912.json` | acks withheld, then socket dropped | 300s |

## What the artifacts support

- **Per-outlet live-frame gaps in steady state, while an outlet is streaming**:
  n=28,244, p50 1.001s, p99 1.003s, max **1.006s**. A metronome. This is the
  number Stage 4's coverage bound for tap keys off, and ~5s clears it with room
  -- but read the next section before treating it as a bound on *coverage*.
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
files, and from a CodeRabbit review of the PR. The commit message (`44182be`) is
not amended; this file is the record.

- **The 1.006s "max" is not a coverage bound; it is an inter-arrival bound.** Both
  reports were produced by a recorder that measured only the gap between
  *consecutive* frames of an outlet. An outlet that stops sending has no next
  frame, so its silence is recorded nowhere, and the report reads clean over it.
  That is not hypothetical: in the steady run, strip `80061119BAE8` (six outlets)
  is present in **240 of 627** live frames, and its per-outlet max is 1.006s like
  everyone else's. About 387s of the 660s window passed with no frame from it,
  and the artifact cannot say whether that was a late discovery, a mid-run
  dropout, or several -- `scripts/measure-live-gaps.py` now records per-outlet
  first/last frame, head and tail gaps, and a roster-checked `coverage_gap` with
  the worst offender named, but these two files predate that and were not
  re-taken (the fleet was unreachable from the dev machine when the recorder was
  fixed). The induced run has every outlet in every frame, so it has no such
  hole.

  What this does and does not change for Stage 4: the ~5s bound is a bound on
  the gap a window may bridge *between* samples while a stream is flowing, and
  the artifacts do support that. An outlet that goes dark for minutes is not a
  gap for the window to tolerate; it is an outlet with no data, and the gate's
  answer to it has to be refusal to fire -- which a 5s bound gives -- followed by
  the staleness sweep marking it unreachable. Do not raise the bound to "cover"
  a dropout like this one.

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

The recorder now reports `coverage_gap` and `worst_coverage_gap` against the
roster tap sends, and counts `connections`/`hellos`, so a re-run answers the two
questions above that these files cannot. Take one before Stage 4 picks its bound.

    uv run python scripts/measure-live-gaps.py --port 8123 --out /tmp/gaps.json --seconds 660
    uv run tap run --buffer-dir /tmp/tapbuf --uplink-url ws://127.0.0.1:8123/ingest \
        --uplink-token x --tap-id measure --web-port 8011

`tap probe` and `tap run` only read. `tap relay` switches a real outlet on a real
machine — it is not part of any measurement.
