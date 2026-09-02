# juice `/api/v2` — client reference

Everything needed to build a client without reading the server source. Shapes
below were captured from a running server against the seeded fixture, not
written from memory.

Companion documents: `domain_model.md` (what juice knows about),
`user_needs.md` (what operators do), `status_vocabulary.md` (why status is
shaped this way). This document is the wire contract; those three are the *why*.

v1 (`/api/*`) still exists and still works. It is frozen — do not build against
it. It will be deleted with the old UI.

---

## 1. The five things worth knowing before you start

Most of this API is unsurprising. These five are not, and each exists because
the v1 equivalent caused a real bug.

**1. `status` is observed; `pending_command` is intended.** A machine mid-reboot
honestly reports `no_draw` — that *is* its state — while `pending_command` says a
reboot is in flight. Render "Rebooting…" from the command and take the status
from `status`. Never fold them together.

**2. A write returning 202 does not mean the action happened.** It means the
device call has started. Wait for the command to reach `confirmed` on the
stream. This matters because actuation is a WAN round trip to the TP-Link cloud
and takes 4–30 seconds.

**3. The stream's `seq` is dense per connection.** If you receive `seq` that
isn't `last + 1`, you missed something: refetch `/api/v2/floor`. This replaces
polling entirely — do not add a refresh timer.

**4. Anything refusable without a network call is refused synchronously.** Locks,
conflicts, unknown machines and bad input come back as 4xx immediately, not as a
`failed` phase later. If you got a 202, the only remaining outcomes are the
device succeeding, failing, or timing out.

**5. Reads are anonymous-or-authenticated; writes need a capability.** There are
three audiences, and the same endpoint returns less to an anonymous caller
rather than 401-ing. See §8.

---

## 2. Conventions

**Base path** `/api/v2`. **Content type** JSON throughout, except
`/api/v2/stream` (`text/event-stream`).

**Identity.** Machines are addressed by `asset_id` (`M0021`) — durable across
outlet moves and printed on the machine. Outlets by `plug_id` (int), strips by
`device_id`, circuits by `circuit_id`, operations by `operation_id`. Do not
assume `asset_id` matches `M\d+`; FlipFix owns the format and other prefixes
exist.

**Timestamps** are RFC 3339 with a real offset (`2026-09-01T21:10:20+00:00`).
Never a bare `Z` appended to a naive value.

**Errors** always take this shape:

```json
{"error": {"code": "machine_locked", "message": "…", "detail": {"lock_mode": "on"}}}
```

Branch on `code`, never on `message`. `detail` is present when there is
something actionable in it. Codes in use:

| code | meaning |
|---|---|
| `unauthenticated` | no session; log in (v1 returns a flat string here; v2 does not) |
| `forbidden` | logged in, lacks `control_power` |
| `bad_request` | malformed input; `detail` says what was expected |
| `unknown_machine` / `unknown_outlet` / `unknown_strip` / `unknown_operation` | no such thing |
| `ambiguous_assignment` | two live outlets claim one asset tag — a Kasa label typo |
| `machine_locked` | a lock forbids this direction |
| `not_controllable` | outlet cannot be switched right now |
| `command_in_flight` | a conflicting command is mid-dispatch |
| `operation_in_progress` | a bulk operation is running |

---

## 3. Status vocabulary

Every machine and outlet carries the same status, derived server-side exactly
once. Do not re-derive it from `relay` and `draw_watts`.

| `status` | meaning | suggested colour |
|---|---|---|
| `unreachable` | device not answering — we know nothing current | grey |
| `off` | relay open | near-black |
| `no_draw` | relay closed, metered, drawing < 2 W | **orange — needs attention** |
| `powered` | drawing (or unmeasurable), activity unknown | blue |
| `attract` | drawing, waiting for a player | blue |
| `playing` | drawing, game in progress | green |
| `abandoned` | game in progress, player walked away | **yellow — needs attention** |

`powered` and `attract` are deliberately the same colour and different labels.
Both are the good, normal case; the distinction is that `attract` is a
measurement and `powered` is an honest "we can't tell". Machines that can't be
classified (low-draw tables, video games) are a permanent part of the collection,
not a backlog.

Supporting fields:

- `activity` — `attract | playing | abandoned | null`. Null whenever the machine
  isn't drawing, isn't reachable, isn't metered, or isn't calibrated.
- `activity_unknown_because` — `not_drawing | uncalibrated | unmetered |
  no_measurement | unreachable | null`. Always set when `activity` is null. Use
  it to say *why* rather than showing a blank.
- `relay` — `"on" | "off"`, or `null` when `status` is `unreachable`. The
  hardware fact. Use for the toggle's direction.
- `draw_watts` — float, or `null` when unmeasurable or `unreachable`. `null` is
  not zero.
- `status_since` — RFC 3339, when this status began. Exact for `off`,
  `no_draw` and `unreachable`; for drawing statuses it means "drawing since".

**`unreachable` reports nothing live.** `relay` and `draw_watts` are both `null`
for an unreachable machine or outlet, because the last values seen before the
device went quiet are indistinguishable from fresh ones once they are in the
live fields — a dead six-outlet strip would otherwise render as six machines
drawing ~120 W. `status_since` says how long ago we last heard anything. A strip
whose outlets are all unreachable reports `draw_watts: null` and counts them all
in `unmeasured_outlets`, rather than totalling stale readings.

**Invariant you can rely on:** a non-null `activity` always implies `status` is
one of `attract`/`playing`/`abandoned`. A payload contradicting that is a bug.

---

## 4. Reading the floor

### `GET /api/v2/floor` — the whole Tier-1 view in one request

This is what the UI should open on, and what it refetches after a stream gap.
No sparklines: it is refetched often and held all day on a tablet.

```json
{
  "counts": {"total": 31, "powered": 23, "playing": 6, "problems": 3},
  "problems": [
    {"asset_id": "M0001", "name": "Blackout", "status": "no_draw",
     "since": "2026-09-01T21:10:20+00:00"}
  ],
  "infrastructure": [
    {"device_id": "…", "name": "Row 2", "kind": "unreachable_device",
     "since": "…", "affects": ["M0005", "M0006"]}
  ],
  "groups": [{"device_id": "…", "name": "Row 1", "machines": [ /* §5 */ ]}],
  "operation": null
}
```

`problems` is a **filter on status** (`no_draw` + `abandoned`), excluding
anything with a `pending_command`. It cannot drift from the tiles because it is
the same data. A machine five seconds into a reboot is genuinely `no_draw` and is
deliberately not listed.

`infrastructure` is one entry per unreachable *device*, not per machine — a dead
six-outlet strip is one thing to go and look at. Anonymous callers get `[]`.

`groups` follows the operator's strip ordering. Anonymous callers get a single
unlabelled group so the ordering still matches what an operator sees.

### `GET /api/v2/machines` and `/api/v2/machines/{asset_id}`

A machine object:

```json
{
  "asset_id": "M0001", "name": "Blackout", "year": null,
  "status": "no_draw", "activity": null,
  "activity_unknown_because": "not_drawing",
  "relay": "on", "draw_watts": 0.0,
  "status_since": "2026-09-01T21:10:20+00:00",
  "lock_mode": null, "pending_command": null,
  "plug_id": 1, "device_id": "…", "strip": "Row 1", "outlet": 1,
  "calibration": {"calibrated": true}
}
```

The last five keys are operator-only (§8). `lock_mode` is `null`, `"on"`
(pinned on) or `"off"` (pinned off, typically broken).

A machine that has moved outlets appears **once**, on its live outlet. Two
*online* outlets claiming one tag is a 409 `ambiguous_assignment` listing both —
fix the Kasa label rather than guessing.

---

## 5. The live stream

### `GET /api/v2/stream` — SSE

Every frame carries `seq` and `type`.

```text
data: {"seq":1,"type":"hello","epoch":"4efb68a9…","current_operation":null}
data: {"seq":2,"type":"reading_tick","machines":[…]}
```

**`hello`** arrives first. `epoch` identifies the server process: a different
epoch on reconnect means sequence numbers restarted and you should full-resync.

**`reading_tick`** — one entry per machine:

```json
{"plug_id": 3, "asset_id": "M0003", "status": "playing", "activity": "playing",
 "activity_unknown_because": null, "status_since": "…",
 "relay": "on", "draw_watts": 220.0}
```

Join on `asset_id`. `plug_id` is present for operators only, like everywhere
else in v2 (§8) — an anonymous subscriber's ticks carry `asset_id` and no
`plug_id`.

**The cadence is not a clock.** A tick is published once per recorder poll, and
a poll is a serial round trip to the TP-Link cloud per strip: measured at 6–9
seconds on production, and slower as more outlets draw. Do not infer anything
from the gap between ticks — the 15 s heartbeat below is the liveness signal,
and a `seq` gap is the staleness signal.

**`command`** — see §6.

**`resync_required`** — you fell behind and events were dropped. Refetch
`/api/v2/floor`.

**Comment frames** (`: ping`) arrive every 15 s while idle. `EventSource`
ignores them; they exist so a proxy-killed connection stops looking merely quiet.

### The client contract

```js
if (msg.type === "hello") {
  // Only `hello` carries `epoch`. Checking it on every frame compares
  // undefined against the known value on each reading tick and refetches
  // forever.
  if (knownEpoch && msg.epoch !== knownEpoch) refetchFloor();
  knownEpoch = msg.epoch;
  lastSeq = msg.seq;
  return;
}
if (msg.seq !== lastSeq + 1) refetchFloor();       // gap: events were dropped
if (msg.type === "resync_required") refetchFloor();
lastSeq = msg.seq;
```

Show a visible "stale" indicator while resyncing, and back off on repeated
resyncs — a client resyncing every second is what overflowed the queue.

Anonymous subscribers receive `hello`, `reading_tick` and `resync_required`
only. Command and operation traffic names people.

---

## 6. Writes and the command lifecycle

### `POST /api/v2/machines/{asset_id}/power` — body `{"on": true|false}`
### `POST /api/v2/machines/{asset_id}/reboot`

```json
202
{"command_id": "cb1643b4…", "kind": "turn_off",
 "expect": {"relay": "off"}, "timeout_ms": 23500,
 "stream": "/api/v2/stream",
 "terminal_phases": ["confirmed","failed","timed_out","refused","superseded"]}
```

Show *pending* immediately, then follow `command_id` on the stream:

```text
accepted → dispatching → retrying* → awaiting_relay → confirmed
                                                    ↘ failed | timed_out
```

`confirmed` is emitted only when a **fresh relay reading** matches `expect` —
never when the write returns. A reboot additionally requires evidence the cycle
happened (an observed off→on, or both cloud legs acknowledged), so the pre-off
"on" cannot settle it prematurely.

`attempt` rises on retries; show "retrying, attempt 3" rather than an
unexplained spinner. `timeout_ms` is the server's, and a retry extends it — the
value on the latest event supersedes the one in the 202.

**Idempotency.** Repeating the *same* action while it is in flight returns the
**same `command_id`** with 202 and does not call the device again. A double-tap
is free.

**Conflicts.** An *opposing* action while a cloud call is mid-dispatch returns
409 `command_in_flight` with the holder attached — render "Dana is rebooting
this. Watch?". Once the call has landed and it is merely `awaiting_relay`, an
opposing command is *allowed* and supersedes the first (which ends
`superseded`). That window is deliberately narrow so an operator can always cut
power to a machine that is smoking.

### Bulk operations

```text
GET  /api/v2/operations/current      → {"operation": null}  (never a bare null)
POST /api/v2/operations              → {"kind": "all_on"|"all_off",
                                        "scope": {"device_id": "…"}}   → 202
POST /api/v2/operations/{id}/cancel
```

Omit `scope` entirely for a museum-wide operation. A present-but-not-an-object
`scope` is a 400 — a malformed scope must never become "the whole museum".

One operation runs at a time (the building is one power domain and the
inter-step stagger limits inrush). A second returns 409
`operation_in_progress` with the running operation attached, enough to render
"Dana started All On — 12 of 31. [Watch] [Cancel]".

Bulk steps mint commands too, correlated by `operation_id`.

Machines are skipped, not failed, when a lock forbids the direction or when the
machine is currently `playing` on an all-off. Say *why* something was skipped;
the server knows.

---

## 7. Collections and metrics

```text
GET /api/v2/outlets              GET /api/v2/outlets/{plug_id}
GET /api/v2/strips               GET /api/v2/strips/{device_id}
GET /api/v2/circuits
GET /api/v2/power-events         ?before=<event_id>&limit=<1..200>&asset_id=…
```

Outlets carry the same status vocabulary as machines (with `activity` always
null — an outlet has no machine to classify) plus `machine`, which is `null`
when unassigned.

Strips carry `outlets`, `draw_watts` and `unmeasured_outlets` — a strip with an
unmetered outlet does not report a total that silently omits it.

`power-events` is the audit log: `event_id`, `ts`, `action`, `source`, `result`
(`ok`/`error`/`refused`), `actor`, `error`, `operation_id`, `machine`.
Cursor-paginate on `next_before`; when it is `null` you have reached the end.
An oversized `limit` is a 400, never a silent clamp.

### Metrics — one window convention

```text
GET /api/v2/metrics/energy        per machine, kWh, with daily breakdown
GET /api/v2/metrics/play-hours    per machine, plus measurable/unmeasurable counts
GET /api/v2/metrics/utilization   dense date × hour grid
GET /api/v2/metrics/cost          operator-only
GET /api/v2/metrics/peaks         ?by=circuit|strip
```

All take **the same window**:

```text
?window=30d          also 7d, 2w, 24h (hours must be a multiple of 24)
?from=2026-08-01&to=2026-08-31
```

Half-open `[from, to)`, anchored on local **America/Chicago** days, because
"Saturday" means the Saturday a human in the museum recognises. Windows over 365
days are a **400, not a clamp**. Sub-day hour windows are a 400 — everything is
day-anchored, so `1h` cannot be honoured and silently widening it would return
more than you asked for.

Every response echoes what it resolved to:

```json
"window": {"spec": "7d", "from": "2026-08-26", "to": "2026-09-02",
           "tz": "America/Chicago", "grain": "day", "days": 7, "hours": 168}
```

Use `hours` rather than `days * 24`: a local day is 23 or 25 hours twice a year.

`utilization` is dense — every day in the window × 24 hours, each cell carrying
`measured`. A `false` cell means no data, which is different from zero play.

`play-hours` reports `measurable_machines` and `unmeasurable_machines`. An
uncalibrated machine has no *measurable* play, which is not the same as zero;
without the counts a short list reads as the whole floor.

---

## 8. Auth and redaction

Three audiences:

| | sees |
|---|---|
| **anonymous** | the floor, machines, usage metrics, the stream's reading ticks |
| **authenticated** | all reads, including wiring, the audit log and cost |
| **`control_power`** | all of the above, plus every write |

Anonymous callers are not rejected from public-readable endpoints — they receive
the same object with operator-only keys **absent**: `plug_id`, `device_id`,
`strip`, `outlet`, `calibration`, and any `actor`. Write endpoints and
operator-only reads return 401.

This is deliberate and long-standing: the museum wants a logged-out page showing
what's on the floor. See `user_needs.md` §1.D. Note the redaction removes
top-level keys, and nested objects redact themselves — `pending_command` drops
its `actor` for anonymous callers.

Sessions are 30-day cookies. Expiry is absolute, not rolling. A lost session
should be **loud and one-tap recoverable**: the old UI's worst bug was that
losing one silently downgraded the page to read-only, so buttons appeared to do
nothing.

---

## 9. Building the client — a suggested order

1. `GET /api/v2/floor`, render `counts`, `problems`, `groups`. That is J3, the
   single largest piece of the interface.
2. Subscribe to `/api/v2/stream`; apply `reading_tick`, handle gaps by
   refetching floor. Delete any polling you were tempted to add.
3. Power and reboot with the 202 → command → `confirmed` cycle, including
   `retrying` and the conflict 409.
4. All-on / all-off with progress and explained skips. That is J1 and J2, ~90%
   of real usage.
5. Machine detail: `/machines/{asset_id}` plus `power-events?asset_id=…`, which
   answers both "who turned this off?" and "what has my colleague tried?".
6. Metrics.

Test against the seeded fixture, which has real problems in it:

```shell
uv run python -m tests.e2e.serve --port 8150 --interactive --with-problems
```

That gives ~31 machines with `no_draw`, `abandoned` and `unreachable` states
present and durations backdated, so the Problems section has something to
render. Without `--with-problems` the fixture is uniformly healthy and any
assertion about problems passes vacuously.
