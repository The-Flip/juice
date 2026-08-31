# Juice — Domain Model

Working reference for the new web interface. Describes *what juice knows about*,
independent of how the current UI happens to present it. Grounded in
`juice/store.py` (schema), `juice/state.py` (classification), `juice/server.py`
(derived values), and `juice/recorder.py` (lifecycle).

Scale as of Aug 2026: **~33 machines**, a handful of HS300 strips (6 outlets each),
a few single plugs, a small number of breaker circuits, 2–3 air monitors.
Readings arrive at **1 Hz**; the readings table is the large one (~100 MB DuckDB).

---

## 1. Physical layer

### Strip (a.k.a. device)
A Kasa power strip or plug, addressed in the TP-Link cloud by `device_id`.

| Field | Source | Notes |
|---|---|---|
| `device_id` | Kasa cloud | stable primary key |
| `name` | `strips` table | operator-editable display name; falls back to the Kasa alias |
| `sort_order` | `strips` table | manual ordering, drives dashboard grouping order |

Kinds of device we see in practice:
- **HS300** — 6 individually switched, individually metered outlets. The workhorse.
- **Single smart plug** — one outlet, may or may not have an energy meter (`has_emeter`).
- **Unsupported (SMART/KLAP, e.g. EP25)** — appears in the cloud device list but every
  read fails. A permanent, known-bad category, not a transient outage.

A device is **offline** after `OFFLINE_FAILURE_THRESHOLD` (3) consecutive failed reads.
Offline is a device-level property that propagates down to every outlet and machine on it.

### Plug (a.k.a. outlet)
One switchable socket. `(device_id, child_id)` unique; `plug_id` is juice's local
small integer and is what the URLs and APIs actually use.

| Field | Notes |
|---|---|
| `plug_id` | local surrogate key, used in `/machine/{plug_id}` and most API paths |
| `device_id`, `child_id` | Kasa addressing |
| `alias` | the Kasa-app label — **this is the assignment mechanism** (see §3) |
| `has_emeter` | false ⇒ we can switch it but never measure it |
| `outlet_number` | derived from `child_id`; physical position 1–6 on the strip |

An outlet may be **unassigned** — a real, switchable socket with no machine on it
(fans, lights, a spare). Unassigned outlets still participate in bulk operations and
still draw measurable power.

### Circuit
An electrical breaker. This is the *building* layer, above strips.

| Field | Notes |
|---|---|
| `circuit_id`, `panel`, `breaker` | `(panel, breaker)` unique |
| `description` | human label, e.g. "Backline wall" |
| `amps` | breaker rating — the headroom denominator |

Membership is **strip → circuit**, one circuit per strip (`circuit_devices` PK on
`device_id`). A machine's circuit is therefore transitive: machine → plug → strip → circuit.
Circuits exist to answer one question: *are we about to trip a breaker?*

---

## 2. Identity layer

### Machine
A pinball machine, arcade cabinet, claw machine, or video game. **Identity is owned by
FlipFix**, not by juice.

| Field | Source | Notes |
|---|---|---|
| `machine_id` | local | surrogate key |
| `asset_id` | FlipFix | e.g. `M0037` — the real-world identity, printed on the machine |
| `name` | FlipFix | e.g. "Godzilla (Premium)", "Duck Locker (Claw Machine)" |
| `year` | FlipFix (`model.year`) | used to order bulk power-on (oldest first) |
| `lock_mode` | local | `NULL` \| `'on'` \| `'off'` — see below |
| `locked` | local | legacy boolean, superseded by `lock_mode` |

**Lock** is an operator override that exempts a machine from bulk operations:
- `lock_mode = 'on'` — keep it on; all-off skips it.
- `lock_mode = 'off'` — keep it off; all-on skips it. (Typical for a broken machine.)
- Locks also block reboot in either direction.

Note the population is heterogeneous. Not everything is a pinball machine, and the
non-pinball members (claw machine, video games, low-draw machines like Lightning)
are precisely the ones that break naive assumptions about power signatures.

### Assignment
The time-versioned binding of a machine to an outlet.

```
assignments(plug_id, machine_id, assigned_from, assigned_until)
```

`assigned_until IS NULL` ⇒ the current binding. History is retained so past readings
can be attributed to the machine that was actually plugged in at the time.

**There is no manual assignment UI, by design.** The recorder extracts an asset tag
matching `M\d+` from the Kasa outlet alias and matches it to a FlipFix machine
(`refresh_metadata`). To reassign, you relabel the outlet in the Kasa app; the
recorder picks it up within ~60s. A machine that moved leaves a stale open assignment
on the old (now offline) outlet, which `handle_machines` suppresses when the same
machine also appears on an online outlet.

---

## 3. Measurement layer

### Reading
One 1 Hz sample per metered outlet.

```
readings(ts, plug_id, watts, voltage, amps, total_kwh)
```

`ts` is UTC. All display is **America/Chicago** (hardcoded — the museum's timezone).
Day boundaries for usage, play hours, and cost are *local* days, so "Saturday" means
the Saturday a human in the building recognises.

Recent readings are also held in memory as a rolling `watt_buffer` per plug
(`BUFFER_SIZE = 3600`, ~60 minutes), which is what feeds sparklines and live state.

### Rollups
Precomputed because the raw table is too big to chart directly.

| Table | Grain | Purpose |
|---|---|---|
| `hourly_usage` | plug × hour | kWh, sample count, peak watts, p99 peak |
| `hourly_strip_peak` | strip × hour | peak simultaneous strip draw |
| `hourly_circuit_peak` | circuit × hour | peak summed draw across the circuit's strips — the breaker-trip number |
| `hourly_play_seconds` | machine × **local** hour | `play_seconds` / `on_seconds` — numerator and denominator for utilization |

p99 variants exist to discard inrush spikes so the "peak" number is one you can
actually plan a breaker against.

### Air reading
Deliberately parallel to the power pipeline — **room/zone-scoped**, with no asset tag,
no machine, and no power control.

```
air_sensors(mac, name, first_seen, last_seen, online)
air_readings(ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
```

Devices report ~every 15 min, so this is small enough to chart raw (no rollups).
Display name comes from the Qingping+ app. Polled every 5 min from a separate cloud
account; the loop is skipped entirely when the Qingping env vars are absent.

---

## 4. Derived / interpreted layer

This is where the current UI's confusion lives. **Treat this section as the vocabulary
contract for the new interface.**

### Relay state vs. draw — the core distinction
These are two different facts and must never be collapsed:

- **Relay on** (`reading.is_on`) — the outlet is energized. A hardware fact.
- **Drawing** (`watts >= OFF_WATTS`, currently **2.0 W**) — a real load is present.

A machine can be relay-on but not drawing (machine's own switch is off, unplugged,
blown fuse). Naming rule already established for this codebase: call watts-derived
values **"drawing"**, and reserve **"on"** / `is_on` for the relay.

### `power_status` — the single presentation-level value
`_power_status(reading, has_emeter, offline)` collapses the above into one of four
strings, and is intended as the one source of truth for every surface:

| Value | Meaning |
|---|---|
| `offline` | the device is unreachable — we know nothing current |
| `off` | relay is off |
| `no_draw` | relay on, metered, drawing < 2 W — machine is off/unplugged/faulted |
| `on` | drawing power, or an unmetered outlet whose relay is on |

Note `no_draw` is the interesting one: it's the "someone turned the machine off at the
machine" / "something is broken" signal, and the current UI barely surfaces it.

### `State` — behavioural classification
`juice/state.py` classifies a *window* of readings, not a single sample, using the
relative standard deviation (RSD) of a 30-sample rolling window over despiked watts:

| State | Meaning |
|---|---|
| `OFF` | drawing < `OFF_WATTS` |
| `ATTRACT` | powered, running attract mode — stable draw |
| `PLAYING` | active game — solenoids firing, high RSD |
| `IDLE` | game started but abandoned — ultra-stable draw |
| `OFFLINE` | *not a real classifier output* — injected by the server for display |

`OFFLINE` being smuggled into the same enum at the presentation layer is one of the
existing concept collisions to resolve in the redesign.

### Calibration
Per-machine RSD thresholds that make `State` meaningful.

```
calibrations(machine_id, idle_max_rsd, play_min_rsd)
```

- `idle_max_rsd = NULL` ⇒ IDLE is impossible for this machine.
- Auto-calibration **can legitimately fail** (`CalibrationError`) when attract and play
  signatures are indistinguishable — true for low-draw machines (Lightning, ~3.5 W) and
  video games with no solenoids.
- Failure falls back to `UNCALIBRATED_CALIBRATION` (`play_min_rsd = inf`), so such a
  machine reads **ATTRACT when drawing** — i.e. honestly "on", never PLAYING, never a
  meaningless gray.
- A handful of machines have hardcoded `SEED_CALIBRATIONS` by name.

**UI consequence:** "calibrated" is a first-class, user-visible property. Play-time and
utilization numbers only mean something for calibrated machines, and the interface must
say so rather than silently reporting zeros.

### Power baseline & overload
```
power_baselines(machine_id, baseline_watts, computed_at)
```
Baseline = p99 of per-minute average watts over the trailing 30 days, refreshed hourly.
A machine needs ≥ 500 minutes of on-history before it is armed (fail-safe).

**Overload** = trailing 120 s mean watts exceeds `max(2.5 × baseline, 80 W)`. This is a
stuck-solenoid detector: real gameplay spikes briefly, a stuck coil sustains. On fire, in
`live` mode, juice **turns the machine off**, files an `unplayable` problem report to
FlipFix, and marks the machine broken. Modes: `live` / `shadow` / `off`
(`JUICE_OVERLOAD_PROTECTION`); unrecognised values fail toward `live`.

This is the highest-stakes thing juice does and currently has almost no UI presence.

### Cost
`COST_PER_KWH = 0.31`. Cost is a pure function of rollup kWh. "Normal day" is defined as
the **3rd-highest-cost day** in the window — a typical busy day, not the peak. Cost is
operator-only; anonymous viewers get 401 on `/api/cost`.

### Play hours & utilization
`play_seconds / on_seconds` per machine per local hour. The denominator matters: a
machine that was off all week isn't "unpopular", it's "unavailable". The busy grid
(`play_utilization_grid`) buckets this by day-of-week × hour-of-day.

---

## 5. Action & audit layer

### Power event (audit log)
Every switch attempt is recorded, successful or not.

```
power_events(event_id, ts, plug_id, action, source, operation_id, actor, result, error)
```

- `action` — on / off / reboot
- `source` — where it came from: manual UI, bulk operation, `reboot`, overload shutdown
- `actor` — the authenticated user, or the system
- `result` — `ok` / `error` / `refused` (refused = blocked by a lock)

This is a genuinely valuable, currently under-exposed dataset: it answers "who turned
this off?" and "did the overload protection fire last night?".

### Operation (bulk power)
An in-flight all-on / all-off, global or scoped to one strip.

| Field | Notes |
|---|---|
| `id`, `kind` (`all_on`/`all_off`), `started_at`, `started_by` | |
| `targets`, `index`, `completed`, `failed`, `current_machine` | progress |
| `state` | `running` / `complete` / `cancelled` |
| `label` | scope, e.g. "Backline strip"; `NULL` = global |

Target selection (`_build_targets`) is non-obvious and is real domain logic:
- Skip plugs whose **relay** is already in the desired state.
- Skip locked machines in the conflicting direction.
- **On all-off, skip machines currently `PLAYING`** — never interrupt a game.
- No live reading yet ⇒ skip on all-off (can't be sure), include on all-on.
- Machines sorted **by year ascending**; unassigned outlets appended **last**.

Execution is partitioned into **instant** (load-free unassigned outlets, switched at
once) and **staggered** (anything carrying a load) to limit inrush current. Operations
are cancellable mid-flight, and progress is pushed over SSE.

### Actuation latency & failure characteristics
Every switch command is a **WAN round-trip to `wap.tplinkcloud.com`** — there is no
local-network path today. That makes actuation slow and occasionally flaky, and the UI
has to be designed around it rather than pretending it is instant.

- Individual power control retries up to **6 attempts**, backing off 0.5 / 1 / 2 / 4 / 4 s
  (~11.5 s of sleep on top of six cloud round-trips) before giving up.
- Bulk operations use a tighter budget — `BULK_OP_MAX_ATTEMPTS = 4`, ~3.5 s per failed
  plug — so one dead plug can't stall the whole sweep.
- **Reboot** is off → hold `REBOOT_HOLD_SECONDS` (3 s) → on, with the power-on running as
  a background task. Best case ~4 s; worst case ~30 s if both halves retry.
- Confirmation lags the command. The relay state juice reports comes from the recorder's
  poll, so after actuating we set `WATCH_WINDOW_SECONDS` (10 s) of forced polling to catch
  a load that appears a beat late. Until then, reported state can disagree with reality.

**UI consequence:** every power action needs an explicit *pending* state, an eventual
*confirmed* or *failed* state, and a visible retry count. An action that appears to do
nothing for 20 s is indistinguishable from a broken button.

### Concurrency — one operation at a time
`RecorderState.current_operation` is a **single global slot**. Global and strip-scoped
bulk operations share it, and starting a second one while another is running returns
**409**. With 2–3 operators working simultaneously this is a shared resource, not a
private one: the running operation's owner (`started_by`) and scope (`label`) are part
of what every operator needs to see.

### Live push
`GET /api/events` is an SSE stream carrying ~1 Hz `readings` ticks plus `power_change`
and operation-progress events. Any new interface should build on this rather than polling.

---

## 6. Identity & permissions

- **Auth**: OAuth2/OIDC (Authorization Code + PKCE) against FlipFix.
- **Capability**: `control_power` gates every write. Read is broader.
- **Session**: an encrypted cookie (`EncryptedCookieStorage`), keyed off a SHA-256 of the
  OAuth client secret — so the key is stable across restarts — with a **30-day
  `SESSION_MAX_AGE`**. The expiry is absolute rather than rolling: aiohttp_session only
  re-issues the cookie when session data changes, so it is 30 days from login, not from
  last use.
- **Public-readable**: some routes (`/`, `/usage`, `/air`) render for anonymous visitors,
  with operational detail redacted — plug IDs, strip names, outlet aliases, calibration
  thresholds, and all cost data are stripped for the public. Sort order is computed
  *before* redaction so public and operator views agree on ordering.

Three effective audiences, then: **anonymous public**, **authenticated viewer**,
**operator with `control_power`**.

---

## 7. Known modelling problems to fix in the redesign

1. **Overlapping "on-ness" concepts.** `is_on` (relay), `watts > OFF_WATTS` (drawing),
   `power_status`, `State.OFF`, and `offline` all encode versions of "is it on", and
   bugs recur where the wrong one is used. The new UI should present exactly one
   status vocabulary and derive everything else from it.
2. **`OFFLINE` injected into the `State` enum** at the server layer, conflating
   "we don't know" with "we classified it".
3. **Uncalibrated is not a failure state** but currently reads like one. It means
   "we can honestly report on/off but not play activity".
4. **`plug_id` is the URL identity** (`/machine/{plug_id}`), so a machine's page URL
   changes when it moves outlets. `asset_id` is the durable identity and is what
   operators actually know.
5. **Strip/circuit/machine hierarchy is flat in the UI.** The data supports
   circuit → strip → outlet → machine; navigation doesn't.
6. **`locked` vs `lock_mode`** — legacy boolean still in the schema alongside the
   tri-state that replaced it.
7. ~~**Sessions expire with the browser.**~~ **Fixed** in #77: the cookie storage had no
   `max_age`, so it issued a *browser-session* cookie that died whenever a phone or the
   front-desk tablet reaped the browser session. The failure was quiet — public-readable
   pages kept rendering, the controls just vanished and writes began returning 401 — which
   is why it presented as two separate complaints, "frequently logged out" and "actions
   don't always work". `SESSION_MAX_AGE` is now 30 days. Kept here because it explains
   what a large share of the reported unreliability actually was.
8. **Actuation is cloud-only and slow**, but the UI models it as instant. See §5.
9. **Bulk operations are a single global slot** with no notion of who else is looking at
   the same problem, despite 2–3 concurrent operators being normal.
