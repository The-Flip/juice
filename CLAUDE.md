# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project

**juice** — Tracks pinball machine usage through power data from Kasa HS300 smart power strips. Python >= 3.14, managed with `uv`.

## Testing

The e2e harness (`tests/e2e/`) runs the real server against a seeded,
production-shaped fixture DuckDB (no tap, no devices) and drives it with
Playwright. `tests/e2e/seed.py` synthesizes data tuned to the live prod profile
(`make backup` + `tests/e2e/characterize.py`); `tests/e2e/serve.py` is the
entrypoint. The CI `e2e` job is **advisory** until proven stable.

## The `/api/v2` rebuild

The web UI is being rebuilt. Four documents in the repo root are the authority,
and they are meant to be read before touching the relevant code:

- **`domain_model.md`** — what juice knows about: strip → plug → machine,
  circuits, readings, rollups, and the derived layer (relay vs drawing,
  calibration, overload). §7 lists known modelling problems.
- **`user_needs.md`** — who uses juice and what for, jobs ranked by frequency.
  The headline: opening and closing the museum is ~90% of usage.
- **`status_vocabulary.md`** — the settled status vocabulary and the naming
  rules. Read before touching anything that reports whether a machine is on.
- **`api_v2.md`** — the `/api/v2` wire contract, written so a client can be
  built without reading the server.

`/api/v2` lives in `juice/api/v2/` and is mounted into the same aiohttp app as
v1. Access is declared per route with `@access(...)` and enforced in the auth
middleware, so a handler cannot forget a capability check. v1 (`/api/*`) is
frozen but still serving the old UI; do not build new features on it.

### The `/api/v2` TUI

`juice/tui/` is a read-only terminal client for `/api/v2` — a machine table plus
a live view of the SSE stream — built to find out whether `api_v2.md` is enough
to write a client from. It **imports no `juice.*` server module** on purpose;
reaching into `juice.server` for a payload shape would defeat the point, so keep
it that way. What the exercise found is written up in **`api_v2_findings.md`**,
which is the actual deliverable; the TUI is the instrument.

    uv run python -m tests.e2e.serve --port 8150 --interactive --with-problems
    uv run juice tui --url http://localhost:8150 --login

`textual` is a **dev** dependency and `juice tui` imports it lazily, so a
production image without the dev group is unaffected. Run it logged out to see
the anonymous redaction; `l` logs in, `r` toggles the stream pane between
humanized lines and raw JSON frames.

Against **production** the anonymous view works as-is
(`juice tui --url https://juice.theflip.museum`), but `--login` cannot: `/login`
redirects into FlipFix's OAuth flow, which no non-browser client can complete —
the client reports `oauth_required` and says so. For the operator view, copy the
`AIOHTTP_SESSION` cookie out of a logged-in browser and pass it:

    uv run juice tui --url https://juice.theflip.museum --cookie 'AIOHTTP_SESSION=<value>'

That cookie is a live 30-day operator session — treat it like a password, and
prefer a shell that doesn't record history.

## `tap` — the local LAN collector

`tap/` is a standalone daemon that polls smart plugs **over the LAN** with
`python-kasa`, buffers readings to day-partitioned SQLite, and streams them to a
server over a WebSocket. It **is the collector**: production cut over to it on
2026-09-16, and the TP-Link cloud recorder it replaced (which could not read
SMART/KLAP hardware at all and polled its devices sequentially with no timeout)
has since been deleted from juice.

It **imports no `juice.*` module**, like `juice/tui/` — and unlike the TUI, that
is enforced by `tests/tap/test_isolation.py` rather than left as a convention.
It knows about plugs and power, never about machines or asset tags. Its
dependency (`python-kasa`) is an **optional** extra, so juice's production image
is unaffected.

    uv sync --extra tap
    uv run tap run --buffer-dir ./data/buffer      # status page on :8010
    uv run tap probe 192.168.2.134                 # one sweep, with timings

With no `[uplink].url` configured it runs standalone — polls, buffers, and shows
what it has. Read **`tap/README.md`** for the design and the measurements behind
it; `tap.toml.example` documents every setting.

It is **deployed on `bumper`**, a box on the museum's fleet subnet, by
`make deploy-tap` (`scripts/deploy-tap.sh`, idempotent, over ssh) into the home
of a `tap` service user: `deploy/tap/bumper.toml` is its committed config,
`deploy/tap/compose.yml` the museum compose file, and `/home/tap/{buffer,logs}`
hold 30 days of readings and a year of day-partitioned logs. `make deploy-tap
ACTION=status|logs|pull` are the operator's tools (`pull` lands in `pulls/`, not
`data/`); the README's "The museum
box" section has the layout. Its uplink points at production.

The juice side of the uplink is **The tap receiver** below, and both
present-tense frames have projections in `juice/collector_tap.py`: the
**`devices` roster frame** onto plugs and assignments (`apply_devices`, with tap
re-sending it whenever an outlet is relabelled), and the **`live` frame** onto
the floor's current state (`LiveProjector` → `cache_reading` / `update_buffer`,
and `overload.check_overload`, plus a 1 Hz
`live_loop` that marks a device unreachable once it has been absent from live
rows for 15 s — tap omits devices it cannot reach rather than reporting them).
Power control runs the other way down the same socket: `TapControl`
(`juice/collector_tap.py`) is the registry of connected taps — `handle_ingest`
registers a session on `hello`, learns which tap reports which device from its
`devices` frames, and hands it every `command_result` — and `TapPlug` is the
`plug_objects` entry whose `turn_on()`/`turn_off()` send a `command` frame and
wait for the result. The power handlers see only the `Controllable` protocol
in `juice/control.py` (the command lifecycle, `call_with_retry`, confirmation
from the next reading), so they cannot care what is behind it. Three rules
there. A retry re-sends
the **same** `command_id` (tap answers from its cache or its in-flight task,
so one intent is never actuated twice; the opposite intent, or the expiry,
ends the reuse). One attempt waits exactly `ATTEMPT_BUDGET_S` for the result
— the figure `CommandRegistry` extends a deadline by per retry, so a client is
never told `timed_out` while the server is still trying; tap's own worst case
on a device is about the same 23.5 s, so a strip that answers on tap's last
try can land its "ok" after juice has recorded `failed` — the relay moves,
the next live reading shows it. And what is retried is a `TimeoutError`
(`control.is_retryable`), which `TapControl` raises for silence, a socket that
closed under the command (tap is a reconnect away and its cache survives), and
a device error tap names as transient
(`RETRYABLE_TAP_ERRORS` — tap's poller raises `ConnectionError` *before* its
own retries when it has dropped the strip); `expired` / `unknown device` are
refused at once, and no connected tap refuses at once with "the collector is
offline". Every answered command is timed send → result on juice's side
(`tap control: <id> ok from bumper in 84 ms`), and `TapControl.latency()` /
`snapshot()` keep p50/p95 over the last 256 for a status view to show.

**`juice serve` is the tap-driven server** (`juice/cli.py::_serve`). It
opens no cloud session and needs no Kasa account: `create_app` gets the three
seams — `roster_projection` for the `devices` frame, a `LiveProjector` for
`live`, a `TapControl` for commands — and `collector_tap.run_tap_collector`
wraps them with the startup (`hydrate_assignments`, `configure_overload_mode`,
the baselines, the FlipFix fetch, `reconcile_from_store`, `seed_buffers`, the
retro migration and one rollup pass), then the 1 Hz `live_loop` and a
minute-cadence `housekeeping_loop` (FlipFix roster with the empty-answer
guard, operator state re-read, assignment reconciliation from the **store's**
aliases — tap re-sends its roster only when an outlet changes, so a FlipFix
rename has to land this way). Rollups, retention and air run beside it. It
**refuses to start without `JUICE_INGEST_TOKEN`**, because a server with no
ingest route looks healthy and collects nothing. With no tap connected
`/api/v2/floor` reports one `collector_offline` infrastructure entry instead
of nine unreachable strips (`collector_silent` when a tap is connected but
sending no live frames — a backfill in progress), and operations and
individual power commands answer 409 `not_controllable` up front rather than
failing every machine in turn (`juice/api/v2/collector.py`).

There is **no rollback to the cloud**: the cloud collector, the `--collector`
switch, the shadow-mode rehearsal and the `ingest-skip` cursor tool were all
removed after the cutover. `JUICE_COLLECTOR`, `JUICE_TAP_SHADOW` and
`JUICE_INGEST_SKIP_TO` left in an environment are inert. `tests/e2e/serve.py
--tap` wires the same three seams (without FlipFix or housekeeping)
so a replayed production day drives the real dashboard and its power buttons
round-trip (`replay.py --mode live --controllable` answers the command frames
by flipping the outlet in the next live frame).

The overload window is gated on coverage (`juice/overload.py`: `MAX_GAP_S`
10 s, from a LAN measurement plus headroom for the WAN; `overload-report
--max-gap 30` for history from before the cutover, which arrived 6–9 s apart)
and its mean is time-weighted. `LiveProjector.gaps` (a `GapMeter`) measures the
real-path inter-arrival gaps — latency across consecutive frames, with device
absences counted separately — and prints them on the five-minute `tap live:`
summary, so the bound can be checked against production before overload
leaves `shadow`.

Two rules in the live projection are load-bearing. **Juice's clock, not tap's**:
a live row's timestamp is used only to detect skew (more than 120 s off and the
frame is dropped, with one ERROR line naming the offset), and everything
downstream — command reconciliation, status durations, the overload window —
is stamped with the admission time, because `CommandRegistry.reconcile` ignores
readings at or before `issued_at` and a tap 30 s slow would time out every
command. And **a frame is applied on its own task, never in the receive loop**:
an apply awaited from `handle_ingest` would hold every `readings` ack. Frames
arriving mid-apply wait in a slot of one (latest wins); an apply older than
15 s is cancelled by the sweep as a hang — which is also why an overload
shutdown is not part of the apply: `check_overload` *starts* it on a task of
its own (`RecorderState.overload_shutdowns`, one per plug) and returns, so six
`turn_off` retries hole no other machine's window and cannot be cancelled as a
hang; a shutdown that fails waits `OVERLOAD_RETRY_COOLDOWN_S` (10 min) before
the window may fire that plug again. The SSE `reading_tick` is published on every
frame; `LIVE_PUBLISH_INTERVAL_S` (0.25 s) is the least time between two ticks,
and a frame inside it *holds* a tick until it elapses rather than dropping it,
so a burst costs one snapshot and no state waits longer than the interval.
That matters because tap sends a live frame **the moment a command moves a
relay** (`Uplink._live_now`, with `DevicePoller.set_relay` writing the switch
into `Health` first, and keeping the switch over a sweep that captured the
outlet before it) rather than at its next 1 Hz tick: the operator's button
settles on the reading that shows the relay where they asked for it, and that
wait is now the command's own ~100 ms, one WAN hop, and at most the interval.
`_readings_snapshot` classifies only the tail of each machine's buffer that
decides its state (`state.classify_last`, the full classification's last
element up to floating-point rounding), ~2 ms where the full hour was ~210 ms
for 33 machines and made the tick every other frame.

### The tap receiver (`/api/v2/ingest`)

`juice/api/v2/ingest.py` is the server half of tap's uplink: a WebSocket that
accepts `hello`/`readings` and answers `welcome`/`ack`/`nack`. It is **gated on
`JUICE_INGEST_TOKEN`** and the route is not registered without one, so
production is unaffected until someone deliberately turns it on.

Three things about it are load-bearing and easy to undo by accident:

- **An ack is a durability claim.** tap advances its cursor on the ack and never
  replays what it believes juice holds, so the ack is sent *after* the commit.
  The rows and the cursor commit in **one transaction** (`ingest_cursors`), so
  they cannot disagree — which is what makes a duplicate impossible to *send*
  rather than something to filter on arrival. `readings` has no unique index and
  cannot affordably be given one at 20M+ rows.
- **Rows never become Python objects.** The raw frame goes to DuckDB, which
  parses, validates, converts milli-units and resolves `(device_id, child_id)`
  to a plug in one pass (`Store.commit_ingest_batch`). Measured at ~92k rows/s
  against ~425 rows/s for `executemany`. Writes run on a **single writer thread**
  with its own connection, so a full-day backfill (~4.2M rows, ~45 s) never
  blocks the event loop.
- **`readings` drives no live state.** No `RecorderState`, no `_publish`, no
  overload check from that channel — replaying days of history through the live
  layer would fire shutdowns for events that ended on Tuesday. The `devices`
  and `live` frames *are* projected, and `command_result` answered to, through
  the `app["tap_devices"]`, `app["tap_live"]` and `app["tap_control"]` seams,
  so the receiver stays a protocol shim; with nothing wired, all three are
  dropped.

Ingest itself never writes an alias — it creates plugs for outlets it has never
seen with an empty one, deliberately, because it has no roster to write. The
roster arrives in the `devices` frame and is projected by
`juice/collector_tap.py`, which is what assigns machines. One guard there is
load-bearing: an **empty FlipFix roster unassigns nothing**, because a frame can
arrive before juice has ever reached FlipFix, and the unassign branch would
otherwise clear every machine on the floor.

**Retention.** `juice/retention.py` prunes raw readings older than
`JUICE_RAW_RETENTION_DAYS` on its own periodic task, plus `uv run juice prune
[--days N] [--dry-run]` by hand. Nearly all of it is refusal: pruning stops at
the rollups' high-water mark, refuses while any rollup table is empty or the
retro play-hours migration has not run, stops at any ingest backfill the
rollups have not covered yet (a tap catching up writes rows *older* than the
high-water mark, so nothing else holds the cutoff back from them), and floors at
31 days. Raw readings are
the only copy, so the default answer is "don't".

### Replaying a production day

`tests/e2e/replay.py` drives the **real** tap `Buffer` and `Uplink` from a
production backup, so the receiver is exercised against the actual client rather
than a stub:

    uv run python -m tests.e2e.serve --port 8099 --db /tmp/copy.duckdb --ingest-token devtoken
    uv run python -m tests.e2e.replay --source data/backups/juice-<ts>.duckdb \
        --day 2026-09-02 --mode backfill --url http://127.0.0.1:8099/api/v2/ingest --token devtoken
    uv run python -m tests.e2e.replay --verify --db /tmp/copy.duckdb --day 2026-09-02

Two reshapings, both deliberate: relay state is **inferred** from the recorder's
write conventions (all-zero means off; `watts > 0` would be wrong, because 5% of
prod rows are a live outlet drawing nothing), and the cadence is raised from
prod's p50 6.7 s to 1 Hz by holding values, which is what makes it a ~4.2M-row
day. `--mode live` paces at 1×; `--mode backfill` is the "tap was offline for a
day" case. Always point `--db` at a **copy**.

To watch the replay drive the **dashboard**, serve the fixture with `--tap` and
replay in `--mode live --anchor start` (the readings land at "now"): every
machine tracks its replayed relay and draw at 1 Hz, and goes `unreachable`
within 15 s of the replay ending. Only paced replay feeds the `live` frame —
`--mode backfill` leaves tap's `Health` empty, which is the real client's own
suppression while it is catching up, so a backfill stores rows and moves
nothing on the floor. The replay carries the real `alias` and `has_emeter`
from the `plugs` table for the same reason a real tap reads them off the
device: the `devices` frame is projected back into `plugs`, and an invented
alias would reassign the floor of the copy.

## Architecture

- **`juice/readings.py`** — `PlugReading`, one outlet's reading as every collector produces it, and `outlet_number`.
- **`juice/control.py`** — The `Controllable` protocol a power handler needs from a plug object, and `call_with_retry`, the retry policy every handler actuates through.
- **`juice/air_collector.py`** — Async layer over the **Qingping** cloud API (nothing to do with the Kasa plugs tap reads on the LAN) for air-quality monitors. OAuth2 client-credentials against `oauth.cleargrass.com`; data from `apis.cleargrass.com`. Core types: `AirSensor`, `AirReading`; `air_record` is the poll loop `serve` runs beside the collector. Air data is room/zone-scoped (no FlipFix asset tag, no power control), so it stays parallel to the power pipeline rather than routed through it.
- **`juice/cli.py`** — Click CLI entry point (`juice`). `serve` is the server; the rest are store-only tools (`doctor`, `overload-report`, `prune`), `air-discover`, and `tui`.
- **`juice/server.py`** — aiohttp web server with API endpoints and HTML dashboard. Serves real-time and historical power data.
- **`juice/store.py`** — DuckDB storage layer. Manages readings, assignments, machines, and sparkline data.
  One rule for every connection that idles (the retention, rollup and ingest
  workers): **`Store.settle(conn)` before the sleep.** The Python client streams
  results, so a table SELECT consumed with `fetchone()` keeps its transaction
  open until the connection runs another statement, and DuckDB then cannot clean
  up any later write in the process — the retention worker's six-hour sleep on
  one guard `fetchone()` grew production from 1.3 GB to 7 GB and tap's ack
  latency from 23 ms to 850 ms in an afternoon. The ingest summary logs the
  retained bytes as `pinned`; flat near zero is healthy, climbing across
  summaries is the signal. `Store._conn` has no idle boundary — `rollup_loop`
  settles it once a minute so the server is bounded rather than clean;
  a handler on `_conn` that idles on a `fetchone()` for less than that is fine.
- **`juice/collector_tap.py`** — The tap collector's juice side: the `devices` roster projection (`apply_devices`, assignment), the `live` projection (`LiveProjector`, `cache_reading`/`update_buffer`, offline-by-absence), `TapControl`/`TapPlug` for power commands, and the startup + housekeeping loops `serve` runs. `RecorderState` hydration (`hydrate_assignments`) lives here too.
- **`juice/overload.py`** — The overload window and modes (pure, backtestable by `overload-report`), and the guard the live projection feeds every reading to: `check_overload` starts a shutdown on its own task, files the FlipFix report, and `configure_overload_mode` reads `JUICE_OVERLOAD_PROTECTION`.
- **`juice/rollups.py`** — The periodic rollup *driver* (the `refresh_hourly_*` implementations stay in `store.py`): which refreshes run and how far back, the one-off retro play-hours migration, the baseline recompute, and the single worker thread and task they all run on. Split out of the recorder because none of it is about collecting: the poll loop went away at tap cutover and the rollups did not. It is its **own task**, not a step in the collector's loop — awaiting a pass there stalls the collector for the pass's whole duration (~44s on a one-day ingest backfill) even with the work on a thread. Every writer of a rollup table goes through the one worker, including the calibration and circuit handlers, because two connections rewriting those rows lose the race destructively.
- **`juice/state.py`** — Classifies machine states (OFF, ATTRACT, PLAYING) from power readings using rolling statistics.
- **`juice/flipfix.py`** — FlipFix API client for looking up machine identity by asset tag.
- **`juice/auth.py`** — OAuth SSO via FlipFix OIDC provider. Session management, auth middleware, login/callback/logout handlers, capability checking.

## Environment Variables

Set via `.envrc` (direnv) or `.env`:

- `QINGPING_APP_KEY` / `QINGPING_APP_SECRET` — Qingping developer App Key/Secret
  (from developer.qingping.co) for the air-quality monitors. `serve` starts
  the air-polling loop **only when both are set** (otherwise air is simply skipped);
  `air-discover` needs them too.
- `FLIPFIX_API_URL` / `FLIPFIX_API_KEY` — FlipFix API for machine identity lookups.
  Overload auto-shutdown also files an `unplayable` problem report and marks the
  machine broken via this key, so it needs the **Can write** flag enabled in
  FlipFix admin (a read-only key just logs a 403; the shutdown still works).
- `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET` — FlipFix OAuth application credentials
- `OAUTH_PROVIDER_URL` — FlipFix base URL (e.g. `https://flipfix.theflip.museum`)
- `OAUTH_REDIRECT_URI` — OAuth callback URL (defaults to `http://host:port/callback`)
- `JUICE_DEV_AUTH` — **local dev only.** When OAuth is **not** configured, set to `1` (or
  pass `--dev-auth`) to enable the one-click dev login shim. Without it, a no-OAuth
  `serve` refuses to start. Has no effect when OAuth is configured. Never set in production.
- `JUICE_BACKUP_TOKEN` — **server-side** secret that enables `GET /api/backup`. Unset ⇒ the
  endpoint is not registered (404). Set it (a long random value) in production only.
- `JUICE_INGEST_TOKEN` — **server-side** secret that enables the tap receiver's WebSocket
  at `/api/v2/ingest`. **Required**: `serve` refuses to start without it, since every
  reading arrives over that route. Must match tap's `TAP_UPLINK_TOKEN`.
- `JUICE_RAW_RETENTION_DAYS` — days of raw `readings` to keep. Default **90**; `0` disables
  pruning. Values below 31 are refused (power baselines read 30 days of raw).
- `JUICE_PROD_URL` — **client-side**, for `make backup` / `make pull-prod` (e.g.
  `https://juice.theflip.museum`)
- `JUICE_PUBLIC_URL` — juice's own public base URL (e.g. `https://juice.theflip.museum`),
  used to deep-link from a FlipFix overload report back to the machine page. Unset ⇒
  the link is omitted from the report text.

## Authentication

Juice uses FlipFix as an OAuth2/OIDC provider (Authorization Code + PKCE). When OAuth
env vars are set, routes require login **except a deliberate public-readable
allow-list** — the dashboard, `/usage`, `/air` and the read-only APIs behind them —
which render for anonymous visitors with operational detail redacted (see
`user_needs.md` §1.D). v1 declares that list as `PUBLIC_READABLE_PATTERNS` in
`juice/auth.py`; v2 declares it per-route as `Access.ANON_READ`. Power control
requires the `control_power` capability.

For local development without FlipFix OAuth, pass `--dev-auth` (or set `JUICE_DEV_AUTH=1`)
to `juice serve`. That installs a **dev login shim** (`setup_dev_auth` in `juice/auth.py`)
so dev mirrors prod: the server starts logged-out (public view), `/login` is a
**one-click** login that mints a local operator session with `control_power` (no FlipFix
round-trip), and `/logout` clears it. It reuses the real gating middleware, so writes still
401 until you log in. **The shim is opt-in and only honoured when OAuth is absent** — a
no-OAuth `serve` without `--dev-auth` **refuses to start** (fail closed), so a deployment
with missing OAuth env can never silently grant one-click `control_power`. When neither
OAuth nor the shim is wired up — `create_app` called directly, e.g. handler-level unit
tests — everyone is treated as the operator.

Setting up the OAuth application and the `control_power` capability in FlipFix admin is a
one-time procedure — see the `juice-ops` skill.

## Operations

Machine → outlet assignment is driven entirely by the **Kasa outlet alias**: the
roster projection extracts an asset tag (`M\d+`) from each outlet's alias and matches
it to a FlipFix machine (`apply_devices` in `juice/collector_tap.py`, from tap's
`devices` frame). There is no manual assignment — relabel the outlet to (re)assign.
The runbook for recovering after a machine moves to a different outlet is in the
`juice-ops` skill.

### Offline plugs

A device tap cannot reach is omitted from its live frames; once one has been absent
for `LIVE_STALE_S` (15 s) juice marks it offline (`LiveProjector.sweep`), logs one
line per offline/recovery transition, and its machines render as **OFFLINE** tiles on
the dashboard instead of vanishing. `uv run juice doctor [--db … --days N]` reads the
store alone and reports outlets quiet for N days (default 7) with the machines still
assigned to them, outlets drawing power under a label with no asset tag, and machines
the store has on two outlets at once; `tap probe <ip>` (in `tap/`) is the device-level
diagnostic.

### Air-quality monitors (Qingping)

Qingping air monitors (temperature / humidity / CO₂ / PM2.5 / PM10 / TVOC / noise /
battery) are polled from the Qingping **cloud** — a separate account from Kasa, set via
`QINGPING_APP_KEY` / `QINGPING_APP_SECRET`. They're **room/zone-scoped**, not tied to a
machine or FlipFix asset tag, so they live in their own tables (`air_sensors`,
`air_readings`) and endpoints rather than the power pipeline. The display name is whatever
the device is called in the **Qingping+ app** — relabel there to rename a sensor.

- The air loop runs inside `serve` (a separate `asyncio` task alongside the collector)
  **only when both env vars are set**; otherwise it's skipped silently. It polls
  every `AIR_POLL_SECONDS` (5 min); devices report ~every 15 min, and repeated snapshots of
  the same device-side timestamp are deduped on `(ts, mac)`, so there are no duplicate rows.
- View live values + 7-day history at **`/air`** (public-readable, like `/usage`). There are
  no hourly rollups — at ~15-min cadence the raw table is small enough to chart directly.
- `uv run juice air-discover` lists each monitor + its latest reading for a quick check.
- Air data is in the same DuckDB, so the `/api/backup` snapshot already includes it.

Obtaining the App Key / Secret from the Qingping developer portal is a one-time procedure —
see the `juice-ops` skill.

### Backup & copying production data to dev

The running server exposes `GET /api/backup`, which produces a **consistent
point-in-time snapshot** of the live DuckDB (via `Store.snapshot_to`, a
transactional `COPY FROM DATABASE`) and streams it. No downtime —
the copy runs inline on the shared connection in ~0.1s and the server keeps
recording; the downloaded file is a clean standalone `.duckdb` with no WAL.

Auth is a **bearer token**, separate from OAuth so scripts/cron can pull:
send `Authorization: Bearer $JUICE_BACKUP_TOKEN`. The endpoint is registered
**only when `JUICE_BACKUP_TOKEN` is set** (404 otherwise), so dev/local never
exposes it.

`make backup` and `make pull-prod` drive the snapshot from a dev machine — see the
`juice-ops` skill.

> **Deploy note:** the backup endpoint is disabled until `JUICE_BACKUP_TOKEN`
> is set. To enable it, set a long random secret in the production
> environment (Railway) and redeploy. The token authorizes a **full data
> export** — treat it like a credential.
