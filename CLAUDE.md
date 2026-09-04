# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project

**juice** — Tracks pinball machine usage through power data from Kasa HS300 smart power strips. Python >= 3.14, managed with `uv`.

## Testing

The e2e harness (`tests/e2e/`) runs the real server cloud-free against a seeded,
production-shaped fixture DuckDB (no Kasa cloud, no recorder) and drives it with
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
server over a WebSocket. It is the intended eventual replacement for the cloud
recorder (`juice/recorder.py` + `juice/collector.py`), which cannot read
SMART/KLAP hardware at all and polls its devices sequentially with no timeout.

It **imports no `juice.*` module**, like `juice/tui/` — and unlike the TUI, that
is enforced by `tests/tap/test_isolation.py` rather than left as a convention.
It knows about plugs and power, never about machines or asset tags. Its
dependency (`python-kasa`) is an **optional** extra, so juice's production image
is unaffected.

    uv sync --extra tap
    uv run tap run --buffer-dir ./data/buffer      # status page on :8010
    uv run tap probe 192.168.4.38                  # one sweep, with timings

With no `[uplink].url` configured it runs standalone — polls, buffers, and shows
what it has. Read **`tap/README.md`** for the design and the measurements behind
it; `tap.toml.example` documents every setting.

Not yet built: the `/api/v2/ingest` endpoint on the juice side, and juice-side
retention (full 1 Hz upstream is ~4.15M rows/day against today's ~85k, into a
store that has never pruned anything). Both are prerequisites for cutover, not
for running `tap`.

## Architecture

- **`juice/collector.py`** — Async layer over the TP-Link cloud API. Handles authentication, device discovery, and reading per-plug power data. Core types: `PlugReading`, `StripReading`.
- **`juice/air_collector.py`** — Async layer over the **Qingping** cloud API (separate from the Kasa cloud) for air-quality monitors. OAuth2 client-credentials against `oauth.cleargrass.com`; data from `apis.cleargrass.com`. Core types: `AirSensor`, `AirReading`. Air data is room/zone-scoped (no FlipFix asset tag, no power control), so it stays parallel to the power pipeline rather than routed through it.
- **`juice/cli.py`** — Click CLI entry point (`juice`). Wraps collector, server, and recorder with `asyncio.run()`.
- **`juice/server.py`** — aiohttp web server with API endpoints and HTML dashboard. Serves real-time and historical power data.
- **`juice/store.py`** — DuckDB storage layer. Manages readings, assignments, machines, and sparkline data.
- **`juice/recorder.py`** — Recording daemon that continuously polls strips and persists readings to the store.
- **`juice/state.py`** — Classifies machine states (OFF, ATTRACT, PLAYING) from power readings using rolling statistics.
- **`juice/flipfix.py`** — FlipFix API client for looking up machine identity by asset tag.
- **`juice/auth.py`** — OAuth SSO via FlipFix OIDC provider. Session management, auth middleware, login/callback/logout handlers, capability checking.

## Environment Variables

Set via `.envrc` (direnv) or `.env`:

- `KASA_USERNAME` / `KASA_PASSWORD` — TP-Link cloud credentials
- `QINGPING_APP_KEY` / `QINGPING_APP_SECRET` — Qingping developer App Key/Secret
  (from developer.qingping.co) for the air-quality monitors. `serve`/`record` start
  the air-polling loop **only when both are set** (otherwise air is simply skipped);
  `air-discover` needs them too. Independent of the Kasa account.
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

Machine → outlet assignment is driven entirely by the **Kasa outlet alias**: the recorder
extracts an asset tag (`M\d+`) from each outlet's alias and matches it to a FlipFix machine
(`refresh_metadata` in `juice/recorder.py`). There is no manual assignment — relabel the
outlet to (re)assign. The runbook for recovering after a machine moves to a different
outlet is in the `juice-ops` skill.

### Offline plugs

A device that fails to respond for `OFFLINE_FAILURE_THRESHOLD` consecutive reads is marked
offline: it's dropped from the 1s poll loop (re-probed only by the 60s refresh, which logs one
line per offline/recovery transition rather than a traceback per cycle), and its machines
render as **OFFLINE** tiles on the dashboard instead of vanishing. `uv run juice doctor`
lists offline devices, online outlets missing an asset tag (relabel candidates), and
assignments whose outlet is no longer discovered (stale — reassign or clear).

### Unsupported (SMART/KLAP) devices

Juice talks to `wap.tplinkcloud.com` via the legacy passthrough API. Newer Kasa models that
use the SMART/KLAP protocol (e.g. **EP25**, KP125M) appear in the cloud device list but every
read returns *Device is offline*, because they don't speak the legacy protocol. `uv run juice
discover` flags them as `[UNSUPPORTED MODEL]` (with their decoded alias) so they're easy to
spot, and the recorder logs one warning per unsupported device per session rather than every
60 seconds. To track power on a machine that's on such a plug, move it to an **HS300 strip
outlet** (per-outlet energy monitoring, works over the cloud path) and relabel the outlet
with the asset tag. Local-network reading of SMART devices via python-kasa would be a future
change; it's not implemented today.

### Air-quality monitors (Qingping)

Qingping air monitors (temperature / humidity / CO₂ / PM2.5 / PM10 / TVOC / noise /
battery) are polled from the Qingping **cloud** — a separate account from Kasa, set via
`QINGPING_APP_KEY` / `QINGPING_APP_SECRET`. They're **room/zone-scoped**, not tied to a
machine or FlipFix asset tag, so they live in their own tables (`air_sensors`,
`air_readings`) and endpoints rather than the power pipeline. The display name is whatever
the device is called in the **Qingping+ app** — relabel there to rename a sensor.

- The air loop runs inside `serve`/`record` (a separate `asyncio` task alongside the power
  recorder) **only when both env vars are set**; otherwise it's skipped silently. It polls
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
transactional `COPY FROM DATABASE`) and streams it. No recorder downtime —
the copy runs inline on the shared connection in ~0.1s and the daemon keeps
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
