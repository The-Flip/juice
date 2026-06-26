# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project

**juice** — Tracks pinball machine usage through power data from Kasa HS300 smart power strips. Python >= 3.14, managed with `uv`.

## Commands

```bash
uv sync                  # Install/sync dependencies
uv add <package>         # Add a dependency
uv run juice --help      # Run the CLI
uv run juice discover    # Find strips on the network
uv run juice status <ip> # Show current power readings
uv run juice monitor <ip> [-i seconds]  # Continuously poll power
uv run juice serve       # Start the web dashboard
uv run juice record      # Start the recording daemon
uv run juice doctor      # Diagnose device/assignment health (offline, untagged, stale)
uv run juice air-discover # List Qingping air monitors + their latest readings
```

### Quality & Testing

```bash
make test       # Run test suite (pytest)
make quality    # Format, lint, and typecheck
make lint       # Ruff linter with auto-fix
make format     # Ruff formatter
make typecheck  # mypy type checking
make precommit  # Run all pre-commit hooks
```

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

## Testing

Tests live in `tests/` and use pytest with pytest-asyncio. HTTP calls are mocked with `aioresponses`.

```bash
uv run pytest              # Run all tests
uv run pytest tests/test_state.py  # Run a specific test file
```

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

Juice uses FlipFix as an OAuth2/OIDC provider (Authorization Code + PKCE). When OAuth env vars are set, all routes require login. Power control requires the `control_power` capability.

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

### FlipFix Admin Setup

1. **Create OAuth Application** at `/admin/oauth2_provider/application/`:
   - Name: Juice Dashboard
   - Client type: Confidential
   - Grant type: Authorization code
   - Redirect URIs: `http://localhost:8000/callback` (dev) / production URL
   - Skip authorization: Yes
   - Algorithm: RS256

2. **Create Capability** at `/admin/oauth/appcapability/`:
   - Application: Juice Dashboard
   - Slug: `control_power`
   - Name: Control Power
   - Description: Turn pinball machines on and off

3. **Grant Capability** at `/admin/oauth/appcapabilitygrant/`:
   - User: (each user who should control power)
   - Capability: Control Power

## Operations

Machine → outlet assignment is driven entirely by the **Kasa outlet alias**: the recorder
extracts an asset tag (`M\d+`) from each outlet's alias and matches it to a FlipFix machine
(`refresh_metadata` in `juice/recorder.py`). There is no manual assignment — relabel the
outlet to (re)assign.

### Recovering after moving a machine to a different outlet

1. In the Kasa app, rename the **new** outlet to include the machine's asset tag, e.g.
   `Star Trip - M0009`.
2. The recorder picks it up within ~60s (`IDLE_RECHECK_SECONDS`) and assigns the machine to
   the new outlet. The machine's stale copy on the old (now-offline) outlet is hidden
   automatically — `handle_machines` drops an offline duplicate when the same machine also
   appears on an online outlet.
3. Verify with `uv run juice doctor`.

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

#### Getting the Qingping App Key / Secret

`QINGPING_APP_KEY` / `QINGPING_APP_SECRET` are the **OAuth App Key/Secret** for Qingping's
cloud-to-cloud API. One pair covers the whole account (all bound monitors), not one per
device. To obtain them:

1. **Qingping+ account with monitors bound.** Install the **Qingping+** app, create an
   account, and add each monitor to it so it reports to the Qingping cloud. A device in
   **HomeKit mode** is *not* reachable via the cloud API — keep it in Qingping+ mode.
2. **Register as a developer** at https://developer.qingping.co/ using that same account.
3. **Apply for cloud-API access.** On the console find *Access management* / *permission
   apply* (https://developer.qingping.co/personal/permissionApply) and request the OAuth /
   cloud-to-cloud ("device access") permission. This can need approval — if the option
   isn't visible, email **support@qingping.co** with your account + device MACs.
4. **Copy the credentials** from the *App information / Access management* page: App Key →
   `QINGPING_APP_KEY`, App Secret → `QINGPING_APP_SECRET`. Put them in `.env`/`.envrc`.
5. **Verify:** `uv run juice air-discover` — it mints a token against `oauth.cleargrass.com`
   and lists each monitor. An auth error here almost always means the cloud-API permission
   (step 3) hasn't been granted yet, not a code problem.

> Portal docs are mostly behind login and the exact menu labels shift between revisions, so
> step 3 is the part most likely to look slightly different than written.

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

- `make backup` → `scripts/backup-prod.sh`: pulls a timestamped snapshot to
  `data/backups/` and verifies it opens.
- `make pull-prod` → `scripts/sync-prod-to-dev.sh`: pulls and replaces the
  local dev `juice.duckdb` (keeping `juice.duckdb.bak`). Refuses to overwrite
  a DB held open by a local `juice serve`/`record` unless `--force`.

Both read `JUICE_PROD_URL` (e.g. `https://juice.theflip.museum`) and
`JUICE_BACKUP_TOKEN` from `.env`.

> **Deploy note:** the backup endpoint is disabled until `JUICE_BACKUP_TOKEN`
> is set. To enable it, set a long random secret in the production
> environment (Railway) and redeploy. The token authorizes a **full data
> export** — treat it like a credential.

## Code Quality

- **Ruff** for linting and formatting (configured in `pyproject.toml`)
- **mypy** for type checking
- **Pre-commit hooks** run ruff and file hygiene checks automatically
