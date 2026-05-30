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
- `FLIPFIX_API_URL` / `FLIPFIX_API_KEY` — FlipFix API for machine identity lookups
- `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET` — FlipFix OAuth application credentials
- `OAUTH_PROVIDER_URL` — FlipFix base URL (e.g. `https://flipfix.theflip.museum`)
- `OAUTH_REDIRECT_URI` — OAuth callback URL (defaults to `http://host:port/callback`)

## Authentication

Juice uses FlipFix as an OAuth2/OIDC provider (Authorization Code + PKCE). When OAuth env vars are set, all routes require login. Power control requires the `control_power` capability.

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

## Code Quality

- **Ruff** for linting and formatting (configured in `pyproject.toml`)
- **mypy** for type checking
- **Pre-commit hooks** run ruff and file hygiene checks automatically
