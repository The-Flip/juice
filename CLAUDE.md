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

## Code Quality

- **Ruff** for linting and formatting (configured in `pyproject.toml`)
- **mypy** for type checking
- **Pre-commit hooks** run ruff and file hygiene checks automatically
