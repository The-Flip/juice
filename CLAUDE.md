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

## Code Quality

- **Ruff** for linting and formatting (configured in `pyproject.toml`)
- **mypy** for type checking
- **Pre-commit hooks** run ruff and file hygiene checks automatically
