"""`python -m juice.tui` — the TUI without going through the click group.

Useful when the `juice` CLI's Kasa options are inconvenient (a machine with no
TP-Link credentials can still point this at a server).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

DEFAULT_URL = "http://127.0.0.1:8000"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="A TUI client for juice's /api/v2")
    parser.add_argument(
        "--url",
        default=os.environ.get("JUICE_TUI_URL", DEFAULT_URL),
        help=f"Base URL of the juice server (default {DEFAULT_URL}, or $JUICE_TUI_URL).",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Log in on startup via the dev-auth shim, so operator-only fields are visible.",
    )
    parser.add_argument(
        "--cookie",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help=COOKIE_HELP,
    )
    args = parser.parse_args(argv)
    return run(args.url, login=args.login, cookies=args.cookie)


COOKIE_HELP = (
    "Session cookie to send, as NAME=VALUE (repeatable, or one whole Cookie "
    "header). Use this against a server with real OAuth, where --login cannot "
    "work: copy the AIOHTTP_SESSION cookie out of a logged-in browser. A bare "
    "value is taken as AIOHTTP_SESSION."
)


def run(url: str, *, login: bool = False, cookies: list[str] | None = None) -> int:
    """Start the TUI. Kept importable so `juice tui` shares one code path."""
    try:
        from juice.tui.app import JuiceTui
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on the env
        sys.stderr.write(f"the TUI needs textual ({exc.name}): uv sync --group dev\n")
        return 1

    from juice.tui.client import ApiError, JuiceClient, parse_cookies

    try:
        jar = parse_cookies(cookies) if cookies else None
    except ValueError as exc:
        sys.stderr.write(f"--cookie: {exc}\n")
        return 2

    client = JuiceClient(url, cookies=jar)

    async def go() -> None:
        try:
            if login:
                try:
                    await client.login()
                except ApiError as exc:
                    # Not fatal: the anonymous view is still worth showing, and
                    # the banner will say which audience we ended up in.
                    sys.stderr.write(f"login failed ({exc.code}): {exc.message}\n")
            await JuiceTui(client).run_async()
        finally:
            await client.close()

    asyncio.run(go())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
