"""FlipFix API client — look up pinball machine identity by asset tag."""

from __future__ import annotations

import logging

import aiohttp

log = logging.getLogger(__name__)


async def get_machines(api_url: str, api_key: str) -> dict[str, str]:
    """Fetch all machines from FlipFix. Returns {asset_id: name}, empty dict on error."""
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(
                f"{api_url}machines/",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            resp.raise_for_status()
            data = await resp.json()
            return {m["asset_id"]: m["name"] for m in data["machines"]}
    except Exception:
        log.warning("Failed to fetch machines from FlipFix", exc_info=True)
        return {}
