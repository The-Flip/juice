"""FlipFix API client — look up pinball machine identity by asset tag."""

from __future__ import annotations

import logging
from typing import TypedDict

import aiohttp

log = logging.getLogger(__name__)

# Short total timeout for the best-effort report — it runs in the recorder poll
# loop, so a stalled FlipFix must not block polling for aiohttp's 300s default.
_REPORT_TIMEOUT = aiohttp.ClientTimeout(total=5)


class MachineInfo(TypedDict):
    name: str
    year: int | None


async def get_machines(api_url: str, api_key: str) -> dict[str, MachineInfo]:
    """Fetch all machines from FlipFix.

    Returns {asset_id: {"name": str, "year": int | None}}, empty dict on error.
    """
    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(
                f"{api_url}machines/",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            resp.raise_for_status()
            data = await resp.json()
            return {
                m["asset_id"]: {
                    "name": m["name"],
                    "year": m.get("model", {}).get("year") if m.get("model") else None,
                }
                for m in data["machines"]
            }
    except Exception:
        log.warning("Failed to fetch machines from FlipFix", exc_info=True)
        return {}


async def report_unplayable(
    api_url: str,
    api_key: str,
    asset_id: str,
    description: str,
    *,
    occurred_at: str | None = None,
    mark_broken: bool = True,
) -> bool:
    """File an 'unplayable' problem report against a machine and mark it broken.

    Best-effort: returns True on success (201 created or 200 idempotent), False
    on any failure (network, 403 from a read-only key, etc.) — never raises, so a
    reporting hiccup can't disrupt the caller. Requires a write-capable API key.
    """
    body: dict[str, object] = {
        "priority": "unplayable",
        "description": description,
        "reported_by_name": "Juice (automated overload detection)",
        "mark_broken": mark_broken,
    }
    if occurred_at is not None:
        body["occurred_at"] = occurred_at
    try:
        async with aiohttp.ClientSession(timeout=_REPORT_TIMEOUT) as session:
            resp = await session.post(
                f"{api_url}machines/{asset_id}/problem-reports/",
                headers={"Authorization": f"Bearer {api_key}"},
                json=body,
            )
            if resp.status in (200, 201):
                return True
            text = await resp.text()
            log.warning(
                "FlipFix problem-report for %s returned %d: %s", asset_id, resp.status, text[:200]
            )
            return False
    except Exception:
        log.warning("Failed to file FlipFix problem report for %s", asset_id, exc_info=True)
        return False
