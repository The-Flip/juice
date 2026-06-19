"""FlipFix API client — look up pinball machine identity by asset tag."""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from typing import TypedDict

import aiohttp

log = logging.getLogger(__name__)

REPORTER_NAME = "Juice (automated overload detection)"

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


@dataclass
class ReportResult:
    """Outcome of filing an unplayable report.

    status: 201 (created), 200 (existing open report returned), another int for an
    HTTP error, or None for a network/timeout/parse failure. report_id is the
    FlipFix problem-report id when the call succeeded (so a recurrence can be
    logged onto it).
    """

    status: int | None
    report_id: int | None = None

    @property
    def ok(self) -> bool:
        return self.status in (200, 201)

    @property
    def created(self) -> bool:
        return self.status == 201


async def report_unplayable(
    api_url: str,
    api_key: str,
    asset_id: str,
    description: str,
    *,
    occurred_at: str | None = None,
    mark_broken: bool = True,
) -> ReportResult:
    """File an 'unplayable' problem report against a machine and mark it broken.

    Best-effort — never raises. 201 = a new report was created; 200 = an open
    unplayable report already existed and was returned (the caller should log the
    recurrence onto it). Requires a write-capable API key.
    """
    body: dict[str, object] = {
        "priority": "unplayable",
        "description": description,
        "reported_by_name": REPORTER_NAME,
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
                report_id = None
                with contextlib.suppress(Exception):
                    report_id = (await resp.json()).get("problem_report", {}).get("id")
                return ReportResult(resp.status, report_id)
            text = await resp.text()
            log.warning(
                "FlipFix problem-report for %s returned %d: %s", asset_id, resp.status, text[:200]
            )
            return ReportResult(resp.status)
    except Exception:
        log.warning("Failed to file FlipFix problem report for %s", asset_id, exc_info=True)
        return ReportResult(None)


async def add_log_entry(
    api_url: str,
    api_key: str,
    report_id: int,
    text: str,
    *,
    occurred_at: str | None = None,
) -> bool:
    """Append a log entry to an existing problem report.

    Used to record a recurrence onto an already-open unplayable report. Best-effort:
    True on a 2xx, False otherwise (logs a warning). Requires a write-capable key.
    """
    body: dict[str, object] = {"text": text, "reported_by_name": REPORTER_NAME}
    if occurred_at is not None:
        body["occurred_at"] = occurred_at
    try:
        async with aiohttp.ClientSession(timeout=_REPORT_TIMEOUT) as session:
            resp = await session.post(
                f"{api_url}problem-reports/{report_id}/log-entries/",
                headers={"Authorization": f"Bearer {api_key}"},
                json=body,
            )
            if 200 <= resp.status < 300:
                return True
            log.warning(
                "FlipFix log-entry on report %d returned %d: %s",
                report_id,
                resp.status,
                (await resp.text())[:200],
            )
            return False
    except Exception:
        log.warning("Failed to add FlipFix log entry to report %d", report_id, exc_info=True)
        return False
