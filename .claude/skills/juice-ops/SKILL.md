---
name: juice-ops
description: Operational runbooks for juice — FlipFix OAuth admin setup, obtaining Qingping cloud API credentials, recovering after moving a machine to a different outlet, pulling production data to dev, and what to watch on the tap collector in production.
---

# juice operational runbooks

One-time setup procedures and occasional recovery runbooks. Everything here is
external-system work (FlipFix admin, the Qingping developer portal, the Kasa app)
or a scripted operation — none of it is derivable from this codebase.

## FlipFix Admin Setup

Juice uses FlipFix as its OAuth2/OIDC provider. To wire up a new deployment:

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

The resulting credentials go in `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET`, with
`OAUTH_PROVIDER_URL` pointing at the FlipFix base URL.

## Recovering after moving a machine to a different outlet

Machine → outlet assignment is driven entirely by the Kasa outlet alias, so a move
is fixed by relabelling, not by editing juice.

1. In the Kasa app, rename the **new** outlet to include the machine's asset tag, e.g.
   `Star Trip - M0009`.
2. tap notices the relabel on its next device sweep and re-sends its `devices` roster;
   juice's roster projection assigns the machine to the new outlet. The machine's stale
   copy on the old (now-offline) outlet is hidden automatically — `handle_machines` drops
   an offline duplicate when the same machine also appears on an online outlet.
3. Verify on the dashboard, or on tap's status page (`http://bumper:8010`, or its
   `/api/status` JSON), which lists every outlet with the alias tap read from the device.
   (A store-only `juice doctor` is planned; the old one needed a cloud session.)

## Getting the Qingping App Key / Secret

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

## Pulling production data to dev

Both scripts read `JUICE_PROD_URL` (e.g. `https://juice.theflip.museum`) and
`JUICE_BACKUP_TOKEN` from `.env`. See `CLAUDE.md` for what `GET /api/backup` does and
why the token is a credential.

- `make backup` → `scripts/backup-prod.sh`: pulls a timestamped snapshot to
  `data/backups/` and verifies it opens.
- `make pull-prod` → `scripts/sync-prod-to-dev.sh`: pulls and replaces the
  local dev `juice.duckdb` (keeping `juice.duckdb.bak`). Refuses to overwrite
  a DB held open by a local `juice serve` unless `--force`.

## The tap collector in production

juice has one collector: the tap daemon on bumper, streaming readings, roster,
live state and power control over `/api/v2/ingest`. Production cut over on
2026-09-16; the TP-Link cloud path and its rollback were removed afterwards, so
there is nothing to flip. `serve` refuses to start without `JUICE_INGEST_TOKEN`.
Read the tap sections of `CLAUDE.md` for the design; this is what to watch.

**Health, in this order:**

- bumper is up and connected: `make deploy-tap ACTION=status` shows every
  strip `online` and `uplink: … connected=True`, with `lag` a few rows, not
  thousands.
- Railway log at boot: `tap collector: N plugs, M assigned, K FlipFix machines;
  waiting for a tap`, then `ingest: tap bumper … connected` and `tap control:
  bumper can now take commands`. bumper reconnects within seconds of a
  redeploy; until it does, and for the first 15 s after boot in any case,
  `/api/v2/floor` carries one `collector_offline` entry and the machines read
  `unreachable` — the honest state, not a fault. If tap connects but has a
  backlog to send first, the entry reads `collector_silent` instead (tap sends
  no live frames while more than 5 minutes behind); commands work in that
  state, the tiles do not move.
- Every 5 min two summary lines sit together: `ingest: tap bumper | … rows/s |
  commit avg …` (tens of ms; `pinned` flat near zero) and `tap live: N frames,
  M applied, … dropped for skew, … superseded, … rows for unknown outlets, …
  devices offline; gaps p50 … p99 … max …; N ever over the 10s overload bound
  (cumulative); M outlet absences`. A non-zero skew count or a growing
  unknown-outlet count is the first thing to chase.
- A power command logs `tap control: <id> ok from bumper in N ms`; a few
  hundred ms is normal.

**Overload protection.** `JUICE_OVERLOAD_PROTECTION` was set to `shadow` for
the cutover and back to `live` on 2026-09-17 12:37Z, after a full open→close
day on tap with every command answered. Detection runs from tap's 1 Hz live
frames; its window refuses to fire across a hole wider than 10 s
(`overload.TAP_MAX_GAP_S`), a bound picked from a LAN measurement plus headroom
for the WAN — one the real path cannot meet would refuse every window and
silently disarm protection. The coverage evidence is on the `tap live:` line:
the `gaps …` tail (p99 well under 10 s, over-bound count not climbing; absences
are parked devices and do not count) and the frame count per 5-minute window
(299 is 1 Hz; 293–295 is a window holding one of the ~50/day Railway-edge
reconnects, each ~1–2 s of missed frames; the cutover ran 32 h at 298–299 with
0 skew before the flip).

**Env vars that no longer do anything**: `JUICE_COLLECTOR`, `JUICE_TAP_SHADOW`,
`JUICE_INGEST_SKIP_TO`, `KASA_USERNAME`, `KASA_PASSWORD` (tap has its own copy
of the Kasa credentials in `deploy/tap/`). Remove them from Railway when
convenient; juice ignores them (click reads the environment only through a
declared option, and these have none).

**What a clock problem looks like**: commands refused as `expired` ("check the
clock on the tap box") long before the live projection's 120 s skew guard
trips. Fix the time on bumper; nothing on juice's side is wrong.
