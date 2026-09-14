---
name: juice-ops
description: Operational runbooks for juice — FlipFix OAuth admin setup, obtaining Qingping cloud API credentials, recovering after moving a machine to a different outlet, pulling production data to dev, and cutting the collector over from the TP-Link cloud to tap (and back).
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
2. The recorder picks it up within ~60s (`IDLE_RECHECK_SECONDS`) and assigns the machine to
   the new outlet. The machine's stale copy on the old (now-offline) outlet is hidden
   automatically — `handle_machines` drops an offline duplicate when the same machine also
   appears on an online outlet.
3. Verify with `uv run juice doctor`.

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
  a DB held open by a local `juice serve`/`record` unless `--force`.

## Cutting the collector over to tap (and back)

juice has two collectors. `cloud` (the default) polls the TP-Link cloud from
the juice process; `tap` takes everything — readings, roster, live state,
power control — from the tap daemon on bumper over `/api/v2/ingest`. The
switch is one Railway variable, `JUICE_COLLECTOR`, and the design goal is that
each direction is a redeploy with nothing to clean up afterwards. Read the tap
sections of `CLAUDE.md` first; this is the order of operations, not the design.

**Before flipping** — all four must hold:

1. bumper is up and connected: `make deploy-tap ACTION=status` shows every
   strip `online` and `uplink: … connected=True`, with `lag` a few rows, not
   thousands.
2. The shadow rehearsal has been clean: Railway logs (`get-logs`, filter
   `"tap shadow"`) show `roster agrees` **and** `live agrees` on every 60 s
   tick for a couple of days, and no `DISAGREES` line in that span. The
   `clean since` clock restarts on every deploy, so read the span across
   deploys rather than one clock. A `NO FlipFix roster` or `not running` line
   is a gap in the gate, not a pass.
3. `JUICE_OVERLOAD_PROTECTION` is `shadow` on Railway. Overload detection runs
   from tap's 1 Hz live frames after cutover; its window refuses to fire across
   a hole wider than 10 s (`overload.TAP_MAX_GAP_S`), a bound picked from a LAN
   measurement plus headroom for the WAN. Shadow mode measures the same gaps on
   the real path and puts them on the `live agrees` line — `gaps p50 … p99 …
   max …; N ever over the 10s overload bound (cumulative); M outlet absences`.
   Absences are parked devices and do not count. Leave overload in `shadow`
   through the cutover, and only set it back to `live` once a day of those
   lines shows p99 well under 10 s and the over-bound count not climbing; a
   bound the real path cannot meet would refuse every window and silently
   disarm protection.
4. A fresh `make backup` exists. Rollback does not need it; a bad week would.

**Flip**: on Railway set `JUICE_COLLECTOR=tap` and set `JUICE_TAP_SHADOW` to `0`
(or remove it — `serve` refuses the two together); keep `JUICE_INGEST_TOKEN`.
The redeploy is the cutover. `KASA_USERNAME`/`KASA_PASSWORD` may stay set; tap
mode never reads them.

**Watch, in this order:**

- Railway log: `tap COLLECTOR mode: no cloud polling…`, then
  `tap collector: N plugs, M assigned, K FlipFix machines; waiting for a tap`,
  then `ingest: tap bumper … connected` and `tap control: bumper can now take
  commands`. bumper reconnects within seconds of the redeploy; until it does,
  and for the first 15 s after boot in any case, `/api/v2/floor` carries one
  `collector_offline` entry and the machines read `unreachable` — the honest
  state, not a fault. If tap connects but has a backlog to send first, the
  entry reads `collector_silent` instead (tap sends no live frames while more
  than 5 minutes behind); commands work in that state, the tiles do not move.
- The dashboard tracks the floor at 1 Hz (`reading_tick` every other frame).
- One open and one close, J1/J2 in `user_needs.md`: all-on with progress and
  explained skips, an individual power and a reboot each reaching
  `confirmed` (`tap control: <id> ok from bumper in N ms` in the log), all-off.
- The next day, `/usage` play hours have accrued, EP10 included.
- Every 5 min two summary lines sit together: `ingest: tap bumper | … rows/s |
  commit avg …` (tens of ms) and `tap live: N frames, M applied, … dropped for
  skew, … superseded, … rows for unknown outlets, … devices offline`. A
  non-zero skew count or a growing unknown-outlet count is the first thing to
  chase.

**Rollback** is two variables, not one: `JUICE_COLLECTOR=cloud` **and**
`JUICE_TAP_SHADOW=1`, then redeploy. The cloud recorder polls again within a
minute, and shadow mode keeps acknowledging tap's stream *without storing it*
while advancing tap's cursor — so tap's buffer never piles up, nothing double
counts, and a later return to `tap` resumes exactly where shadow left off with
nothing to skip. (Setting `cloud` alone with the token still set is the one
configuration to avoid: it *stores* tap's 1 Hz rows beside the cloud
recorder's and every rollup double-counts; `serve` warns about it at start.)

**If the token was unset during a rollback** — or juice was restored from a
backup taken before tap connected — tap's cursor did not advance, and on the
next connection it will resend everything it buffered over hours the cloud
recorder already covered. Skip it before returning to `tap`:

1. Read the buffer's high-water cursor from bumper: the `cursor` value on the
   `buffer:` line of `make deploy-tap ACTION=status` (`newest_cursor` on
   `http://bumper:8010/api/status`). Not `acked=`/`sent_cursor` — those froze
   when the socket went, below everything that needs skipping.
2. Set `JUICE_INGEST_SKIP_TO=bumper=<that cursor>` alongside
   `JUICE_COLLECTOR=tap` and redeploy. The server moves the stored cursor up
   before the ingest route can answer a hello, logs `ingest skip: moved tap
   bumper …`, and never retreats one. It refuses a cursor that is not the
   full 18-digit width, and a tap with more than one buffer stored (a
   replaced buffer directory) until you name it: `bumper:<buffer_id>=<cursor>`,
   with the id from the same status page. Remove the variable after that
   start.
3. `uv run juice ingest-skip --db <file> [--tap-id bumper --cursor <c>]` is the
   same move for a DB no server holds open (DuckDB locks the file, so it
   cannot run against the live server); with no `--cursor` it lists what is
   stored.

**What a clock problem looks like**: commands refused as `expired` ("check the
clock on the tap box") long before the live projection's 120 s skew guard
trips. Fix the time on bumper; nothing on juice's side is wrong.
