---
name: juice-ops
description: Operational runbooks for juice — FlipFix OAuth admin setup, obtaining Qingping cloud API credentials, recovering after moving a machine to a different outlet, and pulling production data to dev.
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
