#!/usr/bin/env bash
# Install, update, or operate tap on a museum box over ssh. Idempotent: run
# `install` as often as you like; a second run with nothing changed syncs
# nothing, rebuilds nothing, and leaves the running container alone.
#
#   scripts/deploy-tap.sh <host> [install|restart|stop|status|logs|pull [DAY...]]
#
# The box ends up laid out like this (see deploy/tap/compose.yml):
#
#   /home/tap/            service user `tap`, uid 10001 — the uid the image runs as
#     app/                synced source: tap/, pyproject.toml, uv.lock, Dockerfile.tap, compose.yml
#     tap.toml            deploy/tap/<host>.toml
#     .env                secrets, written from this shell's environment, 0640
#     buffer/  logs/      readings and day logs, both owned by tap
#     DEPLOYED            what was deployed, when, by whom
#
# Everything under /home/tap is written as `tap` via `sudo -n -u tap`, so the
# ssh user needs passwordless sudo and docker-group membership on the box, and
# nothing ever needs a chown. Secrets come from the local environment (or a
# local .env, sourced if present): KASA_USERNAME/KASA_PASSWORD (optional; IOT
# devices need none) and TAP_UPLINK_URL/TAP_UPLINK_TOKEN (optional; unset means
# tap runs standalone). Deploys the working tree, not HEAD — DEPLOYED records
# the sha with a +dirty marker so a deploy is always attributable.
#
# `pull` copies the day logs and a consistent snapshot of the given buffer days
# (YYYY-MM-DD; default today) to pulls/<host>/, for tests/e2e/replay.py or a
# local look. Not under data/: that directory is the dev DuckDB's, and tap files
# have been wiped out of it together with the DuckDB before.
set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

usage() {
  sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 64
}

[ $# -ge 1 ] || usage
HOST=$1
ACTION=${2:-install}
shift; [ $# -eq 0 ] || shift

SERVICE_USER=tap
SERVICE_UID=10001
REMOTE_HOME=/home/$SERVICE_USER
HOST_CONFIG=deploy/tap/$HOST.toml
SHIPPED=(tap pyproject.toml uv.lock Dockerfile.tap deploy/tap)

# ControlPath=none: `install` adds the ssh user to the tap group, and a
# multiplexed master session would keep the old group list for every later
# command in the same run.
SSH=(ssh -o BatchMode=yes -o ControlPath=none)
as_tap="sudo -n -u $SERVICE_USER"
COMPOSE="docker compose --project-directory $REMOTE_HOME -f $REMOTE_HOME/app/compose.yml"

remote() { "${SSH[@]}" "$HOST" "$@"; }
push() {  # rsync to or from /home/tap, as the tap user on the far end
  rsync -az --chmod=D755,F644 -e "${SSH[*]}" --rsync-path="$as_tap rsync" "$@"
}
say() { printf '\n== %s\n' "$*"; }

both_or_neither() {  # two env var names that only make sense as a pair
  if { [ -n "${!1:-}" ] && [ -z "${!2:-}" ]; } || { [ -z "${!1:-}" ] && [ -n "${!2:-}" ]; }; then
    echo "$1 and $2 must be set together" >&2
    exit 1
  fi
}

provision() {
  say "provisioning $SERVICE_USER on $HOST"
  # shellcheck disable=SC2087  # unescaped variables are meant to expand locally
  remote sudo -n bash -s <<EOF
set -euo pipefail
# Group first, with the gid pinned: the image's files are $SERVICE_UID:$SERVICE_UID, and
# --user-group would hand out the next free *system* gid instead.
reown=0
if getent group $SERVICE_USER >/dev/null; then
  gid=\$(getent group $SERVICE_USER | cut -d: -f3)
  [ "\$gid" = "$SERVICE_UID" ] || { groupmod --gid $SERVICE_UID $SERVICE_USER; echo "moved group $SERVICE_USER to gid $SERVICE_UID"; reown=1; }
else
  useradd_gid_taken=\$(getent group $SERVICE_UID | cut -d: -f1 || true)
  [ -z "\$useradd_gid_taken" ] || { echo "gid $SERVICE_UID is already taken by \$useradd_gid_taken" >&2; exit 1; }
  groupadd --system --gid $SERVICE_UID $SERVICE_USER
fi
if id -u $SERVICE_USER >/dev/null 2>&1; then
  actual=\$(id -u $SERVICE_USER)
  [ "\$actual" = "$SERVICE_UID" ] || { echo "user $SERVICE_USER exists with uid \$actual, not $SERVICE_UID" >&2; exit 1; }
  [ "\$(id -g $SERVICE_USER)" = "$SERVICE_UID" ] || { usermod --gid $SERVICE_USER $SERVICE_USER; reown=1; }
else
  if getent passwd $SERVICE_UID >/dev/null; then
    echo "uid $SERVICE_UID is already taken by \$(getent passwd $SERVICE_UID | cut -d: -f1)" >&2; exit 1
  fi
  useradd --system --uid $SERVICE_UID --gid $SERVICE_USER --create-home --home-dir $REMOTE_HOME \\
          --shell /usr/sbin/nologin $SERVICE_USER
  echo "created user $SERVICE_USER ($SERVICE_UID)"
fi
chmod 750 $REMOTE_HOME
# Only when the ids just changed: a routine run must not walk a month of buffer.
[ "\$reown" = 0 ] || chown -R $SERVICE_USER:$SERVICE_USER $REMOTE_HOME
# The deploying user reads logs and the build context, and compose reads .env.
if ! id -nG "\$SUDO_USER" | tr ' ' '\n' | grep -qx $SERVICE_USER; then
  usermod -aG $SERVICE_USER "\$SUDO_USER"
  echo "added \$SUDO_USER to group $SERVICE_USER"
fi
$as_tap mkdir -p $REMOTE_HOME/app $REMOTE_HOME/buffer $REMOTE_HOME/logs
EOF
}

quoted() {
  # A dotenv value compose reads back verbatim. Single quotes are literal
  # (no interpolation, no escapes) but cannot contain a quote; for those,
  # double quotes with \\ \" and $$ escaped. Checked against `compose config`.
  local v=$1
  if [[ $v != *"'"* ]]; then
    printf "'%s'" "$v"
  else
    v=${v//\\/\\\\}; v=${v//\"/\\\"}; v=${v//\$/\$\$}
    printf '"%s"' "$v"
  fi
}

write_env() {
  say "writing $REMOTE_HOME/.env"
  both_or_neither KASA_USERNAME KASA_PASSWORD
  both_or_neither TAP_UPLINK_URL TAP_UPLINK_TOKEN
  # A shell that simply lacks a secret the box already has is far more likely a
  # colleague's laptop than a decision to drop it — and dropping the uplink pair
  # would quietly put tap back to standalone. Refuse unless told otherwise.
  local have_remote
  have_remote=$(remote "$as_tap sed -n 's/^\([A-Z_]*\)=.*/\1/p' $REMOTE_HOME/.env 2>/dev/null" || true)
  local var
  for var in KASA_USERNAME TAP_UPLINK_URL; do
    if grep -qx "$var" <<<"$have_remote" && [ -z "${!var:-}" ] && [ "${DEPLOY_TAP_UNSET:-}" != 1 ]; then
      echo "$var is set on $HOST but not in this shell; export it, or DEPLOY_TAP_UNSET=1 to drop it" >&2
      exit 1
    fi
  done
  {
    echo "# Written by scripts/deploy-tap.sh; change the deploy environment, not this file."
    [ -z "${KASA_USERNAME:-}" ] || printf 'KASA_USERNAME=%s\nKASA_PASSWORD=%s\n' "$(quoted "$KASA_USERNAME")" "$(quoted "$KASA_PASSWORD")"
    [ -z "${TAP_UPLINK_URL:-}" ] || printf 'TAP_UPLINK_URL=%s\nTAP_UPLINK_TOKEN=%s\n' "$(quoted "$TAP_UPLINK_URL")" "$(quoted "$TAP_UPLINK_TOKEN")"
  } | remote "$as_tap sh -c 'umask 027; cat > $REMOTE_HOME/.env && chmod 640 $REMOTE_HOME/.env'"
  echo "credentials: ${KASA_USERNAME:+configured}${KASA_USERNAME:-none}"
  echo "uplink: ${TAP_UPLINK_URL:+configured}${TAP_UPLINK_URL:-none (standalone)}"
}

deployed_stamp() {
  local sha dirty=""
  sha=$(git rev-parse --short HEAD)
  [ -z "$(git status --porcelain -- "${SHIPPED[@]}")" ] || dirty="+dirty"
  echo "sha=$sha$dirty at=$(date -u +%Y-%m-%dT%H:%M:%SZ) by=$(id -un)@$(hostname)"
}

install() {
  [ -f "$HOST_CONFIG" ] || { echo "no $HOST_CONFIG — write one (deploy/tap/bumper.toml is the model)" >&2; exit 1; }
  # Fail here, with tap's own message, rather than in a crash loop on the box.
  uv run --no-sync python -c "from tap.config import load_config; load_config(path='$HOST_CONFIG', environ={})"
  provision
  write_env
  say "syncing source to $HOST:$REMOTE_HOME/app"
  push --delete --exclude __pycache__ tap/ "$HOST:$REMOTE_HOME/app/tap/"
  push pyproject.toml uv.lock Dockerfile.tap "$HOST:$REMOTE_HOME/app/"
  push deploy/tap/compose.yml "$HOST:$REMOTE_HOME/app/compose.yml"
  # tap.toml is a single-file bind mount, which pins an inode: the container
  # keeps reading the old file after rsync's rename, and compose sees no reason
  # to recreate it because the file is not part of the service config. So a
  # changed config is detected here and forces a recreate below.
  local recreate=()
  if [ -n "$(push -i "$HOST_CONFIG" "$HOST:$REMOTE_HOME/tap.toml")" ]; then
    recreate=(--force-recreate)
    echo "tap.toml changed; the container will be recreated"
  fi
  local stamp
  stamp=$(deployed_stamp)
  echo "$stamp" | remote "$as_tap sh -c 'cat > $REMOTE_HOME/DEPLOYED'"
  echo "$stamp"
  say "building and starting"
  remote "$COMPOSE up -d --build --remove-orphans ${recreate[*]:-}"
  wait_healthy
  status
}

wait_healthy() {
  say "waiting for the container to report healthy"
  local id state=missing
  id=$(remote "$COMPOSE ps -q tap")
  [ -n "$id" ] || { echo "no container is running" >&2; exit 1; }
  # Dockerfile.tap: start-period 90 s, interval 30 s, retries 3 — the first
  # "unhealthy" verdict cannot come before ~180 s, so wait past that.
  for _ in $(seq 1 48); do
    state=$(remote "docker inspect --format '{{.State.Health.Status}}' $id")
    case "$state" in
      healthy) echo "healthy"; return 0 ;;
      unhealthy) break ;;
    esac
    sleep 5
  done
  echo "container is $state; last log lines:" >&2
  remote "$COMPOSE logs --tail=40 tap" >&2
  exit 1
}

summarise() {
  python3 - "$1" <<'PY'
import json, sys

s = json.loads(sys.argv[1])
devices = s["devices"]
by_state: dict[str, list[str]] = {}
for d in devices:
    by_state.setdefault(d["state"], []).append(d["host"])
outlets = sum(len(d["outlets"]) for d in devices)
print(f'tap {s["tap_id"]} v{s["version"]}, up {s["uptime_seconds"] / 3600:.1f} h, '
      f'{len(devices)} devices, {outlets} outlets')
for state, hosts in sorted(by_state.items()):
    print(f'  {state}: {len(hosts)}  {" ".join(hosts)}')
b = s["buffer"]
print(f'buffer: {b["rows_written"]} rows written this run, {len(b["days"])} day files, '
      f'{b["total_bytes"] / 1e6:.0f} MB, newest {b["newest_ts"]} cursor {b.get("newest_cursor")}')
u = s["uplink"]
if u["enabled"]:
    print(f'uplink: {u["url"]} connected={u["connected"]} acked={u["acked_cursor"]} lag={u["lag_rows"]} rows')
else:
    print("uplink: disabled (standalone)")
if s["warnings"]:
    print("WARNINGS: " + "; ".join(s["warnings"]))
PY
}

status() {
  say "$HOST: $(remote "cat $REMOTE_HOME/DEPLOYED 2>/dev/null || echo 'not deployed'")"
  remote "$COMPOSE ps --format 'table {{.Name}}\t{{.Status}}\t{{.RunningFor}}'"
  remote "id=\$($COMPOSE ps -q tap); [ -z \"\$id\" ] || docker inspect --format 'restart policy {{.HostConfig.RestartPolicy.Name}}, restarts {{.RestartCount}}, health {{.State.Health.Status}}' \$id"
  local snapshot
  if snapshot=$(remote "curl -fsS --max-time 5 http://127.0.0.1:8010/api/status"); then
    summarise "$snapshot"
  else
    echo "tap is not answering on $HOST:8010" >&2
    return 1
  fi
}

pull() {
  local dest="pulls/$HOST" days=("$@") day file
  mkdir -p "$dest/buffer" "$dest/logs"
  [ ${#days[@]} -gt 0 ] || days=("$(date -u +%Y-%m-%d)")
  say "pulling logs to $dest/logs"
  push "$HOST:$REMOTE_HOME/logs/" "$dest/logs/"
  for day in "${days[@]}"; do
    [[ $day =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "pull: days are YYYY-MM-DD, not '$day'" >&2; exit 64; }
    file="${day//-/}.sqlite"
    say "pulling buffer day $day to $dest/buffer/$file"
    remote "$as_tap test -f $REMOTE_HOME/buffer/$file" \
      || { echo "no buffer file for $day on $HOST (pruned, or not yet written)" >&2; exit 1; }
    # A live day file has a WAL in flight, so copy a snapshot, not the file.
    # Opened read-only by URI: a plain connect() would *create* an empty day in
    # the live buffer for a date that does not exist, and tap would count it.
    # shellcheck disable=SC2064  # expand now: the path is fixed and the trap must survive set -e
    trap "remote '$as_tap rm -f $REMOTE_HOME/pull-$file'" EXIT
    remote "$as_tap python3 -c \"
import sqlite3
src = sqlite3.connect('file:$REMOTE_HOME/buffer/$file?mode=ro', uri=True)
dst = sqlite3.connect('$REMOTE_HOME/pull-$file')
src.backup(dst)
\""
    push "$HOST:$REMOTE_HOME/pull-$file" "$dest/buffer/$file"
    remote "$as_tap rm -f $REMOTE_HOME/pull-$file"
    trap - EXIT
  done
  ls -la "$dest/buffer" "$dest/logs"
}

case "$ACTION" in
  install|update|start) install ;;
  restart) remote "$COMPOSE restart"; wait_healthy; status ;;
  stop)    remote "$COMPOSE down" ;;
  status)  status ;;
  logs)    "${SSH[@]}" -t "$HOST" "$COMPOSE logs --tail=200 -f" ;;
  pull)    pull "$@" ;;
  *)       usage ;;
esac
