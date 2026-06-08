#!/usr/bin/env bash
# Pull a production backup and load it into the local dev DB (juice.duckdb).
#
# Refuses to clobber a juice.duckdb that a local `juice serve`/`record` is
# holding open (DuckDB is single-writer); pass --force to override. The
# previous dev DB is kept as juice.duckdb.bak.
set -euo pipefail

cd "$(dirname "$0")/.."

force=0
[ "${1:-}" = "--force" ] && force=1

dev_db="juice.duckdb"

if [ "$force" -ne 1 ]; then
  if pgrep -fa "juice (serve|record)" >/dev/null 2>&1; then
    echo "A local 'juice serve'/'record' appears to be running — stop it first" >&2
    echo "(or re-run with --force). Refusing to overwrite an open $dev_db." >&2
    exit 1
  fi
  if [ -f "${dev_db}.wal" ]; then
    echo "${dev_db}.wal present — the DB may be open. Stop the daemon or use --force." >&2
    exit 1
  fi
fi

# backup-prod.sh prints the saved path as its last stdout line.
snapshot="$(./scripts/backup-prod.sh | tail -n 1)"

if [ -f "$dev_db" ]; then
  cp -f "$dev_db" "${dev_db}.bak"
  echo "Existing dev DB backed up to ${dev_db}.bak"
fi
cp -f "$snapshot" "$dev_db"
rm -f "${dev_db}.wal"
echo "Loaded production snapshot into $dev_db"
