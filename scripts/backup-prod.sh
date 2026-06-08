#!/usr/bin/env bash
# Pull a consistent backup of the production juice DB to data/backups/.
#
# Reads JUICE_PROD_URL (e.g. https://juice.theflip.museum) and
# JUICE_BACKUP_TOKEN. Both can live in .env, which is sourced if present.
# The token is sent as an Authorization: Bearer header (never in the URL).
set -euo pipefail

cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

: "${JUICE_PROD_URL:?set JUICE_PROD_URL (e.g. https://juice.theflip.museum)}"
: "${JUICE_BACKUP_TOKEN:?set JUICE_BACKUP_TOKEN (the production backup secret)}"

mkdir -p data/backups
out="data/backups/juice-$(date -u +%Y%m%dT%H%M%SZ).duckdb"

echo "Pulling backup from ${JUICE_PROD_URL}/api/backup ..."
curl -fSL \
  -H "Authorization: Bearer ${JUICE_BACKUP_TOKEN}" \
  "${JUICE_PROD_URL%/}/api/backup" \
  -o "$out"

# Verify it opens and has data before trusting it.
if ! uv run python - "$out" <<'PY'
import sys, duckdb
path = sys.argv[1]
con = duckdb.connect(path, read_only=True)
n = con.execute("SELECT count(*) FROM readings").fetchone()[0]
con.close()
print(f"  OK — {n} readings rows")
PY
then
  echo "Downloaded file failed verification; removing." >&2
  rm -f "$out"
  exit 1
fi

echo "Backup saved to $out"
# Emit the path on stdout so other scripts can capture it.
echo "$out"
