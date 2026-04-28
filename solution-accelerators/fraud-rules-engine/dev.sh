#!/usr/bin/env bash
# Local-only dev wrapper. Sources .env.local and runs the rest of the args.
# Examples:
#   ./dev.sh databricks bundle deploy
#   ./dev.sh databricks bundle validate
#   ./dev.sh ./apps/rules-editor/deploy.sh
#   ./dev.sh make build
#
# Without args: prints the loaded env so you can sanity-check.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env.local ]]; then
  echo "error: .env.local not found. Copy .env.local.example and fill it in." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env.local
set +a

if [[ $# -eq 0 ]]; then
  echo "loaded .env.local. inspecting key vars:"
  for v in DATABRICKS_CONFIG_PROFILE UC_CATALOG UC_SCHEMA UC_VOLUME \
           DATABRICKS_WAREHOUSE_ID DATABRICKS_CLUSTER_ID SCORING_JOB_ID; do
    printf '  %-32s = %s\n' "$v" "${!v:-(empty)}"
  done
  echo
  echo "usage: ./dev.sh <command> [args...]"
  exit 0
fi

exec "$@"
