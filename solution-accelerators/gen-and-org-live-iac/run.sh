#!/usr/bin/env bash
# run.sh — end-to-end: prereqs -> live export -> plane transform -> fmt.
#
#   ./run.sh              full pipeline, single-pass export (needs .env)
#   ./run.sh --fleet      chunked fan-out export via fleet.conf (02_export_fleet.sh)
#   ./run.sh --offline    skip the live export; transform the committed
#                         captured/ snapshots (reproducible, no creds)
#   --collapse            also fold repeated resources into for_each maps
#
# The transform reads the PARENT captured/ dir, so every export chunk
# (an account pass, a metastore pass, N per-workspace passes) is picked up and
# merged into one plane tree — this is what makes chunked exports recombine.
set -euo pipefail
cd "$(dirname "$0")"

OFFLINE=0
COLLAPSE=0
FLEET=0
for a in "$@"; do
  case "$a" in
    --offline)  OFFLINE=1 ;;
    --collapse) COLLAPSE=1 ;;
    --fleet)    FLEET=1 ;;
  esac
done

EXPORT_DIR="captured/exported"   # where a live account pass writes
CAPTURED="captured"              # parent read by the transform (all passes)
OUT_DIR="generated"

# Pick a python3 that has PyYAML — PATH may put a homebrew python (used for
# terraform) ahead of the one with deps installed.
PY=""
for c in python3 "$HOME/.pyenv/shims/python3" /usr/local/bin/python3 /usr/bin/python3; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c 'import yaml' 2>/dev/null; then PY="$c"; break; fi
done
[ -n "$PY" ] || { echo "no python3 with PyYAML found — pip install pyyaml" >&2; exit 1; }

if [ "$OFFLINE" -eq 0 ]; then
  echo "== 0/3 prereqs =="
  eval "$(./00_prereqs.sh | grep '^export TF_PROVIDER_BIN=')"
  if [ "$FLEET" -eq 1 ]; then
    echo "== 1/3 chunked fleet export =="
    ./02_export_fleet.sh
  else
    echo "== 1/3 live export (single pass) =="
    ./01_export.sh "$EXPORT_DIR"
  fi
else
  echo "== offline: using committed snapshots under $CAPTURED/ =="
  [ -n "$(find "$CAPTURED" -name '*.tf' 2>/dev/null | head -1)" ] || {
    echo "no snapshot under $CAPTURED — run once live first" >&2; exit 1; }
fi

echo "== 2/3 transform to planes =="
"$PY" plane_transform.py --exported-dir "$CAPTURED" --out-dir "$OUT_DIR"

if [ "$COLLAPSE" -eq 1 ]; then
  echo "== 2b: collapse repeated resources into for_each maps =="
  "$PY" collapse_foreach.py --tree "$OUT_DIR/databricks-terraform"
fi

echo "== 3/3 terraform fmt =="
if command -v terraform >/dev/null 2>&1; then
  terraform fmt -recursive "$OUT_DIR/databricks-terraform" >/dev/null && echo "formatted."
else
  echo "terraform not on PATH; skipped fmt."
fi

echo
echo "Done. Plane tree: $OUT_DIR/databricks-terraform/"
echo "Next: cd into any environments/<root>/, terraform init, and speculative-plan"
echo "through your VCS-driven Terraform workflow — a first plan should be pure adopt/no-op (zero destroy)."
