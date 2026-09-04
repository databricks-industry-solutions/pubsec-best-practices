#!/usr/bin/env bash
# 04_plan.sh <env-root> [profile] — speculative-plan one plane root against a
# LIVE workspace/account to prove the adopt-not-create property.
#
#   ./04_plan.sh uc-governance-default my-workspace
#   ./04_plan.sh identity              my-account-sp
#
# It copies the generated root into .plan/<root>/, swaps in a profile-based
# provider (the scaffolded providers.tf uses TFE-style vars), drops the
# remote_state data.tf (its TFE workspaces don't exist in a demo), then
# init + plan. A correct result is "N to import ... 0 to destroy".
set -euo pipefail
cd "$(dirname "$0")"

ROOT="${1:?usage: ./04_plan.sh <env-root, e.g. uc-governance-default> [profile]}"
PROFILE="${2:-my-workspace}"
PROVIDER_VERSION="${PROVIDER_VERSION:-1.130.0}"

SRC="generated/databricks-terraform/environments/$ROOT"
[ -d "$SRC" ] || { echo "no such root: $SRC — run ./run.sh --offline --collapse first" >&2; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "terraform not on PATH (brew: /opt/homebrew/bin)" >&2; exit 1; }

WORK=".plan/$ROOT"
rm -rf "$WORK"; mkdir -p "$WORK"
cp "$SRC"/*.tf "$WORK"/ 2>/dev/null || true
rm -f "$WORK/data.tf"   # remote_state stubs reference TFE workspaces that don't exist in a demo

printf 'provider "databricks" {}\n' > "$WORK/providers.tf"
# Account-level roots (account-infra) reference var.databricks_account_id; the bare
# provider swap dropped the scaffolded declaration, so re-declare it and feed the
# value from the chosen profile's account_id.
if grep -rq 'var\.databricks_account_id' "$WORK"/*.tf 2>/dev/null; then
  printf '\nvariable "databricks_account_id" { type = string }\n' >> "$WORK/providers.tf"
  ACCT=$(awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f&&/account_id/{gsub(/[ \t]/,"");split($0,a,"=");print a[2]}' ~/.databrickscfg 2>/dev/null)
  [ -n "$ACCT" ] && export TF_VAR_databricks_account_id="$ACCT"
fi
cat > "$WORK/versions.tf" <<EOF
terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = { source = "databricks/databricks", version = "${PROVIDER_VERSION}" }
  }
}
EOF

export DATABRICKS_CONFIG_PROFILE="$PROFILE"
echo "== init ($ROOT) =="
terraform -chdir="$WORK" init -input=false >/dev/null

echo "== plan ($ROOT via profile $PROFILE) =="
# Capture BOTH streams — provider import errors go to stderr, and the plan's
# exit code is the source of truth (not a grep).
set +e
terraform -chdir="$WORK" plan -input=false -no-color >"$WORK/plan.out" 2>&1
RC=$?
set -e
grep -E "will be (imported|created|destroyed|replaced)|must be replaced|^Plan:|No changes|^Error:|encountered a problem" \
  "$WORK/plan.out" | tail -60 || true

echo
echo "──────── summary ────────"
grep -E "^Plan:|No changes" "$WORK/plan.out" || true
# Destroy count comes from the authoritative "Plan:" line so REPLACEMENTS
# (shown as "must be replaced", not "will be destroyed") are counted.
D=$(grep -oE '[0-9]+ to destroy' "$WORK/plan.out" | grep -oE '[0-9]+' | tail -1); D=${D:-0}
ERRS=$(grep -c '^Error:' "$WORK/plan.out" 2>/dev/null) || true; ERRS=${ERRS:-0}

if [ "$RC" -ne 0 ] || [ "$ERRS" -gt 0 ]; then
  echo "plan: ERROR ❌ (exit $RC, $ERRS error block(s)) — see $WORK/plan.out"
elif [ "$D" -eq 0 ]; then
  echo "zero-destroy gate: PASS ✅ (0 to destroy)"
else
  echo "zero-destroy gate: REVIEW ⚠ — $D to destroy/replace; triage before apply"
fi
echo "full plan: $WORK/plan.out"
