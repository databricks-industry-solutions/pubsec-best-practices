#!/bin/bash
# Deploy the Fraud Rules Editor app to Databricks Apps
# Usage: cd apps/rules-editor && ./deploy.sh
# or:    ./apps/rules-editor/deploy.sh (from project root)

set -e

# Resolve script directory so it works from any CWD
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Source .env.local from repo root if present, so deploys pick up workspace
# IDs without having to manually export them. Forkers without .env.local
# fall back to whatever is already in the shell environment.
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
if [ -f "$REPO_ROOT/.env.local" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$REPO_ROOT/.env.local"
  set +a
fi

# Configurable via env. Override before running for your own workspace.
PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
APP_NAME="${APP_NAME:-irs-rules-editor}"
# Where the staged source uploads to in the workspace. Defaults to the current
# user's workspace home (`databricks current-user me`); override with
# WORKSPACE_PATH=/Workspace/... to target a shared location.
if [ -z "${WORKSPACE_PATH:-}" ]; then
  WORKSPACE_USER="$(databricks current-user me --profile "$PROFILE" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("userName",""))')"
  if [ -z "$WORKSPACE_USER" ]; then
    echo "Could not resolve workspace user. Set WORKSPACE_PATH explicitly or check that profile '$PROFILE' is configured." >&2
    exit 1
  fi
  WORKSPACE_PATH="/Workspace/Users/$WORKSPACE_USER/apps/$APP_NAME"
fi
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/irs-rules-editor-deploy}"
echo "Profile: $PROFILE"
echo "App:     $APP_NAME"
echo "Target:  $WORKSPACE_PATH"

echo "=== Building client ==="
cd client && npm run build && cd ..

echo "=== Assembling deploy package ==="
rm -rf $DEPLOY_DIR
mkdir -p $DEPLOY_DIR/server/routers $DEPLOY_DIR/server/services $DEPLOY_DIR/client

cp -r client/build $DEPLOY_DIR/client/
cp server/*.py $DEPLOY_DIR/server/
cp server/routers/*.py $DEPLOY_DIR/server/routers/
cp server/services/*.py $DEPLOY_DIR/server/services/
cp requirements.txt $DEPLOY_DIR/

# Override app.yaml env values from the shell environment (sourced from
# .env.local at the top of this script). For each `- name: FOO` block, if
# $FOO is set in the shell, replace its `value:` line with the shell value.
# This means .env.local is the single source of truth — forkers either fill
# in app.yaml's defaults or set the same vars in their shell.
python3 - <<'PYEOF' app.yaml "$DEPLOY_DIR/app.yaml"
import os, re, sys
src, dst = sys.argv[1], sys.argv[2]
lines = open(src).read().splitlines(keepends=True)
out = []
i = 0
name_re = re.compile(r'^(\s*)-\s*name:\s*(\S+)\s*$')
val_re  = re.compile(r'^(\s*)value:\s*.*$')
while i < len(lines):
  out.append(lines[i])
  m = name_re.match(lines[i])
  if m and i + 1 < len(lines):
    indent, varname = m.group(1), m.group(2)
    nxt = lines[i + 1]
    vm = val_re.match(nxt)
    env_val = os.environ.get(varname)
    if vm and env_val:
      out.append(f'{vm.group(1)}value: "{env_val}"\n')
      i += 2
      continue
  i += 1
open(dst, 'w').writelines(out)
PYEOF
echo "app.yaml env values:"
grep -E '^\s+value:' "$DEPLOY_DIR/app.yaml" | sed 's/^/  /'

echo "Client JS: $(ls $DEPLOY_DIR/client/build/assets/*.js | xargs basename)"

echo "=== Uploading to workspace ==="
databricks workspace delete "$WORKSPACE_PATH" --recursive --profile $PROFILE 2>&1 || true
databricks workspace import-dir $DEPLOY_DIR "$WORKSPACE_PATH" --profile $PROFILE 2>&1 | tail -3

echo "=== Deploying app ==="
databricks apps deploy $APP_NAME \
  --source-code-path "$WORKSPACE_PATH" \
  --profile $PROFILE

echo "=== Done ==="
databricks apps get $APP_NAME --profile $PROFILE 2>&1 | python3 -c "
import json,sys; a=json.load(sys.stdin)
print(f'URL:    {a.get(\"url\")}')
print(f'Status: {a.get(\"app_status\",{}).get(\"state\")}')
"
