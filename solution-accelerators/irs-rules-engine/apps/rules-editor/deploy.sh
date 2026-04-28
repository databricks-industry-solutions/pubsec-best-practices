#!/bin/bash
# Deploy the IRS Rules Editor app to Databricks Apps
# Usage: cd apps/rules-editor && ./deploy.sh
# or:    ./apps/rules-editor/deploy.sh (from project root)

set -e

# Resolve script directory so it works from any CWD
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

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
cp app.yaml requirements.txt $DEPLOY_DIR/

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
