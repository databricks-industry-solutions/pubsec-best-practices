"""Databricks SDK service — shared client for all routes."""

import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient


# Workspace-specific values. Set via `app.yaml` or environment variables when
# deploying to your own workspace; the placeholders here are not valid for any
# real workspace.
CATALOG = os.environ.get('UC_CATALOG', 'main')
SCHEMA = os.environ.get('UC_SCHEMA', 'irs_rrp')
VOLUME = os.environ.get('UC_VOLUME', 'dmn_rules')
VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'
WAREHOUSE_ID = os.environ.get('DATABRICKS_WAREHOUSE_ID', '')
CLUSTER_ID = os.environ.get('DATABRICKS_CLUSTER_ID', '')
JOB_ID = int(os.environ.get('SCORING_JOB_ID', '0'))
NOTEBOOK_PATH = os.environ.get(
  'SCORING_NOTEBOOK_PATH',
  '/Workspace/Shared/irs_rules_engine/04_batch_scoring',
)


@lru_cache()
def get_workspace_client() -> WorkspaceClient:
  """Get or create the Databricks workspace client."""
  return WorkspaceClient()
