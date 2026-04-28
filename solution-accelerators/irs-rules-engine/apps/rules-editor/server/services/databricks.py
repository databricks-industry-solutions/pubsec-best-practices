"""Databricks SDK service — shared client for all routes."""

import os
from functools import lru_cache

from databricks.sdk import WorkspaceClient


CATALOG = 'services_bureau_catalog'
SCHEMA = 'irs_rrp'
VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/dmn_rules'
WAREHOUSE_ID = os.environ.get('DATABRICKS_WAREHOUSE_ID', '852e667acfb9b179')
# Workspace-specific values. The defaults below are example values — set via
# `app.yaml` or environment variables when deploying to your own workspace.
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
