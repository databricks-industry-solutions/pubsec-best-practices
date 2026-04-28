# Databricks notebook source

# ================================================================
# Lakeflow Job Definition — Pattern 2 (Jobs For Each)
# ================================================================
# This notebook generates the JSON job definition and optionally
# creates the job via the Databricks REST API.
#
# Parameters (via dbutils.widgets):
#   catalog         — Catalog for config/control tables (default: main)
#   schema          — Schema for config/control tables  (default: tmp)
#   checkpoint_base — Base path for streaming checkpoints
#   notebook_base   — Workspace path to the scaffold notebooks
#
# The job has two tasks:
#   1. generate_table_list  — reads config, emits table list
#   2. process_tables       — For Each loop over the table list
#      └─ process_single_table — nested task (one per table)
#
# You can also paste this JSON directly into the Jobs UI or use
# Databricks Asset Bundles (DABs) for CI/CD deployment.
# ================================================================

import json

# ─── Parameters ────────────────────────────────────────────────
dbutils.widgets.text("catalog",         "main",  "Catalog")
dbutils.widgets.text("schema",          "tmp",   "Schema")
dbutils.widgets.text("checkpoint_base", "/tmp/incremental_filter_pipeline/checkpoints", "Checkpoint Base Path")
dbutils.widgets.text("notebook_base",   "/Workspace/Users/justin.boyd@databricks.com/aroll-notebooks/bitiHGC2opkIFuHQgyX8xA", "Notebook Base Path")

CATALOG         = dbutils.widgets.get("catalog").strip()
SCHEMA          = dbutils.widgets.get("schema").strip()
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base").strip()
NOTEBOOK_BASE   = dbutils.widgets.get("notebook_base").strip()

CONFIG_TABLE  = f"{CATALOG}.{SCHEMA}.incremental_filter__table_config"
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.incremental_filter__sensitive_records"

job_definition = {
    "name": "incremental_filter_for_each",
    "description": f"Incrementally read Delta source tables, filter sensitive records, write to targets. Config-driven via {CONFIG_TABLE}.",
    "tasks": [
        {
            "task_key": "generate_table_list",
            "description": "Read config table and emit active table list as task value",
            "notebook_task": {
                "notebook_path": f"{NOTEBOOK_BASE}/01_generate_table_list",
                "base_parameters": {
                    "catalog": CATALOG,
                    "schema":  SCHEMA,
                },
                "source": "WORKSPACE"
            },
            # Lightweight task — single node is sufficient
            "new_cluster": {
                "spark_version": "16.4.x-scala2.12",
                "num_workers": 0,
                "node_type_id": "i3.xlarge"
            },
            "timeout_seconds": 300
        },
        {
            "task_key": "process_tables",
            "description": "For Each: process one source table per iteration",
            "depends_on": [{"task_key": "generate_table_list"}],
            "for_each_task": {
                # Reference the task value emitted by generate_table_list
                "inputs": "{{tasks.generate_table_list.values.table_configs}}",
                # How many tables to process in parallel
                "concurrency": 50,
                "task": {
                    "task_key": "process_single_table",
                    "notebook_task": {
                        "notebook_path": f"{NOTEBOOK_BASE}/02_process_single_table",
                        "base_parameters": {
                            "input":           "{{input}}",
                            "catalog":         CATALOG,
                            "schema":          SCHEMA,
                            "checkpoint_base": CHECKPOINT_BASE,
                        },
                        "source": "WORKSPACE"
                    },
                    "new_cluster": {
                        "spark_version": "16.4.x-scala2.12",
                        "num_workers": 2,
                        "node_type_id": "i3.xlarge",
                        "spark_conf": {
                            "spark.databricks.delta.schema.autoMerge.enabled": "false"
                        }
                    },
                    # Per-table timeout and retry
                    "timeout_seconds": 3600,
                    "retry_on_timeout": True,
                    "max_retries": 2
                }
            }
        }
    ],
    # Run every 2 hours
    "schedule": {
        "quartz_cron_expression": "0 0 */2 * * ?",
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
    },
    # Only one run at a time
    "max_concurrent_runs": 1,
    # Notify on failure
    "email_notifications": {
        "on_failure": ["justin.boyd@databricks.com"]
    }
}

print("=" * 60)
print("LAKEFLOW JOB DEFINITION")
print("=" * 60)
print()
print(json.dumps(job_definition, indent=2))
print()
print("=" * 60)
print("HOW TO DEPLOY")
print("=" * 60)
print(f"""
Parameters used:
  catalog:         {CATALOG}
  schema:          {SCHEMA}
  checkpoint_base: {CHECKPOINT_BASE}
  notebook_base:   {NOTEBOOK_BASE}

Option A — Jobs UI:
  1. Go to Workflows > Create Job
  2. Switch to JSON editor
  3. Paste the JSON above

Option B — REST API:
  POST /api/2.1/jobs/create
  Body: <the JSON above>

Option C — Databricks Asset Bundles (recommended for CI/CD):
  See notebook 06_pydabs_bundle_generator
""")
