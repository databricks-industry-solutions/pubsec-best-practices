# Databricks notebook source

# ================================================================
# PyDABs Integration: Programmatic Job Generation per pipeline_batch
# ================================================================
# This notebook generates a complete Databricks Asset Bundles (DABs)
# project that creates one Lakeflow For Each job per pipeline_batch
# value in the config table.
#
# Architecture:
#   Config table has a `pipeline_batch` column (1..N).
#   This script reads the distinct batch IDs and generates:
#     - One Job per batch, each with:
#         Task 1: generate_table_list (filtered to that batch)
#         Task 2: For Each → process_single_table
#     - A databricks.yml with targets for dev and prod
#     - A resources/ package with load_resources() that creates
#       the jobs dynamically from the config table
#
# Write modes (per table, driven by config):
#   "append" (default) — append-only with mergeSchema
#   "merge"            — upsert via DeltaTable.merge() + withSchemaEvolution()
#                        Requires merge_keys in config. Optional sequence_by for dedup.
#
# How to use:
#   1. Run this notebook to generate the project files
#   2. Copy the output directory to your local machine (or Git repo)
#   3. Run: uv sync && databricks bundle deploy --target dev
#
# References:
#   - PyDABs docs: https://docs.databricks.com/aws/en/dev-tools/bundles/python
#   - ForEachTask example: https://www.databricks.com/blog/how-move-apache-airflowr-databricks-lakeflow-jobs
# ================================================================

import os, base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()

# ─── Project paths ─────────────────────────────────────────────
PROJECT_ROOT = "/Workspace/Users/justin.boyd@databricks.com/aroll-notebooks/bitiHGC2opkIFuHQgyX8xA/incremental_filter_bundle"
SRC_DIR      = f"{PROJECT_ROOT}/src"
RESOURCES_DIR = f"{PROJECT_ROOT}/resources"

# Create directories
for d in [PROJECT_ROOT, SRC_DIR, RESOURCES_DIR]:
    w.workspace.mkdirs(d)

print(f"✅ Project directory created: {PROJECT_ROOT}")


# COMMAND ----------


# ─── File 1: databricks.yml ───────────────────────────────────
# Now includes catalog and schema as first-class bundle variables.
databricks_yml = """
bundle:
  name: incremental_filter_pipeline

python:
  venv_path: .venv
  resources:
    - 'resources:load_resources'

workspace:
  root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: "main"
      schema: "tmp"
      checkpoint_base: "/Volumes/main/tmp/checkpoints"
      concurrency: 10
      node_type_id: "i3.xlarge"
      num_workers: 2

  prod:
    mode: production
    variables:
      catalog: "my_catalog"
      schema: "pipeline_meta"
      checkpoint_base: "/Volumes/my_catalog/pipeline_meta/checkpoints"
      concurrency: 50
      node_type_id: "i3.2xlarge"
      num_workers: 4

variables:
  catalog:
    description: "Catalog for config and control tables"
  schema:
    description: "Schema for config and control tables"
  checkpoint_base:
    description: "Base path for streaming checkpoints"
  concurrency:
    description: "Max parallel For Each iterations per job"
    type: integer
  node_type_id:
    description: "Instance type for processing clusters"
  num_workers:
    description: "Number of workers per processing cluster"
    type: integer
""".strip()

w.workspace.import_(
    path=f"{PROJECT_ROOT}/databricks.yml",
    content=base64.b64encode(databricks_yml.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ databricks.yml")


# COMMAND ----------


# ─── File 2: resources/__init__.py ─────────────────────────────
# Now uses catalog/schema variables instead of fully-qualified
# config_table/control_table. Passes catalog + schema as
# base_parameters to both task notebooks.
resources_init = '''
from databricks.bundles.core import Bundle, Resources, Variable, variables
from databricks.bundles.jobs import (
    Job,
    Task,
    NotebookTask,
    ForEachTask,
    TaskDependency,
    JobCluster,
    JobEmailNotifications,
    CronSchedule,
)


@variables
class Variables:
    catalog: Variable[str]
    schema: Variable[str]
    checkpoint_base: Variable[str]
    concurrency: Variable[int]
    node_type_id: Variable[str]
    num_workers: Variable[int]


def create_batch_job(
    batch_id: int,
    catalog: str,
    schema: str,
    checkpoint_base: str,
    concurrency: int,
    node_type_id: str,
    num_workers: int,
) -> Job:
    """Create a single For Each job for a given pipeline_batch."""

    config_table  = f"{catalog}.{schema}.incremental_filter__table_config"
    control_table = f"{catalog}.{schema}.incremental_filter__sensitive_records"

    job_clusters = [
        JobCluster.from_dict(
            {
                "job_cluster_key": "generator_cluster",
                "new_cluster": {
                    "spark_version": "16.4.x-scala2.12",
                    "num_workers": 0,
                    "node_type_id": node_type_id,
                },
            }
        ),
        JobCluster.from_dict(
            {
                "job_cluster_key": "processing_cluster",
                "new_cluster": {
                    "spark_version": "16.4.x-scala2.12",
                    "num_workers": num_workers,
                    "node_type_id": node_type_id,
                    "spark_conf": {
                        "spark.databricks.delta.schema.autoMerge.enabled": "false",
                    },
                },
            }
        ),
    ]

    # ── Task 1: Generate table list (filtered to this batch) ───
    generate_task = Task(
        task_key="generate_table_list",
        job_cluster_key="generator_cluster",
        notebook_task=NotebookTask(
            notebook_path="src/generate_table_list.py",
            base_parameters={
                "catalog": catalog,
                "schema": schema,
                "config_table": config_table,
                "pipeline_batch": str(batch_id),
            },
        ),
        timeout_seconds=300,
    )

    # ── Task 2: For Each → process one table per iteration ─────
    process_task = Task(
        task_key="process_tables",
        depends_on=[TaskDependency(task_key="generate_table_list")],
        for_each_task=ForEachTask(
            inputs="{{tasks.generate_table_list.values.table_configs}}",
            concurrency=concurrency,
            task=Task(
                task_key="process_single_table",
                job_cluster_key="processing_cluster",
                notebook_task=NotebookTask(
                    notebook_path="src/process_single_table.py",
                    base_parameters={
                        "input": "{{input}}",
                        "catalog": catalog,
                        "schema": schema,
                        "control_table": control_table,
                        "checkpoint_base": checkpoint_base,
                    },
                ),
                timeout_seconds=3600,
                max_retries=2,
                retry_on_timeout=True,
            ),
        ),
    )

    return Job(
        name=f"incremental_filter_batch_{batch_id}",
        job_clusters=job_clusters,
        tasks=[generate_task, process_task],
        schedule=CronSchedule(
            quartz_cron_expression="0 0 */2 * * ?",
            timezone_id="UTC",
        ),
        max_concurrent_runs=1,
        email_notifications=JobEmailNotifications(
            on_failure=["${workspace.current_user.userName}"],
        ),
        tags={
            "project": "incremental_filter",
            "pipeline_batch": str(batch_id),
            "catalog": catalog,
            "schema": schema,
        },
    )


def load_resources(bundle: Bundle) -> Resources:
    """
    Called by the Databricks CLI during `databricks bundle deploy`.

    Reads distinct pipeline_batch values from the config table and
    creates one For Each job per batch.
    """
    resources = Resources()

    catalog         = bundle.resolve_variable(Variables.catalog)
    schema          = bundle.resolve_variable(Variables.schema)
    checkpoint_base = bundle.resolve_variable(Variables.checkpoint_base)
    concurrency     = bundle.resolve_variable(Variables.concurrency)
    node_type_id    = bundle.resolve_variable(Variables.node_type_id)
    num_workers     = bundle.resolve_variable(Variables.num_workers)

    config_table = f"{catalog}.{schema}.incremental_filter__table_config"

    # ── Read distinct batch IDs from the config table ──────────
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id=bundle.resolve_variable(
            Variable("warehouse_id", default="auto")
        )
        if False
        else None,
        statement=f"""
            SELECT DISTINCT pipeline_batch
            FROM {config_table}
            WHERE is_active = true
            ORDER BY pipeline_batch
        """,
    )

    batch_ids = []
    try:
        if result and result.result and result.result.data_array:
            batch_ids = [int(row[0]) for row in result.result.data_array]
    except Exception:
        pass

    if not batch_ids:
        print("⚠️  Could not read config table — using fallback batch list")
        batch_ids = list(range(1, 11))

    for batch_id in batch_ids:
        job = create_batch_job(
            batch_id=batch_id,
            catalog=catalog,
            schema=schema,
            checkpoint_base=checkpoint_base,
            concurrency=concurrency,
            node_type_id=node_type_id,
            num_workers=num_workers,
        )
        resources.add_resource(f"batch_{batch_id}_job", job)

    print(f"✅ Generated {len(batch_ids)} jobs for batches: {batch_ids}")
    print(f"   catalog={catalog}, schema={schema}")
    return resources
'''.strip()

w.workspace.import_(
    path=f"{RESOURCES_DIR}/__init__.py",
    content=base64.b64encode(resources_init.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ resources/__init__.py")


# COMMAND ----------


# ─── File 3: src/generate_table_list.py ────────────────────────
# Batch-aware generator. Accepts catalog/schema widgets for
# interactive use; when called from a job, config_table is
# passed directly as a base_parameter.

generate_table_list_py = '''
# ================================================================
# Task 1: Generate Table List (batch-filtered)
# ================================================================
# Reads the config table, filters to the specified pipeline_batch,
# and emits the list as a task value for the For Each loop.
#
# Parameters (via dbutils.widgets):
#   catalog        — Catalog for config table (default: main)
#   schema         — Schema for config table  (default: tmp)
#   config_table   — FQN override; if set, takes precedence over catalog/schema
#   pipeline_batch — Which batch this job processes (default: 1)
# ================================================================

dbutils.widgets.text("catalog",        "main",  "Catalog")
dbutils.widgets.text("schema",         "tmp",   "Schema")
dbutils.widgets.text("config_table",   "",       "Config Table (FQN override)")
dbutils.widgets.text("pipeline_batch", "1",      "Pipeline Batch")

_catalog  = dbutils.widgets.get("catalog").strip()
_schema   = dbutils.widgets.get("schema").strip()
_override = dbutils.widgets.get("config_table").strip()

# If config_table widget is set, use it directly; otherwise derive from catalog/schema
config_table   = _override if _override else f"{_catalog}.{_schema}.incremental_filter__table_config"
pipeline_batch = int(dbutils.widgets.get("pipeline_batch"))

print(f"📋 Config table: {config_table}")
print(f"   Pipeline batch: {pipeline_batch}")

config_df = (
    spark.read.table(config_table)
    .filter(f"is_active = true AND pipeline_batch = {pipeline_batch}")
)

table_list = [row.asDict() for row in config_df.collect()]

# Clean up values for JSON serialization
for t in table_list:
    t["is_active"] = bool(t["is_active"])
    # Ensure None → empty string for optional fields
    for optional_key in ("write_mode", "merge_keys", "sequence_by", "use_cdf"):
        if t.get(optional_key) is None:
            t[optional_key] = ""

try:
    dbutils.jobs.taskValues.set(key="table_configs", value=table_list)
except Exception:
    pass  # Running interactively

# ── Summary ────────────────────────────────────────────────────
print(f"✅ Batch {pipeline_batch}: emitted {len(table_list)} table configs")
append_count = sum(1 for t in table_list if (t.get("write_mode") or "append").lower() == "append")
merge_count  = sum(1 for t in table_list if (t.get("write_mode") or "").lower() == "merge")
cdf_count    = sum(1 for t in table_list if str(t.get("use_cdf") or "").lower() in ("true", "1", "yes"))
print(f"   Write modes: {append_count} append, {merge_count} merge | CDF enabled: {cdf_count}")

for i, t in enumerate(table_list[:10]):
    src = f"{t['source_catalog']}.{t['source_schema']}.{t['source_table']}"
    tgt = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
    mode = (t.get("write_mode") or "append").lower()
    cdf  = "CDF" if str(t.get("use_cdf") or "").lower() in ("true", "1", "yes") else ""
    keys = t.get("merge_keys", "")
    parts = [mode]
    if keys:
        parts.append(f"keys={keys}")
    if cdf:
        parts.append(cdf)
    print(f"   [{i+1}] {src} → {tgt}  [{', '.join(parts)}]")
if len(table_list) > 10:
    print(f"   ... and {len(table_list) - 10} more")
'''.strip()

w.workspace.import_(
    path=f"{SRC_DIR}/generate_table_list.py",
    content=base64.b64encode(generate_table_list_py.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ src/generate_table_list.py")


# COMMAND ----------


# ─── File 4: src/process_single_table.py ───────────────────────
# CDF + auto-recovery. Now accepts catalog/schema widgets as
# fallback for interactive use; when called from a job, control_table
# and checkpoint_base are passed directly as base_parameters.

process_single_table_py = '''
# ================================================================
# Task 2 (nested): Process a Single Table
# ================================================================
# Called once per source table by the For Each task.
#
# Parameters (via dbutils.widgets):
#   input           — JSON config from For Each {{input}}
#   catalog         — Catalog for control table (default: main)
#   schema          — Schema for control table  (default: tmp)
#   control_table   — FQN override; if set, takes precedence over catalog/schema
#   checkpoint_base — Base path for streaming checkpoints
#
# Read strategies:
#   use_cdf = false (default): Standard incremental readStream.
#   use_cdf = true:            Change Data Feed (inserts, updates, deletes).
#
# Auto-recovery: catches DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE
# and stale-version errors, resets checkpoint, and retries.
#
# Write modes: "append" or "merge" (with CDF: also handles deletes).
# ================================================================

import json
import shutil
from pathlib import Path

# ─── Parse parameters ──────────────────────────────────────────
dbutils.widgets.text("input",           "",                          "For Each Input (JSON)")
dbutils.widgets.text("catalog",         "main",                     "Catalog")
dbutils.widgets.text("schema",          "tmp",                      "Schema")
dbutils.widgets.text("control_table",   "",                          "Control Table (FQN override)")
dbutils.widgets.text("checkpoint_base", "/Volumes/main/tmp/checkpoints", "Checkpoint Base Path")

raw_input       = dbutils.widgets.get("input").strip()
_catalog        = dbutils.widgets.get("catalog").strip()
_schema         = dbutils.widgets.get("schema").strip()
_ctrl_override  = dbutils.widgets.get("control_table").strip()
checkpoint_base = dbutils.widgets.get("checkpoint_base").strip()

# If control_table widget is set, use it; otherwise derive from catalog/schema
ctrl_table = _ctrl_override if _ctrl_override else f"{_catalog}.{_schema}.incremental_filter__sensitive_records"

if raw_input:
    config = json.loads(raw_input)
else:
    raise ValueError("No input provided — this notebook must be called from a For Each task.")

src_fqn     = f"{config['source_catalog']}.{config['source_schema']}.{config['source_table']}"
tgt_fqn     = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
join_key    = config["join_key"]
write_mode  = (config.get("write_mode") or "append").lower().strip()
merge_keys  = [k.strip() for k in (config.get("merge_keys") or "").split(",") if k.strip()]
sequence_by = (config.get("sequence_by") or "").strip() or None
use_cdf     = str(config.get("use_cdf") or "false").lower().strip() in ("true", "1", "yes")

checkpoint_path = f"{checkpoint_base}/{config['source_schema']}_{config['source_table']}"

print(f"🔄 Processing: {src_fqn} → {tgt_fqn}")
print(f"   Join key:       {join_key}")
print(f"   Write mode:     {write_mode}")
print(f"   Use CDF:        {use_cdf}")
if write_mode == "merge":
    print(f"   Merge keys:     {merge_keys}")
    print(f"   Sequence by:    {sequence_by or '(none)'}")
print(f"   Control table:  {ctrl_table}")
print(f"   Checkpoint:     {checkpoint_path}")

# ─── Validate config ──────────────────────────────────────────
if write_mode == "merge" and not merge_keys:
    raise ValueError(
        f"write_mode='merge' requires merge_keys for table {src_fqn}. "
        f"Set merge_keys to comma-separated PK columns (e.g. 'order_id')."
    )

if use_cdf and write_mode == "append":
    print("   ⚠️  CDF + append mode: inserts are appended, updates are appended (postimage only), deletes are IGNORED.")
    print("      Consider using write_mode='merge' with CDF for full change propagation including deletes.")

# ─── Ensure target table has type widening + column mapping ────
def _ensure_target_properties():
    try:
        spark.sql(f"""
            ALTER TABLE {tgt_fqn}
            SET TBLPROPERTIES (
                'delta.enableTypeWidening' = 'true',
                'delta.columnMapping.mode' = 'name'
            )
        """)
    except Exception as e:
        err = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in err or "DELTA_TABLE_NOT_FOUND" in err or "is not a Delta table" in err:
            print(f"   ℹ️  Target {tgt_fqn} does not exist yet — will be created on first write.")
        else:
            raise

_ensure_target_properties()

# ─── Load the control table (fresh every run) ─────────────────
control_df = spark.read.table(ctrl_table).select("sensitive_id")
print(f"   📋 Loaded {control_df.count()} sensitive IDs")

# ─── Helper: check if target table exists ─────────────────────
def _target_exists():
    try:
        spark.sql(f"DESCRIBE TABLE {tgt_fqn}")
        return True
    except Exception:
        return False

# ─── Helper: delete checkpoint directory ──────────────────────
def _reset_checkpoint():
    print(f"   🗑️  Resetting checkpoint: {checkpoint_path}")
    try:
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"   ✅ Checkpoint deleted")
    except Exception as e:
        print(f"   ⚠️  Checkpoint delete warning: {e}")

# ─── Helper: truncate target (for APPEND mode recovery) ───────
def _truncate_target():
    if _target_exists():
        print(f"   🗑️  Truncating target {tgt_fqn} to prevent duplication (append mode recovery)")
        spark.sql(f"TRUNCATE TABLE {tgt_fqn}")

# ================================================================
# MICRO-BATCH PROCESSORS
# ================================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

CDF_META_COLS = {"_change_type", "_commit_version", "_commit_timestamp"}

def _filter_sensitive(df):
    return df.join(control_df, df[join_key] == control_df["sensitive_id"], "left_anti")

def _dedup_by_sequence(df, keys, seq_col):
    window = Window.partitionBy(*keys).orderBy(F.col(seq_col).desc())
    return (df.withColumn("_rank", F.row_number().over(window))
              .filter(F.col("_rank") == 1).drop("_rank"))

def _strip_cdf_columns(df):
    return df.select([c for c in df.columns if c not in CDF_META_COLS])

def _bootstrap_target(df):
    print(f"   🆕 Bootstrapping target {tgt_fqn}")
    (df.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").saveAsTable(tgt_fqn))
    _ensure_target_properties()


def process_batch_standard(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"   Batch {batch_id}: empty — skipping"); return
    filtered = _filter_sensitive(batch_df)
    input_count = batch_df.count(); filtered_count = filtered.count()
    dropped_count = input_count - filtered_count
    if filtered.isEmpty():
        print(f"   Batch {batch_id}: all {input_count} rows filtered as sensitive — skipping"); return

    if write_mode == "append":
        (filtered.write.format("delta").mode("append")
            .option("mergeSchema", "true").saveAsTable(tgt_fqn))
        print(f"   Batch {batch_id} [APPEND]: {filtered_count} written, {dropped_count} filtered"); return

    if sequence_by:
        before = filtered_count
        filtered = _dedup_by_sequence(filtered, merge_keys, sequence_by)
        filtered_count = filtered.count()
        if filtered_count < before:
            print(f"   Batch {batch_id}: deduped {before} → {filtered_count} rows (by {sequence_by})")
    if not _target_exists():
        _bootstrap_target(filtered)
        print(f"   Batch {batch_id} [MERGE-BOOTSTRAP]: created target with {filtered_count} rows"); return

    merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
    (DeltaTable.forName(spark, tgt_fqn).alias("t")
        .merge(filtered.alias("s"), merge_condition)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll()
        .withSchemaEvolution().execute())
    print(f"   Batch {batch_id} [MERGE]: {filtered_count} upserted, {dropped_count} filtered | keys: {merge_keys}")


def process_batch_cdf(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"   Batch {batch_id}: empty — skipping"); return
    inserts = batch_df.filter(F.col("_change_type") == "insert")
    updates = batch_df.filter(F.col("_change_type") == "update_postimage")
    deletes = batch_df.filter(F.col("_change_type") == "delete")
    ins_count = inserts.count(); upd_count = updates.count(); del_count = deletes.count()
    print(f"   Batch {batch_id} CDF: {ins_count} inserts, {upd_count} updates, {del_count} deletes")

    upsert_df = inserts.unionByName(updates, allowMissingColumns=True)
    upsert_df = _filter_sensitive(upsert_df)
    upsert_df = _strip_cdf_columns(upsert_df)
    delete_df = _strip_cdf_columns(deletes) if del_count > 0 else None
    upsert_count = upsert_df.count()
    sens_count = (ins_count + upd_count) - upsert_count

    if write_mode == "append":
        if not upsert_df.isEmpty():
            (upsert_df.write.format("delta").mode("append")
                .option("mergeSchema", "true").saveAsTable(tgt_fqn))
        if del_count > 0:
            print(f"   ⚠️  {del_count} deletes IGNORED in append mode")
        print(f"   Batch {batch_id} [CDF-APPEND]: {upsert_count} written, {sens_count} filtered"); return

    if sequence_by and not upsert_df.isEmpty():
        before = upsert_count
        upsert_df = _dedup_by_sequence(upsert_df, merge_keys, sequence_by)
        upsert_count = upsert_df.count()
        if upsert_count < before:
            print(f"   Batch {batch_id}: deduped upserts {before} → {upsert_count} rows")
    if not _target_exists():
        if not upsert_df.isEmpty():
            _bootstrap_target(upsert_df)
            print(f"   Batch {batch_id} [CDF-MERGE-BOOTSTRAP]: created target with {upsert_count} rows")
        return

    merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
    if not upsert_df.isEmpty():
        (DeltaTable.forName(spark, tgt_fqn).alias("t")
            .merge(upsert_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll()
            .withSchemaEvolution().execute())
    if delete_df is not None and not delete_df.isEmpty():
        delete_cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
        (DeltaTable.forName(spark, tgt_fqn).alias("t")
            .merge(delete_df.alias("s"), delete_cond)
            .whenMatchedDelete().execute())
    print(f"   Batch {batch_id} [CDF-MERGE]: {upsert_count} upserted, {del_count} deleted, {sens_count} filtered")


# ================================================================
# STREAM EXECUTION WITH AUTO-RECOVERY
# ================================================================

def _build_read_stream():
    reader = spark.readStream
    if use_cdf:
        reader = reader.option("readChangeFeed", "true")
        print(f"   📡 Reading Change Data Feed from {src_fqn}")
    else:
        reader = (reader
            .option("schemaTrackingLocation", checkpoint_path)
            .option("allowSourceColumnTypeChange", "always"))
    return reader.table(src_fqn)

def _run_stream():
    processor = process_batch_cdf if use_cdf else process_batch_standard
    mode_label = f"{'CDF-' if use_cdf else ''}{write_mode.upper()}"
    print(f"\\n🚀 Starting incremental stream ({mode_label} mode)...\\n")
    (_build_read_stream().writeStream.foreachBatch(processor)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True).start().awaitTermination())
    print(f"\\n✅ Completed ({mode_label}): {src_fqn} → {tgt_fqn}")

try:
    _run_stream()
except Exception as e:
    error_msg = str(e)
    if "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE" in error_msg:
        print(f"\\n🔄 RECOVERY: Source table {src_fqn} was recreated (new table ID). Resetting checkpoint...")
        _reset_checkpoint()
        if write_mode == "append": _truncate_target()
        _run_stream()
    elif any(msg in error_msg for msg in [
        "is before the earliest version", "Cannot find version",
        "is after the latest version", "DELTA_VERSIONS_NOT_CONTIGUOUS"]):
        print(f"\\n🔄 RECOVERY: Checkpoint references unavailable source version. Resetting...")
        _reset_checkpoint()
        if write_mode == "append": _truncate_target()
        _run_stream()
    else:
        raise
'''.strip()

w.workspace.import_(
    path=f"{SRC_DIR}/process_single_table.py",
    content=base64.b64encode(process_single_table_py.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ src/process_single_table.py")


# COMMAND ----------


# ─── File 5: pyproject.toml ───────────────────────────────────
pyproject_toml = '''
[project]
name = "incremental_filter_bundle"
version = "0.1.0"
description = "Config-driven incremental filter pipeline with For Each jobs per pipeline_batch"
requires-python = ">=3.10"
dependencies = [
    "databricks-bundles>=0.275.0",
    "databricks-sdk>=0.40.0",
]

[build-system]
requires = ["setuptools>=75.0"]
build-backend = "setuptools.build_meta"
'''.strip()

w.workspace.import_(
    path=f"{PROJECT_ROOT}/pyproject.toml",
    content=base64.b64encode(pyproject_toml.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ pyproject.toml")


# COMMAND ----------


# ─── File 6: README.md ────────────────────────────────────────
readme_md = '''
# Incremental Filter Pipeline — PyDABs Bundle

Config-driven pipeline that incrementally reads from Delta source tables,
filters out sensitive records via a control table, and writes to target tables
using **append** or **merge (upsert)** mode. Supports **Change Data Feed (CDF)**
for full change propagation including deletes and source restatements.

All notebooks accept **catalog** and **schema** as `dbutils.widgets` parameters,
so you can point the entire pipeline at a different environment by changing two values.

## Variables (per target)

| Variable | Description | Dev Default | Prod Default |
|---|---|---|---|
| `catalog` | Catalog for config and control tables | `main` | `my_catalog` |
| `schema` | Schema for config and control tables | `tmp` | `pipeline_meta` |
| `checkpoint_base` | Base path for streaming checkpoints | `/Volumes/main/tmp/checkpoints` | `/Volumes/my_catalog/pipeline_meta/checkpoints` |
| `concurrency` | Max parallel For Each iterations | 10 | 50 |
| `node_type_id` | Instance type for clusters | `i3.xlarge` | `i3.2xlarge` |
| `num_workers` | Workers per processing cluster | 2 | 4 |

Config and control table FQNs are derived automatically:
- `{catalog}.{schema}.incremental_filter__table_config`
- `{catalog}.{schema}.incremental_filter__sensitive_records`

## Architecture

```
Bundle Variables: catalog, schema, checkpoint_base
  │
  ├─ resources/__init__.py reads config table at deploy time
  │   → Generates one Job per distinct pipeline_batch
  │
  ├─ Job: incremental_filter_batch_1
  │    ├─ Task 1: generate_table_list
  │    │    Widgets: catalog, schema, config_table, pipeline_batch
  │    └─ Task 2: For Each (concurrency=N)
  │         └─ process_single_table
  │              Widgets: input, catalog, schema, control_table, checkpoint_base
  │
  └─ ... one job per batch
```

## Project Structure

```
incremental_filter_bundle/
├── databricks.yml          # Bundle config with catalog/schema variables
├── pyproject.toml          # Python dependencies
├── resources/
│   └── __init__.py         # PyDABs load_resources() — generates jobs dynamically
└── src/
    ├── generate_table_list.py     # Task 1: read config, emit table list
    └── process_single_table.py    # Task 2: read → filter → write (append/merge/CDF)
```

## Quick Start

```bash
uv sync
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run batch_1_job
```

## Widget Parameters

### generate_table_list.py

| Widget | Default | Job Override | Description |
|---|---|---|---|
| `catalog` | `main` | Bundle variable | Catalog for config table |
| `schema` | `tmp` | Bundle variable | Schema for config table |
| `config_table` | _(empty)_ | Derived from catalog/schema | FQN override (takes precedence) |
| `pipeline_batch` | `1` | Set per job | Which batch to process |

### process_single_table.py

| Widget | Default | Job Override | Description |
|---|---|---|---|
| `input` | _(empty)_ | `{{input}}` from For Each | JSON config for this table |
| `catalog` | `main` | Bundle variable | Catalog for control table |
| `schema` | `tmp` | Bundle variable | Schema for control table |
| `control_table` | _(empty)_ | Derived from catalog/schema | FQN override (takes precedence) |
| `checkpoint_base` | `/Volumes/main/tmp/checkpoints` | Bundle variable | Checkpoint directory |

### Interactive Use

When running notebooks interactively (outside a job), just set the `catalog` and `schema`
widgets at the top of the notebook — everything else is derived automatically:

```python
# In the Databricks UI, change the widget values:
#   catalog = "my_catalog"
#   schema  = "my_schema"
# Then run the notebook — it uses {catalog}.{schema}.incremental_filter__* tables.
```

## Write Modes

| Mode | Mechanism | CDF Deletes | Best For |
|---|---|---|---|
| `append` | `saveAsTable` + `mergeSchema` | Ignored | Event logs, append-only |
| `merge` | `DeltaTable.merge()` + `withSchemaEvolution()` | ✅ Propagated | Dimension tables, upserts |

## Restatement Handling

| Error | Recovery |
|---|---|
| `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE` | Checkpoint reset + retry |
| Stale version / vacuumed history | Checkpoint reset + retry |
| APPEND recovery | Target truncated first to prevent duplication |
| MERGE recovery | Target preserved — upsert is idempotent |

## Config Table Columns

| Column | Type | Required | Description |
|---|---|---|---|
| `source_catalog` | STRING | Yes | Source catalog |
| `source_schema` | STRING | Yes | Source schema |
| `source_table` | STRING | Yes | Source table |
| `target_catalog` | STRING | Yes | Target catalog |
| `target_schema` | STRING | Yes | Target schema |
| `target_table` | STRING | Yes | Target table |
| `join_key` | STRING | Yes | Column for sensitive record filtering |
| `pipeline_batch` | INT | Yes | Batch group |
| `is_active` | BOOLEAN | Yes | Active flag |
| `write_mode` | STRING | No | `append` (default) or `merge` |
| `merge_keys` | STRING | No | Comma-separated PK columns for merge |
| `sequence_by` | STRING | No | Ordering column for dedup |
| `use_cdf` | STRING | No | `true` to read Change Data Feed |
'''.strip()

w.workspace.import_(
    path=f"{PROJECT_ROOT}/README.md",
    content=base64.b64encode(readme_md.encode()).decode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print("✅ README.md")


# COMMAND ----------


# ─── Verify the complete project structure ─────────────────────
print("=" * 60)
print("BUNDLE PROJECT STRUCTURE")
print("=" * 60)

objects = list(w.workspace.list(path=PROJECT_ROOT, recursive=True))
for obj in sorted(objects, key=lambda o: o.path):
    # Indent based on directory depth relative to project root
    rel = obj.path.replace(PROJECT_ROOT, "")
    depth = rel.count("/") - 1
    name = rel.split("/")[-1]
    prefix = "  " * depth + ("📁 " if str(obj.object_type) == "ObjectType.DIRECTORY" else "📄 ")
    print(f"{prefix}{name}")

print()
print("=" * 60)
print("DEPLOYMENT COMMANDS")
print("=" * 60)
print("""
1. Copy this bundle project to your local machine or Git repo:
   databricks workspace export-dir \\
     "{project_root}" \\
     ./incremental_filter_bundle --overwrite

2. Set up the virtual environment:
   cd incremental_filter_bundle
   uv sync

3. Validate the bundle:
   databricks bundle validate --target dev

4. Deploy (creates one job per pipeline_batch):
   databricks bundle deploy --target dev

5. Run a specific batch:
   databricks bundle run batch_1_job

6. Deploy to production:
   databricks bundle deploy --target prod
""".format(project_root=PROJECT_ROOT))

print("=" * 60)
print("HOW IT SCALES")
print("=" * 60)
print("""
• Each pipeline_batch gets its own Lakeflow Job
• Each job runs a For Each loop over that batch's tables
• Adding a new batch = INSERT rows + `bundle deploy`
• No code changes, no manual job creation

Example: 5,000 tables across 10 batches (500 tables each)
  → 10 jobs, each with concurrency=50
  → 50 tables processed in parallel per job
  → All 10 jobs can run on the same schedule
""")


# COMMAND ----------


# ─── Add use_cdf column to the config table if it doesn't exist ─
# This is safe to run multiple times — ALTER TABLE ADD COLUMNS is idempotent
config_table = "main.tmp.incremental_filter__table_config"

try:
    # Check if column already exists
    cols = [c.name for c in spark.table(config_table).schema.fields]
    if "use_cdf" not in cols:
        spark.sql(f"ALTER TABLE {config_table} ADD COLUMNS (use_cdf STRING)")
        print(f"✅ Added 'use_cdf' column to {config_table}")
    else:
        print(f"ℹ️  'use_cdf' column already exists in {config_table}")
    
    # Show current config table schema
    print(f"\nCurrent schema for {config_table}:")
    for c in spark.table(config_table).schema.fields:
        print(f"   {c.name:25s} {str(c.dataType):15s} {'(nullable)' if c.nullable else ''}")
except Exception as e:
    print(f"⚠️  Could not update config table: {e}")
