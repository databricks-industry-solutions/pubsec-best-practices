# Databricks notebook source
# ================================================================
# Task 2 (nested): Process a Single Table — APPEND or MERGE
# ================================================================
# Called once per source table by the For Each task.
# Receives table config as a JSON string via the {{input}} widget.
#
# Parameters (via dbutils.widgets):
#   input          — JSON config from For Each {{input}}
#   catalog        — Catalog for control/config tables (default: main)
#   schema         — Schema for control/config tables  (default: tmp)
#   checkpoint_base — Base path for streaming checkpoints
#
# When running interactively (no For Each input), uses the catalog
# and schema widgets to locate the control table and builds a
# default test config from those values.
#
# Write modes (driven by config table):
#   "append" (default) — append-only inserts via saveAsTable
#   "merge"            — upsert via DeltaTable.merge() with schema evolution
#
# Schema evolution support (both modes):
#   - New columns:     mergeSchema (append) / withSchemaEvolution (merge)
#   - Type widening:   enableTypeWidening on target
#   - Dropped columns: NULLs in target (source needs column mapping)
#   - Incompatible:    Requires checkpoint reset
# ================================================================

import json

# --- Parameters ------------------------------------------------
dbutils.widgets.text("input",           "",                                        "For Each Input (JSON)")
dbutils.widgets.text("catalog",         "main",                                    "Catalog")
dbutils.widgets.text("schema",          "tmp",                                     "Schema")
dbutils.widgets.text("checkpoint_base", "/tmp/incremental_filter_pipeline/checkpoints", "Checkpoint Base Path")

raw_input       = dbutils.widgets.get("input").strip()
CATALOG         = dbutils.widgets.get("catalog").strip()
SCHEMA          = dbutils.widgets.get("schema").strip()
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base").strip()

# Control table FQN derived from catalog/schema
CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.incremental_filter__sensitive_records"

# --- Parse For Each input --------------------------------------
if raw_input:
    config = json.loads(raw_input)
else:
    # Default for interactive testing — uses widget values
    print("No For Each input detected — using default test config\n")
    config = {
        "source_catalog": CATALOG,
        "source_schema":  SCHEMA,
        "source_table":   "src_orders",
        "target_catalog": CATALOG,
        "target_schema":  SCHEMA,
        "target_table":   "tgt_orders_clean",
        "join_key":       "customer_id",
        "write_mode":     "merge",
        "merge_keys":     "order_id",
        "sequence_by":    "order_date",
        "use_cdf":        "",
    }

src_fqn     = f"{config['source_catalog']}.{config['source_schema']}.{config['source_table']}"
tgt_fqn     = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"
join_key    = config["join_key"]
write_mode  = (config.get("write_mode") or "append").lower().strip()
merge_keys  = [k.strip() for k in (config.get("merge_keys") or "").split(",") if k.strip()]
sequence_by = (config.get("sequence_by") or "").strip() or None

# Checkpoint path: unique per source table
checkpoint_path = f"{CHECKPOINT_BASE}/{config['source_schema']}_{config['source_table']}"

print(f"Processing: {src_fqn} -> {tgt_fqn}")
print(f"   Join key:       {join_key}")
print(f"   Write mode:     {write_mode}")
if write_mode == "merge":
    print(f"   Merge keys:     {merge_keys}")
    print(f"   Sequence by:    {sequence_by or '(none — no dedup)'}")
print(f"   Control table:  {CONTROL_TABLE}")
print(f"   Checkpoint:     {checkpoint_path}")

# Validate merge config
if write_mode == "merge" and not merge_keys:
    raise ValueError(
        f"write_mode='merge' requires merge_keys in config for table {src_fqn}. "
        f"Set merge_keys to comma-separated PK columns (e.g. 'order_id' or 'id,region')."
    )

# COMMAND ----------

# --- Ensure target table has type widening + column mapping ----
# This is idempotent — safe to run every time.
# If the target doesn't exist yet, it will be auto-created on first write.
try:
    spark.sql(f"""
        ALTER TABLE {tgt_fqn}
        SET TBLPROPERTIES (
            'delta.enableTypeWidening' = 'true',
            'delta.columnMapping.mode' = 'name'
        )
    """)
    print(f"   Target table properties verified: type widening + column mapping")
except Exception as e:
    err = str(e)
    if "TABLE_OR_VIEW_NOT_FOUND" in err or "DELTA_TABLE_NOT_FOUND" in err or "is not a Delta table" in err:
        print(f"   Target table {tgt_fqn} does not exist yet — will be created on first write.")
    else:
        raise

# COMMAND ----------

# --- Load the sensitive-records control table ------------------
# Read fresh on every run — unlike Pattern 1's stream-snapshot join,
# this always reflects the LATEST exclusion list. Any newly-added
# sensitive IDs take effect immediately.
control_df = (
    spark.read.table(CONTROL_TABLE)
    .select("sensitive_id")
)
sensitive_count = control_df.count()
print(f"   Loaded {sensitive_count} sensitive IDs from control table")

# COMMAND ----------

# --- Define the micro-batch processor --------------------------
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Window

def _target_exists():
    """Check if the target Delta table already exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {tgt_fqn}")
        return True
    except Exception:
        return False

def process_batch(batch_df, batch_id):
    """
    Process each micro-batch with sensitive-record filtering,
    then write using APPEND or MERGE depending on config.

    APPEND mode:
      - Simple saveAsTable with mergeSchema
      - Fast, no key required beyond join_key for filtering

    MERGE mode:
      - DeltaTable.merge() with whenMatchedUpdateAll + whenNotMatchedInsertAll
      - .withSchemaEvolution() auto-adds new source columns to target
      - Optional dedup via sequence_by (keeps latest record per merge key)
      - First batch to a non-existent target falls back to saveAsTable (bootstrap)
    """
    if batch_df.isEmpty():
        print(f"   Batch {batch_id}: empty — skipping")
        return

    # Step 1: Filter out sensitive records (both modes)
    filtered = batch_df.join(
        control_df,
        batch_df[join_key] == control_df["sensitive_id"],
        "left_anti"
    )

    input_count    = batch_df.count()
    filtered_count = filtered.count()
    dropped_count  = input_count - filtered_count

    if filtered.isEmpty():
        print(f"   Batch {batch_id}: all {input_count} rows filtered as sensitive — skipping write")
        return

    # Step 2a: APPEND mode
    if write_mode == "append":
        (filtered.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(tgt_fqn)
        )
        print(f"   Batch {batch_id} [APPEND]: {filtered_count} written, {dropped_count} filtered")
        return

    # Step 2b: MERGE mode

    # Optional: deduplicate within the batch by sequence_by (keep latest)
    if sequence_by:
        window = Window.partitionBy(*merge_keys).orderBy(F.col(sequence_by).desc())
        filtered = (filtered
            .withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
        dedup_count = filtered.count()
        if dedup_count < filtered_count:
            print(f"   Batch {batch_id}: deduped {filtered_count} -> {dedup_count} rows (by {sequence_by})")
            filtered_count = dedup_count

    # Bootstrap: if target doesn't exist yet, create it with first batch
    if not _target_exists():
        print(f"   Batch {batch_id} [MERGE-BOOTSTRAP]: creating target with {filtered_count} rows")
        (filtered.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(tgt_fqn)
        )
        # Set type widening + column mapping on the newly created table
        spark.sql(f"""
            ALTER TABLE {tgt_fqn}
            SET TBLPROPERTIES (
                'delta.enableTypeWidening' = 'true',
                'delta.columnMapping.mode' = 'name'
            )
        """)
        return

    # Build the merge condition: t.key1 = s.key1 AND t.key2 = s.key2
    merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])

    # Execute MERGE with schema evolution
    (DeltaTable.forName(spark, tgt_fqn)
        .alias("t")
        .merge(filtered.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .withSchemaEvolution()
        .execute()
    )

    print(f"   Batch {batch_id} [MERGE]: {filtered_count} upserted, {dropped_count} filtered | keys: {merge_keys}")

print(f"   Batch processor defined (mode={write_mode})")

# COMMAND ----------

# --- Run the incremental stream --------------------------------
# Structured Streaming with trigger(availableNow=True):
#   - Reads ALL new data since last checkpoint in a single trigger
#   - Processes it through foreachBatch -> process_batch()
#   - Then stops automatically -- perfect for scheduled batch jobs
#
# Schema tracking:
#   - schemaTrackingLocation watches for upstream DDL changes
#   - allowSourceColumnTypeChange tolerates widened types (e.g. INT->BIGINT)
#   - If an incompatible change is detected, the stream will fail and
#     require a checkpoint reset (handled operationally)

schema_tracking = f"{checkpoint_path}/_schema_tracking"

print(f"   Starting incremental stream ...")
print(f"      Source:           {src_fqn}")
print(f"      Checkpoint:       {checkpoint_path}")
print(f"      Schema tracking:  {schema_tracking}")

query = (
    spark.readStream
        .option("schemaTrackingLocation", schema_tracking)
        .option("allowSourceColumnTypeChange", "true")
        .table(src_fqn)
    .writeStream
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .foreachBatch(process_batch)
        .start()
)

query.awaitTermination()
print(f"   Stream completed successfully")

# COMMAND ----------

# --- Verify results --------------------------------------------
print("=" * 60)
print("VERIFICATION")
print("=" * 60)

src_count = spark.table(src_fqn).count()
tgt_count = spark.table(tgt_fqn).count()
print(f"\n   Source ({src_fqn}):  {src_count} rows")
print(f"   Target ({tgt_fqn}): {tgt_count} rows")
print(f"   Filtered out: {src_count - tgt_count} sensitive rows\n")

print("-- Source table --")
spark.table(src_fqn).show(truncate=False)

print("-- Target table (sensitive records removed) --")
spark.table(tgt_fqn).show(truncate=False)

print("-- Sensitive IDs that were excluded --")
spark.table(CONTROL_TABLE).show(truncate=False)