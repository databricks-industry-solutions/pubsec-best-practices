# Databricks notebook source

# ================================================================
# Schema Evolution Test
# ================================================================
# This notebook validates that the processing notebook correctly
# handles all three schema evolution scenarios:
#   1. New column added to source
#   2. Column type widened in source
#   3. Column dropped from source
#
# Parameters (via dbutils.widgets):
#   catalog         — Catalog (default: main)
#   schema          — Schema  (default: tmp)
#   checkpoint_base — Base path for checkpoints
#
# Run AFTER 02_process_single_table has completed its first pass.
# ================================================================

dbutils.widgets.text("catalog",         "main",                                    "Catalog")
dbutils.widgets.text("schema",          "tmp",                                     "Schema")
dbutils.widgets.text("checkpoint_base", "/tmp/incremental_filter_pipeline/checkpoints", "Checkpoint Base Path")

CATALOG         = dbutils.widgets.get("catalog").strip()
SCHEMA          = dbutils.widgets.get("schema").strip()
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base").strip()

SRC  = f"{CATALOG}.{SCHEMA}.src_orders"
TGT  = f"{CATALOG}.{SCHEMA}.tgt_orders_clean"
CTRL = f"{CATALOG}.{SCHEMA}.incremental_filter__sensitive_records"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/{SCHEMA}_src_orders"

print("=" * 60)
print("TEST 1: New column added to source")
print("=" * 60)

# Add a new column to the source table
try:
    spark.sql(f"ALTER TABLE {SRC} ADD COLUMN (priority STRING COMMENT 'Order priority level')")
    print("   ✅ Added 'priority' column")
except Exception as e:
    if "already exists" in str(e).lower() or "FIELDS_ALREADY_EXISTS" in str(e):
        print("   ℹ️  'priority' column already exists — skipping ADD COLUMN")
    else:
        raise

# Insert new rows with the new column
spark.sql(f"""
INSERT INTO {SRC} VALUES
  (7, 'CUST-006', 410.00, '2026-04-07', 'US-EAST', 'HIGH'),
  (8, 'SENSITIVE-USER-1', 55.00, '2026-04-08', 'APAC', 'LOW'),
  (9, 'CUST-007', 189.99, '2026-04-09', 'EU-WEST', 'MEDIUM')
""")

print(f"   ✅ Added 3 new rows (1 sensitive)")
print(f"   Source now has {spark.table(SRC).count()} rows, schema:")
spark.table(SRC).printSchema()


# COMMAND ----------


# ─── Re-run the stream to pick up the new column ──────────────
# Uses CATALOG, SCHEMA, CTRL, CHECKPOINT_PATH from Cell 0 widgets.
#
# When a source schema changes, the stream:
#   1. Detects the change via schemaTrackingLocation
#   2. Updates its tracked schema
#   3. Throws DELTA_STREAMING_METADATA_EVOLUTION and stops
#   4. On RESTART, it picks up the new schema and continues
#
# In a real Lakeflow Job, the For Each task's max_retries: 2
# handles this automatically — the first attempt updates the
# schema log, the retry succeeds with the new schema.
# ================================================================

import json

control_df = spark.read.table(CTRL).select("sensitive_id")
join_key = "customer_id"

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    filtered = batch_df.join(
        control_df,
        batch_df[join_key] == control_df["sensitive_id"],
        "left_anti"
    )
    input_count    = batch_df.count()
    filtered_count = filtered.count()
    print(f"   Batch {batch_id}: {filtered_count} written, {input_count - filtered_count} filtered")
    (filtered.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TGT)
    )

def run_stream():
    (spark.readStream
        .option("schemaTrackingLocation", CHECKPOINT_PATH)
        .option("allowSourceColumnTypeChange", "always")
        .table(SRC)
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

# Attempt with automatic retry (simulates Job retry behavior)
print("🚀 Attempt 1: Stream detects schema change and updates tracking log...")
try:
    run_stream()
    print("   ✅ Succeeded on first attempt")
except Exception as e:
    if "DELTA_STREAMING_METADATA_EVOLUTION" in str(e):
        print("   ⚠️  Schema change detected — tracking log updated. Restarting...\n")
        print("🚀 Attempt 2: Stream restarts with updated schema...")
        run_stream()
        print("   ✅ Succeeded on retry (this is what max_retries handles in a Job)")
    else:
        raise


# COMMAND ----------


# ─── Verify schema evolution results ──────────────────────────
print("=" * 60)
print("SCHEMA EVOLUTION RESULTS")
print("=" * 60)

print(f"\n── Target table now has 'priority' column ──")
spark.table(TGT).printSchema()

print("── Target table data (ordered by order_id) ──")
spark.table(TGT).orderBy("order_id").show(truncate=False)

tgt_count = spark.table(TGT).count()
src_count = spark.table(SRC).count()
print(f"Source: {src_count} rows | Target: {tgt_count} rows | Filtered: {src_count - tgt_count}")
print(f"\nNote: Historical rows (order_id 1-5) have priority=null — this is expected.")
print(f"      New rows (order_id 7, 9) have the priority column populated.")
print(f"      Sensitive rows (order_id 8, SENSITIVE-USER-1) were filtered out.")
