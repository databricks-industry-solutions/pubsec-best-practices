# Databricks notebook source

# ================================================================
# CLEANUP — Drop test tables and checkpoints
# ================================================================
# Run this notebook to remove all test artifacts created by the
# scaffolding notebooks. This does NOT affect your production
# config tables — only the sample source/target data.
#
# Parameters (via dbutils.widgets):
#   catalog         — Catalog to clean up (default: main)
#   schema          — Schema to clean up  (default: tmp)
#   checkpoint_base — Base path for streaming checkpoints
#
# ⚠️  Uncomment the lines below to actually drop the tables.
# ================================================================

dbutils.widgets.text("catalog",         "main",                                    "Catalog")
dbutils.widgets.text("schema",          "tmp",                                     "Schema")
dbutils.widgets.text("checkpoint_base", "/tmp/incremental_filter_pipeline/checkpoints", "Checkpoint Base Path")

CATALOG         = dbutils.widgets.get("catalog").strip()
SCHEMA          = dbutils.widgets.get("schema").strip()
CHECKPOINT_BASE = dbutils.widgets.get("checkpoint_base").strip()

tables_to_drop = [
    f"{CATALOG}.{SCHEMA}.src_orders",
    f"{CATALOG}.{SCHEMA}.src_transactions",
    f"{CATALOG}.{SCHEMA}.tgt_orders_clean",
    f"{CATALOG}.{SCHEMA}.tgt_transactions_clean",
    f"{CATALOG}.{SCHEMA}.incremental_filter__table_config",
    f"{CATALOG}.{SCHEMA}.incremental_filter__sensitive_records",
]

print(f"Tables that would be dropped in {CATALOG}.{SCHEMA} (uncomment to execute):\n")
for t in tables_to_drop:
    print(f"   DROP TABLE IF EXISTS {t};")
    # Uncomment the line below to actually drop:
    # spark.sql(f"DROP TABLE IF EXISTS {t}")

print(f"\nCheckpoint directory to remove:")
print(f"   dbutils.fs.rm('{CHECKPOINT_BASE}', recurse=True)")
# Uncomment to actually remove:
# dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)

print("\n⚠️  Nothing was dropped — uncomment the lines above to clean up.")
