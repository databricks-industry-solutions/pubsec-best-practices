# Databricks notebook source

# ================================================================
# Task 1: Generate Table List
# ================================================================
# This notebook is the FIRST task in the Lakeflow Job.
# It reads the config table, builds the list of active tables to
# process, and emits it as a Databricks task value that the
# downstream For Each task iterates over.
#
# Parameters (via dbutils.widgets):
#   catalog — Catalog where config table lives (default: main)
#   schema  — Schema where config table lives  (default: tmp)
#
# In a real job, the For Each task references this output as:
#   {{tasks.generate_table_list.values.table_configs}}
#
# When running interactively (outside a job), the task value set
# will be a no-op, but the output is printed for inspection.
# ================================================================

# ─── Parameters ────────────────────────────────────────────────
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema",  "tmp",  "Schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

CONFIG_TABLE = f"{CATALOG}.{SCHEMA}.incremental_filter__table_config"

# ─── Read active table configs ─────────────────────────────────
config_df = (
    spark.read.table(CONFIG_TABLE)
    .filter("is_active = true")
)

# Convert to a list of dicts — each dict becomes one For Each iteration
table_list = [row.asDict() for row in config_df.collect()]

# Clean up for JSON serialization
for t in table_list:
    t["is_active"] = bool(t["is_active"])
    for optional_key in ("write_mode", "merge_keys", "sequence_by", "use_cdf"):
        if t.get(optional_key) is None:
            t[optional_key] = ""

# ─── Emit as a task value ──────────────────────────────────────
# Max 48 KB via task value references — sufficient for ~5,000 tables
# with compact config dicts.
# For Each reads this as: {{tasks.generate_table_list.values.table_configs}}
try:
    dbutils.jobs.taskValues.set(key="table_configs", value=table_list)
except Exception:
    # Running interactively (not in a job) — task values are not available
    pass

# ─── Print summary ─────────────────────────────────────────────
print(f"✅ Emitted {len(table_list)} table configs from {CONFIG_TABLE}\n")
for i, t in enumerate(table_list[:10]):
    src = f"{t['source_catalog']}.{t['source_schema']}.{t['source_table']}"
    tgt = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
    mode = (t.get("write_mode") or "append").lower()
    cdf  = " CDF" if str(t.get("use_cdf") or "").lower() in ("true", "1", "yes") else ""
    print(f"   [{i+1}] {src} → {tgt}  (key: {t['join_key']}, mode: {mode}{cdf})")
if len(table_list) > 10:
    print(f"   ... and {len(table_list) - 10} more")
