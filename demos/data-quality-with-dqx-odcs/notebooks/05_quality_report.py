# Databricks notebook source
# MAGIC %md
# MAGIC # Task 5: Data Quality Report
# MAGIC
# MAGIC Reads `dq_results` from the DQX checks task and produces a comprehensive
# MAGIC quality report — overall scores, per-rule breakdowns, sample bad rows, and
# MAGIC an SLA compliance check against the contract's 95% threshold.

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main",    "Catalog")
dbutils.widgets.text("schema",  "dq_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Load Results

# COMMAND ----------

from pyspark.sql import functions as F

dq_results   = spark.table(f"{catalog}.{schema}.dq_results")
quarantine   = spark.table(f"{catalog}.{schema}.dq_quarantine")
valid        = spark.table(f"{catalog}.{schema}.curated_orders_valid")
curated      = spark.table(f"{catalog}.{schema}.curated_orders")

total_rows      = curated.count()
valid_rows      = valid.count()
quarantine_rows = quarantine.count()
overall_pass_rate = round(valid_rows / total_rows * 100, 2) if total_rows > 0 else 0

print("=" * 70)
print("  DATA QUALITY REPORT — Curated Orders")
print("=" * 70)
print(f"  Contract       : Curated Orders Data Contract v1.0.0")
print(f"  Table          : {catalog}.{schema}.curated_orders")
print(f"  Total rows     : {total_rows:>8,}")
print(f"  Valid rows     : {valid_rows:>8,}  ({overall_pass_rate:.1f}%)")
print(f"  Quarantined    : {quarantine_rows:>8,}  ({100-overall_pass_rate:.1f}%)")
print("=" * 70)

# COMMAND ----------

# MAGIC %md ## SLA Compliance Check
# MAGIC
# MAGIC Contract SLA: **≥ 95%** of rows must pass all error-level checks.

# COMMAND ----------

SLA_THRESHOLD = 95.0
sla_status    = "✅ PASS" if overall_pass_rate >= SLA_THRESHOLD else "❌ FAIL"

print(f"SLA Threshold   : {SLA_THRESHOLD}%")
print(f"Actual Pass Rate: {overall_pass_rate}%")
print(f"SLA Status      : {sla_status}")

# COMMAND ----------

# MAGIC %md ## Check-Level Results

# COMMAND ----------

display(
    dq_results
    .select("check_name", "criticality", "failing_rows", "total_rows", "pass_rate", "status", "checked_at")
    .orderBy("criticality", F.col("failing_rows").desc())
)

# COMMAND ----------

# MAGIC %md ## Results Summary — Errors vs Warnings

# COMMAND ----------

display(
    dq_results
    .groupBy("criticality", "status")
    .agg(F.count("*").alias("check_count"))
    .orderBy("criticality", "status")
)

# COMMAND ----------

# MAGIC %md ## Violations by Rule (Top 10 Worst)

# COMMAND ----------

display(
    dq_results
    .filter(F.col("status") == "FAIL")
    .orderBy(F.col("failing_rows").desc())
    .limit(10)
    .select(
        "check_name",
        "criticality",
        "failing_rows",
        F.round(F.col("pass_rate") * 100, 1).alias("pass_rate_pct"),
    )
)

# COMMAND ----------

# MAGIC %md ## Sample Quarantined Rows

# COMMAND ----------

# Show quarantined rows with the DQX error messages
error_cols = [c for c in quarantine.columns if c.startswith("_error_") or c.startswith("_warn_")]

display(
    quarantine
    .select(
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "unit_price", "total_amount", "order_status", "region",
        *error_cols,
    )
    .limit(25)
)

# COMMAND ----------

# MAGIC %md ## Violations by Region

# COMMAND ----------

from pyspark.sql.types import BooleanType

# Flag rows that have any error-level violation
error_flag = F.lit(False)
for ec in [c for c in quarantine.columns if c.startswith("_error_")]:
    error_flag = error_flag | (F.col(ec).isNotNull() & (F.col(ec) != ""))

region_quality = (
    curated
    .join(
        quarantine.select("order_id", *[c for c in quarantine.columns if c.startswith("_error_")]),
        on="order_id", how="left"
    )
    .withColumn("has_error", error_flag)
    .groupBy("region")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("has_error").cast("long")).alias("errors"),
    )
    .withColumn("quality_score_pct", F.round((1 - F.col("errors") / F.col("total")) * 100, 1))
    .orderBy("region")
)

display(region_quality)

# COMMAND ----------

# MAGIC %md ## Violations by Order Status

# COMMAND ----------

status_quality = (
    curated
    .join(
        quarantine.select("order_id", *[c for c in quarantine.columns if c.startswith("_error_")]),
        on="order_id", how="left"
    )
    .withColumn("has_error", error_flag)
    .groupBy("order_status")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("has_error").cast("long")).alias("errors"),
    )
    .withColumn("quality_score_pct", F.round((1 - F.col("errors") / F.col("total")) * 100, 1))
    .orderBy(F.col("errors").desc())
)

display(status_quality)

# COMMAND ----------

# MAGIC %md ## Write Quality Score Summary Table

# COMMAND ----------

import datetime
from pyspark.sql import Row

summary_row = Row(
    run_date          = str(datetime.date.today()),
    catalog           = catalog,
    schema            = schema,
    table_name        = "curated_orders",
    total_rows        = total_rows,
    valid_rows        = valid_rows,
    quarantined_rows  = quarantine_rows,
    overall_pass_rate = overall_pass_rate,
    sla_threshold     = SLA_THRESHOLD,
    sla_met           = overall_pass_rate >= SLA_THRESHOLD,
    total_checks      = dq_results.count(),
    failing_checks    = dq_results.filter(F.col("status") == "FAIL").count(),
)

summary_df = spark.createDataFrame([summary_row])
summary_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.dq_summary")

print(f"Quality summary written to {catalog}.{schema}.dq_summary")

# COMMAND ----------

# MAGIC %md ## Final Scorecard

# COMMAND ----------

failing_checks = dq_results.filter(F.col("status") == "FAIL").count()
total_checks   = dq_results.count()

print()
print("╔══════════════════════════════════════════════════════════════════════╗")
print("║              DATA QUALITY SCORECARD — Curated Orders                ║")
print("╠══════════════════════════════════════════════════════════════════════╣")
print(f"║  Overall Pass Rate  : {overall_pass_rate:>6.1f}%   {'✅' if overall_pass_rate >= SLA_THRESHOLD else '❌'} SLA {'MET' if overall_pass_rate >= SLA_THRESHOLD else 'BREACHED'} ({SLA_THRESHOLD}% threshold)             ║")
print(f"║  Valid Rows         : {valid_rows:>8,} / {total_rows:,}                             ║")
print(f"║  Checks Passed      : {total_checks - failing_checks:>3} / {total_checks}                                        ║")
print(f"║  Failing Checks     : {failing_checks:>3}                                             ║")
print("╚══════════════════════════════════════════════════════════════════════╝")
print()

if overall_pass_rate < SLA_THRESHOLD:
    print("⚠️  ACTION REQUIRED: Data quality SLA breached. Review dq_quarantine.")
    # Fail the task so the workflow shows a clear failure signal
    raise Exception(
        f"Data quality SLA breach: {overall_pass_rate:.1f}% < {SLA_THRESHOLD}% threshold. "
        f"{quarantine_rows} rows quarantined."
    )
else:
    print("✅ All SLA thresholds met. Data is ready for downstream consumption.")
