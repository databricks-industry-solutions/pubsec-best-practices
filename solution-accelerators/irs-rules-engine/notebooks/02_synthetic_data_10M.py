# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Generate 10M Synthetic Tax Returns
# MAGIC
# MAGIC Uses Spark-native generation (no Python loops) for speed.
# MAGIC Includes 24 columns covering IRM 25.1.2 fraud indicators across 6 categories:
# MAGIC Income, Deductions, Filing/Conduct, Books/Records, Employment Tax, Digital Assets.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG services_bureau_catalog;
# MAGIC USE SCHEMA irs_rrp;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

N = 10_000_000

# Generate base IDs using Spark range (fast, parallel)
df = spark.range(N).withColumn("return_id", F.format_string("RET-%08d", F.col("id") + 1))

# Filing status — weighted random
df = df.withColumn("_rand_fs", F.rand(seed=42))
df = df.withColumn("filing_status",
    F.when(F.col("_rand_fs") < 0.45, "MFJ")
     .when(F.col("_rand_fs") < 0.70, "S")
     .when(F.col("_rand_fs") < 0.85, "HH")
     .when(F.col("_rand_fs") < 0.95, "MFS")
     .otherwise("QSS")
)

# AGI — log-normal approximation via exp(normal)
df = df.withColumn("agi",
    F.round(F.least(
        F.greatest(F.exp(F.randn(seed=1) * 0.8 + 10.9), F.lit(10000)),
        F.lit(5000000)
    ), -2)
)

# Deductions — 10-70% of AGI, centered ~25%
df = df.withColumn("total_deductions",
    F.round(F.col("agi") * F.abs(F.randn(seed=2) * 0.12 + 0.25), -2)
)

# W2 income — 60-100% of AGI
df = df.withColumn("w2_income",
    F.round(F.col("agi") * F.least(F.abs(F.randn(seed=3) * 0.1 + 0.85), F.lit(1.0)), -2)
)

# Self-employment — 15% have any
df = df.withColumn("se_income",
    F.when(F.rand(seed=4) < 0.15,
        F.round(F.least(F.greatest(F.exp(F.randn(seed=5) * 0.7 + 10.5), F.lit(5000)), F.lit(1000000)), -2)
    ).otherwise(0)
)

# Foreign income — 5% have any
df = df.withColumn("foreign_income",
    F.when(F.rand(seed=6) < 0.05,
        F.round(F.least(F.greatest(F.exp(F.randn(seed=7) * 1.0 + 9.5), F.lit(1000)), F.lit(500000)), -2)
    ).otherwise(0)
)

# Charitable — 0-30% of AGI
df = df.withColumn("charitable_deductions",
    F.round(F.col("agi") * F.abs(F.randn(seed=8) * 0.06 + 0.05), -2)
)

# Capital gains — 20% have any
df = df.withColumn("capital_gains",
    F.when(F.rand(seed=9) < 0.20, F.round(F.randn(seed=10) * 30000 + 15000, -2)).otherwise(0)
)

# Tax credits
df = df.withColumn("tax_credits", F.round(F.abs(F.randn(seed=11) * 1500 + 1500), -2))

# Dependents (verified count)
df = df.withColumn("dependents",
    F.when(F.rand(seed=12) < 0.35, 0)
     .when(F.rand(seed=12) < 0.55, 1)
     .when(F.rand(seed=12) < 0.75, 2)
     .when(F.rand(seed=12) < 0.90, 3)
     .otherwise(4).cast("int")
)

# Prior audit — 3%
df = df.withColumn("prior_audit", F.rand(seed=13) < 0.03)

# ── NEW COLUMNS: IRM 25.1.2 Fraud Indicators ─────────────────────────

# Bank deposits total — most cluster near AGI; ~10% have 30%+ gap (IRM 25.1.2.2(1))
df = df.withColumn("bank_deposits_total",
    F.round(F.col("agi") * F.greatest(
        F.lit(1.0) + F.randn(seed=20) * 0.15,
        F.lit(0.9)
    ), -2)
)

# Number of W-2 forms: 60% have 1, 25% have 2, 10% have 3, 5% have 0 (IRM 25.1.2.2(2))
df = df.withColumn("_rand_w2", F.rand(seed=21))
df = df.withColumn("num_w2_forms",
    F.when(F.col("_rand_w2") < 0.05, 0)
     .when(F.col("_rand_w2") < 0.65, 1)
     .when(F.col("_rand_w2") < 0.90, 2)
     .otherwise(3).cast("int")
)

# Years not filed (out of last 5): 90%=0, 5%=1, 3%=2, 2%=3+ (IRM 25.1.2.4(3))
df = df.withColumn("_rand_ynf", F.rand(seed=22))
df = df.withColumn("years_not_filed",
    F.when(F.col("_rand_ynf") < 0.90, 0)
     .when(F.col("_rand_ynf") < 0.95, 1)
     .when(F.col("_rand_ynf") < 0.98, 2)
     .otherwise(3).cast("int")
)

# Amended returns count (last 3 years): 88%=0, 8%=1, 3%=2, 1%=3+ (IRM 25.1.2.5(1))
df = df.withColumn("_rand_amd", F.rand(seed=23))
df = df.withColumn("amended_returns_count",
    F.when(F.col("_rand_amd") < 0.88, 0)
     .when(F.col("_rand_amd") < 0.96, 1)
     .when(F.col("_rand_amd") < 0.99, 2)
     .otherwise(3).cast("int")
)

# Dependents claimed — matches verified for 92%, inflated by 1-3 for 8% (IRM 25.1.2.3(3))
df = df.withColumn("num_dependents_claimed",
    F.when(F.rand(seed=24) < 0.92, F.col("dependents"))
     .otherwise(F.col("dependents") + F.ceil(F.rand(seed=25) * 3).cast("int"))
     .cast("int")
)

# Employment type: W2 55%, 1099 20%, MIXED 15%, CASH_INTENSIVE 10% (IRM 25.1.2.6)
df = df.withColumn("_rand_emp", F.rand(seed=26))
df = df.withColumn("employment_type",
    F.when(F.col("_rand_emp") < 0.55, "W2")
     .when(F.col("_rand_emp") < 0.75, "1099")
     .when(F.col("_rand_emp") < 0.90, "MIXED")
     .otherwise("CASH_INTENSIVE")
)

# Digital asset transactions: 85%=0, 8% 1-5, 4% 6-20, 3% 21+ (IRM 25.1.2.7)
df = df.withColumn("_rand_dat", F.rand(seed=27))
df = df.withColumn("digital_asset_transactions",
    F.when(F.col("_rand_dat") < 0.85, 0)
     .when(F.col("_rand_dat") < 0.93, F.ceil(F.rand(seed=28) * 5).cast("int"))
     .when(F.col("_rand_dat") < 0.97, F.lit(5) + F.ceil(F.rand(seed=29) * 15).cast("int"))
     .otherwise(F.lit(20) + F.ceil(F.rand(seed=30) * 80).cast("int"))
     .cast("int")
)

# Digital asset proceeds: 0 if no transactions; log-normal ~$15K for those with activity
df = df.withColumn("digital_asset_proceeds",
    F.when(F.col("digital_asset_transactions") == 0, F.lit(0.0))
     .otherwise(F.round(F.least(
        F.greatest(F.exp(F.randn(seed=31) * 1.0 + 9.6), F.lit(100)),
        F.lit(500000)
     ), -2))
)

# ── Derived fields ────────────────────────────────────────────────────

df = df.withColumn("deduction_ratio",
    F.when(F.col("agi") > 0, F.col("total_deductions") / F.col("agi")).otherwise(0)
)
df = df.withColumn("charitable_ratio",
    F.when(F.col("agi") > 0, F.col("charitable_deductions") / F.col("agi")).otherwise(0)
)
df = df.withColumn("reported_income_gap",
    F.when(F.col("agi") > 0,
        (F.col("bank_deposits_total") - F.col("agi")) / F.col("agi")
    ).otherwise(0)
)
df = df.withColumn("w2_agi_ratio",
    F.when(F.col("agi") > 0, F.col("w2_income") / F.col("agi")).otherwise(0)
)

# Drop temp columns and select final schema
result = df.select(
    "return_id", "filing_status", "agi", "total_deductions", "w2_income",
    "se_income", "foreign_income", "capital_gains", "charitable_deductions",
    "tax_credits", "dependents", "prior_audit", "deduction_ratio", "charitable_ratio",
    # New IRM 25.1.2 columns
    "bank_deposits_total", "reported_income_gap", "num_w2_forms", "w2_agi_ratio",
    "years_not_filed", "amended_returns_count", "num_dependents_claimed",
    "employment_type", "digital_asset_transactions", "digital_asset_proceeds"
)

result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "services_bureau_catalog.irs_rrp.tax_returns"
)
count = spark.table("services_bureau_catalog.irs_rrp.tax_returns").count()
print(f"✅ tax_returns: {count} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify distributions
# MAGIC SELECT
# MAGIC     filing_status,
# MAGIC     count(*) as count,
# MAGIC     round(avg(agi)) as avg_agi,
# MAGIC     round(avg(deduction_ratio), 3) as avg_ded_ratio,
# MAGIC     sum(case when prior_audit then 1 else 0 end) as prior_audits,
# MAGIC     sum(case when foreign_income > 0 then 1 else 0 end) as has_foreign,
# MAGIC     sum(case when se_income > 0 then 1 else 0 end) as has_se,
# MAGIC     sum(case when digital_asset_transactions > 0 then 1 else 0 end) as has_crypto,
# MAGIC     round(avg(reported_income_gap), 3) as avg_income_gap,
# MAGIC     sum(case when years_not_filed > 0 then 1 else 0 end) as non_filers,
# MAGIC     sum(case when employment_type = 'CASH_INTENSIVE' then 1 else 0 end) as cash_intensive
# MAGIC FROM services_bureau_catalog.irs_rrp.tax_returns
# MAGIC GROUP BY filing_status
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create scored results table
# MAGIC CREATE TABLE IF NOT EXISTS services_bureau_catalog.irs_rrp.scored_results (
# MAGIC   return_id STRING,
# MAGIC   filing_status STRING,
# MAGIC   agi DOUBLE,
# MAGIC   audit_score DOUBLE,
# MAGIC   recommended_action STRING,
# MAGIC   dmn_version STRING,
# MAGIC   scored_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Tax returns scored by Drools DMN engine';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create evaluation audit log table
# MAGIC CREATE TABLE IF NOT EXISTS services_bureau_catalog.irs_rrp.evaluation_log (
# MAGIC   evaluation_id STRING,
# MAGIC   rule_version_id STRING,
# MAGIC   return_id STRING,
# MAGIC   filing_status STRING,
# MAGIC   audit_score DOUBLE,
# MAGIC   recommended_action STRING,
# MAGIC   evaluated_at TIMESTAMP,
# MAGIC   job_run_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Audit trail of all rule evaluations';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create rule versions governance table
# MAGIC -- `binding_json` carries the v2 binding (input_view + outputs).
# MAGIC -- `input_view` is denormalized for cleanup notebook joins; source of truth is the binding.
# MAGIC CREATE TABLE IF NOT EXISTS services_bureau_catalog.irs_rrp.rule_versions (
# MAGIC   version_id STRING,
# MAGIC   rule_set_name STRING,
# MAGIC   dmn_path STRING,
# MAGIC   binding_json STRING,
# MAGIC   input_view STRING,
# MAGIC   status STRING COMMENT 'DRAFT | ACTIVE | ARCHIVED',
# MAGIC   created_by STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   promoted_by STRING,
# MAGIC   promoted_at TIMESTAMP,
# MAGIC   notes STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Rule version governance — tracks DRAFT/ACTIVE/ARCHIVED lifecycle';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized view: `scoring_input_v3_1`
# MAGIC
# MAGIC The scoring input contract for the v3.1-irm DMN. The notebook reads
# MAGIC this view by name (from the binding's `input_view` field) — no joins,
# MAGIC no per-column SQL in the binding. Adding a new DMN version that needs
# MAGIC different inputs creates a new versioned MV (`scoring_input_v4`,
# MAGIC `scoring_input_v5`, ...) so the prior version stays runnable.
# MAGIC
# MAGIC Defaults are explicit so the DMN never sees a Java null on a numeric
# MAGIC input — that propagates through FEEL composite expressions and shows
# MAGIC up as 100%-null decision columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE MATERIALIZED VIEW IF NOT EXISTS services_bureau_catalog.irs_rrp.scoring_input_v3_1
# MAGIC COMMENT 'Scoring input for DMN v3.1-irm — flattened, defaults applied'
# MAGIC AS
# MAGIC SELECT
# MAGIC   return_id,
# MAGIC   filing_status,
# MAGIC   coalesce(agi, 0.0)                       AS agi,
# MAGIC   coalesce(total_deductions, 0.0)          AS total_deductions,
# MAGIC   coalesce(deduction_ratio, 0.0)           AS deduction_ratio,
# MAGIC   coalesce(charitable_ratio, 0.0)          AS charitable_ratio,
# MAGIC   coalesce(se_income, 0.0)                 AS se_income,
# MAGIC   coalesce(foreign_income, 0.0)            AS foreign_income,
# MAGIC   coalesce(capital_gains, 0.0)             AS capital_gains,
# MAGIC   coalesce(tax_credits, 0.0)               AS tax_credits,
# MAGIC   coalesce(dependents, 0)                  AS dependents,
# MAGIC   coalesce(prior_audit, false)             AS prior_audit,
# MAGIC   coalesce(reported_income_gap, 0.0)       AS reported_income_gap,
# MAGIC   coalesce(num_w2_forms, 0)                AS num_w2_forms,
# MAGIC   coalesce(w2_agi_ratio, 0.0)              AS w2_agi_ratio,
# MAGIC   coalesce(years_not_filed, 0)             AS years_not_filed,
# MAGIC   coalesce(amended_returns_count, 0)       AS amended_returns_count,
# MAGIC   coalesce(num_dependents_claimed, 0)      AS num_dependents_claimed,
# MAGIC   coalesce(employment_type, 'W2')          AS employment_type,
# MAGIC   coalesce(digital_asset_transactions, 0)  AS digital_asset_transactions,
# MAGIC   coalesce(digital_asset_proceeds, 0.0)    AS digital_asset_proceeds
# MAGIC FROM services_bureau_catalog.irs_rrp.tax_returns;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW services_bureau_catalog.irs_rrp.scoring_input_v3_1;

# COMMAND ----------

# Seed initial rule version (v3.1-irm with v2 binding)
from pyspark.sql import Row
from datetime import datetime
import json

V31_BINDING = {
    "version": 2,
    "input_view": "services_bureau_catalog.irs_rrp.scoring_input_v3_1",
    "outputs": {
        "scored_results": {
            "mode": "overwrite",
            "carry_columns": ["return_id", "filing_status", "agi"],
            "decisions": {
                "audit_score": "Tax Review Scoring",
                "recommended_action": "Recommended Action",
            },
            "post_columns": {
                "dmn_version": "__VERSION__",
                "scored_at": "current_timestamp()",
            },
        }
    },
}

initial_version = spark.createDataFrame([Row(
    version_id="v3.1-irm",
    rule_set_name="IRS Tax Return Review",
    dmn_path="/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/irs_tax_review_v3_1_irm.dmn",
    binding_json=json.dumps(V31_BINDING),
    input_view="services_bureau_catalog.irs_rrp.scoring_input_v3_1",
    status="ACTIVE",
    created_by="system",
    created_at=datetime.now(),
    promoted_by="system",
    promoted_at=datetime.now(),
    notes="21-rule IRM 25.1.2 review with chained Recommended Action — v2 binding (input_view contract)"
)])

initial_version.write.mode("append").saveAsTable("services_bureau_catalog.irs_rrp.rule_versions")
print("✅ Initial rule version seeded (v3.1-irm ACTIVE, v2 binding)")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
