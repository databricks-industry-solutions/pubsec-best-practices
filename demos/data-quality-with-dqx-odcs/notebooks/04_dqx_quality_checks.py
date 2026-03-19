# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4: DQX Quality Checks (Contract-Driven)
# MAGIC
# MAGIC Uses **Databricks Labs DQX** to validate `curated_orders` against the rules
# MAGIC extracted from the ODCS data contract in Task 3.
# MAGIC
# MAGIC **Pipeline:**
# MAGIC ```
# MAGIC contract_rules  ──► build DQX check list  ──► apply_checks_by_metadata_and_split
# MAGIC                                                        │
# MAGIC                                        ┌──────────────┴──────────────┐
# MAGIC                                    valid_df                   quarantined_df
# MAGIC                                        │                            │
# MAGIC                                 curated_orders_valid        dq_quarantine
# MAGIC                                                              dq_results  (full detail)
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Install DQX

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx "protobuf>=5.26.1,<6.0.0" --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "main",    "Catalog")
dbutils.widgets.text("schema",  "dq_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Load Contract Rules

# COMMAND ----------

from pyspark.sql import functions as F

rules_df = spark.table(f"{catalog}.{schema}.contract_rules")
rules_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Build DQX Check List from ODCS Rules

# COMMAND ----------

from datetime import date

def odcs_rules_to_dqx(rules_df) -> list:
    """
    Convert ODCS contract_rules rows into DQX check metadata dicts.

    Correct DQX function names and argument keys (validated against live errors):
      is_not_null              → column (singular, one check per col)
      is_not_null_and_not_empty→ column (singular, one check per col)
      is_unique                → columns (list)
      is_in_range              → column, min_limit, max_limit
      is_in_list               → column, allowed
      is_matching_regex        → column, regex
    """
    checks = []
    rules  = rules_df.collect()

    for row in rules:
        col  = row["column_name"]
        rule = row["rule_type"]
        crit = row["criticality"] or "error"

        if rule == "not_null":
            checks.append({
                "criticality": crit,
                "check": {
                    "function": "is_not_null",
                    "arguments": {"column": col},
                },
                "name": f"{col}_not_null",
            })

        elif rule == "not_null_and_not_empty":
            checks.append({
                "criticality": crit,
                "check": {
                    "function": "is_not_null_and_not_empty",
                    "arguments": {"column": col},
                },
                "name": f"{col}_not_empty",
            })

        elif rule == "unique":
            checks.append({
                "criticality": crit,
                "check": {
                    "function": "is_unique",
                    "arguments": {"columns": [col]},
                },
                "name": f"{col}_is_unique",
            })

        elif rule == "is_in_range":
            min_v = row["min_value"]
            max_v = row["max_value"]

            # Substitute "today" with current date string
            if min_v == "today":
                min_v = str(date.today())
            if max_v == "today":
                max_v = str(date.today())

            # Cast to numeric where appropriate; keep as string for date ranges
            try:
                min_v = float(min_v) if min_v and "." in min_v else (int(min_v) if min_v else None)
                max_v = float(max_v) if max_v and "." in max_v else (int(max_v) if max_v else None)
            except (ValueError, TypeError):
                pass

            args = {"column": col}
            if min_v is not None:
                args["min_limit"] = min_v
            if max_v is not None:
                args["max_limit"] = max_v

            checks.append({
                "criticality": crit,
                "check": {"function": "is_in_range", "arguments": args},
                "name": f"{col}_in_range",
            })

        elif rule == "value_in_set":
            allowed = row["allowed_vals"].split(",") if row["allowed_vals"] else []
            checks.append({
                "criticality": crit,
                "check": {
                    "function": "is_in_list",
                    "arguments": {"column": col, "allowed": allowed},
                },
                "name": f"{col}_value_in_set",
            })

        elif rule == "regex_match":
            pattern = row["pattern"]
            if pattern:
                checks.append({
                    "criticality": crit,
                    "check": {
                        "function": "regex_match",
                        "arguments": {"column": col, "regex": pattern},
                    },
                    "name": f"{col}_regex_match",
                })

    return checks


dqx_checks = odcs_rules_to_dqx(rules_df)

print(f"Built {len(dqx_checks)} DQX checks from ODCS contract:")
for c in dqx_checks:
    name = c.get("name", c["check"]["function"])
    print(f"  [{c['criticality'].upper():>5}]  {name}")

# COMMAND ----------

# MAGIC %md ## Run DQX Checks

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
dq = DQEngine(ws)

curated_df = spark.table(f"{catalog}.{schema}.curated_orders")
print(f"Checking {curated_df.count()} rows in curated_orders...")

# apply_checks_by_metadata_and_split returns (valid_df, quarantined_df)
# valid_df        — rows that pass ALL error-level checks
# quarantined_df  — rows that fail at least one error-level check
#                   (warn violations are flagged but do not quarantine)
valid_df, quarantined_df = dq.apply_checks_by_metadata_and_split(curated_df, dqx_checks)

valid_cnt       = valid_df.count()
quarantined_cnt = quarantined_df.count()
total           = curated_df.count()

print(f"\nResults:")
print(f"  Total rows      : {total:>6}")
print(f"  Valid rows      : {valid_cnt:>6}  ({100*valid_cnt/total:.1f}%)")
print(f"  Quarantined rows: {quarantined_cnt:>6}  ({100*quarantined_cnt/total:.1f}%)")

# COMMAND ----------

# MAGIC %md ## Persist Results

# COMMAND ----------

# Valid rows — consumers should read from this table
valid_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.curated_orders_valid")

# Quarantined rows — DQX appends _error and _warn columns per check
quarantined_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.dq_quarantine")

print(f"Written:")
print(f"  {catalog}.{schema}.curated_orders_valid  ({valid_cnt} rows)")
print(f"  {catalog}.{schema}.dq_quarantine          ({quarantined_cnt} rows)")

# COMMAND ----------

# MAGIC %md ## Build Structured DQ Results Table
# MAGIC
# MAGIC Iterates over the `dqx_checks` list we defined and looks up the corresponding
# MAGIC DQX output column (`_error_<name>` / `_warn_<name>`) in the output DataFrames.
# MAGIC Uses an explicit schema so the DataFrame can be created even when all checks pass.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import functions as F
import datetime

# Union valid + quarantined so warn columns (which appear on both) can be counted
all_checked = valid_df.unionByName(quarantined_df, allowMissingColumns=True)

results_rows = []
now = datetime.datetime.utcnow()

for check in dqx_checks:
    name = check.get("name", check["check"]["function"])
    crit = check["criticality"]
    col_name = f"_error_{name}" if crit == "error" else f"_warn_{name}"

    # Error violations live in quarantined_df; warn violations span both splits
    search_df = quarantined_df if crit == "error" else all_checked

    if col_name in search_df.columns:
        failing = int(search_df.filter(F.col(col_name).isNotNull()).count())
    else:
        failing = 0

    results_rows.append((name, crit, failing, total, now))

results_schema = StructType([
    StructField("check_name",   StringType(),    False),
    StructField("criticality",  StringType(),    False),
    StructField("failing_rows", LongType(),      False),
    StructField("total_rows",   LongType(),      False),
    StructField("checked_at",   TimestampType(), False),
])

results_df = (
    spark.createDataFrame(results_rows, schema=results_schema)
    .withColumn("pass_rate", F.round(1 - F.col("failing_rows") / F.col("total_rows"), 4))
    .withColumn("status",    F.when(F.col("failing_rows") == 0, "PASS").otherwise("FAIL"))
)

results_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.dq_results")
print(f"dq_results written: {results_df.count()} check results")

# COMMAND ----------

# MAGIC %md ## Preview Failing Checks

# COMMAND ----------

display(
    spark.table(f"{catalog}.{schema}.dq_results")
    .filter(F.col("status") == "FAIL")
    .orderBy("criticality", F.col("failing_rows").desc())
)
