# Databricks notebook source

# ================================================================
# CONFIGURATION TABLES
# ================================================================
# Run this notebook ONCE to create the metadata tables that drive
# the pipeline. After this, onboarding a new source table is just
# an INSERT into table_config — no code changes needed.
#
# Parameters (via dbutils.widgets):
#   catalog — Target catalog (default: main)
#   schema  — Target schema  (default: tmp)
# ================================================================

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema",  "tmp",  "Schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --- Table Config: one row per source→target mapping ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.incremental_filter__table_config (
  source_catalog    STRING    COMMENT 'Catalog of the source table',
  source_schema     STRING    COMMENT 'Schema of the source table',
  source_table      STRING    COMMENT 'Name of the source table',
  target_catalog    STRING    COMMENT 'Catalog of the target table',
  target_schema     STRING    COMMENT 'Schema of the target table',
  target_table      STRING    COMMENT 'Name of the target table',
  join_key          STRING    COMMENT 'Column to join against the control table (sensitive ID filtering)',
  pipeline_batch    INT       COMMENT 'Batch number for partitioning across pipelines (Pattern 1)',
  is_active         BOOLEAN   COMMENT 'Whether this table is actively processed',
  write_mode        STRING    COMMENT 'Write mode: append (default) or merge (upsert)',
  merge_keys        STRING    COMMENT 'Comma-separated primary key columns for MERGE, e.g. order_id or id,region',
  sequence_by       STRING    COMMENT 'Column for ordering/dedup in merge mode (optional, e.g. updated_at)',
  use_cdf           STRING    COMMENT 'Set to true to read Change Data Feed from source (handles restatements)'
)
COMMENT 'Metadata-driven config: one row per source-to-target table mapping'
""")

# --- Add new columns if table already exists (idempotent migration) ---
for col_name, col_type, col_comment in [
    ("write_mode",  "STRING", "Write mode: append (default) or merge (upsert)"),
    ("merge_keys",  "STRING", "Comma-separated primary key columns for MERGE"),
    ("sequence_by", "STRING", "Column for ordering/dedup in merge mode (optional)"),
    ("use_cdf",     "STRING", "Set to true to read Change Data Feed from source"),
]:
    try:
        spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.incremental_filter__table_config ADD COLUMN {col_name} {col_type} COMMENT '{col_comment}'")
        print(f"   ✅ Added column: {col_name}")
    except Exception as e:
        if "already exists" in str(e).lower() or "FIELDS_ALREADY_EXISTS" in str(e):
            pass  # Column already exists — fine
        else:
            raise

# --- Control Table: sensitive identifiers to exclude ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.incremental_filter__sensitive_records (
  sensitive_id      STRING    COMMENT 'The identifier value to filter out',
  record_type       STRING    COMMENT 'Type of identifier (e.g., SSN, email, user_id)',
  added_date        TIMESTAMP COMMENT 'When this ID was added to the exclusion list'
)
COMMENT 'Exclusion list: rows matching these IDs are filtered from target tables'
""")

print("✅ Configuration tables created (or already exist):")
print(f"   • {CATALOG}.{SCHEMA}.incremental_filter__table_config")
print(f"     Columns: source_catalog, source_schema, source_table, target_catalog,")
print(f"              target_schema, target_table, join_key, pipeline_batch, is_active,")
print(f"              write_mode, merge_keys, sequence_by, use_cdf")
print(f"   • {CATALOG}.{SCHEMA}.incremental_filter__sensitive_records")


# COMMAND ----------


# ================================================================
# SEED SAMPLE DATA for end-to-end testing
# ================================================================
# Creates:
#   1. Two sample source tables (orders, transactions)
#   2. Sample rows in the config table
#   3. Sample sensitive IDs in the control table
#
# Uses the catalog/schema widgets defined in Cell 0.
# ================================================================

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

# --- Create sample source tables with column mapping + type widening ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.src_orders (
  order_id      INT,
  customer_id   STRING,
  amount        DOUBLE,
  order_date    DATE,
  region        STRING
)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableTypeWidening'  = 'true'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.src_transactions (
  txn_id         INT,
  customer_id    STRING,
  txn_amount     DOUBLE,
  txn_timestamp  TIMESTAMP,
  status         STRING
)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableTypeWidening'  = 'true'
)
""")

# --- Seed source data (only if tables are empty) ---
if spark.table(f"{CATALOG}.{SCHEMA}.src_orders").count() == 0:
    # Get actual columns so we insert correctly even if schema evolved
    order_cols = [c.name for c in spark.table(f"{CATALOG}.{SCHEMA}.src_orders").schema.fields]
    base_cols = ["order_id", "customer_id", "amount", "order_date", "region"]
    # Build column list — fill any extra columns with NULL
    select_exprs = ", ".join(base_cols + ["NULL"] * (len(order_cols) - len(base_cols)))
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.src_orders ({', '.join(order_cols)})
    VALUES
      (1, 'CUST-001', 150.00, '2026-04-01', 'US-EAST'{', NULL' * (len(order_cols) - 5)}),
      (2, 'CUST-002', 275.50, '2026-04-02', 'US-WEST'{', NULL' * (len(order_cols) - 5)}),
      (3, 'CUST-003', 99.99,  '2026-04-03', 'EU-WEST'{', NULL' * (len(order_cols) - 5)}),
      (4, 'SENSITIVE-USER-1', 500.00, '2026-04-04', 'US-EAST'{', NULL' * (len(order_cols) - 5)}),
      (5, 'CUST-005', 320.00, '2026-04-05', 'APAC'{', NULL' * (len(order_cols) - 5)}),
      (6, 'SENSITIVE-USER-2', 125.75, '2026-04-06', 'EU-WEST'{', NULL' * (len(order_cols) - 5)})
    """)
    print(f"   ✅ Seeded {CATALOG}.{SCHEMA}.src_orders")
else:
    print(f"   ℹ️  {CATALOG}.{SCHEMA}.src_orders already has data — skipping seed")

if spark.table(f"{CATALOG}.{SCHEMA}.src_transactions").count() == 0:
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.src_transactions VALUES
      (101, 'CUST-001', 50.00,  '2026-04-01T10:00:00', 'completed'),
      (102, 'SENSITIVE-USER-1', 200.00, '2026-04-01T11:00:00', 'completed'),
      (103, 'CUST-003', 75.25,  '2026-04-02T09:30:00', 'pending'),
      (104, 'CUST-002', 180.00, '2026-04-02T14:00:00', 'completed'),
      (105, 'SENSITIVE-USER-2', 90.00,  '2026-04-03T08:15:00', 'completed')
    """)
    print(f"   ✅ Seeded {CATALOG}.{SCHEMA}.src_transactions")
else:
    print(f"   ℹ️  {CATALOG}.{SCHEMA}.src_transactions already has data — skipping seed")

# --- Seed config table (only if empty) ---
if spark.table(f"{CATALOG}.{SCHEMA}.incremental_filter__table_config").count() == 0:
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.incremental_filter__table_config VALUES
      ('{CATALOG}', '{SCHEMA}', 'src_orders',       '{CATALOG}', '{SCHEMA}', 'tgt_orders_clean',       'customer_id', 1, true, NULL, NULL, NULL, NULL),
      ('{CATALOG}', '{SCHEMA}', 'src_transactions', '{CATALOG}', '{SCHEMA}', 'tgt_transactions_clean', 'customer_id', 1, true, NULL, NULL, NULL, NULL)
    """)
    print(f"   ✅ Seeded config table")
else:
    print(f"   ℹ️  Config table already has data — skipping seed")

# --- Seed control table (only if empty) ---
if spark.table(f"{CATALOG}.{SCHEMA}.incremental_filter__sensitive_records").count() == 0:
    spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.incremental_filter__sensitive_records VALUES
      ('SENSITIVE-USER-1', 'user_id', current_timestamp()),
      ('SENSITIVE-USER-2', 'user_id', current_timestamp())
    """)
    print(f"   ✅ Seeded control table")
else:
    print(f"   ℹ️  Control table already has data — skipping seed")

print(f"\n✅ Sample data ready in {CATALOG}.{SCHEMA}")
