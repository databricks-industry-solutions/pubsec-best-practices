# Databricks notebook source

# MAGIC %md
# MAGIC # Incremental Filter Pipeline — Architecture & README
# MAGIC
# MAGIC ## Executive Summary
# MAGIC
# MAGIC This project implements a **scalable, config-driven incremental data pipeline** on Databricks that:
# MAGIC
# MAGIC 1. **Reads data incrementally** from thousands of Delta-formatted source tables using Structured Streaming with `trigger(availableNow=True)`
# MAGIC 2. **Filters out sensitive records** using a LEFT ANTI JOIN against a centralized control table
# MAGIC 3. **Loads filtered data into target tables** using either **APPEND** or **MERGE** (upsert) mode
# MAGIC 4. **Supports schema evolution**, Change Data Feed (CDF), source restatements, and Unity Catalog credential vending
# MAGIC
# MAGIC The pipeline is entirely **metadata-driven** — onboarding a new source table requires only an `INSERT` into the config table. No code changes are needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture Decision: Pattern 2 (Lakeflow Jobs For Each)
# MAGIC
# MAGIC Two architectural patterns were evaluated:
# MAGIC
# MAGIC | Dimension | Pattern 1: SDP (Spark Declarative Pipelines) | Pattern 2: Lakeflow Jobs For Each |
# MAGIC |---|---|---|
# MAGIC | **Runtime** | SDP cluster with continuous or triggered pipelines | Standard Lakeflow Job with For Each task |
# MAGIC | **Table type** | Streaming Tables (SDP-managed) | Regular managed Delta tables |
# MAGIC | **Write modes** | `APPLY CHANGES INTO` for CDC/merge | `DeltaTable.merge()` in `foreachBatch` |
# MAGIC | **Schema evolution** | Automatic in SDP | `mergeSchema` (append) / `withSchemaEvolution()` (merge) |
# MAGIC | **UC Credential Vending** | ❌ **Not supported** for streaming tables | ✅ Fully compatible |
# MAGIC | **Scaling** | One DLT pipeline per batch | One Job per batch, For Each handles parallelism |
# MAGIC | **Cost** | Pro/Advanced edition required for CDC | Standard edition works |
# MAGIC
# MAGIC ### Why Pattern 2 Was Chosen
# MAGIC
# MAGIC **UC table credential vending** is a hard requirement for downstream consumers. External engines (Spark on non-Databricks, Trino, Snowflake, etc.) need to read the target tables via credential vending. This feature:
# MAGIC
# MAGIC - ✅ Works with **regular managed Delta tables** (Pattern 2)
# MAGIC - ❌ Does **NOT** work with **SDP streaming tables** (Pattern 1)
# MAGIC
# MAGIC The architecture requires both:
# MAGIC - **Inbound**: UC external locations for reading source tables
# MAGIC - **Outbound**: Credential vending for downstream consumers reading target tables
# MAGIC
# MAGIC Pattern 2 satisfies both requirements. Pattern 1 fails on outbound credential vending.


# COMMAND ----------


# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                     Lakeflow Job (one per batch)                    │
# MAGIC │                                                                     │
# MAGIC │  ┌──────────────────────┐     ┌──────────────────────────────────┐ │
# MAGIC │  │ Task 1:              │     │ Task 2: For Each                 │ │
# MAGIC │  │ generate_table_list  │────▶│ (one iteration per table)        │ │
# MAGIC │  │                      │     │                                  │ │
# MAGIC │  │ • Reads config table │     │  ┌────────────────────────────┐  │ │
# MAGIC │  │ • Filters by batch   │     │  │ process_single_table       │  │ │
# MAGIC │  │ • Emits JSON list    │     │  │                            │  │ │
# MAGIC │  └──────────────────────┘     │  │ 1. readStream (incremental)│  │ │
# MAGIC │                               │  │ 2. LEFT ANTI JOIN (filter) │  │ │
# MAGIC │                               │  │ 3. APPEND or MERGE (write) │  │ │
# MAGIC │                               │  └────────────────────────────┘  │ │
# MAGIC │                               └──────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Data Flow
# MAGIC
# MAGIC 1. **Task 1 (`01_generate_table_list`)** reads the config table, filters by `catalog`, `schema`, and `is_active = true`, and emits a JSON array of table configs as a task value
# MAGIC 2. **Task 2 (`02_process_single_table`)** runs once per table via Lakeflow's **For Each** task:
# MAGIC    - **Incremental read**: `spark.readStream.table(source)` with `trigger(availableNow=True)` — processes only new data since the last checkpoint
# MAGIC    - **Sensitive filtering**: LEFT ANTI JOIN against the control table removes any rows whose `join_key` matches a sensitive ID
# MAGIC    - **Write**: Either APPEND (simple insert) or MERGE (upsert with schema evolution), driven by the config table's `write_mode` column
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Config Table Schema
# MAGIC
# MAGIC **Table**: `{catalog}.{schema}.incremental_filter__table_config`
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `source_catalog` | STRING | Catalog of the source table |
# MAGIC | `source_schema` | STRING | Schema of the source table |
# MAGIC | `source_table` | STRING | Name of the source table |
# MAGIC | `target_catalog` | STRING | Catalog of the target table |
# MAGIC | `target_schema` | STRING | Schema of the target table |
# MAGIC | `target_table` | STRING | Name of the target table |
# MAGIC | `join_key` | STRING | Column to join against the control table for filtering |
# MAGIC | `pipeline_batch` | INT | Batch number for partitioning across jobs |
# MAGIC | `is_active` | BOOLEAN | Whether this table is actively processed |
# MAGIC | `write_mode` | STRING | `"append"` (default) or `"merge"` (upsert) |
# MAGIC | `merge_keys` | STRING | Comma-separated PK columns for MERGE (e.g. `"order_id"` or `"id,region"`) |
# MAGIC | `sequence_by` | STRING | Ordering column for dedup in merge mode (optional) |
# MAGIC | `use_cdf` | STRING | Set to `"true"` to read Change Data Feed from source |
# MAGIC
# MAGIC ### Control Table
# MAGIC
# MAGIC **Table**: `{catalog}.{schema}.incremental_filter__sensitive_records`
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `sensitive_id` | STRING | The identifier value to filter out |
# MAGIC | `record_type` | STRING | Type of identifier (e.g., SSN, email, user_id) |
# MAGIC | `added_date` | TIMESTAMP | When this ID was added to the exclusion list |


# COMMAND ----------


# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Write Modes: Append vs. MERGE (Upsert)
# MAGIC
# MAGIC The pipeline supports two write modes, configured **per table** in the config table:
# MAGIC
# MAGIC | Feature | APPEND Mode | MERGE Mode |
# MAGIC |---|---|---|
# MAGIC | **Operation** | Simple insert — new rows appended to target | Upsert — matched rows updated, new rows inserted |
# MAGIC | **Config** | `write_mode = "append"` (or NULL/empty) | `write_mode = "merge"`, `merge_keys` required |
# MAGIC | **Deduplication** | None — every micro-batch is appended | Optional via `sequence_by` (keeps latest per merge key) |
# MAGIC | **Schema evolution** | `mergeSchema = true` on write | `.withSchemaEvolution()` on DeltaTable.merge() |
# MAGIC | **Performance** | Fastest — no target scan | Slower — joins against target table |
# MAGIC | **Use case** | Append-only event streams, logs | Dimension tables, slowly changing data |
# MAGIC | **CDF deletes** | N/A (append-only) | Propagates deletes when `use_cdf = "true"` |
# MAGIC
# MAGIC ### MERGE Mechanics
# MAGIC
# MAGIC ```python
# MAGIC # Build condition: t.key1 = s.key1 AND t.key2 = s.key2
# MAGIC merge_condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
# MAGIC
# MAGIC DeltaTable.forName(spark, target)
# MAGIC     .alias("t")
# MAGIC     .merge(filtered_batch.alias("s"), merge_condition)
# MAGIC     .whenMatchedUpdateAll()
# MAGIC     .whenNotMatchedInsertAll()
# MAGIC     .withSchemaEvolution()
# MAGIC     .execute()
# MAGIC ```
# MAGIC
# MAGIC - **Bootstrap**: If the target table doesn't exist, the first batch auto-creates it via `saveAsTable` with `mode("overwrite")`
# MAGIC - **Dedup**: When `sequence_by` is set, each micro-batch is deduplicated by `merge_keys`, keeping only the row with the latest `sequence_by` value
# MAGIC - **Mixed modes**: You can have some tables as `append` and others as `merge` in the same job — the config table drives this per-table
# MAGIC - **Backward compatible**: Existing config rows with NULL `write_mode` default to `"append"` — no migration needed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Schema Evolution Support
# MAGIC
# MAGIC | Scenario | Supported? | Behavior |
# MAGIC |---|---|---|
# MAGIC | **New columns added to source** | ✅ Yes | Auto-added to target via `mergeSchema` (append) or `withSchemaEvolution()` (merge). Existing rows get NULLs for the new column. |
# MAGIC | **Type widening** (e.g., INT → BIGINT) | ✅ Yes | Handled by `delta.enableTypeWidening = true` on the target table + `allowSourceColumnTypeChange = always` on the read stream. |
# MAGIC | **Dropped columns** | ⚠️ Partial | Source must have `delta.columnMapping.mode = name`. Target retains the column with NULLs for new rows. |
# MAGIC | **Incompatible type changes** (e.g., STRING → INT) | ❌ No | Requires manual intervention: checkpoint reset + target recreation. |
# MAGIC
# MAGIC ### Auto-Recovery from Schema/Checkpoint Errors
# MAGIC
# MAGIC The pipeline includes auto-recovery logic for two common streaming errors:
# MAGIC
# MAGIC 1. **`DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`** — source table was replaced or recreated
# MAGIC 2. **Stale Delta version** — checkpoint references a version that's been vacuumed
# MAGIC
# MAGIC Recovery behavior:
# MAGIC - **APPEND mode**: Resets checkpoint, truncates target, re-processes from scratch
# MAGIC - **MERGE mode**: Resets checkpoint only — target data is preserved since MERGE is idempotent on re-read


# COMMAND ----------


# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Change Data Feed (CDF) & Source Restatements
# MAGIC
# MAGIC When `use_cdf = "true"` in the config table, the pipeline reads the source table's Change Data Feed instead of the raw table:
# MAGIC
# MAGIC ```python
# MAGIC stream = spark.readStream.option("readChangeFeed", "true").table(source)
# MAGIC ```
# MAGIC
# MAGIC CDF provides a `_change_type` column with values:
# MAGIC
# MAGIC | `_change_type` | Handling |
# MAGIC |---|---|
# MAGIC | `insert` | Processed normally (filtered + written) |
# MAGIC | `update_postimage` | Processed normally (MERGE updates the target row) |
# MAGIC | `update_preimage` | Dropped — only the post-image is needed |
# MAGIC | `delete` | **MERGE mode only**: Executes a DELETE against the target table using `merge_keys` |
# MAGIC
# MAGIC ### Source Restatements
# MAGIC
# MAGIC When a source table is restated (data corrected retroactively):
# MAGIC
# MAGIC - **Without CDF**: The stream checkpoint tracks committed versions. If the source is `TRUNCATE`+`INSERT`, the checkpoint becomes invalid → auto-recovery kicks in (reset checkpoint, re-process)
# MAGIC - **With CDF**: Restatements appear as `update_postimage` rows in the change feed. MERGE mode handles these naturally — matched rows are updated in the target
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## UC Credential Vending Compatibility
# MAGIC
# MAGIC Unity Catalog **credential vending** allows external engines to read UC-managed tables by vending temporary cloud credentials (AWS STS, Azure SAS, GCP tokens).
# MAGIC
# MAGIC | Table Type | Credential Vending | Notes |
# MAGIC |---|---|---|
# MAGIC | Regular managed Delta tables | ✅ Supported | Pattern 2 writes these |
# MAGIC | External Delta tables | ✅ Supported | Via external locations |
# MAGIC | SDP Streaming Tables | ❌ **Not supported** | Pattern 1 writes these — blocked |
# MAGIC | SDP Materialized Views | ❌ **Not supported** | SDP-managed lifecycle |
# MAGIC
# MAGIC This is the primary reason Pattern 2 was chosen. The architecture requires:
# MAGIC - **Inbound**: External locations (or shares) for reading source tables from partner catalogs
# MAGIC - **Outbound**: Credential vending so downstream consumers (Spark-on-K8s, Trino, Snowflake, dbt) can read the cleaned target tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Scaling to 5,000+ Tables
# MAGIC
# MAGIC ### Batching Strategy
# MAGIC
# MAGIC The `pipeline_batch` column in the config table partitions tables across multiple Lakeflow Jobs:
# MAGIC
# MAGIC | Tables | Recommended Batches | Tables per Batch | Jobs |
# MAGIC |---|---|---|---|
# MAGIC | 100 | 1–2 | 50–100 | 1–2 |
# MAGIC | 500 | 5–10 | 50–100 | 5–10 |
# MAGIC | 2,000 | 20–40 | 50–100 | 20–40 |
# MAGIC | 5,000+ | 50–100 | 50–100 | 50–100 |
# MAGIC
# MAGIC ### Why Batching?
# MAGIC
# MAGIC - **For Each concurrency**: Each For Each task runs iterations in parallel (configurable `max_concurrency`), but a single job shouldn't manage thousands of iterations
# MAGIC - **Failure isolation**: If one batch fails, other batches continue running
# MAGIC - **Resource management**: Each job gets its own cluster, sized appropriately for its batch
# MAGIC - **Scheduling flexibility**: Different batches can run on different schedules (e.g., high-priority tables every 15 min, low-priority daily)
# MAGIC
# MAGIC ### PyDABs Integration
# MAGIC
# MAGIC The **PyDABs bundle generator** (`06_pydabs_bundle_generator`) programmatically creates one Lakeflow Job definition per `pipeline_batch`. This integrates with Databricks Asset Bundles for CI/CD:
# MAGIC
# MAGIC ```
# MAGIC incremental_filter_bundle/
# MAGIC ├── databricks.yml              # Bundle config with catalog/schema variables
# MAGIC ├── pyproject.toml              # Python project metadata
# MAGIC ├── README.md                   # Bundle-specific README
# MAGIC ├── resources/
# MAGIC │   └── __init__.py             # load_resources() → one Job per pipeline_batch
# MAGIC └── src/
# MAGIC     ├── generate_table_list.py  # Task 1: emit table configs as task value
# MAGIC     └── process_single_table.py # Task 2: incremental read → filter → write
# MAGIC ```
# MAGIC
# MAGIC The bundle uses **first-class variables** for `catalog` and `schema`:
# MAGIC - **Dev**: `main.tmp`
# MAGIC - **Prod**: Configured per target (e.g., `my_catalog.pipeline_meta`)
# MAGIC
# MAGIC Deploy with:
# MAGIC ```bash
# MAGIC cd incremental_filter_bundle
# MAGIC databricks bundle deploy --target dev
# MAGIC databricks bundle deploy --target prod
# MAGIC ```


# COMMAND ----------


# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Notebook Inventory
# MAGIC
# MAGIC | Notebook | Purpose | Key Parameters |
# MAGIC |---|---|---|
# MAGIC | [00_setup_config_tables](./00_setup_config_tables) | Creates config + control tables; seeds sample data idempotently | `catalog`, `schema` |
# MAGIC | [01_generate_table_list](./01_generate_table_list) | **Task 1**: Reads config filtered by catalog/schema/is_active, emits JSON table list as task value | `catalog`, `schema` |
# MAGIC | [02_process_single_table](./02_process_single_table) | **Task 2 (nested)**: Incremental read → LEFT ANTI JOIN filter → APPEND or MERGE write | `catalog`, `schema`, `checkpoint_base`, `input` (JSON) |
# MAGIC | [03_test_schema_evolution](./03_test_schema_evolution) | Validates new-column schema evolution end-to-end | `catalog`, `schema`, `checkpoint_base` |
# MAGIC | [04_create_job_definition](./04_create_job_definition) | Generates Lakeflow Job JSON definition with For Each task | `catalog`, `schema`, `checkpoint_base`, `notebook_base` |
# MAGIC | [05_cleanup](./05_cleanup) | Drops test tables (commented out for safety) | `catalog`, `schema`, `checkpoint_base` |
# MAGIC | [06_pydabs_bundle_generator](./06_pydabs_bundle_generator) | Generates complete PyDABs bundle project for CI/CD deployment | `catalog`, `schema` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Start
# MAGIC
# MAGIC ### 1. Set up the config and control tables
# MAGIC
# MAGIC Run [00_setup_config_tables](./00_setup_config_tables) with your target `catalog` and `schema` widgets. This creates the metadata tables and seeds sample data for testing.
# MAGIC
# MAGIC ### 2. Test interactively
# MAGIC
# MAGIC Run [02_process_single_table](./02_process_single_table) **without** providing the `input` widget — it falls back to a default test config that processes `src_orders → tgt_orders_clean` using MERGE mode.
# MAGIC
# MAGIC ### 3. Test schema evolution
# MAGIC
# MAGIC Run [03_test_schema_evolution](./03_test_schema_evolution) to verify that adding a new column to a source table automatically propagates to the target.
# MAGIC
# MAGIC ### 4. Create the Lakeflow Job
# MAGIC
# MAGIC Run [04_create_job_definition](./04_create_job_definition) to generate the job JSON, then create it via the Jobs UI or REST API.
# MAGIC
# MAGIC ### 5. Onboard new tables
# MAGIC
# MAGIC ```sql
# MAGIC INSERT INTO {catalog}.{schema}.incremental_filter__table_config VALUES
# MAGIC   ('source_cat', 'source_sch', 'my_table',
# MAGIC    'target_cat', 'target_sch', 'my_table_clean',
# MAGIC    'customer_id',   -- join_key for sensitive filtering
# MAGIC    1,               -- pipeline_batch
# MAGIC    true,            -- is_active
# MAGIC    'merge',         -- write_mode: 'append' or 'merge'
# MAGIC    'id,region',     -- merge_keys (comma-separated PKs)
# MAGIC    'updated_at',    -- sequence_by (optional dedup column)
# MAGIC    NULL             -- use_cdf: set to 'true' for CDF
# MAGIC   );
# MAGIC ```
# MAGIC
# MAGIC ### 6. Add sensitive IDs to filter
# MAGIC
# MAGIC ```sql
# MAGIC INSERT INTO {catalog}.{schema}.incremental_filter__sensitive_records VALUES
# MAGIC   ('SENSITIVE-USER-42', 'user_id', current_timestamp());
# MAGIC ```
# MAGIC
# MAGIC Newly added sensitive IDs take effect on the **next pipeline run** — no code changes or restarts needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Production Deployment (PyDABs)
# MAGIC
# MAGIC For production, use the PyDABs bundle:
# MAGIC
# MAGIC 1. Run [06_pydabs_bundle_generator](./06_pydabs_bundle_generator) to generate the bundle files
# MAGIC 2. Copy the `incremental_filter_bundle/` directory to your Git repo
# MAGIC 3. Configure `databricks.yml` variables for your environment
# MAGIC 4. Deploy:
# MAGIC    ```bash
# MAGIC    databricks bundle deploy --target prod
# MAGIC    databricks bundle run --target prod incremental_filter_batch_1
# MAGIC    ```
# MAGIC
# MAGIC The bundle automatically creates one Lakeflow Job per distinct `pipeline_batch` in your config table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Design Decisions & Caveats
# MAGIC
# MAGIC 1. **Control table is read fresh every run** — unlike a stream-stream join, this guarantees the latest exclusion list is always used
# MAGIC 2. **`trigger(availableNow=True)`** — processes all available new data then stops (batch-like economics with streaming semantics)
# MAGIC 3. **Checkpoints are per-source-table** — stored at `{checkpoint_base}/{schema}_{table}` to avoid conflicts
# MAGIC 4. **MERGE is more expensive than APPEND** — it joins against the target table; align `merge_keys` with table partitions for large targets
# MAGIC 5. **Source tables need `delta.columnMapping.mode = name`** — required for graceful handling of dropped columns
# MAGIC 6. **Target tables get `delta.enableTypeWidening = true`** — set automatically on first run for type-widening support
# MAGIC 7. **CDF deletes only work in MERGE mode** — APPEND mode ignores `_change_type = delete` since there's no key to match against
