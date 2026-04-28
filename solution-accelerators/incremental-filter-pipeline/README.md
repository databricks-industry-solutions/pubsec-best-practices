# Incremental Filter Pipeline

Config-driven incremental data pipeline with sensitive record filtering, MERGE/APPEND write modes, schema evolution, and Unity Catalog credential vending support.

## Features

| Feature | Description |
|---------|-------------|
| **Config-driven** | Onboard new tables with a single `INSERT` — no code changes needed |
| **Incremental processing** | Structured Streaming with `trigger(availableNow=True)` — only new data processed |
| **Sensitive record filtering** | LEFT ANTI JOIN against a centralized control table removes sensitive rows |
| **APPEND & MERGE modes** | Per-table write mode: simple append or full upsert with deduplication |
| **Schema evolution** | New columns, type widening, and dropped-column handling built in |
| **CDF support** | Change Data Feed propagation including deletes (merge mode) |
| **Auto-recovery** | Detects stale checkpoints and schema mismatches; recovers automatically |
| **UC credential vending** | Writes regular managed Delta tables — fully compatible with external engines |
| **Scalable to 5,000+ tables** | Batching strategy with one Lakeflow Job per batch via PyDABs |

## Quick Start

### 1. Set up the config and control tables

Run `notebooks/00_setup_config_tables` with your target `catalog` and `schema` widgets.
This creates the metadata tables and seeds sample data for testing.

### 2. Test interactively

Run `notebooks/02_process_single_table` without providing the `input` widget — it falls back
to a default test config that processes `src_orders → tgt_orders_clean` using MERGE mode.

### 3. Test schema evolution

Run `notebooks/03_test_schema_evolution` to verify that adding a new column to a source table
automatically propagates to the target.

### 4. Create the Lakeflow Job

Run `notebooks/04_create_job_definition` to generate the job JSON, then create it via the
Jobs UI or REST API.

### 5. Deploy with PyDABs

```bash
cd asset-bundles
databricks bundle deploy --target dev
databricks bundle run --target dev incremental_filter_batch_1
```

### 6. Onboard new tables

```sql
INSERT INTO {catalog}.{schema}.incremental_filter__table_config VALUES
  ('source_cat', 'source_sch', 'my_table',
   'target_cat', 'target_sch', 'my_table_clean',
   'customer_id',   -- join_key for sensitive filtering
   1,               -- pipeline_batch
   true,            -- is_active
   'merge',         -- write_mode: 'append' or 'merge'
   'id,region',     -- merge_keys (comma-separated PKs)
   'updated_at',    -- sequence_by (optional dedup column)
   NULL             -- use_cdf: set to 'true' for CDF
  );
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Lakeflow Job (one per batch)                    │
│                                                                     │
│  ┌──────────────────────┐     ┌──────────────────────────────────┐ │
│  │ Task 1:              │     │ Task 2: For Each                 │ │
│  │ generate_table_list  │────▶│ (one iteration per table)        │ │
│  │                      │     │                                  │ │
│  │ • Reads config table │     │  ┌────────────────────────────┐  │ │
│  │ • Filters by batch   │     │  │ process_single_table       │  │ │
│  │ • Emits JSON list    │     │  │                            │  │ │
│  └──────────────────────┘     │  │ 1. readStream (incremental)│  │ │
│                               │  │ 2. LEFT ANTI JOIN (filter) │  │ │
│                               │  │ 3. APPEND or MERGE (write) │  │ │
│                               │  └────────────────────────────┘  │ │
│                               └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Pattern 2 (Lakeflow Jobs For Each)?

Two patterns were evaluated — SDP (Spark Declarative Pipelines) vs. Lakeflow Jobs For Each.
Pattern 2 was chosen because **UC credential vending does not support SDP streaming tables**.
External engines (Spark-on-K8s, Trino, Snowflake) need credential vending to read target tables,
and this only works with regular managed Delta tables (Pattern 2).

## Config Table Schema

**Table**: `{catalog}.{schema}.incremental_filter__table_config`

| Column | Type | Description |
|--------|------|-------------|
| `source_catalog` | STRING | Catalog of the source table |
| `source_schema` | STRING | Schema of the source table |
| `source_table` | STRING | Name of the source table |
| `target_catalog` | STRING | Catalog of the target table |
| `target_schema` | STRING | Schema of the target table |
| `target_table` | STRING | Name of the target table |
| `join_key` | STRING | Column to join against the control table for filtering |
| `pipeline_batch` | INT | Batch number for partitioning across jobs |
| `is_active` | BOOLEAN | Whether this table is actively processed |
| `write_mode` | STRING | `"append"` (default) or `"merge"` (upsert) |
| `merge_keys` | STRING | Comma-separated PK columns for MERGE |
| `sequence_by` | STRING | Ordering column for dedup in merge mode |
| `use_cdf` | STRING | Set to `"true"` to read Change Data Feed from source |

## Schema Evolution Support

| Scenario | Supported? | Behavior |
|----------|------------|----------|
| New columns added to source | ✅ Yes | Auto-added to target via `mergeSchema` / `withSchemaEvolution()` |
| Type widening (e.g., INT → BIGINT) | ✅ Yes | `delta.enableTypeWidening` + `allowSourceColumnTypeChange = always` |
| Dropped columns | ⚠️ Partial | Source needs `delta.columnMapping.mode = name`; target retains column with NULLs |
| Incompatible type changes | ❌ No | Requires manual checkpoint reset + target recreation |

## Scaling to 5,000+ Tables

| Tables | Recommended Batches | Tables per Batch | Jobs |
|--------|---------------------|------------------|------|
| 100 | 1–2 | 50–100 | 1–2 |
| 500 | 5–10 | 50–100 | 5–10 |
| 2,000 | 20–40 | 50–100 | 20–40 |
| 5,000+ | 50–100 | 50–100 | 50–100 |

## Notebook Inventory

| Notebook | Purpose | Key Parameters |
|----------|---------|----------------|
| `00_setup_config_tables` | Creates config + control tables; seeds sample data | `catalog`, `schema` |
| `01_generate_table_list` | Task 1: Reads config, emits JSON table list as task value | `catalog`, `schema` |
| `02_process_single_table` | Task 2: Incremental read → filter → APPEND or MERGE write | `catalog`, `schema`, `checkpoint_base`, `input` |
| `03_test_schema_evolution` | Validates new-column schema evolution end-to-end | `catalog`, `schema`, `checkpoint_base` |
| `04_create_job_definition` | Generates Lakeflow Job JSON with For Each task | `catalog`, `schema`, `checkpoint_base`, `notebook_base` |
| `05_cleanup` | Drops test tables (commented out for safety) | `catalog`, `schema`, `checkpoint_base` |
| `06_pydabs_bundle_generator` | Generates PyDABs bundle for CI/CD deployment | `catalog`, `schema` |

## Project Structure

```
incremental-filter-pipeline/
├── infra/                          # Terraform infrastructure
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── asset-bundles/                  # Databricks Asset Bundles
│   ├── databricks.yml
│   └── resources/
│       ├── jobs/
│       └── notebooks/
├── apps/                           # Databricks Apps (placeholder)
├── dashboards/                     # Lakeview dashboards (placeholder)
├── src/
│   └── incremental_filter_pipeline/
│       └── __init__.py
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   └── integration/
├── notebooks/                      # All pipeline notebooks
│   ├── README                      # Architecture & documentation
│   ├── 00_setup_config_tables
│   ├── 01_generate_table_list
│   ├── 02_process_single_table
│   ├── 03_test_schema_evolution
│   ├── 04_create_job_definition
│   ├── 05_cleanup
│   └── 06_pydabs_bundle_generator
├── scripts/                        # Utility scripts
├── pyproject.toml
├── pytest.ini
├── variables.yml
├── env.example
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | Authentication token |
| `PIPELINE_CATALOG` | Target catalog (default: `main`) |
| `PIPELINE_SCHEMA` | Target schema (default: `tmp`) |
| `CHECKPOINT_BASE` | Checkpoint path (default: `/tmp/incremental_filter_pipeline/checkpoints`) |

### Asset Bundle Targets

| Target | Catalog | Schema | Description |
|--------|---------|--------|-------------|
| `dev` (default) | `main` | `tmp` | Development/testing |
| `staging` | `main` | `staging` | Pre-production validation |
| `prod` | `my_catalog` | `pipeline_meta` | Production workloads |

## Key Design Decisions

1. **Control table is read fresh every run** — guarantees the latest exclusion list is always used
2. **`trigger(availableNow=True)`** — batch-like economics with streaming semantics
3. **Checkpoints are per-source-table** — stored at `{checkpoint_base}/{schema}_{table}`
4. **MERGE is more expensive than APPEND** — align `merge_keys` with table partitions for large targets
5. **Source tables need `delta.columnMapping.mode = name`** — required for dropped-column handling
6. **Target tables get `delta.enableTypeWidening = true`** — set automatically on first run
7. **CDF deletes only work in MERGE mode** — APPEND mode ignores `_change_type = delete`

## Security

- Sensitive record filtering is enforced at the pipeline layer via LEFT ANTI JOIN
- Control table access should be restricted via UC grants to authorized principals only
- UC credential vending provides secure, time-limited cloud credentials to external consumers

## License

MIT License — see [LICENSE](LICENSE) for details.

## Acknowledgments

Built on [Databricks](https://databricks.com/) with Delta Lake, Structured Streaming, Unity Catalog, and Lakeflow.

**Status**: Production-Ready | **Last Updated**: 2026-04-28 | **Version**: 0.1
