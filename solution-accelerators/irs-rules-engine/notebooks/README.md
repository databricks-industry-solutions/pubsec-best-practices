# IRS RRP Notebooks — Batch Scoring Pipeline

The numbered notebooks form the end-to-end Drools + DMN batch pipeline
for the IRS Return Review Program POC. A v2 binding (one row in
`rule_versions`) ties together a DMN file in a UC Volume and an
`input_view` — a Unity Catalog **table, view, or materialized view**
that is the input contract. The current deployed pipeline uses a
materialized view (`scoring_input_v3_1`); plain tables and views also
work and require no refresh.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Catalog `services_bureau_catalog` (or permission to create schemas/volumes in it)
- Single-User cluster with the Drools shaded JAR attached. Recommended config
  (`Drools Runner v2`):
  - 4× `c5.4xlarge`, Standard runtime (not Photon — Drools is opaque to Catalyst)
  - DBR `17.3.x-scala2.13`
  - Library: `/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/drools-dmn-shaded-2.0.0.jar`
    (built by notebook 06; Drools 9.44.0.Final, shaded ANTLR/Eclipse/XStream/commons)
  - Java 17 `--add-opens` flags on driver and executor extraJavaOptions
- Databricks CLI profile configured (set `DATABRICKS_CONFIG_PROFILE` or pass `--profile`)

## Notebook sequence

| # | Notebook | Purpose | Reads | Writes | When to run |
| --- | --- | --- | --- | --- | --- |
| 00 | `00_validate_binding.py` | Standalone pre-flight: takes a `version_id` widget, compiles its DMN, parses its v2 binding, runs every check the scoring notebook runs (JSON schema, MV column coverage, decision name match, expression whitelist) — without scoring. Exits `SUCCESS:` or `FAILED:`. | `rule_versions`, DMN file, `input_view` MV | — | Before promoting `DRAFT → ACTIVE`. Wire into a promotion gate job. |
| 01 | `01_setup_catalog.py` | Bootstrap UC: `USE` catalog, create schema `irs_rrp`, create volume `dmn_rules`. DMN files are uploaded by the rules-editor app, not this notebook. | — | `services_bureau_catalog.irs_rrp` schema, volume `dmn_rules` | Once, first time. |
| 02 | `02_synthetic_data_10M.py` | Generate **10,000,000** synthetic returns using Spark-native `rand`/`randn` (no Python loops). 24 columns covering all six IRM 25.1.2 categories. Also creates `scored_results`, `evaluation_log`, `rule_versions` (with `binding_json` + `input_view` columns) Delta tables, and the `scoring_input_v3_1` materialized view. Seeds the v3.1-irm row as ACTIVE with a v2 binding. | — | `tax_returns` (10M rows), `scoring_input_v3_1` MV, `scored_results`, `evaluation_log`, `rule_versions` (v3.1-irm seeded) | After 01. Re-run only when regenerating the synthetic dataset. |
| 04 | `04_batch_scoring.py` | **Main production scoring notebook.** Binding-driven; the binding's `input_view` can be a TABLE, VIEW, or MATERIALIZED_VIEW. Loads the `ACTIVE` (or widget-selected) DMN + binding from `rule_versions`, validates the binding via the shared `preflight()` helper, registers a generic `Map[String,String] → Map[String,String]` Spark UDF (executor-cached `(model, runtime)`), repartitions to ~4× cores, scores once per row, writes each binding output table. ~2:21 warm for 10M rows on the Drools Runner cluster. | `rule_versions`, DMN volume, `input_view` (table/view/MV) | All binding `outputs.<table>` (typically `scored_results`) | Every full scoring run. Deployed as job `834669542144086`. |
| 06 | `06_build_drools_shaded_jar.py` | Build `drools-dmn-shaded-2.0.0.jar` (Drools 9.44.0.Final, relocated ANTLR/Eclipse/XStream/commons, SLF4J + Jackson stripped) via embedded `pom.xml` + Maven 3.9.9 on the driver, then copy to `/Volumes/.../dmn_rules/`. | — | `dmn_rules/drools-dmn-shaded-2.0.0.jar` | **Only once** to build the JAR, or when upgrading the Drools version. Restart the Drools Runner cluster after to pick up the new JAR. |
| 08 | `08_cleanup_archived_mvs.py` | Lists every `scoring_input_*` MV in the schema and groups by reference status (active / draft / archived-recent / cleanup-candidate / orphan). With `cleanup_confirm=true`, drops candidates older than `retention_days` (default 30). Default run is dry-run / list-only. | `rule_versions`, `information_schema.tables` | Drops MVs only when `cleanup_confirm=true` | Periodically (e.g. monthly) to prune archived MVs. Always reviews dry-run first. |

The unnumbered `_binding_preflight.py` is a shared helper module imported
by 00 and 04 via `%run`. Don't run it directly.

## Running 04 as a job

`04_batch_scoring.py` is deployed as job
`834669542144086` — **IRS Rules Engine — Batch Scoring**. The rules-editor app
triggers it via `jobs.run_now(...)` with `notebook_params`, which runs as the
job owner (not the app service principal) so that the personal cluster is
usable.

Widget parameters (all optional, all strings; blank = default):

| Widget | Default | Meaning |
| --- | --- | --- |
| `proof_limit` | blank (= all rows) | Score only the first N rows. Used for quick proof runs from the UI. |
| `version_id` | blank (= `ACTIVE`) | Explicit DMN version from `rule_versions` to load; otherwise falls back to the current `ACTIVE` row. |
| `skip_refresh` | `false` | No-op (kept for backwards compatibility). The notebook never refreshes the input — `REFRESH MATERIALIZED VIEW` requires DBSQL Pro/Serverless and so happens out-of-band on a warehouse. |

On exit the notebook emits `SUCCESS: scored=... dmn_version=...` (or
`FAILED: ...` — including a hard gate that detects 100%-null decision columns,
which catches DMN COLLECT/SUM nulls that propagate through composite
expressions) via `dbutils.notebook.exit`, which the app surfaces in run
history.

## Expected outputs

All under `services_bureau_catalog.irs_rrp`:

- `tax_returns` — 10M synthetic returns, 24 columns
- `scoring_input_v3_1` — materialized view, the input contract for v3.1-irm
- `scored_results` — `return_id, filing_status, agi, audit_score, recommended_action, dmn_version, scored_at`
- `rule_versions` — DMN version governance (`DRAFT` / `ACTIVE` / `ARCHIVED`) with `binding_json` + `input_view`
- `evaluation_log` — audit-trail schema (created by 02; populated by extensions)
- Volume `dmn_rules` — DMN XML files and the Drools shaded JAR

## Adding rules, inputs, or outputs

Edit the DMN + the input view (table / view / MV), **not the notebook**.
See [`../ADDING_INPUTS.md`](../ADDING_INPUTS.md).
