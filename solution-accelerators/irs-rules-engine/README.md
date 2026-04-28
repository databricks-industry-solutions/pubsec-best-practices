# IRS Return Review Program — Rules Engine POC

A Databricks-native replacement for **FICO Blaze Advisor**, built on open standards (DMN) and open source (Apache Drools) for the IRS Return Review Program. Rules are authored in a DMN editor, stored in Unity Catalog, and executed as a Spark SQL UDF at batch scale — 10M returns scored in **~3:36 cold / ~2:21 warm** on a 4-node cluster with zero errors. Adding a new rule is a DMN edit; the notebook never changes.

> **Disclaimer:** This is a proof-of-concept. All tax return data in this repository is **synthetic**. No real IRS data, taxpayer information, or PII is used anywhere in the codebase.

## Quick stats

| | |
|---|---|
| Rules | 21 fraud-detection rules, 6 categories, grounded in **IRM 25.1.2** |
| Engine | Apache Drools 9.44.0 (DMN Level 3 conformance, FEEL) |
| Scale | 10,000,000 synthetic returns in **~3:36 cold / ~2:21 warm** on `c5.4xlarge × 4` (64 cores) |
| Thresholds | PASS < 30 &middot; REVIEW 30–59 &middot; FLAG ≥ 60 |
| License | Apache 2.0 |

## Architecture at a glance

1. **Author** — DMN XML authored in the React rule-editor app (Apache KIE DMN Editor component) with a FastAPI backend, deployed as a Databricks App.
2. **Store** — DMN files are saved to a Unity Catalog Volume (`/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/`) and registered in the `rule_versions` Delta table along with a v2 `binding_json` (names the `input_view` and lists which DMN decisions land in which output columns) and an `input_view` field.
3. **Input contract** — `input_view` points at a Unity Catalog table, view, or materialized view whose columns name-match the DMN inputs (lowercase snake_case). All flattening, defaulting, and derivation lives in that view's SQL — the binding has no joins, no per-column SQL.
4. **Promote** — A row in `rule_versions` is marked `ACTIVE`; batch scoring picks up the latest `ACTIVE` version (or an explicit `version_id`). `notebooks/00_validate_binding.py` runs every pre-flight check the scoring job runs, without scoring — wire it into a promotion gate.
5. **Score** — A Spark Scala SQL UDF (`drools_score`) compiles the DMN once per task (broadcast XML + `ConcurrentHashMap` cache) and returns a `Map[String,String]` of every decision result. The notebook reads `spark.table(input_view)`, builds the input map by name-matching, evaluates once per row, then extracts each requested decision into the right output column.
6. **Shaded runtime** — Drools, ANTLR, Eclipse JDT, XStream, and Commons are all packaged into `drools-dmn-shaded-2.0.0.jar` to avoid classpath conflicts on DBR 17.3.

See [`DROOLS_DATABRICKS_ARCHITECTURE.md`](./DROOLS_DATABRICKS_ARCHITECTURE.md) for the deep dive (the five problems we solved, the UDF pattern, scaling story).

## Repo layout

```
notebooks/              # 00 validate binding, 01 UC bootstrap, 02 synthetic data,
                        # 04 batch scoring, 06 build JAR, 08 cleanup archived MVs,
                        # _binding_preflight.py (shared helper)
apps/rules-editor/      # Databricks App — React + Vite frontend, FastAPI backend
  client/               # KIE DMN Editor UI
  server/               # FastAPI — routes for DMN CRUD, versioning, evaluation
  dmn/                  # Seed DMN files (v2, v3-irm, advanced)
drools-shaded/          # Maven pom for the shaded Drools fat JAR
resources/              # Lakeview dashboard definition + bundle resources
databricks.yml          # Databricks Asset Bundle entrypoint
```

## Getting started

### Option 1 — Run the scoring pipeline on Databricks

Prerequisites: a Databricks workspace with Unity Catalog, the Databricks CLI configured with a profile (the bundle defaults to a profile named `services-bureau` — replace with your own in `databricks.yml`), and permission to create a cluster.

1. Configure your CLI profile (substitute your own profile name):
   ```bash
   databricks auth login --profile <your-profile>
   ```
2. Deploy the bundle:
   ```bash
   databricks bundle deploy --profile <your-profile>
   ```
3. On a cluster running DBR 17.3 with the shaded JAR attached as a library, run the notebooks in order:
   - `06_build_drools_shaded_jar.py` — first time only: builds `drools-dmn-shaded-2.0.0.jar` on-cluster and copies it to the UC Volume. Rebuild only when upgrading Drools.
   - `01_setup_catalog.py` — creates the catalog, schema, and DMN volume.
   - `02_synthetic_data_10M.py` — generates 10M synthetic returns (24 columns), the `rule_versions` / `scored_results` / `evaluation_log` Delta tables, and the `scoring_input_v3_1` materialized view.
   - `00_validate_binding.py` *(optional)* — pre-flight a `version_id` before promoting it (DMN compile, binding validation, MV column coverage). Wire into a promotion gate.
   - `04_batch_scoring.py` — binding-driven batch scoring against the `ACTIVE` DMN. Deployed as job `834669542144086`.
   - `08_cleanup_archived_mvs.py` *(periodic)* — lists `scoring_input_*` materialized views by reference status and drops candidates older than `retention_days` (default 30) when `cleanup_confirm=true`.

### Option 2 — Run the rule-editor app

The editor can run locally or be deployed as a Databricks App. See [`apps/rules-editor/README.md`](./apps/rules-editor/README.md) for setup, local dev, and deployment instructions.

## Configuration

- **Asset Bundle:** `databricks.yml` — targets `dev` (default) and `prod`. Both reference a workspace profile by name; replace with your own.
- **Cluster:** `Drools Runner v2` — DBR 17.3.x-scala2.13, Single User, driver `m5d.xlarge`, 4× `c5.4xlarge` workers fixed, **Standard runtime** (not Photon — Drools is opaque to Catalyst), `drools-dmn-shaded-2.0.0.jar` attached as a library. No init scripts.
- **Storage:** Unity Catalog — catalog `services_bureau_catalog`, schema `irs_rrp`. Delta tables `tax_returns`, `scored_results`, `rule_versions`, `evaluation_log`. DMN files and the shaded JAR live in the `dmn_rules` volume.
- **App environment:** `apps/rules-editor/app.yaml` sets `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CLUSTER_ID`, and `SCORING_NOTEBOOK_PATH` for the deployed app.

## Open design questions

> **Multi-rule-set support — decision pending.** Today, exactly one
> `ACTIVE` rule version is supported at a time. Running multiple rule
> sets concurrently (e.g. individual / business / digital-asset) is on
> the table, but the right *result shape* (per-set tables vs. a unified
> `scored_results` with a `rule_set_name` column vs. a hybrid view)
> depends on how analysts and dashboards consume the output. Decision
> deferred until consumption patterns are clearer.

## Further reading

| Doc | What's in it |
|---|---|
| [`DROOLS_DATABRICKS_ARCHITECTURE.md`](./DROOLS_DATABRICKS_ARCHITECTURE.md) | Deep architecture: shaded JAR, UDF caching pattern, scaling to 10M, cluster sizing |
| [`IRS_RULES_ENGINE_ARCHITECTURE.md`](./IRS_RULES_ENGINE_ARCHITECTURE.md) | Broader POC story — mapping Blaze Advisor capabilities to Databricks equivalents |
| [`ADDING_INPUTS.md`](./ADDING_INPUTS.md) | How to add rules, inputs, and output tables — DMN edit + (sometimes) input-view edit + promote |
| [`apps/rules-editor/README.md`](./apps/rules-editor/README.md) | Rule-editor app setup and development |

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
