# IRS Rules Engine — Comprehensive POC Architecture

## The Story We're Telling

> "The IRS Return Review Program currently runs on FICO Blaze Advisor — a legacy,
> proprietary rules engine with vendor lock-in, limited scalability, and no native
> integration with the modern data platform. Here's how Databricks replaces it
> entirely — rules authoring, execution, governance, and monitoring — using open
> standards (DMN) and open source (Drools) on the same platform that already runs
> your data pipelines."

---

## What Blaze Advisor Does Today

| Capability | Blaze Advisor | Databricks Replacement |
|------------|--------------|----------------------|
| **Rule Authoring** | Blaze Advisor Rule Maintenance Application (RMA) — thick client or web UI | KIE DMN Editor (React) → Databricks App |
| **Rule Format** | Proprietary Blaze XML/PMML | DMN (open OMG standard) — exportable from Blaze |
| **Rule Execution** | Blaze runtime on dedicated servers | Drools DMN Engine on Databricks cluster JVM |
| **Batch Scoring** | Blaze batch jobs | Lakeflow Job → Spark UDF wrapping Drools → Delta table |
| **Real-time Scoring** | Blaze API endpoint | Model Serving endpoint (Drools in container) or cluster UDF |
| **Rule Versioning** | Manual file management | Unity Catalog Volumes + Delta table version history |
| **Governance** | Manual promotion process | Lakeflow pipeline: draft → test → validate → promote |
| **Audit Trail** | Separate logging system | Delta table with full lineage via Unity Catalog |
| **Monitoring** | Separate dashboards | Lakeview dashboard — native, no additional tooling |
| **Scale** | Fixed server capacity | Serverless Spark — autoscale to any volume |

---

## POC Components (What We Build)

### 1. Rule Editor App (Databricks App — React + FastAPI)

**What it does:**
- Visual DMN decision table editor (KIE DMN Editor — full open-source, Apache KIE project)
- Import DMN files (Blaze Advisor export → drag & drop)
- Create new rules from scratch
- Save versions to Unity Catalog Volume
- Test individual returns against rules (calls cluster for evaluation)
- View execution results inline

**Tech:** React frontend with KIE DMN Editor component, FastAPI backend, deployed as Databricks App.

**Already proven:** KIE DMN Editor renders, loads DMN XML, edits, exports XML.

### 2. Rule Execution Engine (Databricks Cluster + Lakeflow)

**What it does:**
- Loads DMN from Unity Catalog Volume
- Evaluates tax return facts via Drools DMN engine (Scala on JVM)
- Exposes as Spark SQL UDF for batch scoring
- Single-return evaluation for testing
- Writes scored results to Delta table

**Lakeflow Job pipeline:**
```
Trigger (schedule or manual)
  → Load latest active DMN from UC Volume
  → Read unscored returns from Delta table
  → Score via Drools UDF (parallelized across cluster)
  → Write results to scored_returns Delta table
  → Log execution metadata to audit_log Delta table
  → Update Lakeview dashboard
```

**Already proven:** Drools 9.44.0 compiles and evaluates DMN on Databricks cluster (DBR 17.3, Java 17, shaded JAR).

### 3. Governance Pipeline (Lakeflow + Delta Tables)

**What it does:**
- Rule versions stored as files in UC Volume + metadata in Delta table
- Promotion workflow: DRAFT → ACTIVE → ARCHIVED
- Pre-flight validator (`notebooks/00_validate_binding.py`) compiles the DMN, parses the binding, runs every check the scoring job runs (JSON schema, MV column coverage, decision name match, expression whitelist) — wire it into a promotion gate. Promotion fails if any check fails.
- Only one rule set ACTIVE at a time (mirrors IRS governance requirement)
- Full audit trail: who changed what, when, why

**Delta tables (under `<your-catalog>.<your-schema>`):**
```sql
-- Rule version metadata + binding
CREATE TABLE rule_versions (
  version_id STRING,
  rule_set_name STRING,
  dmn_path STRING,         -- UC Volume path to .dmn file
  binding_json STRING,     -- v2 binding: input_view + outputs (see ADDING_INPUTS.md)
  input_view STRING,       -- fully-qualified UC table / view / materialized view
  status STRING,           -- DRAFT | ACTIVE | ARCHIVED
  created_by STRING,
  created_at TIMESTAMP,
  promoted_by STRING,
  promoted_at TIMESTAMP,
  notes STRING
);

-- Evaluation audit log (schema defined; populated by extensions)
CREATE TABLE evaluation_log (
  evaluation_id STRING,
  rule_version_id STRING,
  return_id STRING,
  filing_status STRING,
  audit_score DOUBLE,
  recommended_action STRING,
  evaluated_at TIMESTAMP,
  job_run_id STRING
);
```

### 4. Monitoring Dashboard (Lakeview)

**What it shows:**
- **Scoring Distribution** — histogram of audit scores across all evaluated returns
- **Action Breakdown** — PASS / REVIEW / FLAG counts and percentages
- **Rule Fire Rates** — which rules fire most often (identifies dominant risk factors)
- **Job Run History** — Lakeflow execution timeline with success/fail indicators
- **Version Timeline** — which rule version was active when, with promotion events
- **Trend Analysis** — scoring patterns over time (do new rules change the distribution?)

**Built as:** Lakeview Dashboard connected to the Delta tables above.

### 5. Batch Processing Pipeline (Lakeflow)

**The production-scale pattern** — `notebooks/04_batch_scoring.py`, intended
to be deployed as a Lakeflow job. The notebook is **binding-driven**: the input view, the
output decisions/columns, and any post-write expressions all come from
`rule_versions.binding_json` (v2). Adding a rule is a DMN edit + (sometimes) a
new versioned input view; the notebook itself never changes.

```python
# 1. Resolve version (ACTIVE or explicit version_id widget) and pull the
#    DMN XML + v2 binding + input_view from rule_versions
row = spark.sql("""
    SELECT version_id, dmn_path, binding_json, input_view
    FROM <your-catalog>.<your-schema>.rule_versions
    WHERE status = 'ACTIVE' ORDER BY promoted_at DESC LIMIT 1
""").first()
binding = json.loads(row.binding_json)
dmn_xml = dbutils.fs.head(row.dmn_path, 1048576)
# input_view points at a UC table / view / materialized view whose columns
# name-match the DMN inputs (lowercase snake_case). All flattening, defaulting,
# and derivation lives in that view's SQL — the binding has no joins.

# 2. Compile DMN on driver (Scala cell), capture input + decision names

# 3. Pre-flight: shared helper validates JSON schema, MV column coverage,
#    decision name match, and expression whitelist. Fails fast on errors.
pf = preflight(spark, row.binding_json, dmn_inputs, dmn_decisions, ...)

# 4. Register generic Map[String,String] -> Map[String,String] UDF
#    (broadcasts the XML, ConcurrentHashMap caches compiled (model, runtime) per task)

# 5. Read the input view (no joins — the view IS the input contract),
#    repartition for parallelism, score once per row
df = (spark.table(pf.input_view)
        .repartition(max(default_parallelism * 4, n_rows // 40000))
        .withColumn("__score_map",
                    drools_score(F.create_map(*name_matched_pairs))))

# 6. For each binding output, extract the requested decisions, cast to FEEL
#    types, add post_columns (substituting __VERSION__), write with overwriteSchema
for out_name, out_spec in binding["outputs"].items():
    out_df = df.select(*carry_columns, *decision_columns, *post_columns)
    out_df.write.mode(out_spec.get("mode", "overwrite")) \
          .option("overwriteSchema", "true") \
          .saveAsTable(fq(out_name))

# 7. Exit gate: fail the run if any decision column is 100% null
#    (catches FEEL COLLECT/SUM nulls propagating through composite expressions)
```

---

## Demo Script (15 minutes)

### Minute 0-3: The Problem
"The IRS Return Review Program scores 150M+ returns per year using FICO Blaze Advisor.
Here's what that looks like today — and here's what it looks like on Databricks."

### Minute 3-6: Import Rules from Blaze
- Open the Rule Editor App
- Click Import → upload a DMN file (exported from Blaze)
- "These are your actual rules, in an open standard. No conversion needed."
- Show the decision table with 21 rules grounded in IRM 25.1.2 (Income, Deductions, Filing/Conduct, Books/Records, Employment Tax, Digital Asset)
- Point out: COLLECT+SUM hit policy, FEEL expressions — standard DMN

### Minute 6-8: Edit Rules
- Modify a threshold: change "AGI > 500,000" to "> 400,000"
- Add a new rule: "Crypto Income > $50,000 → score 20"
- Save as new version → stored in Unity Catalog
- "Business analysts do this. No code. No developer required."

### Minute 8-11: Execute on Databricks
- Click "Run Test" with a sample return → see score immediately
- Switch to Lakeflow → trigger batch scoring job
- Show the job running: "This is evaluating 10M returns right now — ~3:36 cold, ~2:21 warm."
- Job completes → scored results in Delta table
- "Same rules, but now running on Spark. Scales to any volume."

### Minute 11-14: Monitor & Govern
- Open Lakeview Dashboard
  - Scoring distribution: most returns PASS, some REVIEW, few FLAG
  - Rule fire rates: "High Income fires 12%, Foreign Income fires 3%"
  - Job history: "Last 5 runs, all successful, 10M returns scored in ~3:36 cold / ~2:21 warm on 4× c5.4xlarge (64 cores, Standard runtime)"
- Show governance:
  - Version history: v1 (original Blaze import), v2 (threshold change), v3 (crypto rule)
  - Promotion: "v3 is DRAFT. Promote to ACTIVE."
  - Validation job runs → "All test cases pass. Promoted."
  - Audit log: "Every evaluation is traced to a rule version."

### Minute 14-15: The Pitch
"One platform. Open standards. No vendor lock-in. Full audit trail.
The rules are in DMN — you can take them to any engine.
The data is in Delta — Unity Catalog governs it.
The jobs are in Lakeflow — observable and schedulable.
And it all runs on the same Databricks you already have."

---

## Status

All POC components are built. See [`DROOLS_DATABRICKS_ARCHITECTURE.md`](./DROOLS_DATABRICKS_ARCHITECTURE.md)
for the deep dive on what's running today (binding-driven `04_batch_scoring.py`,
chained `Recommended Action` decision, 10M rows in 2:21 warm on `c5.4xlarge × 4`).

The pre-flight validator (`notebooks/00_validate_binding.py`) is the promotion
gate — it compiles the DMN, parses the binding, and runs every check the
scoring job runs without scoring. A regression-style test-suite harness
(known inputs → expected outputs) is not yet wired up; pre-flight catches
structural breakage but not semantic drift across rule edits.
