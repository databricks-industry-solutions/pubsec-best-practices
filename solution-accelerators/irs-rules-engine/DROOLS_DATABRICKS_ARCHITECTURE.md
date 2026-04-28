# Drools DMN on Databricks — Architecture & Implementation Guide

## Overview

This document describes the architecture for running Apache Drools DMN (Decision Model and Notation) rule evaluation at batch scale on Databricks, replacing FICO Blaze Advisor for the IRS Return Review Program.

The pipeline scores 10,000,000 synthetic tax returns against 21 DMN decision rules (IRM 25.1.2) plus a chained `Recommended Action` decision in **~3:36 cold / ~2:21 warm** on a 4-node `c5.4xlarge` cluster (64 cores). Zero errors. No silent failures.

## Why Drools on Databricks

FICO Blaze Advisor is being replaced. The IRS needs an open-source, open-standard rules engine that:

- Accepts DMN files exported from Blaze Advisor (no translation layer)
- Evaluates FEEL expressions at DMN Level 3 conformance
- Supports COLLECT+SUM hit policy (additive risk scoring)
- Runs on Databricks alongside the data it scores

**Drools 9.44.0 (Apache KIE)** is the only engine that meets all four. ZEN Engine (used in the M001 prototype) lacks DMN support. SpiffWorkflow implements only 2 of 9 hit policies with no FEEL. Drools has 20+ years of government adoption and native DMN/FEEL support.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Databricks Cluster (JVM)                         │
│                        DBR 17.3, Single User                            │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  drools-dmn-shaded-2.0.0.jar (cluster library)                   │  │
│  │                                                                   │  │
│  │  Drools 9.44.0.Final                                              │  │
│  │  ├── kie-dmn-core (DMN compiler + runtime)                        │  │
│  │  ├── kie-dmn-feel (FEEL expression parser)                        │  │
│  │  ├── drools-engine, drools-mvel, drools-core                      │  │
│  │  │                                                                │  │
│  │  Shaded (relocated to avoid classpath conflicts):                 │  │
│  │  ├── org.antlr  → drools.shaded.antlr                            │  │
│  │  ├── org.eclipse → drools.shaded.eclipse (JDT compiler)          │  │
│  │  ├── com.thoughtworks.xstream → drools.shaded.xstream            │  │
│  │  └── org.apache.commons → drools.shaded.commons                  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌─────────────┐    ┌───────────────────────┐    ┌────────────────────┐│
│  │ DMN XML      │───▶│ Broadcast[String]     │───▶│ Spark SQL UDF      ││
│  │ (UC Volume)  │    │ ConcurrentHashMap     │    │ drools_score       ││
│  │ + binding    │    │ cache keyed by        │    │ Map[String,String] ││
│  │ JSON +       │    │ xml.hashCode →        │    │ → Map[String,      ││
│  │ input_view   │    │ (DMNModel, Runtime)   │    │       String]      ││
│  └─────────────┘    │ compile-once per task │    └─────────┬──────────┘│
│                      └───────────────────────┘              │           │
│                                                             ▼           │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Spark Engine — driven by binding_json from rule_versions        │  │
│  │  spark.table(input_view)        ← MV is the input contract       │  │
│  │    .repartition(cores*4)                                         │  │
│  │    .withColumn("score_map", drools_score(map_from_columns(...))) │  │
│  │    .withColumn("audit_score", score_map["Tax Review Scoring"])   │  │
│  │    .withColumn("recommended_action",                             │  │
│  │                score_map["Recommended Action"])                  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Unity Catalog — services_bureau_catalog.irs_rrp                        │
│                                                                         │
│  tax_returns (10M raw rows)                                             │
│         │                                                               │
│         ▼   (table / view / MV — refresh out-of-band on a warehouse)    │
│  scoring_input_v3_1 (MV, 10M rows, 21 columns name-matched to DMN)      │
│         │                                                               │
│         ▼   (drools_score UDF, 1 eval/row, all decisions in one map)    │
│  scored_results (10M rows)                                              │
│  ├── return_id, filing_status, agi   (carry_columns)                    │
│  ├── audit_score                     (decision: Tax Review Scoring)     │
│  ├── recommended_action (PASS/REVIEW/FLAG) (decision: Recommended Action)│
│  ├── dmn_version, scored_at          (post_columns)                     │
│                                                                         │
│  rule_versions                   dmn_rules/ (UC Volume)                 │
│  ├── version_id                  ├── irs_tax_review_v3_1_irm.dmn        │
│  ├── status (ACTIVE/ARCHIVED)    └── drools-dmn-shaded-2.0.0.jar        │
│  ├── dmn_path                                                           │
│  ├── binding_json (v2)                                                  │
│  └── input_view                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Input Contract: Materialized View + Binding

The pipeline has two contracts the IRS owns:

1. **The DMN** — declares input names (`agi`, `filing_status`, ...) and decisions (`Tax Review Scoring`, `Recommended Action`).
2. **A Unity Catalog Materialized View** named in the binding's `input_view` field. Every DMN input must appear as a same-named column on this view (lowercase snake_case is the convention).

The binding (`rule_versions.binding_json`, v2) is small — ~20 lines — and only describes:

- Which MV to read (`input_view`)
- Which DMN decisions land in which Delta-table columns (`outputs`)
- Optional per-input overrides (`columns`, rare — for unit conversion)

There are **no joins, no per-column SQL inside the binding**. All flattening, defaulting, and derivation lives in the MV's SQL, which the IRS owns and refreshes. This means:

- **Adding a column** is an MV change (a new versioned MV: `scoring_input_v3_2`), not a binding-syntax exercise.
- **Validation is mechanical**: pre-flight (`notebooks/_binding_preflight.py`) DESCRIBEs the MV, asserts every DMN input has a name-matched column, and runs the same checks `04_batch_scoring.py` runs.
- **Rollback is free**: each DMN version points at its own versioned MV, so promoting a new rule never breaks an old one.
- **MV cleanup is safe**: `notebooks/08_cleanup_archived_mvs.py` lists MVs by reference status (active / draft / archived-recent / cleanup-candidate / orphan) and only drops candidates older than the retention window (default 30 days) when `cleanup_confirm=true`.

The pre-flight whitelist also constrains what SQL can appear in optional `columns` overrides and `post_columns` — only `coalesce`, `cast`, `current_timestamp`, conditionals, and other read-only scalar functions are allowed; `select`, `from`, semicolons, and comments are rejected.

## The Five Problems We Solved

The initial implementation had a batch scoring pipeline that appeared to hang. The root cause was not one bug but five independent issues, compounding each other behind a blanket `catch (Exception e) → -1.0` handler that silently swallowed every failure. Some of these problems were specific to the Drools 8.30.0 we started on; the shaded JAR was later upgraded to 9.44.0.Final, but the fixes (relocation, explicit inputData, error propagation, separate lazy vals) all carried forward and are still load-bearing today.

### Problem 1: ANTLR Version Conflict

**The conflict:** Drools uses ANTLR4 to parse FEEL expressions. The ANTLR version bundled with Drools must be wire-format compatible with the ANTLR on the Databricks classpath. Different DBR versions ship different ANTLR versions:

| DBR | System ANTLR | ATN Format |
|-----|-------------|------------|
| 16.2 | 4.9.3 | v3 |
| 17.3 | 4.10+ | v4 |

Drools 8.30.0 bundles ANTLR 4.9.2 (ATN format v3). On DBR 17.3, the system ANTLR classes win the classpath race and try to deserialize format-v3 ATN data as format-v4 → `UnsupportedOperationException: Could not deserialize ATN`.

**The fix:** Shade (relocate) ANTLR inside the fat JAR. The Maven shade plugin rewrites all `org.antlr.*` references to `drools.shaded.antlr.*`, so Drools uses its own bundled ANTLR runtime regardless of what's on the system classpath. This makes the JAR portable across any DBR version.

```xml
<relocation>
  <pattern>org.antlr</pattern>
  <shadedPattern>drools.shaded.antlr</shadedPattern>
</relocation>
```

### Problem 2: DMN Input Variable Resolution

**The failure:** Drools 8.30.0's FEEL compiler in strict mode requires all variables referenced in decision table input expressions to be declared as `<inputData>` elements with `<informationRequirement>` links to the decision. Without them: `Error compiling FEEL expression 'agi': Unknown variable 'agi'`.

The original DMN XML only had a `<decision>` with a `<decisionTable>` — no `<inputData>` declarations. Drools 8.44.0 was lenient about this; 8.30.0 follows the DMN spec strictly.

**The fix:** Add explicit `<inputData>` elements for every input variable, and wire them to the decision via `<informationRequirement>`:

```xml
<inputData id="input_agi" name="agi">
  <variable id="var_agi" name="agi" typeRef="number"/>
</inputData>

<decision id="tax_review_scoring" name="Tax Review Scoring">
  <informationRequirement id="ir_agi">
    <requiredInput href="#input_agi"/>
  </informationRequirement>
  <!-- ... decision table ... -->
</decision>
```

### Problem 3: Silent Error Swallowing

**The trap:** The original UDF wrapped everything in `catch (Exception e) → -1.0`. Every failure — ANTLR conflicts, thread-safety races, Java 17 module access violations, FEEL compilation errors — produced the same symptom: rows scored as -1.0, no error in the job log, pipeline appears to "hang" or produce wrong results silently.

**The fix:** Errors propagate as `RuntimeException` from the UDF, which surfaces as a Spark task failure in the UI. A post-scoring verification gate fails the notebook if any decision column comes back 100% null — which catches the FEEL-null trap where a COLLECT/SUM with no rules firing returns null and propagates through composite literal sums.

### Problem 4: Scala Lazy Val Deadlock

**The deadlock:** The `DroolsHolder` singleton initially used a destructured tuple for the compiled model and runtime:

```scala
// DEADLOCKS — do not use this pattern
@transient private lazy val (_model, _runtime) = {
  val model = new DMNCompilerImpl().compile(defs)
  val runtime = new DMNRuntimeImpl(null)
  (model, runtime)
}
```

Scala implements `lazy val` with a `synchronized` monitor on the enclosing object. Tuple destructuring binds both fields to a single monitor. During `DMNCompilerImpl.compile()`, Drools' ASM bytecode generator triggers classloading that re-enters the same monitor → classic deadlock. The thread spins forever.

**The fix:** Split into two independent `@transient lazy val` fields. Each gets its own monitor:

```scala
@transient private lazy val _model: DMNModelImpl = {
  // Drools compilation happens here — may trigger re-entrant classloading
  new DMNCompilerImpl().compile(defs).asInstanceOf[DMNModelImpl]
}

@transient private lazy val _runtime: DMNRuntimeImpl =
  new DMNRuntimeImpl(null)
```

### Problem 5: Classpath Conflicts (Eclipse, XStream, Commons)

**The conflicts:** Beyond ANTLR, Drools' transitive dependencies include Eclipse JDT (used by MVEL), XStream, and Apache Commons — all of which have different versions on the Databricks classpath.

**The fix:** Shade all of them in the fat JAR:

```xml
<relocation><pattern>org.eclipse</pattern>
  <shadedPattern>drools.shaded.eclipse</shadedPattern></relocation>
<relocation><pattern>com.thoughtworks.xstream</pattern>
  <shadedPattern>drools.shaded.xstream</shadedPattern></relocation>
<relocation><pattern>org.apache.commons</pattern>
  <shadedPattern>drools.shaded.commons</shadedPattern></relocation>
```

SLF4J classes are excluded entirely (Databricks provides its own logging implementation).

## The UDF Pattern

The UDF compiles the DMN model and creates a runtime once per task — per-row compile is 50-500ms and hits thread-safety issues in `DMNCompilerImpl`. The current implementation is a **generic `Map[String,String] → Map[String,String]` UDF** with an inline `ConcurrentHashMap` cache. Inputs are wired by name from the binding (no fixed parameter list), so adding a new DMN input never touches the notebook:

```scala
val _xmlBc = spark.sparkContext.broadcast(spark.conf.get("irs.dmn.xml"))

spark.udf.register("drools_score", {
  val _cache = new java.util.concurrent.ConcurrentHashMap[Int,
    (org.kie.dmn.core.impl.DMNModelImpl, org.kie.dmn.core.impl.DMNRuntimeImpl)]()

  (inputs: Map[String, String]) => {
    val xml = _xmlBc.value
    val (model, runtime) = _cache.computeIfAbsent(xml.hashCode, _ => {
      val defs = DMNMarshallerFactory.newDefaultMarshaller()
                   .unmarshal(new StringReader(xml))
      val m    = new DMNCompilerImpl().compile(defs).asInstanceOf[DMNModelImpl]
      (m, new DMNRuntimeImpl(null))
    })
    val ctx = new DMNContextImpl()
    // For each declared DMN input, coerce the string to the FEEL type the
    // model expects (BigDecimal for number, java.lang.Boolean for boolean,
    // String otherwise). Skip absent inputs so the DMN sees them as null.
    model.getInputs.asScala.foreach { input =>
      Option(inputs.get(input.getName).orNull).foreach { v =>
        ctx.set(input.getName, coerce(v, input.getType))
      }
    }
    runtime.evaluateAll(model, ctx)
      .getDecisionResults.asScala
      .map(dr => dr.getDecisionName -> Option(dr.getResult).map(_.toString).orNull)
      .toMap
  }
})
```

**How it works:**

- `_xmlBc` is a `Broadcast[String]` — serializable, ships once to each executor
- The inline `ConcurrentHashMap` caches compiled `(model, runtime)` per task, keyed by `xml.hashCode`. A promoted DMN gets a new hashCode and rebuilds
- The lambda captures only `_xmlBc` and `_cache` — both `val`s in the same cell, so no `$iw` cross-cell class ref pollutes the closure
- Returning **all** decision results in one map means chained decisions like `Recommended Action` (which depends on `Tax Review Scoring`) cost zero extra evaluations — the notebook just extracts both keys from the same map
- The notebook builds the input map by reading `spark.table(input_view)` — every column is already a DMN input, name-matched. It casts each output decision back to the FEEL-declared type (number→double, boolean→boolean, etc.) for the Delta write
- Combined with `returns.repartition(cores*4)`, each partition pays at most one DMN compile, then scores ~30k rows serially

## The Shaded JAR

Maven resolution fails on Databricks clusters (missing `xmlpull:1.2.0` transitive dependency from Maven Central). The shaded JAR bundles all Drools dependencies into a single file and relocates conflicting namespaces.

Built on-cluster via `notebooks/06_build_drools_shaded_jar.py`:

```
drools-dmn-shaded-2.0.0.jar (~45MB)
├── org.kie.dmn.*         (Drools DMN, unmodified)
├── org.drools.*          (Drools core, unmodified)
├── drools.shaded.antlr.* (relocated from org.antlr)
├── drools.shaded.eclipse.* (relocated from org.eclipse)
├── drools.shaded.xstream.* (relocated from com.thoughtworks.xstream)
├── drools.shaded.commons.* (relocated from org.apache.commons)
└── META-INF/kie.conf     (merged from all Drools modules)
```

Stored in UC Volume at `/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/drools-dmn-shaded-2.0.0.jar`. Installed as a cluster library — no init scripts needed.

## The Rules

21 IRS fraud detection rules grounded in IRM Part 25.1.2 (Recognizing and Developing Fraud), implemented as a DMN COLLECT+SUM decision table. Each rule contributes independently to a cumulative audit score, with maximum possible score around 200.

**Six IRM categories:**

| Category | IRM Section | Rules |
|----------|-------------|-------|
| Income | 25.1.2.2 | AGI thresholds, foreign income, reported-income gap, W2 vs AGI ratio |
| Deductions | 25.1.2.3 | Deduction ratio, charitable ratio, dependents claimed |
| Filing / Conduct | 25.1.2.4 | Filing status (MFS), years not filed, amended return count, prior audit |
| Books / Records | 25.1.2.5 | Multiple W2s, employment-type mismatches |
| Employment Tax | 25.1.2.6 | Self-employment income thresholds |
| Digital Asset | 25.1.2.7 | Digital asset transaction count, proceeds |

**Scoring thresholds:**
- **PASS** (score < 30): No action needed
- **REVIEW** (30 ≤ score < 60): Manual review recommended
- **FLAG** (score ≥ 60): High-priority audit candidate

## Scaling to 10M Rows

The pipeline originally used a Spark SQL UDF that called Drools `evaluateAll()` per row. At small scale (100K rows) it looked fine — but the Drools call is opaque to Spark, so with only 8 partitions for 10M rows we got 8 giant tasks showing zero progress in the Spark UI. The job appeared hung even when it was actually progressing.

**Three changes unlocked 10M:**

1. **Repartition before scoring.** `returns.repartition(max(defaultParallelism*4, rows/40000))` produces ~256 small tasks that each finish in seconds. The Spark UI shows steady progress instead of 8 opaque tasks. This alone gave a 5.6× speedup at 1M rows.
2. **Wider cluster.** `c5.4xlarge × 4` (64 cores) replaced `i3.xlarge × 2–8 autoscale` (8 cores typical). Drools is CPU-bound and doesn't shuffle, so compute-optimized nodes win on cost per core. 4× fixed workers beats autoscale for a short batch job — autoscale can't ramp fast enough.
3. **Drop Photon.** Photon cannot accelerate JVM UDFs (Drools is opaque to Catalyst). On `Drools Runner v2` the runtime is `STANDARD`, saving ~60% DBU with zero throughput cost.

**Results at scale:**

| Config | Rows | Duration | Notes |
|---|---|---|---|
| i3.xlarge × 2–8, Photon, no repartition | 10M | 513s | Original baseline — 8 opaque tasks |
| i3.xlarge × 2–8, Photon, + repartition | 1M | 80s (was 455s — 5.6×) | Cores still saturated at 8 |
| i3.xlarge × 2–8, Photon, + repartition | 10M | 449s | Bottlenecked on 8 cores |
| **c5.4xlarge × 4, Standard, + repartition** (cold) | **10M** | **216s (3:36)** | Fresh cluster, first run |
| c5.4xlarge × 4, Standard, + repartition (warm) | 10M | 141s (2:21) | JIT + DMN compile cache hot |
| c5.4xlarge × 4, Standard, + repartition (warm) | 1M | 45s | Regression test |
| c5.4xlarge × 4, Standard, + repartition (warm) | 10K | ~2s scoring + overhead | Smoke test |

Zero errors across all runs. All 21 rules executed. Use the **cold 216s** figure as the conservative planning number — that's what a scheduled batch job will see after cluster auto-termination.

## Cluster Configuration

Two clusters are usable:

**Drools Runner v2 (recommended, current)** — sized for 10M-row batch runs.

| Setting | Value |
|---------|-------|
| DBR | 17.3.x-scala2.13 |
| Access mode | Single User |
| Driver | m5d.xlarge (4 cores, 16GB) |
| Workers | c5.4xlarge × 4 fixed (16 cores × 4 = 64 cores) |
| Runtime engine | STANDARD (not Photon — Drools UDF is opaque to Catalyst) |
| Availability | SPOT_WITH_FALLBACK, first_on_demand=1 |
| EBS | 1 × 100GB GP SSD per worker |
| Auto-termination | 30 minutes |
| Library | `drools-dmn-shaded-2.0.0.jar` from UC Volume |
| Init scripts | None |

**Drools Runner (legacy)** — single-node-ish POC config, still works at small scale.

| Setting | Value |
|---------|-------|
| DBR | 17.3.x-scala2.13 |
| Node type | i3.xlarge, autoscale 2–8 workers |
| Runtime | Photon (overkill, not recommended) |
| Library | `drools-dmn-shaded-2.0.0.jar` |

## Key Files

| File | Purpose |
|------|---------|
| `notebooks/00_validate_binding.py` | Standalone pre-flight: validates a `version_id`'s DMN + binding + MV without scoring. Wire into a promotion gate. |
| `notebooks/01_setup_catalog.py` | UC catalog, schema, volume bootstrap |
| `notebooks/02_synthetic_data_10M.py` | 10M synthetic tax returns + `scoring_input_v3_1` MV + `rule_versions` seed (v3.1-irm v2 binding) |
| `notebooks/04_batch_scoring.py` | Production batch scoring — reads `input_view` (table/view/MV), runs pre-flight, generic map UDF, multi-output writes. Does **not** refresh MVs (that requires DBSQL Pro/Serverless and runs out-of-band on a warehouse). |
| `notebooks/06_build_drools_shaded_jar.py` | Builds the shaded JAR on-cluster (rare; only when upgrading Drools) |
| `notebooks/08_cleanup_archived_mvs.py` | Lists scoring input MVs by reference status; drops cleanup candidates older than `retention_days` (default 30) when `cleanup_confirm=true` |
| `notebooks/_binding_preflight.py` | Shared `preflight()` helper — JSON schema validation, expression whitelist, MV column coverage, decision name match |
| `/Volumes/.../dmn_rules/drools-dmn-shaded-2.0.0.jar` | The shaded Drools JAR |
| `/Volumes/.../dmn_rules/irs_tax_review_v3_1_irm.dmn` | The active DMN (v3.1-irm: 21 IRM rules + chained `Recommended Action`) |
| `services_bureau_catalog.irs_rrp.scoring_input_v3_1` | Materialized view — input contract for v3.1-irm |
| `services_bureau_catalog.irs_rrp.rule_versions` | Governance — `version_id`, `dmn_path`, `binding_json`, `input_view`, `status` (DRAFT/ACTIVE/ARCHIVED) |
