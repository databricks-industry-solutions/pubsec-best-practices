# Databricks notebook source
# MAGIC %md
# MAGIC # IRS RRP — Batch Scoring (binding-driven, MV-as-input-contract)
# MAGIC
# MAGIC Loads the ACTIVE DMN + its v2 `binding_json` from `rule_versions`. The
# MAGIC binding names a single `input_view` (a UC Materialized View that the
# MAGIC IRS owns and refreshes) plus one or more `outputs`. There are no joins
# MAGIC or per-column SQL inside the binding — the MV is the contract.
# MAGIC
# MAGIC **Adding a new rule = edit the DMN + (sometimes) edit the MV.**
# MAGIC No notebook changes. No UDF changes. No deploys.

# COMMAND ----------

dbutils.widgets.text('proof_limit', '',     'Limit (blank = all)')
dbutils.widgets.text('version_id',  '',     'Version ID (blank = ACTIVE)')
dbutils.widgets.text('skip_refresh', 'false', 'skip_refresh (true|false)')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Resolve version, refresh MV, load DMN

# COMMAND ----------

import sys, os
sys.path.insert(0, os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())) if False else None
# Notebooks live in the same folder; use %run for the helper so it executes
# in this notebook's namespace.

# COMMAND ----------

# MAGIC %run ./_binding_preflight

# COMMAND ----------

import json

CATALOG = "services_bureau_catalog"
SCHEMA  = "irs_rrp"

_version_param = dbutils.widgets.get('version_id').strip()

if _version_param:
    rows = spark.sql(f"""
        SELECT version_id, dmn_path, binding_json, input_view
        FROM {CATALOG}.{SCHEMA}.rule_versions
        WHERE version_id = '{_version_param}'
    """).collect()
    if not rows:
        raise RuntimeError(f"Version '{_version_param}' not found")
else:
    rows = spark.sql(f"""
        SELECT version_id, dmn_path, binding_json, input_view
        FROM {CATALOG}.{SCHEMA}.rule_versions
        WHERE status = 'ACTIVE'
        ORDER BY promoted_at DESC LIMIT 1
    """).collect()
    if not rows:
        raise RuntimeError("No ACTIVE rule version found")

version_id  = rows[0]["version_id"]
dmn_path    = rows[0]["dmn_path"]
binding_raw = rows[0]["binding_json"]
input_view  = rows[0]["input_view"]
print(f"Version:    {version_id} ({'explicit' if _version_param else 'ACTIVE'})")
print(f"DMN:        {dmn_path}")
print(f"input_view: {input_view}")

if not binding_raw:
    raise RuntimeError(f"Version '{version_id}' has no binding_json. Backfill it before scoring.")

dmn_xml = dbutils.fs.head(dmn_path, 1048576)
spark.conf.set("irs.dmn.xml",     dmn_xml)
spark.conf.set("irs.dmn.version", version_id)

# COMMAND ----------

# Inspect input_view type so the operator sees what's actually being read.
# We never call REFRESH here — REFRESH MATERIALIZED VIEW is rejected on
# general compute (requires DBSQL Pro/Serverless). MV freshness is the
# operator's responsibility (scheduled refresh on a warehouse, a separate
# Lakeflow task, or `databricks pipelines start-update`). Plain TABLE / VIEW
# / EXTERNAL inputs work as-is — `spark.table(input_view)` is type-agnostic.
# `skip_refresh` is kept as a no-op widget for backwards compatibility.
_iv_parts = input_view.split(".")
if len(_iv_parts) == 3:
    _iv_cat, _iv_sch, _iv_name = _iv_parts
else:
    _iv_cat, _iv_sch, _iv_name = CATALOG, SCHEMA, _iv_parts[-1]
_iv_type_rows = spark.sql(f"""
    SELECT table_type FROM {_iv_cat}.information_schema.tables
    WHERE table_schema = '{_iv_sch}' AND table_name = '{_iv_name}'
""").collect()
_iv_type = _iv_type_rows[0]["table_type"] if _iv_type_rows else "UNKNOWN"
print(f"input_view type:   {_iv_type}")
if _iv_type == "MATERIALIZED_VIEW":
    print(f"   (note: MV refresh runs on a SQL warehouse, not this cluster)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Compile DMN on driver, capture inputs and decisions

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC locally {
# MAGIC   val xml   = spark.conf.get("irs.dmn.xml")
# MAGIC   val marsh = org.kie.dmn.backend.marshalling.v1x.DMNMarshallerFactory.newDefaultMarshaller()
# MAGIC   val defs  = marsh.unmarshal(new java.io.StringReader(xml))
# MAGIC   val m     = new org.kie.dmn.core.compiler.DMNCompilerImpl()
# MAGIC                 .compile(defs).asInstanceOf[org.kie.dmn.core.impl.DMNModelImpl]
# MAGIC   val errs  = m.getMessages.asScala.filter(_.getSeverity.toString == "ERROR").toList
# MAGIC   if (errs.nonEmpty) throw new RuntimeException(
# MAGIC     s"DMN compile error: ${errs.map(_.getText).mkString("; ")}")
# MAGIC   val inputNames    = m.getInputs.asScala.map(_.getName).toList
# MAGIC   val decisionTypes = m.getDecisions.asScala.map { d =>
# MAGIC     val tname = try {
# MAGIC       val t = d.getResultType
# MAGIC       if (t == null || t.getName == null) "string" else t.getName.toLowerCase
# MAGIC     } catch { case _: Throwable => "string" }
# MAGIC     s"${d.getName}::$tname"
# MAGIC   }.toList
# MAGIC   spark.conf.set("irs.dmn.input_names",    inputNames.mkString("|"))
# MAGIC   spark.conf.set("irs.dmn.decision_types", decisionTypes.mkString("|"))
# MAGIC   println(s"✅ DMN '${m.getName}' compiled — ${inputNames.size} inputs, ${decisionTypes.size} decisions")
# MAGIC }

# COMMAND ----------

dmn_input_names = spark.conf.get("irs.dmn.input_names").split("|")
dmn_decision_types = {
    pair.split("::", 1)[0]: pair.split("::", 1)[1]
    for pair in spark.conf.get("irs.dmn.decision_types").split("|")
}
dmn_decision_names = list(dmn_decision_types.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Pre-flight: validate binding against DMN + MV

# COMMAND ----------

pf = preflight(
    spark,
    binding_raw,
    list(dmn_input_names),
    dmn_decision_names,
    catalog=CATALOG,
    schema=SCHEMA,
)
binding      = pf.binding
input_view   = pf.input_view  # in case the binding overrode it (still the same source of truth)
column_exprs = pf.column_exprs

print("=" * 70)
print("PRE-FLIGHT")
print("=" * 70)
for w in pf.warnings:
    print(f"  ⚠️  {w}")
print(f"✅ Pre-flight passed ({len(pf.warnings)} warning(s))")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Register Drools UDF (Map[String,String] → Map[String,String])

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC
# MAGIC val _xmlBc = spark.sparkContext.broadcast(spark.conf.get("irs.dmn.xml"))
# MAGIC
# MAGIC type DroolsCacheEntry = (
# MAGIC   org.kie.dmn.core.impl.DMNModelImpl,
# MAGIC   org.kie.dmn.core.impl.DMNRuntimeImpl,
# MAGIC   java.util.Map[String, String]
# MAGIC )
# MAGIC
# MAGIC spark.udf.register("drools_score", {
# MAGIC   val _cache = new java.util.concurrent.ConcurrentHashMap[Int, DroolsCacheEntry]()
# MAGIC
# MAGIC   (inputs: Map[String, String]) => {
# MAGIC     val xml = _xmlBc.value
# MAGIC     val entry: DroolsCacheEntry = _cache.computeIfAbsent(xml.hashCode, _ => {
# MAGIC       val marsh = org.kie.dmn.backend.marshalling.v1x.DMNMarshallerFactory.newDefaultMarshaller()
# MAGIC       val defs  = marsh.unmarshal(new java.io.StringReader(xml))
# MAGIC       val m     = new org.kie.dmn.core.compiler.DMNCompilerImpl()
# MAGIC                     .compile(defs).asInstanceOf[org.kie.dmn.core.impl.DMNModelImpl]
# MAGIC       val errs  = m.getMessages.asScala.filter(_.getSeverity.toString == "ERROR").toList
# MAGIC       if (errs.nonEmpty) throw new RuntimeException(
# MAGIC         s"DMN compile error: ${errs.map(_.getText).mkString("; ")}")
# MAGIC       val tm = new java.util.HashMap[String, String]()
# MAGIC       m.getInputs.asScala.foreach { in =>
# MAGIC         val tname: String = try {
# MAGIC           val t = in.getType
# MAGIC           if (t == null || t.getName == null) "string" else t.getName.toLowerCase
# MAGIC         } catch { case _: Throwable => "string" }
# MAGIC         tm.put(in.getName, tname)
# MAGIC       }
# MAGIC       (m, new org.kie.dmn.core.impl.DMNRuntimeImpl(null), tm)
# MAGIC     })
# MAGIC     val model   = entry._1
# MAGIC     val runtime = entry._2
# MAGIC     val typeMap = entry._3
# MAGIC     val ctx     = new org.kie.dmn.core.impl.DMNContextImpl()
# MAGIC     val it      = typeMap.entrySet.iterator
# MAGIC     while (it.hasNext) {
# MAGIC       val e   = it.next()
# MAGIC       val k   = e.getKey
# MAGIC       val raw = inputs.getOrElse(k, null)
# MAGIC       if (raw != null) {
# MAGIC         val coerced: AnyRef = e.getValue match {
# MAGIC           case "number"  => new java.math.BigDecimal(raw)
# MAGIC           case "boolean" => java.lang.Boolean.valueOf(raw)
# MAGIC           case _         => raw
# MAGIC         }
# MAGIC         ctx.set(k, coerced)
# MAGIC       }
# MAGIC     }
# MAGIC     val result = runtime.evaluateAll(model, ctx)
# MAGIC     if (result.hasErrors) throw new RuntimeException(
# MAGIC       s"Eval error: ${result.getMessages.asScala.map(_.getText).mkString("; ")}")
# MAGIC     val out = scala.collection.mutable.Map[String, String]()
# MAGIC     val drs = result.getDecisionResults.iterator()
# MAGIC     while (drs.hasNext) {
# MAGIC       val dr = drs.next()
# MAGIC       val v = dr.getResult
# MAGIC       val s: String = v match {
# MAGIC         case null                     => null
# MAGIC         case bd: java.math.BigDecimal => bd.toPlainString
# MAGIC         case n:  Number               => n.toString
# MAGIC         case b:  java.lang.Boolean    => b.toString
# MAGIC         case other                    => other.toString
# MAGIC       }
# MAGIC       out.put(dr.getDecisionName, s)
# MAGIC     }
# MAGIC     out.toMap
# MAGIC   }
# MAGIC })
# MAGIC println("✅ drools_score registered (Map[String,String] → Map[String,String])")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Read MV and score

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(input_view)

_limit_str = dbutils.widgets.get('proof_limit').strip()
limit      = int(_limit_str) if _limit_str.isdigit() else None
if limit:
    df = df.limit(limit)

default_parallelism = spark.sparkContext.defaultParallelism
input_rows          = df.count()
target_partitions   = max(default_parallelism * 4, input_rows // 40000)
print(f"input rows:        {input_rows:,}")
print(f"target partitions: {target_partitions}")
df = df.repartition(target_partitions)

# Build the score-map expression from preflight-resolved column expressions.
# Default is name-matching (column_exprs[name] == name); overrides flow through verbatim.
map_pairs = ", ".join(
    f"'{n}', cast({expr} as string)" for n, expr in column_exprs.items()
)
score_map_expr = f"drools_score(map({map_pairs}))"

scored = df.withColumn("_score_map", F.expr(score_map_expr)).cache()
scored_count = scored.count()
print(f"✅ scored {scored_count:,} rows (single DMN evaluation per row)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Write each output table per binding

# COMMAND ----------

_FEEL_TO_SPARK_CAST = {
    "number":  "double",
    "boolean": "boolean",
    "long":    "bigint",
    "integer": "int",
}

def _fq(t): return t if "." in t else f"{CATALOG}.{SCHEMA}.{t}"

for out_table, out_spec in binding["outputs"].items():
    out_df    = scored
    carry     = out_spec.get("carry_columns") or []
    decisions = out_spec.get("decisions") or {}
    post_cols = out_spec.get("post_columns") or {}
    mode      = out_spec.get("mode") or "overwrite"

    for out_col, dec_name in decisions.items():
        feel_type  = dmn_decision_types.get(dec_name, "string")
        spark_type = _FEEL_TO_SPARK_CAST.get(feel_type)
        col = F.col("_score_map").getItem(dec_name)
        if spark_type:
            col = col.cast(spark_type)
        out_df = out_df.withColumn(out_col, col)

    select_cols = list(carry) + list(decisions.keys())
    for col_name, expr in post_cols.items():
        # Substitute the version token with a single-quoted SQL string
        # literal. Keeps single quotes out of the JSON binding entirely.
        sub = expr.replace("__VERSION__", f"'{version_id}'")
        out_df = out_df.withColumn(col_name, F.expr(sub))
        select_cols.append(col_name)

    final = out_df.select(*select_cols)

    fq = _fq(out_table)
    (final.write
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(fq))
    print(f"  → wrote {fq} ({mode})  cols={select_cols}")

scored.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Verify

# COMMAND ----------

first_out      = next(iter(binding["outputs"]))
first_out_spec = binding["outputs"][first_out]
decision_cols  = list((first_out_spec.get("decisions") or {}).keys())
fq_out         = _fq(first_out)
out_tbl        = spark.table(fq_out)
total          = out_tbl.count()
print(f"{fq_out}: {total:,} rows")

if total < 1:
    dbutils.notebook.exit(f"FAILED: no rows written to {fq_out}")

null_counts = out_tbl.agg(*[
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in decision_cols
]).collect()[0].asDict()
all_null_cols = [c for c, n in null_counts.items() if n == total]
print(f"null counts: {null_counts}")
if all_null_cols:
    dbutils.notebook.exit(
        f"FAILED: decision column(s) {all_null_cols} are 100% null in {fq_out} — "
        f"the DMN returned null for every row. Check sub-decision results "
        f"(COLLECT SUM with no rules firing returns null in FEEL — "
        f"wrap each summand: 'if X = null then 0 else X')."
    )
dbutils.notebook.exit(f"SUCCESS: scored={total} dmn_version={version_id}")
