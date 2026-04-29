# Databricks notebook source
# MAGIC %md
# MAGIC # Quickstart 02 — Score one table with one DMN
# MAGIC
# MAGIC The simplest possible end-to-end: point this notebook at a DMN file and
# MAGIC a Unity Catalog table, get a scored output table back. **No** binding
# MAGIC contract, **no** `rule_versions` registry, **no** materialized views.
# MAGIC
# MAGIC **How input mapping works:**
# MAGIC The notebook reads the DMN's `<inputData>` names and matches them to
# MAGIC columns in your input table by exact lowercase snake_case name. Anything
# MAGIC the DMN asks for that the table doesn't have is dropped to NULL — the
# MAGIC DMN's own logic handles that case.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Cluster has `drools-dmn-shaded-2.0.0.jar` attached (run `01_build_jar.py` first)
# MAGIC - DMN file is somewhere readable by `dbutils.fs.head` (UC Volume, Workspace path, dbfs)
# MAGIC - Input table is queryable as `spark.table(input_table)`
# MAGIC
# MAGIC **Output shape:** all input columns + one column per DMN decision.

# COMMAND ----------

dbutils.widgets.text(
  'dmn_path',
  '/Volumes/main/irs_rrp/dmn_rules/sample_rules.dmn',
  'DMN file path (UC Volume / dbfs / Workspace)',
)
dbutils.widgets.text(
  'input_table',
  'main.irs_rrp.tax_returns',
  'Fully-qualified input table',
)
dbutils.widgets.text(
  'output_table',
  'main.irs_rrp.quickstart_scored',
  'Fully-qualified output table',
)
dbutils.widgets.text(
  'row_limit',
  '',
  'Row limit (blank = all rows)',
)

DMN_PATH     = dbutils.widgets.get('dmn_path').strip()
INPUT_TABLE  = dbutils.widgets.get('input_table').strip()
OUTPUT_TABLE = dbutils.widgets.get('output_table').strip()
_limit_raw   = dbutils.widgets.get('row_limit').strip()
ROW_LIMIT    = int(_limit_raw) if _limit_raw.isdigit() else None

print(f"DMN:        {DMN_PATH}")
print(f"Input:      {INPUT_TABLE}")
print(f"Output:     {OUTPUT_TABLE}")
print(f"Row limit:  {ROW_LIMIT if ROW_LIMIT else 'none'}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Load DMN into spark conf so the Scala UDF can broadcast it

# COMMAND ----------

dmn_xml = dbutils.fs.head(DMN_PATH, 1048576)
spark.conf.set("quickstart.dmn.xml", dmn_xml)
print(f"✅ Loaded {len(dmn_xml):,} chars of DMN XML")

# COMMAND ----------

# MAGIC %md ## Step 2 — Compile DMN on driver to discover inputs + decisions

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC locally {
# MAGIC   val xml   = spark.conf.get("quickstart.dmn.xml")
# MAGIC   val marsh = org.kie.dmn.backend.marshalling.v1x.DMNMarshallerFactory.newDefaultMarshaller()
# MAGIC   val defs  = marsh.unmarshal(new java.io.StringReader(xml))
# MAGIC   val m     = new org.kie.dmn.core.compiler.DMNCompilerImpl()
# MAGIC                 .compile(defs).asInstanceOf[org.kie.dmn.core.impl.DMNModelImpl]
# MAGIC   val errs  = m.getMessages.asScala.filter(_.getSeverity.toString == "ERROR").toList
# MAGIC   if (errs.nonEmpty) throw new RuntimeException(
# MAGIC     s"DMN compile error: ${errs.map(_.getText).mkString("; ")}")
# MAGIC   val inputNames = m.getInputs.asScala.map(_.getName).toList
# MAGIC   val decisionTypes = m.getDecisions.asScala.map { d =>
# MAGIC     val tname = try {
# MAGIC       val t = d.getResultType
# MAGIC       if (t == null || t.getName == null) "string" else t.getName.toLowerCase
# MAGIC     } catch { case _: Throwable => "string" }
# MAGIC     s"${d.getName}::$tname"
# MAGIC   }.toList
# MAGIC   spark.conf.set("quickstart.dmn.input_names",    inputNames.mkString("|"))
# MAGIC   spark.conf.set("quickstart.dmn.decision_types", decisionTypes.mkString("|"))
# MAGIC   println(s"✅ DMN '${m.getName}' compiled — ${inputNames.size} inputs, ${decisionTypes.size} decisions")
# MAGIC }

# COMMAND ----------

dmn_input_names = spark.conf.get("quickstart.dmn.input_names").split("|")
dmn_decision_types = {
  pair.split("::", 1)[0]: pair.split("::", 1)[1]
  for pair in spark.conf.get("quickstart.dmn.decision_types").split("|")
}
dmn_decision_names = list(dmn_decision_types.keys())

print(f"DMN inputs:     {list(dmn_input_names)}")
print(f"DMN decisions:  {dmn_decision_names}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Match DMN inputs to columns in the input table by name

# COMMAND ----------

input_df = spark.table(INPUT_TABLE)
input_cols = set(input_df.columns)

matched   = [n for n in dmn_input_names if n in input_cols]
unmatched = [n for n in dmn_input_names if n not in input_cols]

print(f"Matched ({len(matched)}):    {matched}")
if unmatched:
  print(f"Unmatched ({len(unmatched)}):  {unmatched}  ← will be passed as NULL")

if not matched:
  raise RuntimeError(
    f"None of the DMN inputs {list(dmn_input_names)} match any column in "
    f"{INPUT_TABLE}. Rename DMN inputs (lowercase snake_case) to match columns, "
    f"or pick a different input table."
  )

# COMMAND ----------

# MAGIC %md ## Step 4 — Register `drools_score` UDF (compiled once per task)

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC
# MAGIC val _xmlBc = spark.sparkContext.broadcast(spark.conf.get("quickstart.dmn.xml"))
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

# MAGIC %md ## Step 5 — Score the table

# COMMAND ----------

from pyspark.sql import functions as F

df = input_df
if ROW_LIMIT:
  df = df.limit(ROW_LIMIT)

input_rows = df.count()
target_partitions = max(spark.sparkContext.defaultParallelism * 4, input_rows // 40000)
df = df.repartition(target_partitions)
print(f"input rows: {input_rows:,}   partitions: {target_partitions}")

# Build the input map: name-match every DMN input. Unmatched inputs get NULL.
map_pairs = ", ".join(
  f"'{n}', cast({n} as string)" if n in input_cols else f"'{n}', cast(null as string)"
  for n in dmn_input_names
)
score_map_expr = f"drools_score(map({map_pairs}))"

scored = df.withColumn("_score_map", F.expr(score_map_expr))

# COMMAND ----------

# MAGIC %md ## Step 6 — Project decisions and write

# COMMAND ----------

_FEEL_TO_SPARK_CAST = {
  "number":  "double",
  "boolean": "boolean",
  "long":    "bigint",
  "integer": "int",
}

out_df = scored
for dec_name, feel_type in dmn_decision_types.items():
  spark_type = _FEEL_TO_SPARK_CAST.get(feel_type)
  col = F.col("_score_map").getItem(dec_name)
  if spark_type:
    col = col.cast(spark_type)
  out_df = out_df.withColumn(dec_name, col)

out_df = out_df.drop("_score_map")

(out_df.write
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable(OUTPUT_TABLE))
print(f"✅ wrote {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md ## Step 7 — Verify

# COMMAND ----------

result = spark.table(OUTPUT_TABLE)
total = result.count()
print(f"Rows: {total:,}")

null_counts = result.agg(*[
  F.sum(F.col(c).isNull().cast("int")).alias(c) for c in dmn_decision_names
]).collect()[0].asDict()
print(f"Null counts per decision: {null_counts}")

print("\nSample (first 5 rows, decision columns only):")
result.select(*dmn_decision_names).show(5, truncate=False)

all_null_cols = [c for c, n in null_counts.items() if n == total]
if all_null_cols:
  dbutils.notebook.exit(
    f"WARNING: decision(s) {all_null_cols} are 100% null. Check that "
    f"DMN input names match input table columns, or that your FEEL "
    f"handles missing inputs (e.g. `if X = null then 0 else X`)."
  )

dbutils.notebook.exit(f"SUCCESS: {total} rows written to {OUTPUT_TABLE}")
