# Databricks notebook source
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %md
# MAGIC # Drools DMN Enterprise Solution — Task 2: Evaluate and Write Output
# MAGIC This notebook reads metadata from Task 1, registers the Scala UDF bridge, evaluates input rows on executors, and writes the output Delta table. It can also run cell-by-cell manually.
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 1 — Read parameters
# COMMAND ----------
dbutils.widgets.text("dmn_file_path", "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume/customer-approval-v1.dmn")
dbutils.widgets.text("input_table", "default.drools_dmn_customer_approval_input")
dbutils.widgets.text("output_table", "default.drools_dmn_customer_approval_output")
dbutils.widgets.text("row_limit", "0")
dbutils.widgets.text("repartition_count", "0")
dbutils.widgets.text("write_mode", "overwrite")
DMN_FILE_PATH_WIDGET = dbutils.widgets.get("dmn_file_path")
INPUT_TABLE = dbutils.widgets.get("input_table")
OUTPUT_TABLE = dbutils.widgets.get("output_table")
ROW_LIMIT = int(dbutils.widgets.get("row_limit") or "0")
REPARTITION_COUNT = int(dbutils.widgets.get("repartition_count") or "0")
WRITE_MODE = dbutils.widgets.get("write_mode") or "overwrite"
print("INPUT_TABLE:", INPUT_TABLE)
print("OUTPUT_TABLE:", OUTPUT_TABLE)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 2 — Read task values from Task 1
# MAGIC If running manually, task values may be absent; the notebook falls back to widgets and local metadata parsing.
# COMMAND ----------
def get_task_value(key: str, default_value: str = "") -> str:
    try:
        return dbutils.jobs.taskValues.get(taskKey="parse_dmn_metadata", key=key, default=default_value, debugValue=default_value)
    except Exception:
        return default_value
DMN_FILE_PATH = get_task_value("dmn_file_path", DMN_FILE_PATH_WIDGET)
dmn_namespace_from_task = get_task_value("dmn_namespace", "")
dmn_model_name_from_task = get_task_value("dmn_model_name", "")
dmn_input_names_from_task = get_task_value("dmn_input_names", "")
dmn_decision_names_from_task = get_task_value("dmn_decision_names", "")
dmn_decision_types_from_task = get_task_value("dmn_decision_types", "")
print("DMN_FILE_PATH:", DMN_FILE_PATH)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 3 — Read DMN XML and set Spark conf
# COMMAND ----------
dmn_xml = dbutils.fs.head(DMN_FILE_PATH, 1024 * 1024 * 20)
spark.conf.set("droolsdemo.dmn.xml", dmn_xml)
spark.conf.set("droolsdemo.dmn.file_path", DMN_FILE_PATH)
for k, v in {
    "droolsdemo.dmn.namespace": dmn_namespace_from_task,
    "droolsdemo.dmn.model_name": dmn_model_name_from_task,
    "droolsdemo.dmn.input_names": dmn_input_names_from_task,
    "droolsdemo.dmn.decision_names": dmn_decision_names_from_task,
    "droolsdemo.dmn.decision_types": dmn_decision_types_from_task,
}.items():
    if v: spark.conf.set(k, v)
print("DMN XML size:", len(dmn_xml))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 4 — Manual fallback metadata compile
# MAGIC If Task 1 metadata is missing, this compiles the DMN locally so cell-by-cell execution still works.
# COMMAND ----------
# MAGIC %scala
# MAGIC import org.kie.api.io.Resource
# MAGIC import org.kie.dmn.api.core._
# MAGIC import org.kie.dmn.core.internal.utils.DMNRuntimeBuilder
# MAGIC import java.nio.charset.StandardCharsets
# MAGIC import java.util.Collections
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC val hasMetadata = spark.conf.getOption("droolsdemo.dmn.namespace").exists(_.nonEmpty) && spark.conf.getOption("droolsdemo.dmn.model_name").exists(_.nonEmpty) && spark.conf.getOption("droolsdemo.dmn.input_names").exists(_.nonEmpty) && spark.conf.getOption("droolsdemo.dmn.decision_names").exists(_.nonEmpty) && spark.conf.getOption("droolsdemo.dmn.decision_types").exists(_.nonEmpty)
# MAGIC if (hasMetadata) println("DMN metadata already exists from Task 1. Skipping fallback compile.") else {
# MAGIC   val dmnXml: String = spark.conf.get("droolsdemo.dmn.xml")
# MAGIC   locally {
# MAGIC     val resource: Resource = org.kie.internal.io.ResourceFactory.newByteArrayResource(dmnXml.getBytes(StandardCharsets.UTF_8)).setSourcePath("enterprise-dmn-fallback.dmn")
# MAGIC     val dmnRuntime = DMNRuntimeBuilder.fromDefaults().buildConfiguration().fromResources(Collections.singletonList(resource)).getOrElseThrow(e => new RuntimeException("Failed to compile/build DMN runtime", e))
# MAGIC     val models = dmnRuntime.getModels.asScala.toList
# MAGIC     if (models.isEmpty) throw new RuntimeException("DMN compile failed: no DMN models were found in XML.")
# MAGIC     val model = models.head
# MAGIC     if (model.hasErrors) throw new RuntimeException("DMN compile/model errors:\n" + model.getMessages.asScala.map(_.toString).mkString("\n"))
# MAGIC     val namespace = model.getNamespace; val modelName = model.getName
# MAGIC     val inputNames = model.getInputs.asScala.map(_.getName).mkString("|")
# MAGIC     val decisionNames = model.getDecisions.asScala.map(_.getName).mkString("|")
# MAGIC     val decisionTypes = model.getDecisions.asScala.map(d => s"${d.getName}:${Option(d.getResultType).map(_.getName).getOrElse("string")}").mkString("|")
# MAGIC     spark.conf.set("droolsdemo.dmn.namespace", namespace); spark.conf.set("droolsdemo.dmn.model_name", modelName); spark.conf.set("droolsdemo.dmn.input_names", inputNames); spark.conf.set("droolsdemo.dmn.decision_names", decisionNames); spark.conf.set("droolsdemo.dmn.decision_types", decisionTypes)
# MAGIC   }
# MAGIC }
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 5 — Register Scala UDF bridge
# MAGIC Broadcasts DMN XML/metadata and registers `dmn_enterprise_score`.
# COMMAND ----------
# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC import org.apache.spark.sql.functions.udf
# MAGIC val dmnXmlBroadcast = spark.sparkContext.broadcast(spark.conf.get("droolsdemo.dmn.xml"))
# MAGIC val namespaceBroadcast = spark.sparkContext.broadcast(spark.conf.get("droolsdemo.dmn.namespace"))
# MAGIC val modelNameBroadcast = spark.sparkContext.broadcast(spark.conf.get("droolsdemo.dmn.model_name"))
# MAGIC val decisionNamesBroadcast = spark.sparkContext.broadcast(spark.conf.get("droolsdemo.dmn.decision_names"))
# MAGIC val dmnEnterpriseScore = udf { m: scala.collection.Map[String, String] => if (m == null) Map.empty[String, String] else com.example.dmnenterprise.DmnEnterpriseEngine.score(m.asJava, dmnXmlBroadcast.value, namespaceBroadcast.value, modelNameBroadcast.value, decisionNamesBroadcast.value).asScala.toMap }
# MAGIC spark.udf.register("dmn_enterprise_score", dmnEnterpriseScore)
# MAGIC println("Registered UDF: dmn_enterprise_score")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 6 — Read metadata into Python
# COMMAND ----------
dmn_input_names = spark.conf.get("droolsdemo.dmn.input_names").split("|")
dmn_decision_types = dict(x.split(":", 1) for x in spark.conf.get("droolsdemo.dmn.decision_types").split("|") if ":" in x)
dmn_decision_names = list(dmn_decision_types.keys())
dmn_namespace = spark.conf.get("droolsdemo.dmn.namespace")
dmn_model_name = spark.conf.get("droolsdemo.dmn.model_name")
FEEL_TO_SPARK_CAST = {"number":"double", "string":"string", "boolean":"boolean", "date":"date", "dateTime":"timestamp", "time":"string", "duration":"string", "Any":"string", "any":"string"}
print("dmn_input_names:", dmn_input_names)
print("dmn_decision_names:", dmn_decision_names)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 7 — Read input table
# MAGIC This is lazy until the final write action.
# COMMAND ----------
df = spark.table(INPUT_TABLE)
input_cols = df.columns
if ROW_LIMIT and ROW_LIMIT > 0:
    df = df.limit(ROW_LIMIT)
if REPARTITION_COUNT and REPARTITION_COUNT > 0:
    df = df.repartition(REPARTITION_COUNT)
print("Input table columns:", input_cols)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 8 — Match DMN inputs to table columns
# COMMAND ----------
def normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())
def to_snake(name: str) -> str:
    out=[]; prev=False
    for ch in name.strip():
        if ch.isalnum():
            if ch.isupper() and prev: out.append("_")
            out.append(ch.lower()); prev=ch.islower() or ch.isdigit()
        else:
            if out and out[-1] != "_": out.append("_")
            prev=False
    return "".join(out).strip("_")
lookup_exact={c:c for c in input_cols}; lookup_lower={c.lower():c for c in input_cols}; lookup_norm={normalize_name(c):c for c in input_cols}
matched_inputs={}; unmatched_inputs=[]
for input_name in dmn_input_names:
    matched_col = lookup_exact.get(input_name) or lookup_lower.get(input_name.lower()) or lookup_lower.get(to_snake(input_name)) or lookup_norm.get(normalize_name(input_name))
    matched_inputs[input_name]=matched_col
    if matched_col is None: unmatched_inputs.append(input_name)
print("Matched DMN inputs:", matched_inputs)
print("Unmatched DMN inputs:", unmatched_inputs)
if len([v for v in matched_inputs.values() if v is not None]) == 0:
    raise ValueError(f"No DMN inputData names matched input table columns. DMN inputs={dmn_input_names}, table columns={input_cols}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 9 — Build input map and call UDF
# MAGIC Still lazy; execution starts at final write.
# COMMAND ----------
map_pairs = ", ".join(f"'{input_name}', cast({matched_inputs[input_name]} as string)" if matched_inputs[input_name] is not None else f"'{input_name}', cast(null as string)" for input_name in dmn_input_names)
score_map_expr = f"dmn_enterprise_score(map({map_pairs}))"
print("score_map_expr:", score_map_expr)
scored = df.withColumn("_score_map", F.expr(score_map_expr))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 10 — Expand output map and add audit columns
# COMMAND ----------
result_df = scored
for decision_name in dmn_decision_names:
    spark_type = FEEL_TO_SPARK_CAST.get(dmn_decision_types.get(decision_name, "string"), "string")
    result_df = result_df.withColumn(to_snake(decision_name), F.col("_score_map").getItem(decision_name).cast(spark_type))
result_df = (result_df.withColumn("dmn_model_name", F.lit(dmn_model_name)).withColumn("dmn_namespace", F.lit(dmn_namespace)).withColumn("dmn_file_path", F.lit(DMN_FILE_PATH)).withColumn("dmn_run_ts", F.current_timestamp()).withColumn("dmn_output_map_json", F.to_json(F.col("_score_map"))).drop("_score_map"))
result_df.printSchema()
display(result_df.limit(100))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 11 — Write output table
# MAGIC This is the action that submits work to executors.
# COMMAND ----------
result_df.write.format("delta").mode(WRITE_MODE).saveAsTable(OUTPUT_TABLE)
print("Output written to:", OUTPUT_TABLE)
