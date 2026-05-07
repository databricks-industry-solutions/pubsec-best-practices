# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

DMN_FILE_PATH = "dbfs:/Volumes/services_bureau_catalog/irs_rrp/dmn_rules/customer-decision-v2.dmn"
#INPUT_TABLE = "services_bureau_catalog.irs_rrp.customer_dmn_input"
#OUTPUT_TABLE = "services_bureau_catalog.irs_rrp.customer_dmn_map_udf_output"
#ROW_LIMIT = 1000
#REPARTITION_COUNT = 2

INPUT_TABLE = "services_bureau_catalog.irs_rrp.customer_dmn_input_10m"
OUTPUT_TABLE = "services_bureau_catalog.irs_rrp.customer_dmn_map_udf_output_10m"
ROW_LIMIT = 0
REPARTITION_COUNT = 200

print("DMN_FILE_PATH:", DMN_FILE_PATH)
print("INPUT_TABLE:", INPUT_TABLE)
print("OUTPUT_TABLE:", OUTPUT_TABLE)
print("ROW_LIMIT:", ROW_LIMIT)
print("REPARTITION_COUNT:", REPARTITION_COUNT)

dmn_xml = dbutils.fs.head(DMN_FILE_PATH, 1024 * 1024 * 10)
spark.conf.set("droolsdemo.dmn.xml", dmn_xml)
print("DMN XML size:", len(dmn_xml))

# COMMAND ----------

# MAGIC %scala
# MAGIC import javax.xml.parsers.DocumentBuilderFactory
# MAGIC import org.xml.sax.InputSource
# MAGIC import java.io.StringReader
# MAGIC
# MAGIC val dmnXml = spark.conf.get("droolsdemo.dmn.xml")
# MAGIC
# MAGIC val factory = DocumentBuilderFactory.newInstance()
# MAGIC factory.setNamespaceAware(true)
# MAGIC
# MAGIC val doc = factory
# MAGIC   .newDocumentBuilder()
# MAGIC   .parse(new InputSource(new StringReader(dmnXml)))
# MAGIC
# MAGIC val root = doc.getDocumentElement
# MAGIC val namespace = root.getAttribute("namespace")
# MAGIC val modelName = root.getAttribute("name")
# MAGIC
# MAGIC def nodeSeq(localName: String) = {
# MAGIC   val nl = doc.getElementsByTagNameNS("*", localName)
# MAGIC   (0 until nl.getLength).map(i => nl.item(i))
# MAGIC }
# MAGIC
# MAGIC val inputNames = nodeSeq("inputData")
# MAGIC   .map(n => n.getAttributes.getNamedItem("name").getNodeValue)
# MAGIC   .mkString("|")
# MAGIC
# MAGIC val decisionNodes = nodeSeq("decision")
# MAGIC
# MAGIC val decisionNames = decisionNodes
# MAGIC   .map(n => n.getAttributes.getNamedItem("name").getNodeValue)
# MAGIC   .mkString("|")
# MAGIC
# MAGIC val decisionTypes = decisionNodes.map { d =>
# MAGIC   val decisionName = d.getAttributes.getNamedItem("name").getNodeValue
# MAGIC   val vars = d.asInstanceOf[org.w3c.dom.Element].getElementsByTagNameNS("*", "variable")
# MAGIC   val typeRef =
# MAGIC     if (vars.getLength > 0)
# MAGIC       vars.item(0).getAttributes.getNamedItem("typeRef").getNodeValue
# MAGIC     else
# MAGIC       "string"
# MAGIC
# MAGIC   s"${decisionName}:${typeRef}"
# MAGIC }.mkString("|")
# MAGIC
# MAGIC spark.conf.set("droolsdemo.dmn.namespace", namespace)
# MAGIC spark.conf.set("droolsdemo.dmn.model_name", modelName)
# MAGIC spark.conf.set("droolsdemo.dmn.input_names", inputNames)
# MAGIC spark.conf.set("droolsdemo.dmn.decision_names", decisionNames)
# MAGIC spark.conf.set("droolsdemo.dmn.decision_types", decisionTypes)
# MAGIC
# MAGIC println(s"DMN namespace: $namespace")
# MAGIC println(s"DMN model name: $modelName")
# MAGIC println(s"DMN input names: $inputNames")
# MAGIC println(s"DMN decision names: $decisionNames")
# MAGIC println(s"DMN decision types: $decisionTypes")

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC import org.apache.spark.sql.functions.udf
# MAGIC
# MAGIC val dmnXmlForUdf: String = spark.conf.get("droolsdemo.dmn.xml")
# MAGIC val namespaceForUdf: String = spark.conf.get("droolsdemo.dmn.namespace")
# MAGIC val modelNameForUdf: String = spark.conf.get("droolsdemo.dmn.model_name")
# MAGIC val decisionNamesForUdf: String = spark.conf.get("droolsdemo.dmn.decision_names")
# MAGIC
# MAGIC val droolsScore = udf { m: scala.collection.Map[String, String] =>
# MAGIC   if (m == null) {
# MAGIC     Map.empty[String, String]
# MAGIC   } else {
# MAGIC     val javaOutputMap = com.example.dmn.DmnMapEngine.score(
# MAGIC       m.asJava,
# MAGIC       dmnXmlForUdf,
# MAGIC       namespaceForUdf,
# MAGIC       modelNameForUdf,
# MAGIC       decisionNamesForUdf
# MAGIC     )
# MAGIC
# MAGIC     javaOutputMap.asScala.toMap
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC spark.udf.register("drools_score", droolsScore)
# MAGIC println("Registered typed Scala wrapper UDF: drools_score")

# COMMAND ----------

dmn_input_names = spark.conf.get("droolsdemo.dmn.input_names").split("|")
dmn_decision_types = dict(x.split(":", 1) for x in spark.conf.get("droolsdemo.dmn.decision_types").split("|") if ":" in x)
dmn_decision_names = list(dmn_decision_types.keys())

FEEL_TO_SPARK_CAST = {
    "number": "double",
    "string": "string",
    "boolean": "boolean",
    "date": "date",
    "dateTime": "timestamp",
    "time": "string",
    "duration": "string",
    "Any": "string",
    "any": "string",
}

print("dmn_input_names:", dmn_input_names)
print("dmn_decision_types:", dmn_decision_types)
print("dmn_decision_names:", dmn_decision_names)
print("FEEL_TO_SPARK_CAST:", FEEL_TO_SPARK_CAST)

df = spark.table(INPUT_TABLE)
input_cols = df.columns
print("Input table columns:", input_cols)

if ROW_LIMIT and ROW_LIMIT > 0:
    df = df.limit(ROW_LIMIT)
    print(f"Applied row limit: {ROW_LIMIT}")

if REPARTITION_COUNT and REPARTITION_COUNT > 0:
    df = df.repartition(REPARTITION_COUNT)
    print(f"Applied repartition: {REPARTITION_COUNT}")

def normalize_name(name):
    return "".join(ch.lower() for ch in name if ch.isalnum())

def to_snake(name):
    out, prev = [], False
    for ch in name.strip():
        if ch.isalnum():
            if ch.isupper() and prev:
                out.append("_")
            out.append(ch.lower())
            prev = ch.islower() or ch.isdigit()
        else:
            if out and out[-1] != "_":
                out.append("_")
            prev = False
    return "".join(out).strip("_")

lookup_exact = {c: c for c in input_cols}
lookup_lower = {c.lower(): c for c in input_cols}
lookup_norm = {normalize_name(c): c for c in input_cols}

matched_inputs = {}
unmatched_inputs = []
for input_name in dmn_input_names:
    matched_col = None
    if input_name in lookup_exact:
        matched_col = lookup_exact[input_name]
    elif input_name.lower() in lookup_lower:
        matched_col = lookup_lower[input_name.lower()]
    elif to_snake(input_name) in lookup_lower:
        matched_col = lookup_lower[to_snake(input_name)]
    elif normalize_name(input_name) in lookup_norm:
        matched_col = lookup_norm[normalize_name(input_name)]
    matched_inputs[input_name] = matched_col
    if matched_col is None:
        unmatched_inputs.append(input_name)

print("Matched DMN inputs to Delta columns:", matched_inputs)
print("Unmatched DMN inputs:", unmatched_inputs)

if len([v for v in matched_inputs.values() if v is not None]) == 0:
    raise ValueError(f"No DMN inputData names matched input table columns. DMN inputs={dmn_input_names}, table columns={input_cols}")

if unmatched_inputs:
    print("WARNING: These DMN inputs will be passed as NULL:", unmatched_inputs)

map_pairs = ", ".join(
    f"'{input_name}', cast({matched_inputs[input_name]} as string)"
    if matched_inputs[input_name] is not None
    else f"'{input_name}', cast(null as string)"
    for input_name in dmn_input_names
)

score_map_expr = f"drools_score(map({map_pairs}))"
print("score_map_expr:", score_map_expr)

scored = df.withColumn("_score_map", F.expr(score_map_expr))

result_df = scored
for decision_name in dmn_decision_names:
    feel_type = dmn_decision_types.get(decision_name, "string")
    spark_type = FEEL_TO_SPARK_CAST.get(feel_type, "string")
    output_col = to_snake(decision_name)
    result_df = result_df.withColumn(output_col, F.col("_score_map").getItem(decision_name).cast(spark_type))

result_df = result_df.drop("_score_map")

print("Final output schema:")
result_df.printSchema()
display(result_df)

result_df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)
print("Output written to:", OUTPUT_TABLE)
