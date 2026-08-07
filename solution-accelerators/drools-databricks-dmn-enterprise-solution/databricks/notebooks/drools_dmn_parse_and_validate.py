# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Drools DMN Enterprise Solution — Task 1: Parse and Validate DMN
# MAGIC This notebook reads one DMN file, compiles it with Drools DMN APIs, extracts metadata, and publishes the metadata to downstream tasks using `dbutils.jobs.taskValues`.
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 1 — Read parameter
# COMMAND ----------
dbutils.widgets.text("dmn_file_path", "dbfs:/Volumes/dbx_practice_ws/default/dbx-practice-volume/customer-approval-v1.dmn")
DMN_FILE_PATH = dbutils.widgets.get("dmn_file_path")
print("DMN_FILE_PATH:", DMN_FILE_PATH)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 2 — Read DMN XML
# MAGIC Reads the XML from storage and stores it in Spark conf for Scala compilation.
# COMMAND ----------
dmn_xml = dbutils.fs.head(DMN_FILE_PATH, 1024 * 1024 * 20)
spark.conf.set("droolsdemo.dmn.xml", dmn_xml)
spark.conf.set("droolsdemo.dmn.file_path", DMN_FILE_PATH)
print("DMN XML size:", len(dmn_xml))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 3 — Compile and validate DMN with Drools APIs
# MAGIC This fails fast if the DMN has compile/model errors and extracts inputs, decisions, and output types.
# COMMAND ----------
# MAGIC %scala
# MAGIC import org.kie.api.io.Resource
# MAGIC import org.kie.dmn.api.core._
# MAGIC import org.kie.dmn.core.internal.utils.DMNRuntimeBuilder
# MAGIC import java.nio.charset.StandardCharsets
# MAGIC import java.util.Collections
# MAGIC import scala.jdk.CollectionConverters._
# MAGIC val dmnXml: String = spark.conf.get("droolsdemo.dmn.xml")
# MAGIC locally {
# MAGIC   val resource: Resource = org.kie.internal.io.ResourceFactory.newByteArrayResource(dmnXml.getBytes(StandardCharsets.UTF_8)).setSourcePath("enterprise-dmn-parse-task.dmn")
# MAGIC   val dmnRuntime = DMNRuntimeBuilder.fromDefaults().buildConfiguration().fromResources(Collections.singletonList(resource)).getOrElseThrow(e => new RuntimeException("Failed to compile/build DMN runtime", e))
# MAGIC   val models = dmnRuntime.getModels.asScala.toList
# MAGIC   if (models.isEmpty) throw new RuntimeException("DMN compile failed: no DMN models were found in XML.")
# MAGIC   val model = models.head
# MAGIC   if (model.hasErrors) throw new RuntimeException("DMN compile/model errors:\n" + model.getMessages.asScala.map(_.toString).mkString("\n"))
# MAGIC   val namespace = model.getNamespace
# MAGIC   val modelName = model.getName
# MAGIC   val inputNames = model.getInputs.asScala.map(_.getName).mkString("|")
# MAGIC   val decisionNames = model.getDecisions.asScala.map(_.getName).mkString("|")
# MAGIC   val decisionTypes = model.getDecisions.asScala.map(d => s"${d.getName}:${Option(d.getResultType).map(_.getName).getOrElse("string")}").mkString("|")
# MAGIC   spark.conf.set("droolsdemo.dmn.namespace", namespace)
# MAGIC   spark.conf.set("droolsdemo.dmn.model_name", modelName)
# MAGIC   spark.conf.set("droolsdemo.dmn.input_names", inputNames)
# MAGIC   spark.conf.set("droolsdemo.dmn.decision_names", decisionNames)
# MAGIC   spark.conf.set("droolsdemo.dmn.decision_types", decisionTypes)
# MAGIC   println(s"DMN namespace: $namespace")
# MAGIC   println(s"DMN model name: $modelName")
# MAGIC   println(s"DMN input names: $inputNames")
# MAGIC   println(s"DMN decision names: $decisionNames")
# MAGIC   println(s"DMN decision types: $decisionTypes")
# MAGIC }
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cell 4 — Publish metadata using Databricks task values
# MAGIC Spark conf does not reliably persist across job tasks, so Task 2 reads these task values.
# COMMAND ----------
metadata = {
    "dmn_file_path": spark.conf.get("droolsdemo.dmn.file_path"),
    "dmn_namespace": spark.conf.get("droolsdemo.dmn.namespace"),
    "dmn_model_name": spark.conf.get("droolsdemo.dmn.model_name"),
    "dmn_input_names": spark.conf.get("droolsdemo.dmn.input_names"),
    "dmn_decision_names": spark.conf.get("droolsdemo.dmn.decision_names"),
    "dmn_decision_types": spark.conf.get("droolsdemo.dmn.decision_types"),
}
for k, v in metadata.items():
    dbutils.jobs.taskValues.set(key=k, value=v)
    print(f"{k}: {v}")
print("Task 1 complete.")
