# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Validate Binding
# MAGIC
# MAGIC Standalone pre-flight: pick a `version_id`, this notebook
# MAGIC compiles its DMN, parses its v2 binding, and runs the same `preflight`
# MAGIC checks that `04_batch_scoring.py` does — without scoring or writing
# MAGIC anything. Use this before promoting `DRAFT → ACTIVE`.
# MAGIC
# MAGIC Exits `SUCCESS: ...` or `FAILED: ...` so it can be wired into a
# MAGIC promotion gate job.

# COMMAND ----------

dbutils.widgets.text('version_id', '', 'Version ID (blank = ACTIVE)')

# COMMAND ----------

# MAGIC %run ./_binding_preflight

# COMMAND ----------

import json

CATALOG = "services_bureau_catalog"
SCHEMA  = "irs_rrp"

_version_param = dbutils.widgets.get('version_id').strip()
where = (f"version_id = '{_version_param}'" if _version_param
         else "status = 'ACTIVE' ORDER BY promoted_at DESC LIMIT 1")
rows = spark.sql(f"""
    SELECT version_id, dmn_path, binding_json, status
    FROM {CATALOG}.{SCHEMA}.rule_versions
    WHERE {where}
""").collect()
if not rows:
    dbutils.notebook.exit(f"FAILED: no rule_versions row found for {where}")

version_id  = rows[0]["version_id"]
dmn_path    = rows[0]["dmn_path"]
binding_raw = rows[0]["binding_json"]
status      = rows[0]["status"]
print(f"Version: {version_id} (status={status})")
print(f"DMN:     {dmn_path}")

if not binding_raw:
    dbutils.notebook.exit(f"FAILED: version '{version_id}' has empty binding_json")

dmn_xml = dbutils.fs.head(dmn_path, 1048576)
spark.conf.set("irs.dmn.xml", dmn_xml)

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
# MAGIC   val decisionNames = m.getDecisions.asScala.map(_.getName).toList
# MAGIC   spark.conf.set("irs.dmn.input_names",    inputNames.mkString("|"))
# MAGIC   spark.conf.set("irs.dmn.decision_names", decisionNames.mkString("|"))
# MAGIC   println(s"✅ DMN '${m.getName}' compiled — ${inputNames.size} inputs, ${decisionNames.size} decisions")
# MAGIC }

# COMMAND ----------

dmn_input_names    = spark.conf.get("irs.dmn.input_names").split("|")
dmn_decision_names = spark.conf.get("irs.dmn.decision_names").split("|")

try:
    pf = preflight(
        spark, binding_raw,
        list(dmn_input_names), list(dmn_decision_names),
        catalog=CATALOG, schema=SCHEMA,
    )
except RuntimeError as e:
    print(str(e))
    dbutils.notebook.exit(f"FAILED: {version_id}: {str(e).splitlines()[0]}")

print("=" * 70)
print(f"✅ {version_id} — binding is valid")
print("=" * 70)
print(f"input_view:    {pf.input_view}")
print(f"DMN inputs:    {len(dmn_input_names)} resolved against MV columns")
print(f"DMN decisions: {len(dmn_decision_names)}")
for w in pf.warnings:
    print(f"  ⚠️  {w}")

dbutils.notebook.exit(f"SUCCESS: {version_id} validated ({len(pf.warnings)} warning(s))")
