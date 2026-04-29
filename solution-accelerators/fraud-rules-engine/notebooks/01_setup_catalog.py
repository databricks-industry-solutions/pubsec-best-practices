# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup Unity Catalog Resources
# MAGIC
# MAGIC Bootstraps the catalog, schema, and volume for the tax authority fraud detection pipeline.
# MAGIC DMN files are uploaded by the rules-editor app, not this notebook.

# COMMAND ----------

dbutils.widgets.text('catalog', 'main', 'UC catalog')
dbutils.widgets.text('schema',  'irs_rrp',                 'UC schema')
dbutils.widgets.text('volume',  'dmn_rules',               'UC volume')

CATALOG = dbutils.widgets.get('catalog').strip() or 'main'
SCHEMA  = dbutils.widgets.get('schema').strip()  or 'irs_rrp'
VOLUME  = dbutils.widgets.get('volume').strip()  or 'dmn_rules'

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA}
    COMMENT 'Tax authority fraud detection workflow — Drools DMN rules engine POC'
""")
spark.sql(f"USE SCHEMA {SCHEMA}")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {VOLUME}
    COMMENT 'DMN rule definition files and the Drools shaded JAR'
""")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
