# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup Unity Catalog Resources
# MAGIC
# MAGIC Bootstraps the catalog, schema, and volume for the IRS RRP pipeline.
# MAGIC DMN files are uploaded by the rules-editor app, not this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG services_bureau_catalog;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS irs_rrp
# MAGIC COMMENT 'IRS Return Review Program — Drools DMN rules engine POC';
# MAGIC
# MAGIC USE SCHEMA irs_rrp;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS dmn_rules
# MAGIC COMMENT 'DMN rule definition files and the Drools shaded JAR';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN services_bureau_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN services_bureau_catalog.irs_rrp;

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
