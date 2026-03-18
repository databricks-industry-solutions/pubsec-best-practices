# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Parse & Display ODCS Data Contract
# MAGIC
# MAGIC Reads the **ODCS v3 data contract**, extracts schema and quality rules,
# MAGIC and writes a machine-readable `contract_rules` table that downstream tasks
# MAGIC consume to configure DQX checks automatically.
# MAGIC
# MAGIC The contract YAML is **embedded in this notebook** — making the bundle
# MAGIC fully self-contained with no DBFS or external file dependencies.
# MAGIC
# MAGIC > **ODCS** — Open Data Contract Standard (https://bitol-io.github.io/open-data-contract-standard/)

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install pyyaml --quiet

# COMMAND ----------

dbutils.widgets.text("catalog",       "main",    "Catalog")
dbutils.widgets.text("schema",        "dq_demo", "Schema")
dbutils.widgets.text("contract_path", "/tmp/orders_contract.yaml", "Contract YAML path")

catalog       = dbutils.widgets.get("catalog")
schema        = dbutils.widgets.get("schema")
contract_path = dbutils.widgets.get("contract_path")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Using {catalog}.{schema}")
print(f"Contract path: {contract_path}")

# COMMAND ----------

# MAGIC %md ## Write Embedded Contract to Disk
# MAGIC
# MAGIC The ODCS contract YAML is embedded here so this notebook is fully self-contained.
# MAGIC At runtime it writes the contract to `contract_path` (default `/tmp/`) before parsing.

# COMMAND ----------

CONTRACT_YAML = """\
apiVersion: v3.0.0
kind: DataContract

id: curated-orders-dq-demo-001
status: active
name: Curated Orders Data Contract
version: 1.0.0
tenant: field-engineering-demos
domain: ecommerce

description:
  purpose: >
    Governs the curated_orders table which combines raw order transactions
    with enriched customer and product dimensions. This contract defines the
    schema, business rules, and data quality expectations that must be satisfied
    before data is consumed by downstream analytics and reporting.
  usage: >
    Analytical reporting, revenue dashboards, customer 360, order fulfilment KPIs.

owner:
  name: Data Engineering Team
  email: data-eng@example.com
  team: Platform Engineering

tags:
  - ecommerce
  - orders
  - curated
  - dq-demo

schema:
  - name: curated_orders
    physicalName: curated_orders
    description: Enriched order-level table joining orders, customers, and products.
    tags:
      - orders
      - enriched

    columns:
      - name: order_id
        businessName: Order Identifier
        logicalType: string
        physicalType: string
        isNullable: false
        isUnique: true
        description: Globally unique order identifier in format ORD-NNNNN.
        quality:
          - rule: not_null
            description: Every order must have an order ID.
          - rule: unique
            description: No two orders can share the same ID.
          - rule: regex_match
            pattern: "^ORD-[0-9]{5}$"
            description: Must match format ORD-NNNNN.

      - name: customer_id
        businessName: Customer Identifier
        logicalType: string
        physicalType: string
        isNullable: false
        description: Reference to the customer who placed the order.
        quality:
          - rule: not_null
            description: Every order must be linked to a customer.

      - name: customer_name
        businessName: Customer Name
        logicalType: string
        physicalType: string
        isNullable: false
        description: Full name of the customer.
        quality:
          - rule: not_null_and_not_empty
            description: Customer name must be populated.

      - name: customer_email
        businessName: Customer Email
        logicalType: string
        physicalType: string
        isNullable: true
        description: Customer contact email.
        quality:
          - rule: regex_match
            pattern: "^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$"
            description: When present, must be a valid email format.
            criticality: warn

      - name: product_id
        businessName: Product Identifier
        logicalType: string
        physicalType: string
        isNullable: false
        description: Reference to the ordered product.
        quality:
          - rule: not_null
            description: Every order must reference a product.

      - name: product_name
        businessName: Product Name
        logicalType: string
        physicalType: string
        isNullable: false
        description: Name of the product at time of order.
        quality:
          - rule: not_null_and_not_empty
            description: Product name must be populated.

      - name: product_category
        businessName: Product Category
        logicalType: string
        physicalType: string
        isNullable: true
        description: Category of the product.
        quality:
          - rule: value_in_set
            allowedValues:
              - Electronics
              - Clothing
              - Home & Garden
              - Sports
              - Books
              - Toys
            description: Category must be one of the approved taxonomy values.
            criticality: warn

      - name: order_date
        businessName: Order Date
        logicalType: date
        physicalType: date
        isNullable: false
        description: Date the order was placed (UTC).
        quality:
          - rule: not_null
            description: Every order must have a date.
          - rule: is_in_range
            minValue: "2020-01-01"
            maxValue: "today"
            description: Order date must be between 2020-01-01 and today.

      - name: quantity
        businessName: Order Quantity
        logicalType: integer
        physicalType: integer
        isNullable: false
        description: Number of units ordered.
        quality:
          - rule: not_null
            description: Quantity must be provided.
          - rule: is_in_range
            minValue: 1
            maxValue: 1000
            description: Quantity must be between 1 and 1000 inclusive.

      - name: unit_price
        businessName: Unit Price (USD)
        logicalType: double
        physicalType: double
        isNullable: false
        description: Price per unit at time of order in USD.
        quality:
          - rule: not_null
            description: Unit price must be provided.
          - rule: is_in_range
            minValue: 0.01
            maxValue: 10000.00
            description: Unit price must be between $0.01 and $10,000.

      - name: total_amount
        businessName: Order Total (USD)
        logicalType: double
        physicalType: double
        isNullable: false
        description: Total order value in USD.
        quality:
          - rule: not_null
            description: Total amount must be provided.
          - rule: is_in_range
            minValue: 0.01
            maxValue: 1000000.00
            description: Total amount must be positive.

      - name: order_status
        businessName: Order Status
        logicalType: string
        physicalType: string
        isNullable: false
        description: Current lifecycle status of the order.
        quality:
          - rule: not_null
            description: Order status must be provided.
          - rule: value_in_set
            allowedValues:
              - pending
              - processing
              - shipped
              - delivered
              - cancelled
            description: Status must be one of the approved lifecycle values.

      - name: region
        businessName: Shipping Region
        logicalType: string
        physicalType: string
        isNullable: false
        description: Geographic region where the order is shipped.
        quality:
          - rule: not_null
            description: Shipping region must be provided.
          - rule: value_in_set
            allowedValues:
              - North
              - South
              - East
              - West
            description: Region must be one of the four approved territories.

servicelevels:
  availability:
    description: Table refreshed daily by 06:00 UTC.
    percentage: "99.5%"
  freshness:
    description: Data must not be older than 25 hours.
    threshold: 25h
  completeness:
    description: >
      At least 95% of rows must pass all error-level quality checks.
    threshold: "95%"

support:
  channel: "#data-quality-alerts"
  on_call: data-eng-oncall@example.com
"""

import os
os.makedirs(os.path.dirname(contract_path), exist_ok=True) if os.path.dirname(contract_path) else None
with open(contract_path, "w") as fh:
    fh.write(CONTRACT_YAML)

print(f"Contract written to: {contract_path}")

# COMMAND ----------

# MAGIC %md ## Load & Display Contract

# COMMAND ----------

import yaml

with open(contract_path, "r") as fh:
    contract = yaml.safe_load(fh)

print(f"Contract ID   : {contract['id']}")
print(f"Name          : {contract['name']}")
print(f"Version       : {contract['version']}")
print(f"Status        : {contract['status']}")
print(f"Domain        : {contract['domain']}")
print(f"Description   : {contract['description']['purpose'][:80]}...")

# COMMAND ----------

# MAGIC %md ## Service Level Agreements

# COMMAND ----------

sla = contract.get("servicelevels", {})
print("Service Level Agreements:")
for key, val in sla.items():
    desc      = val.get("description", "")
    threshold = val.get("threshold", val.get("percentage", ""))
    print(f"  {key:<15}: {desc.strip()}  [{threshold}]")

# COMMAND ----------

# MAGIC %md ## Extract Column-Level Quality Rules → `contract_rules`

# COMMAND ----------

from pyspark.sql import Row

rules = []

for table in contract.get("schema", []):
    table_name = table["name"]
    for col in table.get("columns", []):
        col_name = col["name"]
        for rule in col.get("quality", []):
            rules.append(Row(
                table_name   = table_name,
                column_name  = col_name,
                rule_type    = rule["rule"],
                criticality  = rule.get("criticality", "error"),
                description  = rule.get("description", ""),
                min_value    = str(rule["minValue"])    if "minValue"    in rule else None,
                max_value    = str(rule["maxValue"])    if "maxValue"    in rule else None,
                pattern      = rule.get("pattern"),
                allowed_vals = ",".join(rule["allowedValues"]) if "allowedValues" in rule else None,
            ))

rules_df = spark.createDataFrame(rules)
rules_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.contract_rules")
print(f"Extracted {rules_df.count()} quality rules → {catalog}.{schema}.contract_rules")

# COMMAND ----------

# MAGIC %md ## Preview Rules

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.contract_rules").orderBy("table_name", "column_name"))

# COMMAND ----------

# MAGIC %md ## Rule Count by Type

# COMMAND ----------

from pyspark.sql import functions as F

display(
    spark.table(f"{catalog}.{schema}.contract_rules")
    .groupBy("rule_type", "criticality")
    .agg(F.count("*").alias("rule_count"))
    .orderBy("criticality", "rule_type")
)
