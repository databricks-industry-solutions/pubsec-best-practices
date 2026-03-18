# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Transform Raw Data → Curated Orders
# MAGIC
# MAGIC Joins `raw_orders`, `raw_customers`, and `raw_products` into a unified
# MAGIC **`curated_orders`** table that the data contract governs.
# MAGIC
# MAGIC Some data quality issues from the raw layer **intentionally persist** so DQX
# MAGIC has meaningful violations to catch.

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dq_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Load Raw Tables

# COMMAND ----------

raw_orders    = spark.table(f"{catalog}.{schema}.raw_orders")
raw_customers = spark.table(f"{catalog}.{schema}.raw_customers")
raw_products  = spark.table(f"{catalog}.{schema}.raw_products")

print(f"raw_orders:    {raw_orders.count():>6} rows")
print(f"raw_customers: {raw_customers.count():>6} rows")
print(f"raw_products:  {raw_products.count():>6} rows")

# COMMAND ----------

# MAGIC %md ## Transform & Enrich

# COMMAND ----------

from pyspark.sql import functions as F

# Deduplicate customers — keep first occurrence per customer_id
customers_deduped = (
    raw_customers
    .withColumn("_rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("customer_id").orderBy("signup_date")
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# Enrich orders with customer and product details
curated = (
    raw_orders.alias("o")
    .join(customers_deduped.alias("c"), on="customer_id", how="left")
    .join(raw_products.alias("p"),      on="product_id",  how="left")
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("c.customer_name"),
        F.col("c.email").alias("customer_email"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category").alias("product_category"),
        F.to_date("o.order_date").alias("order_date"),
        F.col("o.quantity").cast("integer"),
        F.col("o.unit_price").cast("double"),
        # Keep raw total_amount (may be wrong — DQX will flag it)
        F.col("o.total_amount").cast("double"),
        # Derived: what total SHOULD be
        F.round(F.col("o.quantity").cast("double") * F.col("o.unit_price"), 2)
          .alias("expected_total"),
        F.lower(F.col("o.order_status")).alias("order_status"),   # normalise case
        F.col("o.shipping_region").alias("region"),
        F.current_timestamp().alias("processed_at"),
    )
)

curated.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.curated_orders")
print(f"curated_orders written: {curated.count()} rows")

# COMMAND ----------

# MAGIC %md ## Preview

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.curated_orders").limit(20))

# COMMAND ----------

# MAGIC %md ## Row Counts vs Raw

# COMMAND ----------

raw_cnt     = raw_orders.count()
curated_cnt = spark.table(f"{catalog}.{schema}.curated_orders").count()

print(f"raw_orders    : {raw_cnt}")
print(f"curated_orders: {curated_cnt}")
print(f"Delta         : {raw_cnt - curated_cnt} rows dropped (none expected — join is LEFT)")
