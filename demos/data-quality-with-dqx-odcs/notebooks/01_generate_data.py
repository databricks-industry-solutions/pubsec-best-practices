# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Generate Synthetic Data
# MAGIC
# MAGIC This notebook generates synthetic e-commerce data with **intentional quality issues**
# MAGIC that will be caught later by DQX checks derived from the ODCS data contract.
# MAGIC
# MAGIC **Data generated:**
# MAGIC - `raw_customers` — customer master records (with some nulls, duplicates)
# MAGIC - `raw_products`  — product catalog (with some invalid prices)
# MAGIC - `raw_orders`    — order transactions (with nulls, out-of-range values, invalid statuses)

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dq_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"Using {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Generate: Customers

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import random
from datetime import date, timedelta

random.seed(42)

def rand_date(start_year=2020, end_year=2024):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

regions = ["North", "South", "East", "West"]

# 200 clean customers + 5 with injected issues
customers = []
for i in range(1, 201):
    customers.append(Row(
        customer_id   = f"CUST-{i:04d}",
        customer_name = f"Customer {i}",
        email         = f"customer{i}@example.com",
        region        = random.choice(regions),
        signup_date   = str(rand_date()),
    ))

# Inject issues: null names, null regions, duplicate IDs
customers.append(Row(customer_id="CUST-0001", customer_name="Customer 1 Duplicate", email="dup@example.com", region="North", signup_date="2022-01-01"))  # duplicate
customers.append(Row(customer_id="CUST-9001", customer_name=None,          email="null_name@example.com", region="East",  signup_date="2023-03-15"))  # null name
customers.append(Row(customer_id="CUST-9002", customer_name="No Region",   email="no_region@example.com",  region=None,   signup_date="2023-06-01"))  # null region

customers_df = spark.createDataFrame(customers)
customers_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_customers")
print(f"raw_customers: {customers_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Generate: Products

# COMMAND ----------

categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]

products = []
for i in range(1, 51):
    products.append(Row(
        product_id   = f"PROD-{i:03d}",
        product_name = f"Product {i}",
        category     = random.choice(categories),
        unit_price   = round(random.uniform(5.0, 999.99), 2),
        in_stock     = random.choice([True, True, True, False]),
    ))

# Inject issues: negative price, zero price, null name
products.append(Row(product_id="PROD-901", product_name="Bad Price Item",  category="Electronics", unit_price=-9.99,  in_stock=True))   # negative price
products.append(Row(product_id="PROD-902", product_name="Free Item",       category="Books",       unit_price=0.0,    in_stock=True))   # zero price
products.append(Row(product_id="PROD-903", product_name=None,              category="Toys",        unit_price=19.99,  in_stock=False))  # null name

products_df = spark.createDataFrame(products)
products_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_products")
print(f"raw_products: {products_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Generate: Orders

# COMMAND ----------

statuses       = ["pending", "processing", "shipped", "delivered", "cancelled"]
bad_statuses   = ["UNKNOWN", "refunded", "error", ""]  # invalid values

orders = []
customer_ids = [f"CUST-{i:04d}" for i in range(1, 201)]
product_ids  = [f"PROD-{i:03d}" for i in range(1, 51)]

for i in range(1, 1001):
    cid = random.choice(customer_ids)
    pid = random.choice(product_ids)
    qty = random.randint(1, 20)
    price = round(random.uniform(5.0, 499.99), 2)
    orders.append(Row(
        order_id       = f"ORD-{i:05d}",
        customer_id    = cid,
        product_id     = pid,
        order_date     = str(rand_date()),
        quantity       = qty,
        unit_price     = price,
        total_amount   = round(qty * price, 2),   # correct calculation
        order_status   = random.choice(statuses),
        shipping_region= random.choice(regions),
    ))

# ── Inject quality issues ─────────────────────────────────────────────────────
# Null order_id
orders.append(Row(order_id=None,         customer_id="CUST-0010", product_id="PROD-001", order_date="2023-05-01", quantity=2,    unit_price=19.99,  total_amount=39.98,  order_status="pending",    shipping_region="North"))
# Null customer_id
orders.append(Row(order_id="ORD-10001",  customer_id=None,        product_id="PROD-002", order_date="2023-06-15", quantity=1,    unit_price=49.99,  total_amount=49.99,  order_status="shipped",    shipping_region="South"))
# Duplicate order_id
orders.append(Row(order_id="ORD-00001",  customer_id="CUST-0020", product_id="PROD-010", order_date="2023-07-01", quantity=3,    unit_price=99.99,  total_amount=299.97, order_status="delivered",  shipping_region="East"))
# Out-of-range quantity (> 1000)
orders.append(Row(order_id="ORD-10002",  customer_id="CUST-0030", product_id="PROD-015", order_date="2023-08-20", quantity=5000, unit_price=5.00,   total_amount=25000.0, order_status="processing", shipping_region="West"))
# Negative quantity
orders.append(Row(order_id="ORD-10003",  customer_id="CUST-0040", product_id="PROD-020", order_date="2023-09-10", quantity=-1,   unit_price=75.00,  total_amount=-75.00, order_status="cancelled",  shipping_region="North"))
# Invalid status
orders.append(Row(order_id="ORD-10004",  customer_id="CUST-0050", product_id="PROD-025", order_date="2023-10-01", quantity=2,    unit_price=30.00,  total_amount=60.00,  order_status="UNKNOWN",    shipping_region="South"))
orders.append(Row(order_id="ORD-10005",  customer_id="CUST-0060", product_id="PROD-030", order_date="2023-11-05", quantity=1,    unit_price=200.00, total_amount=200.00, order_status="refunded",   shipping_region="East"))
# Invalid region
orders.append(Row(order_id="ORD-10006",  customer_id="CUST-0070", product_id="PROD-035", order_date="2023-12-15", quantity=4,    unit_price=55.00,  total_amount=220.00, order_status="pending",    shipping_region="Pacific"))
# Mismatched total_amount (qty * unit_price doesn't match)
orders.append(Row(order_id="ORD-10007",  customer_id="CUST-0080", product_id="PROD-040", order_date="2024-01-10", quantity=3,    unit_price=100.00, total_amount=999.99, order_status="shipped",    shipping_region="West"))
# Future order date
orders.append(Row(order_id="ORD-10008",  customer_id="CUST-0090", product_id="PROD-005", order_date="2030-06-01", quantity=1,    unit_price=25.00,  total_amount=25.00,  order_status="pending",    shipping_region="North"))
# Null order_date
orders.append(Row(order_id="ORD-10009",  customer_id="CUST-0100", product_id="PROD-008", order_date=None,         quantity=2,    unit_price=80.00,  total_amount=160.00, order_status="delivered",  shipping_region="South"))
# Zero unit_price
orders.append(Row(order_id="ORD-10010",  customer_id="CUST-0110", product_id="PROD-012", order_date="2024-02-14", quantity=5,    unit_price=0.00,   total_amount=0.00,   order_status="processing", shipping_region="East"))

orders_df = spark.createDataFrame(orders)
orders_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.raw_orders")
print(f"raw_orders: {orders_df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("=" * 60)
print("Data Generation Complete")
print("=" * 60)
for tbl, label in [("raw_customers", "Customers"), ("raw_products", "Products"), ("raw_orders", "Orders")]:
    cnt = spark.table(f"{catalog}.{schema}.{tbl}").count()
    print(f"  {label:<12}: {cnt:>6} rows  →  {catalog}.{schema}.{tbl}")
print("=" * 60)
print("Intentional quality issues injected — ready for DQX checks.")
