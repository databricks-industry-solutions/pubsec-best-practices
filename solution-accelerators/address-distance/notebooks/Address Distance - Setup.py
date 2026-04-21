# Databricks notebook source
# MAGIC %md
# MAGIC # Address Distance — Setup
# MAGIC
# MAGIC Creates the Unity Catalog volume, loads the demo address table, and loads the
# MAGIC ZCTA (ZIP Code Tabulation Area) centroid gazetteer that the geocoding notebook
# MAGIC uses for ZIP-level geocoding.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Creates volume `{catalog}.{schema}.{volume}` (default `address_distance`)
# MAGIC 2. Creates Delta table `{catalog}.{schema}.address_demo` from `data/sample-addresses.csv`
# MAGIC    — 1 anchor (IRS HQ) + 20 comparison addresses with real lat/lon
# MAGIC 3. Creates Delta table `{catalog}.{schema}.zcta_centroids` from the sample
# MAGIC    gazetteer — used by the geocoding notebook's ZCTA path
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - DBR 17.1+ (for `ST_DISTANCESPHERE`). Older runtimes work with the H3 or
# MAGIC   Python-UDF fallbacks documented in the Core notebook.
# MAGIC - Notebook running from a Workspace Git folder (so the `data/` sibling directory
# MAGIC   is visible on the driver filesystem). If not, upload the CSVs to the volume
# MAGIC   manually via Catalog Explorer and re-run.
# MAGIC
# MAGIC ## Production gazetteer
# MAGIC
# MAGIC The bundled `us-zcta-gazetteer-sample.csv` covers only the demo ZCTAs. For
# MAGIC real workloads, download the full Census ZCTA gazetteer
# MAGIC (~1.6 MB, ~33K rows) from:
# MAGIC
# MAGIC https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html
# MAGIC
# MAGIC Upload it to the volume as `us-zcta-gazetteer.csv` with columns:
# MAGIC `zcta,lat,lon,state`. This notebook detects and prefers the full file if present.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "user_sandbox",    "Catalog")
dbutils.widgets.text("schema",  "",                "Schema (blank = derive from current_user)")
dbutils.widgets.text("volume",  "address_distance","Volume")

# COMMAND ----------

import re

def _username_to_schema(user: str) -> str:
    # Strip email domain, replace non-alphanumerics with underscores.
    # e.g. "first.m.last@agency.gov" -> "first_m_last"
    local = user.split("@", 1)[0]
    return re.sub(r"[^a-zA-Z0-9]+", "_", local).strip("_").lower()

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

if not SCHEMA:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    SCHEMA = _username_to_schema(current_user)
    print(f"Schema derived from current_user ({current_user}): {SCHEMA}")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Quote identifiers for SQL — required when names contain hyphens or other
# non-standard characters (e.g. "my-catalog-with-hyphens").
def q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"

CAT_Q       = q(CATALOG)
SCH_Q       = q(SCHEMA)
VOL_Q       = q(VOLUME)
FQ_SCHEMA   = f"{CAT_Q}.{SCH_Q}"
FQ_VOLUME   = f"{CAT_Q}.{SCH_Q}.{VOL_Q}"

print(f"Target volume: {VOLUME_PATH}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ_VOLUME}")
print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md ## Load CSVs to the volume

# COMMAND ----------

import os
import shutil

NOTEBOOK_DIR  = "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
REPO_DATA_DIR = os.path.join(NOTEBOOK_DIR, "..", "data")

print(f"Notebook dir:      {NOTEBOOK_DIR}")
print(f"Repo data dir:     {REPO_DATA_DIR}")
print(f"Repo data present: {os.path.isdir(REPO_DATA_DIR)}")

# COMMAND ----------

# Files we expect on the volume after this cell.
# Each entry: volume filename → (repo filename, optional alternate repo filename preferred if present)
FILES = {
    "sample-addresses.csv":     ("sample-addresses.csv", None),
    # Prefer the full gazetteer if the customer has uploaded it; otherwise fall back to the sample.
    "us-zcta-gazetteer.csv":    ("us-zcta-gazetteer.csv", "us-zcta-gazetteer-sample.csv"),
}

for dest_name, (primary, fallback) in FILES.items():
    volume_file = f"{VOLUME_PATH}/{dest_name}"

    if os.path.isfile(volume_file):
        print(f"{dest_name}: already on volume, skipping.")
        continue

    repo_primary  = os.path.join(REPO_DATA_DIR, primary)
    repo_fallback = os.path.join(REPO_DATA_DIR, fallback) if fallback else None

    if os.path.isfile(repo_primary):
        src = repo_primary
    elif repo_fallback and os.path.isfile(repo_fallback):
        src = repo_fallback
        print(f"{dest_name}: using sample fallback ({fallback}) — upload full file for production use.")
    else:
        raise FileNotFoundError(
            f"{dest_name}: not found on volume or in repo data/. "
            f"Upload to {volume_file} manually via Catalog Explorer, or run this notebook from a Workspace Git folder."
        )

    size_kb = os.path.getsize(src) / 1024
    print(f"{dest_name}: copying from {src} ({size_kb:.0f} KB)")
    shutil.copy(src, volume_file)

print("\nVolume contents:")
display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md ## Create Delta tables

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

address_schema = StructType([
    StructField("id",      IntegerType()),
    StructField("label",   StringType()),
    StructField("address", StringType()),
    StructField("city",    StringType()),
    StructField("state",   StringType()),
    StructField("zip",     StringType()),
    StructField("lat",     DoubleType()),
    StructField("lon",     DoubleType()),
    StructField("kind",    StringType()),
])

(spark.read
    .option("header", True)
    .schema(address_schema)
    .csv(f"{VOLUME_PATH}/sample-addresses.csv")
    .write.mode("overwrite")
    .saveAsTable(f"{FQ_SCHEMA}.address_demo"))

print(f"Created {FQ_SCHEMA}.address_demo")
display(spark.table(f"{FQ_SCHEMA}.address_demo"))

# COMMAND ----------

zcta_schema = StructType([
    StructField("zcta",  StringType()),
    StructField("lat",   DoubleType()),
    StructField("lon",   DoubleType()),
    StructField("state", StringType()),
])

(spark.read
    .option("header", True)
    .schema(zcta_schema)
    .csv(f"{VOLUME_PATH}/us-zcta-gazetteer.csv")
    .write.mode("overwrite")
    .saveAsTable(f"{FQ_SCHEMA}.zcta_centroids"))

print(f"Created {FQ_SCHEMA}.zcta_centroids")
display(spark.table(f"{FQ_SCHEMA}.zcta_centroids").limit(5))

# COMMAND ----------

# MAGIC %md ## Done
# MAGIC
# MAGIC Next: run `Address Distance - Core` to compute distances against the demo table.
