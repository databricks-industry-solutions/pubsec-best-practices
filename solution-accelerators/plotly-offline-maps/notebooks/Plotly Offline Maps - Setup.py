# Databricks notebook source
# MAGIC %md
# MAGIC # Plotly Offline Maps — Setup
# MAGIC
# MAGIC Loads GeoJSON boundary files into a Unity Catalog Volume so that the main
# MAGIC notebook can render Plotly choropleth maps with **no internet access required**.
# MAGIC
# MAGIC ## Sources (checked in priority order)
# MAGIC
# MAGIC | # | Source | When it applies |
# MAGIC |---|--------|-----------------|
# MAGIC | 1 | **Volume** | Files already uploaded manually via Catalog Explorer UI |
# MAGIC | 2 | **Repo `data/` folder** | Notebook running from a Workspace Git folder |
# MAGIC | 3 | **Download** | First-time setup with cluster internet access |
# MAGIC
# MAGIC ### Manual upload (no git, no cluster internet)
# MAGIC 1. Download from the GitHub repo to your laptop:
# MAGIC    - `solution-accelerators/plotly-offline-maps/data/us-states.json`
# MAGIC    - `solution-accelerators/plotly-offline-maps/data/geojson-counties-fips.json`
# MAGIC 2. In **Catalog Explorer** → your volume → **Upload to this volume**
# MAGIC 3. Upload both files, then run this notebook — it detects them automatically.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "services_bureau_catalog", "Catalog")
dbutils.widgets.text("schema",  "default",                 "Schema")
dbutils.widgets.text("volume",  "plotly_topojson",         "Volume")

# COMMAND ----------

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
VOLUME      = dbutils.widgets.get("volume")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Target volume: {VOLUME_PATH}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md ## Load boundary files

# COMMAND ----------

import os
import shutil
import urllib.request

# Notebook directory — works in Workspace Git folders
NOTEBOOK_DIR  = "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
REPO_DATA_DIR = os.path.join(NOTEBOOK_DIR, "..", "data")

print(f"Notebook dir:      {NOTEBOOK_DIR}")
print(f"Repo data dir:     {REPO_DATA_DIR}")
print(f"Repo data present: {os.path.isdir(REPO_DATA_DIR)}")
print()

# dest_json  = filename on the Volume
# repo_file  = filename in data/
# fallback   = download URL if neither Volume nor repo file is present
FILES = {
    "us-states.json": (
        "us-states.json",
        "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json",
    ),
    "geojson-counties-fips.json": (
        "geojson-counties-fips.json",
        "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
    ),
    # TopoJSON base map used by window.PlotlyGeoAssets pre-injection.
    # Pre-populating this cache before plotly.min.js loads suppresses all CDN
    # topojson fetches — making go.Choropleth and go.Scattergeo work offline.
    "usa_110m.json": (
        "usa_110m.json",
        "https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/usa_110m.json",
    ),
}

os.makedirs("/tmp/plotly_geo", exist_ok=True)

for dest_name, (repo_file, fallback_url) in FILES.items():
    volume_json = f"{VOLUME_PATH}/{dest_name}"
    repo_path   = os.path.join(REPO_DATA_DIR, repo_file)
    local_path  = f"/tmp/plotly_geo/{dest_name}"

    if os.path.isfile(volume_json):
        # Source 1 — already on Volume
        print(f"{dest_name}: already on Volume, skipping.")
        continue

    elif os.path.isfile(repo_path):
        # Source 2 — running from Workspace Git folder
        print(f"{dest_name}: loading from repo data/")
        shutil.copy(repo_path, local_path)

    else:
        # Source 3 — download fallback (requires internet)
        print(f"{dest_name}: downloading from {fallback_url}")
        urllib.request.urlretrieve(fallback_url, local_path)

    size_kb = os.path.getsize(local_path) / 1024
    print(f"  → {size_kb:.0f} KB, copying to Volume...")
    shutil.copy(local_path, volume_json)
    print(f"  → done: {volume_json}")

print("\nVolume contents:")
display(dbutils.fs.ls(VOLUME_PATH))
