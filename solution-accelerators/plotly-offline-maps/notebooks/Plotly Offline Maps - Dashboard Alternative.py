# Databricks notebook source
# MAGIC %md
# MAGIC # Plotly Offline Maps — Dashboard Alternative
# MAGIC
# MAGIC Renders US state and county choropleth maps using **Databricks AI/BI Lakeview
# MAGIC dashboards** — no Plotly, no CDN, no browser-side JavaScript fetch.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why a Dashboard Alternative?
# MAGIC
# MAGIC The main `Plotly Offline Maps` notebook solves the air-gap problem with
# MAGIC `go.Scattergeo` + `fill='toself'`. That approach works well but has trade-offs:
# MAGIC
# MAGIC | Issue | `go.Scattergeo` approach | Dashboard approach |
# MAGIC |-------|--------------------------|-------------------|
# MAGIC | Rendering location | Browser (client-side) | Databricks server (server-side) |
# MAGIC | Internet required? | No (after fix) | **Never — fully server-side** |
# MAGIC | Colorscale/colorbar | Manual Python math | Built-in, automatic |
# MAGIC | Shareable URL | No — lives in notebook cell | **Yes — dashboard has its own URL** |
# MAGIC | Embeds in reports | No | **Yes — iframes, PDF export** |
# MAGIC | Filters / slicers | Manual widget wiring | **Built-in — click to filter** |
# MAGIC | Performance (counties) | Slow (~3000 traces) | Fast — rendered as image tiles |
# MAGIC | Requires Plotly | Yes | **No** |
# MAGIC
# MAGIC ## How Lakeview Maps Work (Air-Gap Safety)
# MAGIC
# MAGIC Databricks AI/BI dashboards render map visualizations **on the Databricks server**,
# MAGIC not in the user's browser. The browser receives a rendered image tile, not raw
# MAGIC GeoJSON + JavaScript that it must execute.
# MAGIC
# MAGIC Geographic boundary data (US state outlines, county outlines) is bundled into the
# MAGIC Databricks platform and served from the same origin as the workspace — no CDN,
# MAGIC no external tile server.
# MAGIC
# MAGIC ### Supported native location types (no GeoJSON file needed)
# MAGIC
# MAGIC | Location type | Column value format | Example |
# MAGIC |---------------|---------------------|---------|
# MAGIC | `us_state_abbr` | 2-letter postal code | `"CA"` |
# MAGIC | `us_county_fips` | 5-digit FIPS string | `"06037"` |
# MAGIC | `country` | ISO 3166-1 alpha-2 | `"US"` |
# MAGIC | `latitude` / `longitude` | Decimal degrees | `37.77`, `-122.42` |
# MAGIC
# MAGIC These location types are resolved by Databricks — no external geocoding call,
# MAGIC no browser-side topojson fetch.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC 1. Creates two Delta tables in your catalog/schema:
# MAGIC    - `plotly_maps_states` — one row per US state with a random metric value
# MAGIC    - `plotly_maps_counties` — one row per US county (FIPS) with a random metric value
# MAGIC 2. Prints instructions for creating an AI/BI dashboard over those tables.
# MAGIC
# MAGIC After running this notebook, create the dashboard manually in the Databricks UI
# MAGIC (see instructions in the final cell), or use the Databricks SDK to create it
# MAGIC programmatically via the Lakeview API.
# MAGIC
# MAGIC **Prerequisites:** Run `Plotly Offline Maps - Setup` first to populate the Volume,
# MAGIC OR use the synthetic data option below (no Volume required).

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main",            "Catalog")
dbutils.widgets.text("schema",  "default",         "Schema")
dbutils.widgets.text("volume",  "plotly_topojson", "Volume")

# COMMAND ----------

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
VOLUME      = dbutils.widgets.get("volume")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

STATES_TABLE   = f"{CATALOG}.{SCHEMA}.plotly_maps_states"
COUNTIES_TABLE = f"{CATALOG}.{SCHEMA}.plotly_maps_counties"

print(f"States table:   {STATES_TABLE}")
print(f"Counties table: {COUNTIES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the States Table
# MAGIC
# MAGIC The state abbreviation column (`state_abbr`) maps to Lakeview's `us_state_abbr`
# MAGIC location type. Lakeview resolves state boundaries internally — no GeoJSON file
# MAGIC needed.

# COMMAND ----------

import random
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# All 50 states + DC
STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
]

random.seed(42)
state_rows = [Row(state_abbr=code, metric_value=random.randint(10, 100)) for code in STATE_CODES]

schema = StructType([
    StructField("state_abbr",   StringType(),  False),
    StructField("metric_value", IntegerType(), False),
])

df_states = spark.createDataFrame(state_rows, schema)
df_states.write.format("delta").mode("overwrite").saveAsTable(STATES_TABLE)

print(f"Wrote {df_states.count()} rows to {STATES_TABLE}")
display(df_states.orderBy("state_abbr"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Counties Table
# MAGIC
# MAGIC Option A: Load FIPS codes from the Volume GeoJSON (requires Setup notebook).
# MAGIC Option B: Inline fallback if the Volume isn't available.
# MAGIC
# MAGIC The `county_fips` column maps to Lakeview's `us_county_fips` location type.

# COMMAND ----------

import json
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

COUNTIES_GEOJSON = f"{VOLUME_PATH}/geojson-counties-fips.json"

if os.path.isfile(COUNTIES_GEOJSON):
    # Load FIPS codes from the Volume GeoJSON
    with open(COUNTIES_GEOJSON) as f:
        counties_geojson = json.load(f)
    fips_codes = [feat["id"] for feat in counties_geojson["features"]]
    print(f"Loaded {len(fips_codes)} FIPS codes from Volume")
else:
    # Fallback: a representative subset of FIPS codes so the table is still useful
    # without requiring the Volume. Replace with your actual data in production.
    print(f"Volume not found at {COUNTIES_GEOJSON} — using representative sample")
    fips_codes = [
        # Alabama
        "01001","01003","01005","01007","01009",
        # California
        "06001","06037","06073","06059","06065",
        # Texas
        "48001","48113","48201","48439","48453",
        # New York
        "36005","36047","36061","36081","36085",
        # Florida
        "12011","12086","12095","12057","12099",
    ]
    print(f"Using {len(fips_codes)} sample FIPS codes")

random.seed(99)
county_rows = [
    Row(county_fips=fips, metric_value=round(random.uniform(0, 100), 1))
    for fips in fips_codes
]

schema = StructType([
    StructField("county_fips",  StringType(), False),
    StructField("metric_value", DoubleType(), False),
])

df_counties = spark.createDataFrame(county_rows, schema)
df_counties.write.format("delta").mode("overwrite").saveAsTable(COUNTIES_TABLE)

print(f"Wrote {df_counties.count()} rows to {COUNTIES_TABLE}")
display(df_counties.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the AI/BI Dashboard
# MAGIC
# MAGIC ### Option A: Databricks UI (recommended first time)
# MAGIC
# MAGIC 1. In the left sidebar, click **Dashboards** (or go to **New → Dashboard**)
# MAGIC 2. Click **+ Add visualization**
# MAGIC 3. Set **Dataset** to `plotly_maps_states` (or `plotly_maps_counties`)
# MAGIC 4. Set **Visualization type** to **Map**
# MAGIC 5. Configure the map widget:
# MAGIC
# MAGIC    **For the states map:**
# MAGIC    | Field | Value |
# MAGIC    |-------|-------|
# MAGIC    | Location column | `state_abbr` |
# MAGIC    | Location type | `United States (abbrev.)` |
# MAGIC    | Color column | `metric_value` |
# MAGIC    | Color scale | Blues (or your preference) |
# MAGIC
# MAGIC    **For the counties map:**
# MAGIC    | Field | Value |
# MAGIC    |-------|-------|
# MAGIC    | Location column | `county_fips` |
# MAGIC    | Location type | `US county (FIPS)` |
# MAGIC    | Color column | `metric_value` |
# MAGIC    | Color scale | YlOrRd (or your preference) |
# MAGIC
# MAGIC 6. Click **Save** and optionally **Publish**
# MAGIC
# MAGIC The map renders server-side — no browser CDN fetch, no JavaScript topojson
# MAGIC download, no internet access required.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option B: Lakeview API (programmatic)
# MAGIC
# MAGIC Run the cell below to create the dashboard via the Databricks SDK.
# MAGIC Requires `databricks-sdk>=0.20` (available on DBR 14+).

# COMMAND ----------

# Programmatic dashboard creation via Lakeview API
# Requires databricks-sdk>=0.20 — skip this cell if not available.

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Dashboard
    import base64, json as _json

    w = WorkspaceClient()

    dashboard_spec = {
        "datasets": [
            {
                "name": "ds_states",
                "displayName": "US States",
                "queryLines": [f"SELECT state_abbr, metric_value FROM {STATES_TABLE}"],
            },
            {
                "name": "ds_counties",
                "displayName": "US Counties",
                "queryLines": [f"SELECT county_fips, metric_value FROM {COUNTIES_TABLE}"],
            },
        ],
        "pages": [
            {
                "name": "page_maps",
                "displayName": "Maps",
                "layout": [
                    {
                        "widget": {
                            "name": "states_map",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "ds_states",
                                        "fields": [
                                            {"name": "state_abbr", "expression": "`state_abbr`"},
                                            {"name": "metric_value", "expression": "`metric_value`"},
                                        ],
                                        "disaggregated": False,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 1,
                                "widgetType": "choropleth-map",
                                "encodings": {
                                    "region": {"fieldName": "state_abbr", "regionType": "mapbox-v4-postal-us"},
                                    "color": {"fieldName": "metric_value", "displayName": "Metric Value"},
                                },
                                "frame": {"showTitle": True, "title": "US States Choropleth"},
                            },
                        },
                        "position": {"x": 0, "y": 0, "width": 3, "height": 6},
                    },
                    {
                        "widget": {
                            "name": "counties_map",
                            "queries": [
                                {
                                    "name": "main_query",
                                    "query": {
                                        "datasetName": "ds_counties",
                                        "fields": [
                                            {"name": "county_fips", "expression": "`county_fips`"},
                                            {"name": "metric_value", "expression": "`metric_value`"},
                                        ],
                                        "disaggregated": False,
                                    },
                                }
                            ],
                            "spec": {
                                "version": 1,
                                "widgetType": "choropleth-map",
                                "encodings": {
                                    "region": {"fieldName": "county_fips", "regionType": "mapbox-v4-fips-us"},
                                    "color": {"fieldName": "metric_value", "displayName": "Metric Value"},
                                },
                                "frame": {"showTitle": True, "title": "US Counties Choropleth"},
                            },
                        },
                        "position": {"x": 3, "y": 0, "width": 3, "height": 6},
                    },
                ],
            }
        ],
    }

    serialized = _json.dumps(dashboard_spec)

    dashboard = w.lakeview.create(
        display_name="Plotly Offline Maps — Dashboard Alternative",
        serialized_dashboard=serialized,
    )

    print(f"Dashboard created: {dashboard.dashboard_id}")
    print(f"URL: {w.config.host}/sql/dashboardsv3/{dashboard.dashboard_id}")

except ImportError:
    print("databricks-sdk not available — use the UI steps in the markdown cell above.")
except Exception as e:
    print(f"SDK creation failed: {e}")
    print("Use the UI steps in the markdown cell above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Air-Gap Safety Summary
# MAGIC
# MAGIC | Component | Plotly `go.Choropleth` | Plotly `go.Scattergeo` fix | **Lakeview Dashboard** |
# MAGIC |-----------|----------------------|--------------------------|----------------------|
# MAGIC | Map library | CDN fetch (fails) | Inline bundle | Server-side |
# MAGIC | Geographic boundaries | CDN topojson fetch (fails) | UC Volume GeoJSON | **Bundled in platform** |
# MAGIC | Rendering location | Browser | Browser | **Server** |
# MAGIC | Internet required | Yes | No | **No** |
# MAGIC | Shareable URL | No | No | **Yes** |
# MAGIC | Interactive filters | Manual | Manual | **Built-in** |
# MAGIC | Colorbar / legend | Automatic | Manual Python math | **Automatic** |
# MAGIC
# MAGIC For notebook-embedded maps (e.g. in a runbook or EDA notebook), use the
# MAGIC `go.Scattergeo` approach from `Plotly Offline Maps`.
# MAGIC
# MAGIC For published, shared, or operational maps, use Lakeview dashboards — they are
# MAGIC simpler to configure and natively air-gap safe with no workarounds required.
