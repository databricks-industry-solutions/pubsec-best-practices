# Databricks notebook source
# MAGIC %md
# MAGIC # Plotly Offline Maps
# MAGIC
# MAGIC Renders US state and county choropleth maps in Databricks notebooks with
# MAGIC **no internet access required** — no CDN, no proxy, no tile server.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Background: Why Plotly Maps Fail in Air-Gapped Environments
# MAGIC
# MAGIC When Plotly renders a geographic map in a Databricks notebook, it uses
# MAGIC `displayHTML()` to inject an HTML/JavaScript blob into the cell output.
# MAGIC That JavaScript runs **in the user's browser** — not on the cluster — and it
# MAGIC makes outbound network calls that fail silently when internet access is blocked.
# MAGIC
# MAGIC There are two separate CDN calls to understand:
# MAGIC
# MAGIC ### 1. `plotly.min.js` (the rendering library)
# MAGIC
# MAGIC By default, Plotly generates:
# MAGIC ```html
# MAGIC <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
# MAGIC ```
# MAGIC **Fix:** Call `get_plotlyjs()` which reads `plotly.min.js` directly from the
# MAGIC cluster's Python package and injects it inline. No network call needed.
# MAGIC
# MAGIC ### 2. TopoJSON base map files (geographic boundaries)
# MAGIC
# MAGIC This is the subtler problem. Plotly's geo rendering pipeline checks
# MAGIC `window.PlotlyGeoAssets.topojson[name]` before every render. If the key is
# MAGIC missing or undefined, it fires a CDN fetch:
# MAGIC
# MAGIC ```js
# MAGIC // In plotly.min.js — fires for every geo trace
# MAGIC PlotlyGeoAssets.topojson[name] === void 0 &&
# MAGIC     o.push(i.fetchTopojson())  // <-- CDN fetch, fires if cache is empty
# MAGIC ```
# MAGIC
# MAGIC This fetch is triggered by:
# MAGIC - `go.Choropleth` — always, via `locationmode`
# MAGIC - `go.Scattergeo` with `scope=` set — via base map layer flags (`showcoastlines`, `showsubunits`, etc.)
# MAGIC - `go.Scattergeo` with `projection_type=` set — via default `showcoastlines=True`
# MAGIC
# MAGIC **Fix:** Pre-populate `window.PlotlyGeoAssets.topojson` with the TopoJSON file
# MAGIC content **before** `plotly.min.js` loads. Plotly checks the cache first — if the
# MAGIC key exists, the CDN fetch is skipped entirely.
# MAGIC
# MAGIC ```js
# MAGIC // Inject this BEFORE plotly.min.js
# MAGIC window.PlotlyGeoAssets = { topojson: { "usa_110m": <json content> } };
# MAGIC ```
# MAGIC
# MAGIC The TopoJSON file (`usa_110m.json`, ~49 KB) is stored in the UC Volume alongside
# MAGIC the GeoJSON boundary files and loaded at render time from the cluster filesystem.
# MAGIC
# MAGIC ### Affected Plotly chart types
# MAGIC
# MAGIC | Chart type | CDN fetch trigger | Air-gap safe |
# MAGIC |---|---|---|
# MAGIC | `go.Choropleth` | `locationmode` always set | **Yes — with pre-injection** |
# MAGIC | `go.Scattergeo` (any scope/projection) | Base map layer flags | **Yes — with pre-injection** |
# MAGIC | `go.Choroplethmapbox` | Mapbox tile server | No — needs tile server |
# MAGIC | `go.Scattermapbox` | Mapbox tile server | No — needs tile server |
# MAGIC | `px.choropleth()` | Same as `go.Choropleth` | **Yes — with pre-injection** |
# MAGIC | `px.scatter_geo()` | Same as `go.Scattergeo` | **Yes — with pre-injection** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Solution: `window.PlotlyGeoAssets` Pre-Injection
# MAGIC
# MAGIC Pre-populate the TopoJSON cache before `plotly.min.js` executes. The injection
# MAGIC must come first in the HTML — Plotly initializes `PlotlyGeoAssets` with a guard
# MAGIC (`PlotlyGeoAssets === void 0`) so a pre-existing object is left intact.
# MAGIC
# MAGIC ```html
# MAGIC <!-- 1. Pre-populate cache — MUST come before plotly.min.js -->
# MAGIC <script>window.PlotlyGeoAssets = { topojson: { "usa_110m": {...} } };</script>
# MAGIC <!-- 2. Plotly bundle — reads from cluster package, no CDN -->
# MAGIC <script>/* plotly.min.js contents */</script>
# MAGIC <!-- 3. Figure HTML -->
# MAGIC <div id="plot"></div>
# MAGIC <script>Plotly.newPlot(...)</script>
# MAGIC ```
# MAGIC
# MAGIC With this in place, `go.Choropleth` works normally with full Albers USA projection,
# MAGIC automatic colorscale, colorbar, and hover — all offline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Notebook cell
# MAGIC   │
# MAGIC   ├─ Reads usa_110m.json from UC Volume    (cluster filesystem — no internet)
# MAGIC   ├─ Reads GeoJSON from UC Volume           (cluster filesystem — no internet)
# MAGIC   ├─ Builds go.Choropleth figure            (Python — no internet)
# MAGIC   ├─ Calls get_plotlyjs()                   (reads from cluster's plotly package — no internet)
# MAGIC   └─ displayHTML("<script>PlotlyGeoAssets...</script><script>plotly.min.js</script>...")
# MAGIC         │
# MAGIC         └─ Renders in browser iframe  (all JS + data embedded inline — no internet)
# MAGIC ```
# MAGIC
# MAGIC **Prerequisites:** Run `Plotly Offline Maps - Setup` once to populate the Volume
# MAGIC (including `usa_110m.json`).

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

print(f"Volume path: {VOLUME_PATH}")

# COMMAND ----------

# Plotly ships with DBR 13+. Only install if missing.
try:
    import plotly
    print(f"Plotly {plotly.__version__} already available")
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly", "--quiet"])
    import plotly
    print(f"Plotly {plotly.__version__} installed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: `offline_map_html()`
# MAGIC
# MAGIC Renders a Plotly figure as a fully offline HTML string suitable for `displayHTML()`.
# MAGIC
# MAGIC Pre-injects `window.PlotlyGeoAssets` with the TopoJSON base map before
# MAGIC `plotly.min.js` loads. Plotly checks this cache before every CDN fetch — if the
# MAGIC key exists, the fetch is skipped. This makes `go.Choropleth`, `go.Scattergeo`,
# MAGIC and all other geo trace types work without any internet access.

# COMMAND ----------

import plotly.io as pio
from plotly.offline import get_plotlyjs

# Load once — reused by every map cell
with open(f"{VOLUME_PATH}/usa_110m.json") as f:
    _USA_TOPOJSON = f.read()

_PLOTLYJS = get_plotlyjs()

def offline_map_html(fig):
    """
    Return a self-contained HTML string for displayHTML() that renders the
    figure with no outbound network calls.

    Pre-injects window.PlotlyGeoAssets before plotly.min.js so Plotly skips
    its CDN topojson fetch. Works with go.Choropleth, go.Scattergeo, and any
    other geo trace type.

    Args:
        fig: A plotly.graph_objects.Figure with geo traces.

    Returns:
        HTML string — pass directly to displayHTML().
    """
    fig_json = pio.to_json(fig)
    return (
        f"<script>window.PlotlyGeoAssets={{topojson:{{\"usa_110m\":{_USA_TOPOJSON}}}}};"
        f"{_PLOTLYJS}</script>"
        f"<div id='plot'></div>"
        f"<script>(function(){{var f={fig_json};"
        f"Plotly.newPlot('plot',f.data,f.layout,{{responsive:true}})}})();</script>"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## US States Choropleth
# MAGIC
# MAGIC Loads state boundaries from the UC Volume and renders a choropleth using
# MAGIC `go.Choropleth` with inline GeoJSON. Replace `state_values` with your own
# MAGIC `{state_code: value}` dict.

# COMMAND ----------

import json
import random
import plotly.graph_objects as go

with open(f"{VOLUME_PATH}/us-states.json") as f:
    states_geojson = json.load(f)

# Replace with your data — dict of {2-letter state code: numeric value}
random.seed(42)
state_values = {
    feat["id"]: random.randint(10, 100)
    for feat in states_geojson["features"]
}

fig_states = go.Figure(go.Choropleth(
    geojson=states_geojson,
    locations=list(state_values.keys()),
    z=list(state_values.values()),
    featureidkey="id",
    colorscale="Blues",
    marker_line_color="white",
    marker_line_width=0.5,
    colorbar_title="Value",
))
fig_states.update_geos(
    scope="usa",
    showland=True,  landcolor="#f0f0f0",
    showcoastlines=False,
    showframe=False,
    bgcolor="rgba(0,0,0,0)",
)
fig_states.update_layout(
    title_text="US States Choropleth",
    height=500,
    margin=dict(l=0, r=0, t=40, b=0),
)

displayHTML(offline_map_html(fig_states))

# COMMAND ----------

# MAGIC %md
# MAGIC ## US Counties Choropleth
# MAGIC
# MAGIC Same pattern as states but using FIPS codes. `_USA_TOPOJSON` and `_PLOTLYJS`
# MAGIC are already loaded from the cell above — `offline_map_html()` reuses them.

# COMMAND ----------

with open(f"{VOLUME_PATH}/geojson-counties-fips.json") as f:
    counties_geojson = json.load(f)

# Replace with your data — dict of {5-digit FIPS string: numeric value}
random.seed(99)
county_values = {
    feat["id"]: round(random.uniform(0, 100), 1)
    for feat in counties_geojson["features"]
}

fig_counties = go.Figure(go.Choropleth(
    geojson=counties_geojson,
    locations=list(county_values.keys()),
    z=list(county_values.values()),
    featureidkey="id",
    colorscale="YlOrRd",
    marker_line_color="white",
    marker_line_width=0,
    colorbar_title="Value",
))
fig_counties.update_geos(
    scope="usa",
    showland=True,  landcolor="#f0f0f0",
    showcoastlines=False,
    showframe=False,
    bgcolor="rgba(0,0,0,0)",
)
fig_counties.update_layout(
    title_text="US Counties Choropleth",
    height=500,
    margin=dict(l=0, r=0, t=40, b=0),
)

displayHTML(offline_map_html(fig_counties))
