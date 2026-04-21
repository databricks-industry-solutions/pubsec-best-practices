# Databricks notebook source
# MAGIC %md # Plotly Offline Maps — Diagnostic
# MAGIC
# MAGIC Run each cell in order. Each one tests a specific layer of the stack.
# MAGIC
# MAGIC | Cell | Tests |
# MAGIC |------|-------|
# MAGIC | 1 | Plotly version + `get_plotlyjs()` bundle size |
# MAGIC | 2 | Minimal inline Choropleth — no Volume, hardcoded topojson pre-injection |
# MAGIC | 3 | Volume connectivity — lists files and sizes |
# MAGIC | 4 | States map from Volume |
# MAGIC | 5 | Counties map from Volume |
# MAGIC
# MAGIC **If cell 2 is blank:** Plotly JS is not loading or `displayHTML()` is suppressed.
# MAGIC
# MAGIC **If cells 4–5 are blank but cell 2 renders:** Volume connectivity or missing `usa_110m.json` — check cell 3 output.

# COMMAND ----------

# MAGIC %md ## 1. Plotly version + environment

# COMMAND ----------

import sys
import plotly
import plotly.graph_objects as go
import plotly.io as pio
from plotly.offline import get_plotlyjs

print(f"Python:      {sys.version}")
print(f"Plotly:      {plotly.__version__}")
print(f"plotlyjs:    {len(get_plotlyjs()):,} bytes")

# COMMAND ----------

# MAGIC %md ## 2. Minimal inline map — no Volume required
# MAGIC
# MAGIC Uses a hardcoded minimal topojson stub injected into `window.PlotlyGeoAssets`
# MAGIC before `plotly.min.js` loads. Plotly checks this cache before every CDN fetch —
# MAGIC if the key exists, the fetch is skipped entirely.
# MAGIC
# MAGIC If this cell is blank, the issue is with Plotly JS loading or `displayHTML()` itself.

# COMMAND ----------

# Minimal valid usa_110m topojson stub — just enough for Plotly to not fetch,
# with three approximate state bounding boxes for CA, TX, NY
minimal_topo = """{
  "type":"Topology","objects":{"states":{"type":"GeometryCollection","geometries":[
    {"type":"Polygon","id":"06","arcs":[[0]]},
    {"type":"Polygon","id":"48","arcs":[[1]]},
    {"type":"Polygon","id":"36","arcs":[[2]]}
  ]}},
  "arcs":[
    [[-124,32],[-114,32],[-114,42],[-124,42],[-124,32]],
    [[-106,26],[-94,26],[-94,36],[-106,36],[-106,26]],
    [[-80,40],[-72,40],[-72,45],[-80,45],[-80,40]]
  ],
  "transform":{"scale":[1,1],"translate":[0,0]}
}"""

fig = go.Figure(go.Choropleth(
    locations=["CA", "TX", "NY"],
    locationmode="USA-states",
    z=[10, 50, 90],
    colorscale="Blues",
))
fig.update_geos(scope="usa", showland=False, showframe=False)
fig.update_layout(title_text="DIAGNOSTIC: minimal inline map", height=400)

js = get_plotlyjs()
fj = pio.to_json(fig)
displayHTML(
    f"<script>window.PlotlyGeoAssets={{topojson:{{\"usa_110m\":{minimal_topo}}}}};"
    f"{js}</script>"
    f"<div id='p'></div>"
    f"<script>(function(){{var f={fj};Plotly.newPlot('p',f.data,f.layout,{{responsive:true}})}})();</script>"
)

# COMMAND ----------

# MAGIC %md ## 3. Volume connectivity check

# COMMAND ----------

dbutils.widgets.text("catalog", "services_bureau_catalog", "Catalog")
dbutils.widgets.text("schema",  "default",                 "Schema")
dbutils.widgets.text("volume",  "plotly_topojson",         "Volume")

# COMMAND ----------

import os

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
VOLUME      = dbutils.widgets.get("volume")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

files = os.listdir(VOLUME_PATH)
print(f"Volume: {VOLUME_PATH}")
print(f"Files:  {files}")

for fname in files:
    path = f"{VOLUME_PATH}/{fname}"
    size = os.path.getsize(path)
    print(f"  {fname}: {size:,} bytes ({size/1024:.0f} KB)")

# Expected:
#   us-states.json:             ~87 KB
#   geojson-counties-fips.json: ~1,700 KB
#   usa_110m.json:              ~49 KB  ← required for offline rendering

# COMMAND ----------

# MAGIC %md ## 4. States map from Volume

# COMMAND ----------

import json

with open(f"{VOLUME_PATH}/usa_110m.json") as f:
    topo = f.read()
print(f"usa_110m.json: {len(topo):,} bytes")

with open(f"{VOLUME_PATH}/us-states.json") as f:
    states_geojson = json.load(f)

sample = [(feat.get("id"), feat["properties"].get("name")) for feat in states_geojson["features"][:5]]
print("Sample (id, name):", sample)

# COMMAND ----------

import random

random.seed(42)
state_values = {feat["id"]: random.randint(10, 100) for feat in states_geojson["features"]}

fig_states = go.Figure(go.Choropleth(
    geojson=states_geojson,
    locations=list(state_values.keys()),
    z=list(state_values.values()),
    featureidkey="id",
    colorscale="Blues",
    marker_line_color="white",
    marker_line_width=0.5,
))
fig_states.update_geos(scope="usa", showland=True, landcolor="#f0f0f0", showframe=False)
fig_states.update_layout(title_text="DIAGNOSTIC: States from Volume", height=450)

js = get_plotlyjs()
fj = pio.to_json(fig_states)
displayHTML(
    f"<script>window.PlotlyGeoAssets={{topojson:{{\"usa_110m\":{topo}}}}};"
    f"{js}</script>"
    f"<div id='p'></div>"
    f"<script>(function(){{var f={fj};Plotly.newPlot('p',f.data,f.layout,{{responsive:true}})}})();</script>"
)

# COMMAND ----------

# MAGIC %md ## 5. Counties map from Volume

# COMMAND ----------

with open(f"{VOLUME_PATH}/geojson-counties-fips.json") as f:
    counties_geojson = json.load(f)

print(f"County features: {len(counties_geojson['features'])}")

random.seed(99)
county_values = {feat["id"]: round(random.uniform(0, 100), 1) for feat in counties_geojson["features"]}

fig_counties = go.Figure(go.Choropleth(
    geojson=counties_geojson,
    locations=list(county_values.keys()),
    z=list(county_values.values()),
    featureidkey="id",
    colorscale="YlOrRd",
    marker_line_width=0,
))
fig_counties.update_geos(scope="usa", showland=True, landcolor="#f0f0f0", showframe=False)
fig_counties.update_layout(title_text="DIAGNOSTIC: Counties from Volume", height=450)

fj = pio.to_json(fig_counties)
displayHTML(
    f"<script>window.PlotlyGeoAssets={{topojson:{{\"usa_110m\":{topo}}}}};"
    f"{js}</script>"
    f"<div id='p'></div>"
    f"<script>(function(){{var f={fj};Plotly.newPlot('p',f.data,f.layout,{{responsive:true}})}})();</script>"
)
