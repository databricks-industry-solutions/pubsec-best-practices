# Databricks notebook source
# MAGIC %md
# MAGIC # Address Distance — Quickstart
# MAGIC
# MAGIC **Paste addresses with lat/lon, get distances + a map.** No Delta tables, no
# MAGIC setup notebook, no sibling accelerator.
# MAGIC
# MAGIC ## Prerequisite — one-time file upload
# MAGIC
# MAGIC Download **`world_110m.json`** from plotly.js and upload it to a volume in
# MAGIC your workspace:
# MAGIC
# MAGIC - Download: https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/world_110m.json  (~50 KB, MIT license)
# MAGIC - Also grab `usa_110m.json` if you want to use the other notebooks in this accelerator:
# MAGIC   https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/usa_110m.json
# MAGIC - Upload via **Catalog Explorer → your volume → Upload to this volume**
# MAGIC - Set the `geo_volume_path` widget below to your volume path
# MAGIC
# MAGIC Why: plotly's geo rendering fetches topojson from `cdn.plot.ly` in the
# MAGIC browser. In air-gapped workspaces this fails silently and the map cell
# MAGIC renders blank. This notebook pre-injects `window.PlotlyGeoAssets` with the
# MAGIC local copy before `plotly.min.js` loads, so no CDN fetch happens. See the
# MAGIC `plotly-offline-maps` accelerator for the full mechanism.
# MAGIC
# MAGIC ## What this does
# MAGIC
# MAGIC 1. You edit the `ANCHOR` and `COMPARISONS` Python lists below.
# MAGIC 2. It computes great-circle distances from anchor → each comparison.
# MAGIC 3. It renders an interactive map (land + country borders + coastlines) with
# MAGIC    a red-star anchor, colored comparison circles, and connector lines.
# MAGIC
# MAGIC ## When to graduate to the other notebooks
# MAGIC
# MAGIC - **Many addresses** → move to `Address Distance - Core` (Delta-backed, uses
# MAGIC   `ST_DISTANCESPHERE` at cluster scale).
# MAGIC - **Addresses without lat/lon** → `Address Distance - Geocoding`.
# MAGIC - **Shareable dashboard** → `docs/address-distance-dashboard.lvdash.json`.

# COMMAND ----------

# MAGIC %md ## Inputs — edit these two lists

# COMMAND ----------

dbutils.widgets.text(
    "geo_volume_path",
    "/Volumes/edp/sandbox_bronze/plotly_geo",
    "Plotly geo volume (must contain world_110m.json)",
)
GEO_VOLUME_PATH = dbutils.widgets.get("geo_volume_path")

# COMMAND ----------

# Anchor: one (label, lat, lon) tuple.
ANCHOR = ("IRS HQ, Washington DC", 38.8921, -77.0306)

# Comparisons: list of (label, lat, lon) tuples.
COMPARISONS = [
    ("Austin Campus",      30.2213,  -97.7279),
    ("Atlanta Campus",     33.8896,  -84.2820),
    ("Cincinnati Campus",  39.0877,  -84.5080),
    ("Kansas City Campus", 39.0865,  -94.5838),
    ("Memphis Campus",     35.0571,  -89.9420),
    ("Ogden Campus",       41.2713, -112.0183),
    ("Fresno Campus",      36.7587, -119.6842),
    ("Philadelphia Campus",39.9559,  -75.1899),
    ("Andover Campus",     42.6583,  -71.1368),
    ("Brookhaven Campus",  40.8118,  -73.0432),
]

# COMMAND ----------

# MAGIC %md ## Compute distances (haversine — works on any DBR)

# COMMAND ----------

import math
import pandas as pd

def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

a_label, a_lat, a_lon = ANCHOR
rows = []
for label, lat, lon in COMPARISONS:
    rows.append({
        "label": label,
        "lat": lat,
        "lon": lon,
        "miles": round(haversine_miles(a_lat, a_lon, lat, lon), 1),
    })

distances = pd.DataFrame(rows).sort_values("miles").reset_index(drop=True)
display(distances)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Render the map
# MAGIC
# MAGIC Uses `go.Scattergeo` with `projection_type="natural earth"` — a global
# MAGIC projection that handles any input (CONUS, Alaska, Hawaii, international).
# MAGIC All base-map layers that would trigger CDN topojson fetches are disabled,
# MAGIC so this is air-gap safe **with no volume or setup required**. Only the
# MAGIC plotly JS library is inlined via `get_plotlyjs()`.

# COMMAND ----------

import plotly.graph_objects as go
import plotly.io as pio
from plotly.offline import get_plotlyjs

# Pad the viewport 10% around the data so the map frames the actual points.
all_lats = [a_lat] + [r["lat"] for r in rows]
all_lons = [a_lon] + [r["lon"] for r in rows]
lat_min, lat_max = min(all_lats), max(all_lats)
lon_min, lon_max = min(all_lons), max(all_lons)
lat_pad = max((lat_max - lat_min) * 0.10, 2)
lon_pad = max((lon_max - lon_min) * 0.10, 2)

# Connector lines — one trace per line
line_traces = [
    go.Scattergeo(
        lon=[a_lon, r["lon"]], lat=[a_lat, r["lat"]],
        mode="lines",
        line=dict(width=1, color="rgba(80,80,80,0.35)"),
        hoverinfo="skip", showlegend=False,
    )
    for r in rows
]

# Comparison markers — scaled by distance
comparisons_trace = go.Scattergeo(
    lon=[r["lon"] for r in rows],
    lat=[r["lat"] for r in rows],
    text=[f"{r['label']}<br>{r['miles']:.1f} mi" for r in rows],
    hoverinfo="text",
    mode="markers",
    marker=dict(
        size=10,
        color=[r["miles"] for r in rows],
        colorscale="Viridis",
        reversescale=True,
        colorbar=dict(title="Miles"),
        line=dict(width=0.5, color="white"),
    ),
    name="Comparison",
)

# Anchor — red star
anchor_trace = go.Scattergeo(
    lon=[a_lon], lat=[a_lat],
    text=[f"{a_label}<br>Anchor"],
    hoverinfo="text",
    mode="markers",
    marker=dict(size=18, symbol="star", color="red", line=dict(width=1.5, color="white")),
    name="Anchor",
)

fig = go.Figure(data=line_traces + [comparisons_trace, anchor_trace])
fig.update_geos(
    projection_type="natural earth",
    lataxis_range=[lat_min - lat_pad, lat_max + lat_pad],
    lonaxis_range=[lon_min - lon_pad, lon_max + lon_pad],
    showland=True,    landcolor="#f0f0f0",
    showcountries=True, countrycolor="#d0d0d0",
    showsubunits=True,  subunitcolor="#d0d0d0",
    showcoastlines=True, coastlinecolor="#bbbbbb", coastlinewidth=0.5,
    showframe=False,
    bgcolor="rgba(0,0,0,0)",
)
fig.update_layout(
    title_text=f"Distance from {a_label}",
    height=600,
    margin=dict(l=0, r=0, t=40, b=0),
    legend=dict(x=0.01, y=0.01),
)

# Render with inline plotly.min.js + pre-injected world topojson. Any go.Scattergeo
# with show{land,countries,coastlines,subunits}=True triggers a fetch for
# world_110m.json (scope defaults to "world", resolution to 110). We pre-populate
# window.PlotlyGeoAssets.topojson.world_110m before plotly.min.js loads so the
# CDN fetch is skipped. Air-gap safe.
import os

_world_path = f"{GEO_VOLUME_PATH}/world_110m.json"
if not os.path.isfile(_world_path):
    raise FileNotFoundError(
        f"world_110m.json not found at {_world_path}\n\n"
        "Download it and upload to your volume:\n"
        "  https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/world_110m.json\n\n"
        "Then set the geo_volume_path widget to the volume that contains the file."
    )

with open(_world_path) as f:
    _WORLD_TOPOJSON = f.read()

fig_json = pio.to_json(fig)
displayHTML(
    f"<script>window.PlotlyGeoAssets={{topojson:{{\"world_110m\":{_WORLD_TOPOJSON}}}}};"
    f"{get_plotlyjs()}</script>"
    f"<div id='plot'></div>"
    f"<script>(function(){{var f={fig_json};"
    f"Plotly.newPlot('plot',f.data,f.layout,{{responsive:true}})}})();</script>"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## That's it
# MAGIC
# MAGIC Change `ANCHOR` and `COMPARISONS` at the top, re-run. For production workflows
# MAGIC see the other notebooks in this accelerator:
# MAGIC
# MAGIC - `Address Distance - Core` — `ST_DISTANCESPHERE` at scale on Delta tables
# MAGIC - `Address Distance - Geocoding` — address strings → lat/lon
# MAGIC - `Address Distance - Visualization` — full version with UC-backed data
