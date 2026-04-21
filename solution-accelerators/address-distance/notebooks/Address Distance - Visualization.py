# Databricks notebook source
# MAGIC %md
# MAGIC # Address Distance — Visualization
# MAGIC
# MAGIC Renders the anchor-vs-comparison result three different ways. All air-gap
# MAGIC safe — no CDN, no tile server, no topojson fetch.
# MAGIC
# MAGIC | Approach | Produces | Best for |
# MAGIC |---|---|---|
# MAGIC | **`go.Scattergeo`** + `offline_map_html()` | Interactive map in a notebook cell | **Notebook reports** — analysts delivering findings in a notebook |
# MAGIC | **matplotlib + cartopy** | Static PNG in the notebook | PDF / publication output |
# MAGIC | **Lakeview native map** (separate dashboard) | Shareable URL, filters | Operational / recurring reports |
# MAGIC
# MAGIC This notebook demonstrates the first two. The Lakeview option is a separate
# MAGIC artifact — import `docs/address-distance-dashboard.lvdash.json`.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - `Address Distance - Setup` and `Address Distance - Core` have been run.
# MAGIC - For the `go.Scattergeo` option: `plotly-offline-maps/Plotly Offline Maps - Setup`
# MAGIC   has been run to populate `usa_110m.json` on the plotly volume.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog",        "user_sandbox",    "Catalog")
dbutils.widgets.text("schema",         "",                "Schema (blank = derive from current_user)")
dbutils.widgets.text("plotly_volume",  "plotly_topojson", "Plotly volume (from plotly-offline-maps)")

# COMMAND ----------

import re

def _username_to_schema(user: str) -> str:
    local = user.split("@", 1)[0]
    return re.sub(r"[^a-zA-Z0-9]+", "_", local).strip("_").lower()

CATALOG        = dbutils.widgets.get("catalog")
SCHEMA         = dbutils.widgets.get("schema")
PLOTLY_VOLUME  = dbutils.widgets.get("plotly_volume")

if not SCHEMA:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    SCHEMA = _username_to_schema(current_user)
    print(f"Schema derived from current_user ({current_user}): {SCHEMA}")

PLOTLY_PATH    = f"/Volumes/{CATALOG}/{SCHEMA}/{PLOTLY_VOLUME}"

def q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"

FQ_SCHEMA = f"{q(CATALOG)}.{q(SCHEMA)}"

distances = spark.table(f"{FQ_SCHEMA}.address_distances").orderBy("miles").toPandas()
print(f"{len(distances)} comparisons loaded")
display(distances)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option A — `go.Scattergeo` interactive map
# MAGIC
# MAGIC Reuses `offline_map_html()` from the sibling `plotly-offline-maps` accelerator.
# MAGIC Pre-injects `window.PlotlyGeoAssets` so Plotly's CDN topojson fetch is skipped.
# MAGIC
# MAGIC **Anchor**: red star. **Comparisons**: circles sized by distance, colored by
# MAGIC distance bucket. **Connector lines**: anchor → each comparison.

# COMMAND ----------

# Ensure plotly is available
try:
    import plotly
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly", "--quiet"])
    import plotly

# COMMAND ----------

# Offline HTML helper — same pattern as plotly-offline-maps
import plotly.io as pio
from plotly.offline import get_plotlyjs

with open(f"{PLOTLY_PATH}/usa_110m.json") as f:
    _USA_TOPOJSON = f.read()

_PLOTLYJS = get_plotlyjs()

def offline_map_html(fig):
    fig_json = pio.to_json(fig)
    return (
        f"<script>window.PlotlyGeoAssets={{topojson:{{\"usa_110m\":{_USA_TOPOJSON}}}}};"
        f"{_PLOTLYJS}</script>"
        f"<div id='plot'></div>"
        f"<script>(function(){{var f={fig_json};"
        f"Plotly.newPlot('plot',f.data,f.layout,{{responsive:true}})}})();</script>"
    )

# COMMAND ----------

import plotly.graph_objects as go

def anchor_comparison_figure(distances_df, title="Address Distance from Anchor"):
    """
    Build a Scattergeo figure: red star for the anchor, comparison circles sized
    and colored by distance, connector lines from anchor to each comparison.

    Expects columns: label, lat, lon, miles, anchor_label, anchor_lat, anchor_lon.
    """
    anchor_lat = distances_df["anchor_lat"].iloc[0]
    anchor_lon = distances_df["anchor_lon"].iloc[0]
    anchor_lbl = distances_df["anchor_label"].iloc[0]

    # Connector lines — one trace per line so hover works per-segment
    line_traces = []
    for _, row in distances_df.iterrows():
        line_traces.append(go.Scattergeo(
            lon=[anchor_lon, row["lon"]],
            lat=[anchor_lat, row["lat"]],
            mode="lines",
            line=dict(width=1, color="rgba(80,80,80,0.35)"),
            hoverinfo="skip",
            showlegend=False,
        ))

    # Comparison markers — scaled size, colorbar by miles
    comparisons_trace = go.Scattergeo(
        lon=distances_df["lon"],
        lat=distances_df["lat"],
        text=distances_df.apply(
            lambda r: f"{r['label']}<br>{r['city']}, {r['state']}<br>{r['miles']:.1f} mi",
            axis=1,
        ),
        hoverinfo="text",
        mode="markers",
        marker=dict(
            size=10,
            color=distances_df["miles"],
            colorscale="Viridis",
            reversescale=True,
            cmin=0,
            cmax=float(distances_df["miles"].max()),
            colorbar=dict(title="Miles"),
            line=dict(width=0.5, color="white"),
        ),
        name="Comparison",
    )

    # Anchor marker — red star
    anchor_trace = go.Scattergeo(
        lon=[anchor_lon],
        lat=[anchor_lat],
        text=[f"{anchor_lbl}<br>Anchor"],
        hoverinfo="text",
        mode="markers",
        marker=dict(size=18, symbol="star", color="red", line=dict(width=1.5, color="white")),
        name="Anchor",
    )

    fig = go.Figure(data=line_traces + [comparisons_trace, anchor_trace])

    # Fit viewport to the data so the map frames the actual points. Pad 10% on
    # each side. Works globally — not clipped to CONUS like scope="usa".
    lat_min = min(distances_df["lat"].min(), anchor_lat)
    lat_max = max(distances_df["lat"].max(), anchor_lat)
    lon_min = min(distances_df["lon"].min(), anchor_lon)
    lon_max = max(distances_df["lon"].max(), anchor_lon)
    lat_pad = max((lat_max - lat_min) * 0.10, 2)
    lon_pad = max((lon_max - lon_min) * 0.10, 2)

    fig.update_geos(
        projection_type="natural earth",
        lataxis_range=[lat_min - lat_pad, lat_max + lat_pad],
        lonaxis_range=[lon_min - lon_pad, lon_max + lon_pad],
        showland=True,   landcolor="#f0f0f0",
        showcountries=True, countrycolor="#d0d0d0",
        showsubunits=True,  subunitcolor="#d0d0d0",  # state borders
        showcoastlines=True, coastlinecolor="#bbbbbb", coastlinewidth=0.5,
        showframe=False,
        bgcolor="rgba(0,0,0,0)",
    )
    fig.update_layout(
        title_text=title,
        height=600,
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(x=0.01, y=0.01),
    )
    return fig

# COMMAND ----------

fig = anchor_comparison_figure(distances, title=f"Distance from {distances['anchor_label'].iloc[0]}")
displayHTML(offline_map_html(fig))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Closest-N filter
# MAGIC
# MAGIC Same helper, filtered to the 10 closest comparisons — useful for "nearest
# MAGIC field offices" reports.

# COMMAND ----------

closest_10 = distances.head(10)
fig_closest = anchor_comparison_figure(closest_10, title="10 Nearest Comparisons")
displayHTML(offline_map_html(fig_closest))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B — matplotlib static map (publication / PDF)
# MAGIC
# MAGIC For formal reports that render to PDF. `matplotlib` + `cartopy` ship with
# MAGIC built-in Natural Earth coastlines and state boundaries — no external
# MAGIC fetches. Rendered to PNG server-side.
# MAGIC
# MAGIC `cartopy` is in DBR ML. On non-ML runtimes, install it via cluster libraries
# MAGIC (wheel from an internal mirror in air-gapped environments).

# COMMAND ----------

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    import matplotlib.pyplot as plt
    CARTOPY_OK = True
except ImportError:
    print("cartopy not available — install via cluster libraries to render static maps.")
    CARTOPY_OK = False

# COMMAND ----------

if CARTOPY_OK:
    fig_mpl, ax = plt.subplots(
        figsize=(12, 7),
        subplot_kw={"projection": ccrs.AlbersEqualArea(central_longitude=-96, central_latitude=37.5)},
    )
    ax.set_extent([-125, -66, 24, 50], crs=ccrs.PlateCarree())

    ax.add_feature(cfeature.LAND,    facecolor="#f0f0f0")
    ax.add_feature(cfeature.COASTLINE, linewidth=0.4)
    ax.add_feature(cfeature.STATES,    linewidth=0.3, edgecolor="#888")

    anchor_lat = distances["anchor_lat"].iloc[0]
    anchor_lon = distances["anchor_lon"].iloc[0]

    # Connector lines
    for _, row in distances.iterrows():
        ax.plot(
            [anchor_lon, row["lon"]], [anchor_lat, row["lat"]],
            color="gray", alpha=0.35, linewidth=0.8,
            transform=ccrs.Geodetic(),
        )

    # Comparison markers
    sc = ax.scatter(
        distances["lon"], distances["lat"],
        c=distances["miles"], cmap="viridis_r",
        s=50, edgecolor="white", linewidth=0.5,
        transform=ccrs.PlateCarree(),
        zorder=3,
    )

    # Anchor
    ax.scatter(
        [anchor_lon], [anchor_lat],
        marker="*", color="red", s=260, edgecolor="white", linewidth=1.2,
        transform=ccrs.PlateCarree(),
        zorder=4,
    )

    cbar = plt.colorbar(sc, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("Miles")
    ax.set_title(f"Distance from {distances['anchor_label'].iloc[0]}")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option C — Lakeview dashboard (operational)
# MAGIC
# MAGIC For shareable dashboards with filters and slicers, import the Lakeview
# MAGIC template at `docs/address-distance-dashboard.lvdash.json`:
# MAGIC
# MAGIC 1. In the workspace, go to **Dashboards → Create → Import dashboard**
# MAGIC 2. Upload `address-distance-dashboard.lvdash.json`
# MAGIC 3. Point the dataset at `{catalog}.{schema}.address_distances`
# MAGIC 4. Publish
# MAGIC
# MAGIC Lakeview's native map widget accepts lat/lon columns directly and renders
# MAGIC server-side — no topojson, no CDN, no JavaScript workarounds. Fully air-gap safe.
