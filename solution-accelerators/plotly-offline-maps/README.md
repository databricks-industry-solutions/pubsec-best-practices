# Plotly Offline Maps

Render Plotly choropleth maps in Databricks notebooks with **no internet access required** — no CDN, no tile server, no proxy. Includes a Databricks AI/BI Lakeview dashboard alternative that is natively air-gap safe.

## The Problem

Plotly's JavaScript renderer fetches two resources from `cdn.plot.ly` at render time:

1. `plotly.min.js` — the rendering library
2. TopoJSON files — geographic boundary data for built-in map scopes (US states, world, etc.)

In air-gapped environments (e.g. GovCloud, classified networks), these fetches silently fail and the map cell renders blank.

### Why `go.Choropleth` always fails in air-gapped environments

`go.Choropleth` sets `locationmode` internally on every trace. This triggers `fetchTopojson()` deep inside `plotly.js` regardless of whether you pass `geojson=` data inline. The relevant code in the minified bundle:

```js
f.locationmode && (s = true);
// ...
if (s) {
    PlotlyGeoAssets.topojson[name] === void 0 &&
    o.push(i.fetchTopojson())  // CDN fetch, always fires for Choropleth
}
```

Passing `geojson=my_dict` only controls which data is plotted on top of the base map — it does not replace the base map fetch. Setting `topojsonURL` redirects the fetch to a custom URL, but that URL must be reachable from the **browser**, not the cluster. A local HTTP server on the driver node is not accessible from the user's browser tab.

**The only solution for `go.Choropleth` is to not use it.**

### Affected chart types

| Chart type | Sets `locationmode` | CDN topojson fetch | Air-gap safe |
|---|---|---|---|
| `go.Choropleth` | Always | Yes — always | No |
| `go.Choroplethmapbox` | No | No (Mapbox tiles instead) | No — needs tile server |
| `go.Scattergeo` | Never | No | **Yes** |
| `go.Scattermapbox` | No | No (Mapbox tiles instead) | No — needs tile server |
| `px.choropleth()` | Always | Yes — always | No (wraps `go.Choropleth`) |
| `px.choropleth_mapbox()` | No | No | No — needs tile server |
| `px.scatter_geo()` | Never | No | **Yes** |

## Solutions

### Option 1: `go.Scattergeo` with `fill='toself'` (notebook-embedded maps)

`go.Scattergeo` draws geographic shapes from raw lon/lat coordinate arrays. With `fill='toself'`, each trace fills its own polygon — producing a choropleth effect without triggering the topojson CDN fetch.

This is the approach used in `notebooks/Plotly Offline Maps`. The `geojson_to_scattergeo()` helper converts any GeoJSON FeatureCollection into Scattergeo traces with a manual colorscale.

**Use this when:** You need maps embedded directly in a notebook cell.

### Option 2: Databricks AI/BI Lakeview Dashboard (shareable, operational maps)

Lakeview dashboards render map visualizations **server-side** on Databricks — the browser receives rendered image tiles, not raw JavaScript. Geographic boundary data (US states, counties, countries) is bundled in the platform; no external fetch of any kind occurs.

Native location types supported (no GeoJSON file needed):

| Type | Column format | Example |
|------|--------------|---------|
| `us_state_abbr` | 2-letter postal code | `"CA"` |
| `us_county_fips` | 5-digit FIPS string | `"06037"` |
| `country` | ISO 3166-1 alpha-2 | `"US"` |

**Use this when:** You need a shareable URL, built-in filters/slicers, automatic colorscale, or a published operational map.

See `notebooks/Plotly Offline Maps - Dashboard Alternative` for setup instructions.

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `Plotly Offline Maps - Setup` | Loads GeoJSON boundary files into a UC Volume (run once) |
| `Plotly Offline Maps` | State and county choropleth maps via `go.Scattergeo` |
| `Plotly Offline Maps - Dashboard Alternative` | Creates Delta tables + dashboard for Lakeview maps |
| `Plotly Offline Maps - Diagnostic` | Layered diagnostics — use to troubleshoot blank maps |

## Quick Start (Scattergeo approach)

### Step 1 — Get the boundary files onto your Volume

**Option A: Manual upload (no git, no cluster internet)**
1. Download from this repo to your laptop:
   - [`data/us-states.json`](data/us-states.json) (87 KB)
   - [`data/geojson-counties-fips.json`](data/geojson-counties-fips.json) (1.7 MB)
2. Download from plotly.js (MIT, not bundled in this repo):
   - [`usa_110m.json`](https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/usa_110m.json) (~50 KB)
   - [`world_110m.json`](https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/world_110m.json) (~50 KB, only needed for global-scope maps)
3. In **Catalog Explorer** → your volume → **Upload to this volume**
4. Upload all files

**Option B: Workspace Git folder**
Clone this repo into your Databricks workspace. The setup notebook detects the `data/` folder automatically.

**Option C: Cluster with internet**
Run the setup notebook — it downloads and uploads in one step.

### Step 2 — Run the setup notebook

Open `notebooks/Plotly Offline Maps - Setup` and run it. Set the catalog/schema/volume widgets to match your environment. This only needs to run once per workspace.

### Step 3 — Run the main notebook

Open `notebooks/Plotly Offline Maps` and run it. Both state and county maps should render without any outbound network calls.

## Supported Map Types

| Map type | Technique | Supported |
|----------|-----------|-----------|
| US states choropleth | Scattergeo + Volume GeoJSON | Yes |
| US counties choropleth | Scattergeo + Volume GeoJSON (FIPS) | Yes |
| World/country choropleth | Scattergeo + bring your own GeoJSON | Yes |
| US states (dashboard) | Lakeview native `us_state_abbr` | Yes |
| US counties (dashboard) | Lakeview native `us_county_fips` | Yes |
| Mapbox raster tiles | Tile server required | No — requires external tile server |

## Data Sources

| File | Source | License |
|------|--------|---------|
| `us-states.json` | [PublicaMundi/MappingAPI](https://github.com/PublicaMundi/MappingAPI) | Public domain |
| `geojson-counties-fips.json` | [plotly/datasets](https://github.com/plotly/datasets) | MIT |

## Configuration Widgets

| Widget | Default | Description |
|--------|---------|-------------|
| `catalog` | `main` | Unity Catalog catalog name |
| `schema` | `default` | Schema name |
| `volume` | `plotly_topojson` | Volume name for GeoJSON storage |
