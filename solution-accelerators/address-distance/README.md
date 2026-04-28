# Address Distance

Calculate distances between addresses and visualize an **anchor** address against **N comparison** addresses on a map — in air-gapped Databricks environments (GovCloud, classified networks). Supports both notebook-embedded reports and shareable Lakeview dashboards.

## What This Solves

Given one "anchor" address and a set of comparison addresses, this accelerator answers:
- How far is each comparison address from the anchor? (miles / km)
- Which comparisons are within X miles?
- Where are they all on a map, with distance lines drawn from the anchor?

Typical UBA use cases:
- Field-office proximity for taxpayer/case routing
- Detecting clustered addresses (potential fraud rings, return-prep schemes)
- Proximity-based case assignment
- Geographic outlier detection

## The Three Problems

Computing distance + visualizing on a map in an air-gapped environment has three separate problems, each with a clean answer on modern DBR:

1. **Distance calculation** — solved by built-in SQL (`ST_DISTANCESPHERE` / `h3_distance`). No UDFs, no Python helpers, no custom libraries.
2. **Visualization** — solved three different ways depending on where the report lives (notebook cell / Lakeview dashboard / static report).
3. **Geocoding** — the only genuinely hard part in air-gap. Multiple valid approaches depending on accuracy needs and data footprint tolerance.

---

## 1. Distance Calculation

**Assume DBR 17.1+.** All options are pure SQL, run at cluster scale, no external dependencies.

| Approach | DBR | Unit | Accuracy | Use when |
|---|---|---|---|---|
| **`ST_DISTANCESPHERE`** | 17.1+ | meters | Great-circle (WGS84 sphere) | **Default.** Exact haversine-equivalent on the server. |
| **`ST_DISTANCE` + geography** | 17.1+ | meters | Ellipsoidal (WGS84) | When sub-meter accuracy matters |
| **Python haversine UDF** | any | miles/km | Great-circle | Only if stuck on pre-17.1 DBR |

> **Do not use `h3_distance` for this.** It returns grid-cell hops along the H3
> icosahedron and fails with `H3_UNDEFINED_GRID_DISTANCE` whenever two cells
> don't share a connected grid path — which happens for almost every
> cross-country address pair at any resolution. H3 is for *bucketing*; use
> `ST_DISTANCESPHERE` for actual distance.

### ST_DISTANCESPHERE — the default

```sql
SELECT
  comparison_address,
  ST_DISTANCESPHERE(
    ST_POINT(anchor_lon,    anchor_lat),
    ST_POINT(comparison_lon, comparison_lat)
  ) / 1609.344 AS miles
FROM comparisons
ORDER BY miles;
```

See `notebooks/Address Distance - Core` for full examples including Python interop, DataFrame patterns, and within-radius filters.

---

## 2. Visualization

Three different shapes of output, three different tools. All air-gap safe.

| Option | What it produces | When to use |
|---|---|---|
| **Lakeview map widget** | Shareable dashboard URL with native lat/lon scatter, filters, slicers | Operational reports, recurring reviews, anything stakeholders need to revisit |
| **`go.Scattergeo` + `offline_map_html()`** | Interactive map embedded in a notebook cell | Analyst reports, ad-hoc investigations, anything delivered as a notebook |
| **matplotlib + cartopy** | Static PNG in the notebook | Publication output, PDF reports, formal deliverables |

### Option A: Lakeview dashboard (operational)

Lakeview dashboards render map visualizations **server-side** on Databricks. The browser receives rendered image tiles — no CDN, no topojson fetch, no tile server.

Native location types supported:

| Type | Column format | Example |
|---|---|---|
| Lat/lon scatter | two numeric columns | `38.8977, -77.0365` |
| `us_state_abbr` | 2-letter postal code | `"CA"` |
| `us_county_fips` | 5-digit FIPS string | `"06037"` |

See `docs/address-distance-dashboard.lvdash.json` for a ready-to-import template.

### Option B: Notebook-embedded interactive map

Reuses the sibling `plotly-offline-maps` accelerator's `offline_map_html()` helper. `go.Scattergeo` is air-gap safe once the `window.PlotlyGeoAssets` TopoJSON cache is pre-populated — see that accelerator's README for the underlying mechanism.

Adds a new helper `anchor_comparison_figure()` specific to this use case:
- Red star marker for the anchor
- Circles for comparisons, sized by distance, colored by distance bucket
- Line traces drawn from anchor → each comparison
- Hover: address + distance

Requires the `plotly-offline-maps` setup notebook to have been run once (populates `usa_110m.json` in the Volume).

### Option C: Static matplotlib

For formal reports and PDFs. `matplotlib` + `cartopy` ship with built-in coastline and state boundary data — no external fetches. Renders to PNG on the driver. Less pretty than plotly; bulletproof.

---

## 3. Geocoding (the actual hard problem)

Converting address strings → lat/lon in an air-gapped environment is the only genuinely difficult part of this workflow. There is no single right answer — the choice depends on accuracy requirements, data footprint tolerance, and what the customer already has.

| Approach | Accuracy | Data size | Setup | Use when |
|---|---|---|---|---|
| **Pre-geocoded input** | Source-dependent | 0 | None | **Default.** UBA tables typically already have lat/lon (Census GEOID, prior ETL). |
| **ZCTA centroid** | ~1–5 mi | 1.6 MB | Trivial | ZIP-level aggregates, heat maps, "addresses near X ZIP" |
| **Census TIGER address ranges** | Block-face interpolation | ~8 GB | Moderate | Most IRS field-office / case-routing use cases |
| **OpenAddresses (full US)** | Rooftop (best) | ~30 GB | Moderate | Commercial / forensic / premium accuracy |
| **Internal Pelias / Nominatim service** | Rooftop | Service | High (infra) | Customer already runs a geocoder |
| **libpostal (parse + normalize)** | N/A — parsing only | ~2 GB | Low | Cleanup layer in front of any of the above |

### Pre-geocoded input (recommended default)

Most UBA / IRS tables already carry lat/lon from upstream Census GEOID joins or prior geocoding ETL. If so, skip geocoding entirely — the accelerator's distance + visualization layers work directly on lat/lon columns. This is the path the main notebook demonstrates end-to-end.

### ZCTA centroid

Ships with this accelerator (`data/us-zcta-gazetteer.csv`, Census public domain, ~1.6 MB). Good-enough for any analysis at ZIP-level granularity or coarser. Loaded into a Delta table in the setup notebook; usage is a simple `JOIN ON zip = zcta`.

### Census TIGER / OpenAddresses

Too large to ship in a repo (8 GB / 30 GB). The accelerator **documents the pattern** — loading into a UC Volume, building a lookup Delta table, joining on normalized street + city + state + ZIP. See `notebooks/Address Distance - Geocoding` for the reference implementation skeleton.

### Internal geocoder service

If the customer already runs Pelias / Nominatim / a vendor geocoder internally, wrap it in a `geocode_address()` function. The notebook includes a plug-in stub.

---

## 4. Road Distance (out of scope)

This accelerator computes **great-circle** distance. Road / drive-time distance requires a routing engine (OSRM, Valhalla, GraphHopper) running as a service with routable road network data. That is a separate infrastructure project — see the "Road Distance" appendix in `Address Distance - Core`.

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `Address Distance - Quickstart` | **Paste addresses, get a map.** Zero setup, zero dependencies. Basic — for anything beyond ad-hoc exploration use the Core / Visualization notebooks below. |
| `Address Distance - Setup` | Create volume, load ZCTA gazetteer + demo address table |
| `Address Distance - Core` | `ST_DISTANCESPHERE`, H3, anchor-vs-N patterns, within-radius filters |
| `Address Distance - Geocoding` | Pre-geocoded / ZCTA / TIGER / OpenAddresses / plug-in-stub reference implementations |
| `Address Distance - Visualization` | Lakeview / `Scattergeo` / matplotlib — same data, three outputs |

## Quick Start

**Try it in about a minute:** open `Address Distance - Quickstart`, edit the `ANCHOR` and `COMPARISONS` lists at the top, set the `geo_volume_path` widget, run all. Uses a Python haversine UDF on inline data — **fine for a handful of addresses, not a production pattern.** For anything real, use the Delta-backed workflow below.

### One-time prerequisite — upload topojson to a volume

Plotly's geo rendering fetches topojson files from `cdn.plot.ly` in the browser. In air-gapped workspaces these fetches fail and map cells render blank. Download the files once and upload to a volume; the notebooks pre-inject them via `window.PlotlyGeoAssets` so no CDN fetch happens.

| File | Download URL | Needed by |
|---|---|---|
| `world_110m.json` | https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/world_110m.json | `Quickstart`, any global `go.Scattergeo` |
| `usa_110m.json` | https://raw.githubusercontent.com/plotly/plotly.js/master/dist/topojson/usa_110m.json | `Visualization` (CONUS-scoped views), `plotly-offline-maps` |

Both files are MIT-licensed and ~50 KB each. Upload via **Catalog Explorer → your volume → Upload to this volume**, then set the relevant volume-path widget in the notebook.

**For real workloads (Delta-backed, production pattern):**
1. Run `Address Distance - Setup` once per workspace — creates the volume, loads the ZCTA gazetteer, and writes the demo addresses to a Delta table.
2. (Optional, for the interactive map) Run `plotly-offline-maps/notebooks/Plotly Offline Maps - Setup` once per workspace.
3. Run `Address Distance - Core` — reads from the Delta table, computes distances via `ST_DISTANCESPHERE` at cluster scale, and **writes a reusable `address_distances` view** that other notebooks and the Lakeview dashboard consume.
4. Run `Address Distance - Visualization` — renders all three output forms from the view.
5. Point the Lakeview dashboard (`docs/address-distance-dashboard.lvdash.json`) at the `address_distances` view for a shareable, filterable operational report.

The Delta-table pattern is what you want for recurring reports, multi-user access, and large datasets. The Quickstart notebook is only for one-off exploration or customer demos.

## Configuration Widgets

| Widget | Default | Description |
|---|---|---|
| `catalog` | `user_sandbox` | Unity Catalog catalog |
| `schema` | *(blank)* | Schema name. Blank derives from `current_user()` — e.g. `first.m.last@agency.gov` → `first_m_last`. |
| `volume` | `address_distance` | Volume for gazetteer + geocoder reference data |
| `plotly_volume` | `plotly_topojson` | Volume from `plotly-offline-maps` (for interactive map option) |

## Data Sources

| File | Source | License |
|---|---|---|
| `us-zcta-gazetteer.csv` | [US Census Gazetteer](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html) | Public domain |
| `sample-addresses.csv` | Synthetic — IRS HQ anchor + 20 comparison addresses | N/A |
