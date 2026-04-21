# R Package Bundle — Air-Gapped Databricks Deployment

Installs `zipcodeR` and its full geospatial dependency stack (`sf`, `terra`, `units`, `tidycensus`, `tigris`) on a Databricks cluster with no outbound internet.

---

## Prerequisites

- Python 3 (no third-party packages needed)
- Internet access to `packagemanager.posit.co` and `archive.ubuntu.com`
- A way to transfer files to a UC Volume on the target Databricks workspace

---

## Step 1 — Download the bundle (bastion host)

```bash
python3 bundle_deps.py --out ./bundle
```

Takes ~10 min. Produces:
```
bundle/
  *.tar.gz          # 62 R package binaries
  syslibs/debs/     # 241 Ubuntu system library .deb files
  syslibs/*.so*     # extracted shared objects
```

---

## Step 2 — Upload to UC Volume

Upload the contents of `bundle/` to a UC volume on the target workspace, e.g.:

```
/Volumes/<catalog>/<schema>/<volume>/
```

Also upload `init_r_packages.sh` to the same volume root.

---

## Step 3 — Configure the init script

Edit the two variables at the top of `init_r_packages.sh`:

```bash
VOLUME_PATH="/Volumes/<catalog>/<schema>/<volume>"
R_LIB="${VOLUME_PATH}/r_lib"
```

---

## Step 4 — Attach init script to the cluster

In the Databricks cluster configuration, add an init script pointing to:
```
/Volumes/<catalog>/<schema>/<volume>/init_r_packages.sh
```

Restart the cluster. First start takes ~5 min (compiles packages). Subsequent starts ~2 min.

---

## Step 5 — Validate

Open `validate_packages.ipynb` on the cluster and run all cells. Expected result:

```
PASS — 7/7 packages loaded
sf versions: 3.12.1 | 3.8.4 | 9.4.0 | true | true | 9.4.0
```

If you don't see the volume path first in `.libPaths()`, run this at the top of your notebook:

```r
.libPaths(c("/Volumes/<catalog>/<schema>/<volume>/r_lib", .libPaths()))
```

---

## Packages included

| Package | Purpose |
|---|---|
| `zipcodeR` | ZIP code lookup and geocoding |
| `sf` | Vector geospatial (GDAL, GEOS, PROJ) |
| `terra` | Raster geospatial |
| `units` | Unit-of-measure arithmetic |
| `tidycensus` | Census data API client |
| `tigris` | Census TIGER/Line shapefiles |
| `s2` | Spherical geometry |
| + 55 dependencies | |
