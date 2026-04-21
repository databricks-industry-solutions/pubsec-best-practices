# Databricks notebook source
# MAGIC %md
# MAGIC # Address Distance — Core
# MAGIC
# MAGIC Computes great-circle distance between an **anchor** address and N **comparison**
# MAGIC addresses. Ranks comparisons by distance, filters within a radius, and buckets
# MAGIC by H3 cell for aggregate analysis.
# MAGIC
# MAGIC ## Three ways to compute distance
# MAGIC
# MAGIC | Approach | DBR | Notes |
# MAGIC |---|---|---|
# MAGIC | `ST_DISTANCESPHERE` | 17.1+ | **Default.** Pure SQL, runs at cluster scale. Returns meters. |
# MAGIC | `h3_distance` | 11.2+ | H3 grid distance in cell-hops. Approximate, fast, good for bucketing. |
# MAGIC | Python haversine UDF | any | Fallback for pre-17.1 runtimes. |
# MAGIC
# MAGIC All three work in air-gapped environments — no external calls.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run `Address Distance - Setup` first to create `{catalog}.{schema}.address_demo`.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "user_sandbox", "Catalog")
dbutils.widgets.text("schema",  "",             "Schema (blank = derive from current_user)")
dbutils.widgets.text("radius_miles", "1500",    "Within-radius filter (miles)")

# COMMAND ----------

import re

def _username_to_schema(user: str) -> str:
    local = user.split("@", 1)[0]
    return re.sub(r"[^a-zA-Z0-9]+", "_", local).strip("_").lower()

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
RADIUS_MILES = float(dbutils.widgets.get("radius_miles"))

if not SCHEMA:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    SCHEMA = _username_to_schema(current_user)
    print(f"Schema derived from current_user ({current_user}): {SCHEMA}")

# Quote identifiers for SQL — required when names contain hyphens.
def q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"

CAT_Q     = q(CATALOG)
SCH_Q     = q(SCHEMA)
FQ_SCHEMA = f"{CAT_Q}.{SCH_Q}"
TABLE     = f"{FQ_SCHEMA}.address_demo"

print(f"Source table: {TABLE}")
print(f"Radius filter: {RADIUS_MILES} miles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 1 — `ST_DISTANCESPHERE` (DBR 17.1+, recommended)
# MAGIC
# MAGIC Great-circle distance on the WGS84 sphere. Returns meters. Divide by 1609.344
# MAGIC for miles, or 1000 for km.

# COMMAND ----------

spark.sql(f"USE CATALOG {CAT_Q}")
spark.sql(f"USE SCHEMA {SCH_Q}")

# COMMAND ----------

# MAGIC %md ### Anchor vs. all comparisons, ranked by distance

# COMMAND ----------

distances_df = spark.sql(f"""
WITH anchor AS (
  SELECT lat AS a_lat, lon AS a_lon
  FROM address_demo
  WHERE kind = 'anchor'
)
SELECT
  d.id,
  d.label,
  d.city,
  d.state,
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1609.344,
    1
  ) AS miles,
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1000.0,
    1
  ) AS km
FROM address_demo d
CROSS JOIN anchor a
WHERE d.kind = 'comparison'
ORDER BY miles
""")

display(distances_df)

# COMMAND ----------

# MAGIC %md ### Within-radius filter

# COMMAND ----------

within_radius_df = spark.sql(f"""
WITH anchor AS (
  SELECT lat AS a_lat, lon AS a_lon
  FROM address_demo
  WHERE kind = 'anchor'
)
SELECT
  d.id,
  d.label,
  d.city,
  d.state,
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1609.344,
    1
  ) AS miles
FROM address_demo d
CROSS JOIN anchor a
WHERE d.kind = 'comparison'
  AND ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1609.344 <= {RADIUS_MILES}
ORDER BY miles
""")

print(f"{within_radius_df.count()} comparisons within {RADIUS_MILES} miles of the anchor")
display(within_radius_df)

# COMMAND ----------

# MAGIC %md ### Pairwise distance matrix (every-vs-every)
# MAGIC
# MAGIC Useful for detecting clusters of addresses.

# COMMAND ----------

pairwise_df = spark.sql("""
SELECT
  a.label AS from_label,
  b.label AS to_label,
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(a.lon, a.lat), ST_POINT(b.lon, b.lat)) / 1609.344,
    1
  ) AS miles
FROM address_demo a
JOIN address_demo b
  ON a.id < b.id
ORDER BY miles
""")

print("Closest 10 pairs:")
display(pairwise_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 2 — H3 for bucketing (not for distance)
# MAGIC
# MAGIC H3 is useful for **clustering addresses into cells** (joining, grouping,
# MAGIC aggregating at a fixed grid resolution). It is **not** useful for computing
# MAGIC distance between arbitrary points.
# MAGIC
# MAGIC ### Why `h3_distance` fails on cross-country pairs
# MAGIC
# MAGIC `h3_distance` returns the number of H3 cells between two cells **along the
# MAGIC grid**. The H3 grid is built on an icosahedron (20 triangular faces). Cells
# MAGIC that fall across a face boundary have no connected grid path between them,
# MAGIC and `h3_distance` returns the error `H3_UNDEFINED_GRID_DISTANCE`.
# MAGIC
# MAGIC For any dataset spanning a large geographic area (e.g. anchor in DC,
# MAGIC comparison in Honolulu), `h3_distance` will fail on most pairs at any
# MAGIC resolution. This is inherent to how H3 is constructed, not a bug.
# MAGIC
# MAGIC **Use `ST_DISTANCESPHERE` for actual distance.** Use H3 only for what it's
# MAGIC good at: bucketing points into cells for joins and aggregations.
# MAGIC
# MAGIC ### Correct use: H3 bucketing + `ST_DISTANCESPHERE` for distance

# COMMAND ----------

h3_buckets_df = spark.sql("""
WITH anchor AS (
  SELECT lat AS a_lat, lon AS a_lon FROM address_demo WHERE kind = 'anchor'
)
SELECT
  d.label,
  d.city,
  d.state,
  -- H3 cell at resolution 5 (~8.5 km edge) — useful for regional clustering
  h3_longlatash3(d.lon, d.lat, 5) AS h3_cell_r5,
  -- Distance via ST_DISTANCESPHERE — always works, any distance
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1609.344,
    1
  ) AS miles
FROM address_demo d
CROSS JOIN anchor a
WHERE d.kind = 'comparison'
ORDER BY miles
""")

display(h3_buckets_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### When H3 `k-ring` helps: "within N cells" proximity
# MAGIC
# MAGIC If you do want an H3-native proximity check that handles the icosahedron
# MAGIC edge issue gracefully, use `h3_kring` — it returns the set of neighbor cells
# MAGIC within a radius, handling boundary wraparound internally. Good for "within
# MAGIC ~40 km of the anchor at resolution 5" style queries.

# COMMAND ----------

h3_kring_df = spark.sql("""
WITH anchor AS (
  SELECT h3_longlatash3(lon, lat, 5) AS a_h3 FROM address_demo WHERE kind = 'anchor'
),
neighborhood AS (
  -- 5 cells out at res 5 ≈ ~40 km radius
  SELECT explode(h3_kring(a_h3, 5)) AS cell FROM anchor
)
SELECT
  d.label, d.city, d.state,
  h3_longlatash3(d.lon, d.lat, 5) AS h3_cell_r5
FROM address_demo d
JOIN neighborhood n ON h3_longlatash3(d.lon, d.lat, 5) = n.cell
WHERE d.kind = 'comparison'
""")

print("Comparisons within ~40 km of the anchor (via h3_kring):")
display(h3_kring_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 3 — Python haversine UDF (fallback for older DBR)
# MAGIC
# MAGIC If the workspace is pinned to a DBR older than 17.1 and H3 grid distance is
# MAGIC not precise enough, register a Python UDF. Slower than `ST_DISTANCESPHERE`
# MAGIC but portable across all runtimes.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math

@udf(DoubleType())
def haversine_miles(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 3958.7613  # Earth radius in miles
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

spark.udf.register("haversine_miles", haversine_miles)

# COMMAND ----------

udf_df = spark.sql("""
WITH anchor AS (
  SELECT lat AS a_lat, lon AS a_lon FROM address_demo WHERE kind = 'anchor'
)
SELECT
  d.label,
  d.city,
  d.state,
  ROUND(haversine_miles(d.lat, d.lon, a.a_lat, a.a_lon), 1) AS miles
FROM address_demo d
CROSS JOIN anchor a
WHERE d.kind = 'comparison'
ORDER BY miles
""")

display(udf_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish a distances view
# MAGIC
# MAGIC Creates `{catalog}.{schema}.address_distances` — an anchor-vs-comparisons view
# MAGIC that the Visualization notebook and the Lakeview dashboard both consume. Makes
# MAGIC the downstream notebooks self-contained.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ_SCHEMA}.address_distances AS
WITH anchor AS (
  SELECT id AS a_id, label AS a_label, lat AS a_lat, lon AS a_lon
  FROM {FQ_SCHEMA}.address_demo
  WHERE kind = 'anchor'
)
SELECT
  d.id,
  d.label,
  d.address,
  d.city,
  d.state,
  d.zip,
  d.lat,
  d.lon,
  a.a_label AS anchor_label,
  a.a_lat   AS anchor_lat,
  a.a_lon   AS anchor_lon,
  ROUND(ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1609.344, 1) AS miles,
  ROUND(ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(a.a_lon, a.a_lat)) / 1000.0,   1) AS km,
  h3_longlatash3(d.lon, d.lat, 7) AS h3_cell_r7
FROM {FQ_SCHEMA}.address_demo d
CROSS JOIN anchor a
WHERE d.kind = 'comparison'
""")

print(f"Created view {FQ_SCHEMA}.address_distances")
display(spark.table(f"{FQ_SCHEMA}.address_distances").orderBy("miles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix — Road / drive-time distance (out of scope)
# MAGIC
# MAGIC `ST_DISTANCESPHERE` computes great-circle distance — the straight-line path
# MAGIC across the Earth's surface. It does **not** account for road networks.
# MAGIC
# MAGIC For road distance or drive time in an air-gapped environment you need a routing
# MAGIC engine running as an internal service with routable road network data:
# MAGIC
# MAGIC | Engine | Data | Hosting |
# MAGIC |---|---|---|
# MAGIC | OSRM | OpenStreetMap PBF extract | Docker container, REST API |
# MAGIC | Valhalla | OpenStreetMap PBF extract | Docker container, REST/gRPC |
# MAGIC | GraphHopper | OpenStreetMap PBF extract | JVM service, REST API |
# MAGIC
# MAGIC The workflow is: address → lat/lon (Geocoding notebook) → routing engine API →
# MAGIC road distance. Building that routing engine is a separate infrastructure project
# MAGIC outside the scope of this accelerator.
