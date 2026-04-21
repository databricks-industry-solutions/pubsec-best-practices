# Databricks notebook source
# MAGIC %md
# MAGIC # Address Distance — Geocoding
# MAGIC
# MAGIC Converts address strings to lat/lon in an air-gapped environment. This is the
# MAGIC only genuinely hard part of the workflow — there is no single right answer,
# MAGIC and the choice depends on accuracy requirements and data footprint tolerance.
# MAGIC
# MAGIC This notebook documents four real paths and provides a plug-in stub for a
# MAGIC fifth (customer-hosted geocoder service).
# MAGIC
# MAGIC ## Options
# MAGIC
# MAGIC | Approach | Accuracy | Data size | Setup | Best for |
# MAGIC |---|---|---|---|---|
# MAGIC | **1. Pre-geocoded input** | Source-dependent | 0 | None | **Default.** UBA tables with existing lat/lon. |
# MAGIC | **2. ZCTA centroid** | ~1–5 mi | 1.6 MB | Trivial | ZIP-level aggregates, heat maps. |
# MAGIC | **3. Census TIGER address ranges** | Block-face | ~8 GB | Moderate | Field-office routing, most IRS use cases. |
# MAGIC | **4. OpenAddresses (full US)** | Rooftop | ~30 GB | Moderate | Forensic, commercial, premium. |
# MAGIC | **5. Internal geocoder service** | Rooftop | Service | High (infra) | Customer already runs Pelias / Nominatim. |
# MAGIC
# MAGIC All paths are air-gap safe. None of them call external services.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "user_sandbox",    "Catalog")
dbutils.widgets.text("schema",  "",                "Schema (blank = derive from current_user)")
dbutils.widgets.text("volume",  "address_distance","Volume")

# COMMAND ----------

import re

def _username_to_schema(user: str) -> str:
    local = user.split("@", 1)[0]
    return re.sub(r"[^a-zA-Z0-9]+", "_", local).strip("_").lower()

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

if not SCHEMA:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    SCHEMA = _username_to_schema(current_user)
    print(f"Schema derived from current_user ({current_user}): {SCHEMA}")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

def q(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"

CAT_Q     = q(CATALOG)
SCH_Q     = q(SCHEMA)
FQ_SCHEMA = f"{CAT_Q}.{SCH_Q}"

spark.sql(f"USE CATALOG {CAT_Q}")
spark.sql(f"USE SCHEMA {SCH_Q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1 — Pre-geocoded input (recommended default)
# MAGIC
# MAGIC Most UBA / IRS tables already carry lat/lon from Census GEOID joins, prior
# MAGIC ETL, or field systems. If so, skip geocoding entirely — the rest of the
# MAGIC accelerator works directly on lat/lon columns.
# MAGIC
# MAGIC ### Data quality check
# MAGIC
# MAGIC Before trusting pre-geocoded lat/lon, validate that the values are in range
# MAGIC and that the point actually falls inside the claimed state.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  COUNT(*)                                    AS total,
  SUM(CASE WHEN lat IS NULL OR lon IS NULL THEN 1 ELSE 0 END) AS missing_coords,
  SUM(CASE WHEN lat NOT BETWEEN -90  AND 90   THEN 1 ELSE 0 END) AS bad_lat,
  SUM(CASE WHEN lon NOT BETWEEN -180 AND 180  THEN 1 ELSE 0 END) AS bad_lon,
  SUM(CASE WHEN lat = 0 AND lon = 0          THEN 1 ELSE 0 END) AS null_island
FROM address_demo
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2 — ZCTA centroid geocoding
# MAGIC
# MAGIC The setup notebook loaded `zcta_centroids` from the Census Gazetteer.
# MAGIC `JOIN` your addresses on the ZIP column. This gives you the centroid of the
# MAGIC ZIP Code Tabulation Area — accurate to 1–5 miles depending on ZCTA size.
# MAGIC
# MAGIC **Use when:** you only need ZIP-level granularity (heat maps, aggregates, "count
# MAGIC addresses within 25 miles of X"). **Do not use when:** you need to compute
# MAGIC distances between individual addresses within the same ZIP.

# COMMAND ----------

zcta_joined = spark.sql(f"""
SELECT
  d.id,
  d.label,
  d.address,
  d.zip,
  d.lat AS street_lat,
  d.lon AS street_lon,
  z.lat AS zcta_lat,
  z.lon AS zcta_lon,
  ROUND(
    ST_DISTANCESPHERE(ST_POINT(d.lon, d.lat), ST_POINT(z.lon, z.lat)) / 1609.344,
    2
  ) AS centroid_offset_miles
FROM address_demo d
LEFT JOIN zcta_centroids z ON z.zcta = d.zip
""")

display(zcta_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC The `centroid_offset_miles` column shows how far each address is from its
# MAGIC ZIP centroid — helpful for understanding the error ZCTA geocoding introduces
# MAGIC in your specific dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3 — Census TIGER address ranges (skeleton)
# MAGIC
# MAGIC Census TIGER/Line publishes `ADDR` and `ADDRFEAT` shapefiles per county,
# MAGIC with address range and street centerline data. Geocoding is block-face
# MAGIC interpolation:
# MAGIC
# MAGIC 1. Normalize the input address (USPS format, uppercase, strip punctuation)
# MAGIC 2. Match street name + ZIP to find the right block
# MAGIC 3. Check whether the house number falls in the block's range
# MAGIC 4. Interpolate along the line geometry to produce lat/lon
# MAGIC
# MAGIC ### Setup pattern (run once, out-of-band)
# MAGIC
# MAGIC ```python
# MAGIC # On a cluster with internet access, or download to laptop + upload to volume:
# MAGIC # https://www2.census.gov/geo/tiger/TIGER2024/ADDRFEAT/
# MAGIC #
# MAGIC # Per-county ZIP files, ~5-20 MB each. Full US is ~8 GB.
# MAGIC #
# MAGIC # Load into a Delta table partitioned by state FIPS:
# MAGIC
# MAGIC import geopandas as gpd  # available on DBR ML, or pip install on DBR
# MAGIC
# MAGIC gdf = gpd.read_file(f"{VOLUME_PATH}/tiger/ADDRFEAT_36061.zip")  # NY County
# MAGIC (spark.createDataFrame(gdf.drop(columns=["geometry"]).assign(
# MAGIC         wkt=gdf.geometry.to_wkt()))
# MAGIC   .write.mode("append")
# MAGIC   .partitionBy("STATEFP")
# MAGIC   .saveAsTable(f"{CATALOG}.{SCHEMA}.tiger_addrfeat"))
# MAGIC ```
# MAGIC
# MAGIC ### Geocode function skeleton

# COMMAND ----------

# Reference implementation — not runnable without TIGER data loaded.
# Copy and adapt this cell once you have tiger_addrfeat populated.

TIGER_GEOCODE_SQL = """
-- Input: addresses to geocode (street_num, street_name, state, zip)
-- Output: interpolated lat/lon per address
WITH parsed AS (
  SELECT
    id,
    CAST(regexp_extract(address, '^(\\\\d+)', 1) AS INT) AS street_num,
    UPPER(regexp_replace(address, '^\\\\d+\\\\s+', '')) AS street_name,
    state,
    zip
  FROM address_demo_to_geocode
),
matched AS (
  SELECT
    p.id,
    p.street_num,
    -- Which side of the street? Address range on left vs right
    CASE
      WHEN p.street_num BETWEEN t.LFROMHN AND t.LTOHN THEN 'L'
      WHEN p.street_num BETWEEN t.RFROMHN AND t.RTOHN THEN 'R'
    END AS side,
    -- Linear interpolation fraction along the edge
    CASE
      WHEN p.street_num BETWEEN t.LFROMHN AND t.LTOHN
        THEN (p.street_num - t.LFROMHN) / NULLIF(t.LTOHN - t.LFROMHN, 0)
      WHEN p.street_num BETWEEN t.RFROMHN AND t.RTOHN
        THEN (p.street_num - t.RFROMHN) / NULLIF(t.RTOHN - t.RFROMHN, 0)
    END AS frac,
    t.wkt
  FROM parsed p
  JOIN tiger_addrfeat t
    ON UPPER(t.FULLNAME) = p.street_name
   AND t.ZIPL = p.zip
)
SELECT
  id,
  -- ST_LINEINTERPOLATEPOINT along the edge geometry
  ST_X(ST_LINEINTERPOLATEPOINT(ST_GEOMFROMWKT(wkt), frac)) AS lon,
  ST_Y(ST_LINEINTERPOLATEPOINT(ST_GEOMFROMWKT(wkt), frac)) AS lat
FROM matched
"""

print("TIGER geocode SQL skeleton:")
print(TIGER_GEOCODE_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 4 — OpenAddresses (skeleton)
# MAGIC
# MAGIC OpenAddresses publishes rooftop-accurate address → lat/lon data aggregated
# MAGIC from city/county open data portals. Full US is ~30 GB gzipped. Available at
# MAGIC batch.openaddresses.io.
# MAGIC
# MAGIC ### Setup pattern
# MAGIC
# MAGIC ```python
# MAGIC # Download the "us-northeast.zip" / "us-south.zip" regional bundles to a laptop,
# MAGIC # unzip, upload to the volume. Format is per-source CSV with columns:
# MAGIC # LON, LAT, NUMBER, STREET, UNIT, CITY, DISTRICT, REGION, POSTCODE, ID, HASH
# MAGIC
# MAGIC openaddr_df = (spark.read
# MAGIC   .option("header", True)
# MAGIC   .csv(f"{VOLUME_PATH}/openaddresses/us/**/*.csv"))
# MAGIC
# MAGIC # Normalize and write partitioned by state
# MAGIC (openaddr_df
# MAGIC   .withColumn("street_norm", upper(regexp_replace(col("STREET"), "[^A-Z0-9 ]", "")))
# MAGIC   .withColumn("num",         col("NUMBER").cast("int"))
# MAGIC   .select("num", "street_norm", "CITY", "REGION", "POSTCODE",
# MAGIC           col("LAT").cast("double"), col("LON").cast("double"))
# MAGIC   .write.mode("overwrite")
# MAGIC   .partitionBy("REGION")
# MAGIC   .saveAsTable(f"{CATALOG}.{SCHEMA}.openaddresses_us"))
# MAGIC ```
# MAGIC
# MAGIC ### Geocode query
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.id,
# MAGIC   oa.lat,
# MAGIC   oa.lon
# MAGIC FROM address_demo_to_geocode d
# MAGIC LEFT JOIN openaddresses_us oa
# MAGIC   ON  oa.num          = CAST(regexp_extract(d.address, '^(\\\\d+)', 1) AS INT)
# MAGIC   AND oa.street_norm  = UPPER(regexp_replace(d.address, '^\\\\d+\\\\s+', ''))
# MAGIC   AND oa.REGION       = d.state
# MAGIC   AND oa.POSTCODE     = d.zip
# MAGIC ```
# MAGIC
# MAGIC ### Accuracy tradeoff
# MAGIC
# MAGIC OpenAddresses is rooftop when a match is found, but coverage is uneven —
# MAGIC some counties have 100% coverage, others have none. Plan for a fallback
# MAGIC chain: OpenAddresses → TIGER → ZCTA centroid.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 5 — Customer-hosted geocoder service (plug-in stub)
# MAGIC
# MAGIC If the customer already runs Pelias, Nominatim, or a commercial geocoder as
# MAGIC an internal service, wrap it in a Python UDF that calls the service.
# MAGIC
# MAGIC **Make sure the service URL is reachable from the cluster** (check workspace
# MAGIC network firewall / VPC peering). Rate-limit with `mapInPandas` if the service
# MAGIC cannot handle high concurrency.

# COMMAND ----------

# Stub — customer fills in the URL and any auth headers.

import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType

GEOCODER_URL = os.environ.get("INTERNAL_GEOCODER_URL", "https://geocoder.internal.example.com/v1/search")

@udf(StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
]))
def geocode_via_internal_service(address, city, state, zip_code):
    """
    Plug-in stub. Replace body with a call to your internal geocoder.
    Returns (lat, lon) or (None, None) on failure.
    """
    # import requests  # uncomment and pin in cluster libraries
    # resp = requests.get(
    #     GEOCODER_URL,
    #     params={"text": f"{address}, {city}, {state} {zip_code}"},
    #     timeout=5,
    # )
    # resp.raise_for_status()
    # feat = resp.json().get("features", [None])[0]
    # if feat is None:
    #     return (None, None)
    # lon, lat = feat["geometry"]["coordinates"]
    # return (float(lat), float(lon))
    return (None, None)

spark.udf.register("geocode_via_internal_service", geocode_via_internal_service)

print("Stub registered. Edit the function body to call your internal geocoder.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommended chain
# MAGIC
# MAGIC For real UBA workloads, combine options with a coalesce chain:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.id,
# MAGIC   COALESCE(d.lat, oa.lat, tiger.lat, z.lat) AS lat,
# MAGIC   COALESCE(d.lon, oa.lon, tiger.lon, z.lon) AS lon,
# MAGIC   CASE
# MAGIC     WHEN d.lat IS NOT NULL      THEN 'pre_geocoded'
# MAGIC     WHEN oa.lat IS NOT NULL     THEN 'openaddresses'
# MAGIC     WHEN tiger.lat IS NOT NULL  THEN 'tiger'
# MAGIC     WHEN z.lat IS NOT NULL      THEN 'zcta_centroid'
# MAGIC     ELSE                            'ungeocoded'
# MAGIC   END AS geocode_source
# MAGIC FROM source_addresses d
# MAGIC LEFT JOIN openaddresses_us oa  ON ... /* street+zip match */
# MAGIC LEFT JOIN tiger_addrfeat   tiger ON ... /* street+zip match */
# MAGIC LEFT JOIN zcta_centroids   z   ON z.zcta = d.zip
# MAGIC ```
# MAGIC
# MAGIC Always emit `geocode_source` so downstream reports can show which records
# MAGIC have rooftop accuracy vs. ZIP-centroid fallbacks.
