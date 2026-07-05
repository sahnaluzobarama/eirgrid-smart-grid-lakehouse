# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 01 · Bronze Ingestion — ENTSO-E → `bronze.grid_raw`
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev.bronze`
# MAGIC
# MAGIC ## Purpose
# MAGIC Incremental ingestion of Irish electricity generation data from the
# MAGIC [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) into the
# MAGIC Delta Lake bronze layer.
# MAGIC
# MAGIC - **Source:** ENTSO-E REST API (country code: `IE`)
# MAGIC - **Frequency:** Nightly (scheduled via Lakeflow Job at 02:00 Europe/Dublin)
# MAGIC - **Target:** `eirgrid_dev.bronze.grid_raw` (append, idempotent on timestamp)
# MAGIC - **Credentials:** API key stored in Databricks secret scope `eirgrid-secrets`
# MAGIC
# MAGIC ## Data Lineage
# MAGIC ```
# MAGIC ENTSO-E API → entsoe-py client → bronze.grid_raw (Delta, raw append)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install ENTSO-E Python client
# Note: must be re-run on every serverless kernel restart
%pip install entsoe-py --quiet

# COMMAND ----------

from datetime import datetime, timezone, timedelta
from entsoe import EntsoePandasClient
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG        = "eirgrid_dev"
SCHEMA         = "bronze"
TABLE          = f"{CATALOG}.{SCHEMA}.grid_raw"
COUNTRY_CODE   = "IE"
SECRET_SCOPE   = "eirgrid-secrets"
SECRET_KEY     = "entsoe-api-key"

# Ingestion window: last 30 days (idempotent — duplicates handled in silver)
END_DT   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
START_DT = END_DT - timedelta(days=30)

print(f"Target table : {TABLE}")
print(f"Ingest window: {START_DT.date()}  →  {END_DT.date()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Fetch from ENTSO-E API

# COMMAND ----------

# Retrieve API key from Databricks secret scope
api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
client  = EntsoePandasClient(api_key=api_key)

start_ts = pd.Timestamp(START_DT)
end_ts   = pd.Timestamp(END_DT)

# Fetch actual generation per production type
gen_df = client.query_generation(COUNTRY_CODE, start=start_ts, end=end_ts)

# Fetch total load (demand)
load_df = client.query_load(COUNTRY_CODE, start=start_ts, end=end_ts)

print(f"Generation rows fetched : {len(gen_df):,}")
print(f"Load rows fetched       : {len(load_df):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Flatten & Join

# COMMAND ----------

# Flatten multi-level generation columns → single DataFrame
gen_flat = gen_df.copy()
gen_flat.columns = [
    "_".join(col).strip("_").lower().replace(" ", "_")
    if isinstance(col, tuple) else col.lower().replace(" ", "_")
    for col in gen_flat.columns
]
gen_flat = gen_flat.reset_index().rename(columns={"index": "timestamp"})

# Keep only wind and relevant generation types
wind_cols = [c for c in gen_flat.columns if "wind" in c]
wind_mw   = gen_flat[wind_cols].sum(axis=1) if wind_cols else 0

combined = pd.DataFrame({
    "timestamp" : gen_flat["timestamp"],
    "wind_mw"   : wind_mw,
})

# Join load
load_flat = load_df.reset_index()
load_flat.columns = ["timestamp", "demand_mw"]

raw = combined.merge(load_flat, on="timestamp", how="inner")
raw["country_code"]  = COUNTRY_CODE
raw["ingested_at"]   = datetime.now(timezone.utc)
raw["source_system"] = "entsoe"

print(f"Merged rows: {len(raw):,}")
print(raw.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Write to Delta Bronze (Append)

# COMMAND ----------

# Convert to Spark DataFrame
spark_df = (
    spark.createDataFrame(raw)
    .withColumn("timestamp",    F.col("timestamp").cast(TimestampType()))
    .withColumn("ingested_at",  F.col("ingested_at").cast(TimestampType()))
    .withColumn("wind_mw",      F.col("wind_mw").cast(DoubleType()))
    .withColumn("demand_mw",    F.col("demand_mw").cast(DoubleType()))
    .select("timestamp", "wind_mw", "demand_mw",
            "country_code", "ingested_at", "source_system")
)

# Append to bronze (silver handles deduplication)
(
    spark_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE)
)

count = spark.read.table(TABLE).count()
print(f" Append complete. Total rows in {TABLE}: {count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Validation

# COMMAND ----------

# Confirm recent rows landed
spark.sql(f"""
    SELECT
        DATE(timestamp)   AS date,
        COUNT(*)          AS intervals,
        ROUND(AVG(wind_mw),  2) AS avg_wind_mw,
        ROUND(AVG(demand_mw),2) AS avg_demand_mw
    FROM {TABLE}
    WHERE timestamp >= current_timestamp() - INTERVAL 3 DAYS
    GROUP BY DATE(timestamp)
    ORDER BY date DESC
""").show()

# COMMAND ----------

# Null check on critical columns
nulls = spark.sql(f"""
    SELECT
        SUM(CASE WHEN timestamp  IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
        SUM(CASE WHEN wind_mw    IS NULL THEN 1 ELSE 0 END) AS null_wind_mw,
        SUM(CASE WHEN demand_mw  IS NULL THEN 1 ELSE 0 END) AS null_demand_mw
    FROM {TABLE}
    WHERE timestamp >= current_timestamp() - INTERVAL 3 DAYS
""").collect()[0]

assert nulls["null_timestamps"] == 0, " Null timestamps detected in recent data"
print(" No null timestamps in recent ingest window")
print(f"   Null wind_mw  : {nulls['null_wind_mw']}")
print(f"   Null demand_mw: {nulls['null_demand_mw']}")
