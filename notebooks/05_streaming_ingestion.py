# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 05 · Streaming Ingestion — CDF Pipeline & Gold Refresh
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev`
# MAGIC
# MAGIC ## Purpose
# MAGIC Demonstrates Structured Streaming patterns used in the Eirgrid pipeline:
# MAGIC
# MAGIC | Pattern | Description |
# MAGIC |---------|-------------|
# MAGIC | Delta readStream | Read `bronze.grid_raw` as a continuous stream |
# MAGIC | Derived column streaming | Add `wind_coverage_stream` metric in-flight |
# MAGIC | `trigger(availableNow=True)` | Process all available data then stop (micro-batch) |
# MAGIC | Change Data Feed (CDF) | Read only changed rows from `silver.generation_cleaned` |
# MAGIC | `foreachBatch` | Incrementally refresh `gold.surplus_annual` on each silver change batch |
# MAGIC
# MAGIC ## Data Lineage
# MAGIC ```
# MAGIC bronze.grid_raw ──readStream──► wind_coverage_stream ──writeStream──► [stream_test / silver]
# MAGIC silver.generation_cleaned ──CDF readStream──► foreachBatch ──► gold.surplus_annual
# MAGIC ```
# MAGIC
# MAGIC ## Notes
# MAGIC - Checkpoints stored in Unity Catalog Volume: `eirgrid_dev.bronze.checkpoints`
# MAGIC - `trigger(availableNow=True)` is the modern replacement for deprecated `trigger(once=True)`
# MAGIC - The production pipeline uses the Lakeflow DLT version; this notebook is the
# MAGIC   reference implementation showing the same patterns in plain PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG           = "eirgrid_dev"
BRONZE_TABLE      = f"{CATALOG}.bronze.grid_raw"
SILVER_TABLE      = f"{CATALOG}.silver.generation_cleaned"
GOLD_TABLE        = f"{CATALOG}.gold.surplus_annual"
CHECKPOINT_BASE   = f"/Volumes/{CATALOG}/bronze/checkpoints"
NEAR_MISS_THRESHOLD = 0.9

# Ensure checkpoint volume exists
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
print(f"Checkpoint volume ready: {CHECKPOINT_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Delta readStream — Bronze as a Stream

# COMMAND ----------

# Read bronze table as a continuous Delta stream
stream_df = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

print(f"Is streaming  : {stream_df.isStreaming}")
print(f"Source table  : {BRONZE_TABLE}")
stream_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Add Derived Metric In-Flight

# COMMAND ----------

# Compute wind_coverage in the stream (same logic as silver batch transforms)
stream_with_coverage = (
    stream_df
    .withColumn(
        "wind_coverage_stream",
        F.when(F.col("demand_mw") != 0,
               F.round(F.col("wind_mw") / F.col("demand_mw"), 6))
    )
    .select("timestamp", "wind_mw", "demand_mw",
            "wind_coverage_stream", "country_code")
)

print("Transformed stream schema:")
stream_with_coverage.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Write Stream — `trigger(availableNow=True)`
# MAGIC
# MAGIC Processes all available data in micro-batches then stops cleanly.
# MAGIC This is the modern replacement for the deprecated `trigger(once=True)`.

# COMMAND ----------

STREAM_TARGET    = f"{CATALOG}.bronze.stream_test"
STREAM_CHECKPOINT = f"{CHECKPOINT_BASE}/stream_test"

query = (
    stream_with_coverage.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", STREAM_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(STREAM_TARGET)
)
query.awaitTermination()

count = spark.sql(f"SELECT COUNT(*) AS n FROM {STREAM_TARGET}").collect()[0]["n"]
print(f" Stream complete — {count:,} rows written to {STREAM_TARGET}")

# Preview
spark.sql(f"""
    SELECT timestamp, wind_mw, demand_mw,
           ROUND(wind_coverage_stream, 4) AS wind_coverage_stream
    FROM {STREAM_TARGET}
    ORDER BY timestamp DESC
    LIMIT 5
""").show(truncate=False)

# Clean up test table
spark.sql(f"DROP TABLE IF EXISTS {STREAM_TARGET}")
print("Stream test table dropped (cleanup)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Change Data Feed — Read Silver Changes
# MAGIC
# MAGIC CDF was enabled on `silver.generation_cleaned` at version 10.
# MAGIC `table_changes()` returns only the rows that changed since a given version —
# MAGIC much more efficient than a full table scan for incremental gold refreshes.

# COMMAND ----------

# Inspect CDF history to find the version where CDF was enabled
spark.sql(f"""
    DESCRIBE HISTORY {SILVER_TABLE} LIMIT 5
""").select("version", "timestamp", "operation").show()

# COMMAND ----------

# Read changes since CDF was enabled (version 10)
CDF_START_VERSION = (
    spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 1")
    .collect()[0]["version"]
)

cdf_stream = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", CDF_START_VERSION)
    .table(SILVER_TABLE)
)

print(f"CDF stream schema (startingVersion={CDF_START_VERSION}):")
cdf_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · `foreachBatch` — Incremental Gold Refresh
# MAGIC
# MAGIC On each micro-batch of silver changes, recompute `gold.surplus_annual`
# MAGIC using a `CREATE OR REPLACE TABLE` from the full silver dataset.
# MAGIC This ensures gold is always consistent with the latest silver state.

# COMMAND ----------

# Clear stale checkpoint — required after schema/version changes
try:
    dbutils.fs.rm(f"{CHECKPOINT_BASE}/gold_refresh", recurse=True)
    print(" Checkpoint cleared")
except Exception as e:
    print(f"No checkpoint found (already clean): {e}")

def refresh_gold_layer(micro_batch_df, batch_id):
    """
    foreachBatch handler: triggered on each new batch of silver CDF changes.
    Recomputes the full gold aggregation from silver to keep it consistent.

    Args:
        micro_batch_df : DataFrame containing changed silver rows in this batch
        batch_id       : Unique batch sequence number
    """
    row_count = micro_batch_df.count()
    print(f"Batch {batch_id}: {row_count} changed rows received from silver CDF")

    if row_count == 0:
        print("No new rows — skipping gold refresh")
        return

    spark.sql(f"""
        CREATE OR REPLACE TABLE {GOLD_TABLE}
        USING DELTA
        COMMENT 'Annual wind coverage and near-miss surplus analysis. Updated by streaming pipeline.'
        AS
        SELECT
            YEAR(timestamp)                                                            AS year,
            COUNT(*)                                                                   AS total_intervals,
            ROUND(AVG(wind_coverage), 4)                                               AS avg_wind_coverage,
            ROUND(MAX(wind_coverage), 4)                                               AS max_wind_coverage,
            SUM(CASE WHEN is_surplus_event              THEN 1 ELSE 0 END)             AS surplus_events,
            SUM(CASE WHEN wind_coverage > {NEAR_MISS_THRESHOLD} THEN 1 ELSE 0 END)    AS near_miss_events,
            ROUND(
              SUM(CASE WHEN wind_coverage > {NEAR_MISS_THRESHOLD} THEN 1 ELSE 0 END)
              * 100.0 / COUNT(*), 2
            )                                                                          AS near_miss_pct,
            ROUND(AVG(demand_mw), 2)                                                   AS avg_demand_mw
        FROM {SILVER_TABLE}
        GROUP BY YEAR(timestamp)
        ORDER BY year
    """)
    print(f"   Gold table refreshed at batch {batch_id}")


# Run the foreachBatch streaming query
gold_query = (
    cdf_stream.writeStream
    .foreachBatch(refresh_gold_layer)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_refresh")
    .trigger(availableNow=True)
    .start()
)
gold_query.awaitTermination()
print("Stream complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Verify Gold Output

# COMMAND ----------

spark.sql(f"SELECT * FROM {GOLD_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Platform Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │          EIRGRID SMART GRID PLATFORM — PRODUCTION DESIGN   │
# MAGIC │                                                             │
# MAGIC │  DATA SOURCES                                               │
# MAGIC │  ┌─────────────────────┐   ┌─────────────────────────────┐ │
# MAGIC │  │ ENTSO-E REST API    │   │ IoT Grid Sensors            │ │
# MAGIC │  │ Historical data     │   │ Real-time telemetry         │ │
# MAGIC │  └────────┬────────────┘   └────────────┬────────────────┘ │
# MAGIC │           │                             │                   │
# MAGIC │           ▼                             ▼                   │
# MAGIC │  ┌─────────────────────┐   ┌─────────────────────────────┐ │
# MAGIC │  │ ADF Pipeline        │   │ Azure Event Hubs            │ │
# MAGIC │  │ (batch ingestion)   │   │ (streaming ingestion)       │ │
# MAGIC │  └────────┬────────────┘   └────────────┬────────────────┘ │
# MAGIC │           └──────────────┬──────────────┘                  │
# MAGIC │                          ▼                                  │
# MAGIC │  ┌───────────────────────────────────────────────────────┐ │
# MAGIC │  │              DATABRICKS LAKEHOUSE                     │ │
# MAGIC │  │                                                       │ │
# MAGIC │  │  BRONZE : eirgrid_dev.bronze.grid_raw                 │ │
# MAGIC │  │             ↓  Lakeflow ETL Pipeline                  │ │
# MAGIC │  │  SILVER : eirgrid_dev.silver.generation_cleaned       │ │
# MAGIC │  │             ↓  Materialized View                      │ │
# MAGIC │  │  GOLD   : eirgrid_dev.gold.surplus_annual             │ │
# MAGIC │  │                                                       │ │
# MAGIC │  │  Security : Entra ID + Key Vault + UC grants          │ │
# MAGIC │  │  Quality  : Lakeflow expectations (EXPECT patterns)   │ │
# MAGIC │  │  Schedule : Lakeflow Job (0 2 * * *)                  │ │
# MAGIC │  └───────────────────────────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
