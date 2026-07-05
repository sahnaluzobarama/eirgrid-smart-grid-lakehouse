# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 02 · Silver Transforms — `bronze.grid_raw` → `silver.generation_cleaned`
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev.silver`
# MAGIC
# MAGIC ## Purpose
# MAGIC Cleanse and enrich raw bronze data into a query-ready silver layer:
# MAGIC
# MAGIC | Transform | Description |
# MAGIC |-----------|-------------|
# MAGIC | Deduplication | Remove duplicate rows on `(timestamp, country_code)` |
# MAGIC | Null coalescing | Replace null MW values with `0.0` |
# MAGIC | `surplus_mw` | `wind_mw - demand_mw` — positive = surplus |
# MAGIC | `wind_coverage` | `wind_mw / demand_mw` — fraction of demand met by wind |
# MAGIC | `is_surplus_event` | Boolean flag: `wind_mw > demand_mw` |
# MAGIC | `processed_at` | UTC timestamp of this transform run |
# MAGIC
# MAGIC ## Data Lineage
# MAGIC ```
# MAGIC bronze.grid_raw → [dedup → coalesce → derive] → silver.generation_cleaned
# MAGIC ```
# MAGIC
# MAGIC ## Notes
# MAGIC - Silver table has **Change Data Feed (CDF)** enabled from version 10
# MAGIC - `INSERT OVERWRITE` is used (not CTAS) to preserve table metadata and grants
# MAGIC - On serverless, re-run the full cell block — variables don't persist across restarts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG       = "eirgrid_dev"
BRONZE_TABLE  = f"{CATALOG}.bronze.grid_raw"
SILVER_TABLE  = f"{CATALOG}.silver.generation_cleaned"

print(f"Source : {BRONZE_TABLE}")
print(f"Target : {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Read Bronze

# COMMAND ----------

df_bronze = spark.read.table(BRONZE_TABLE)

print(f"Bronze rows (raw): {df_bronze.count():,}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Deduplicate

# COMMAND ----------

# Keep latest ingested row per (timestamp, country_code) window
df_deduped = (
    df_bronze
    .dropDuplicates(["timestamp", "country_code"])
)

dropped = df_bronze.count() - df_deduped.count()
print(f"Rows before dedup : {df_bronze.count():,}")
print(f"Duplicate rows    : {dropped:,}")
print(f"Rows after dedup  : {df_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Coalesce Nulls & Derive Columns

# COMMAND ----------

PROCESSED_AT = datetime.now(timezone.utc)

df_silver = (
    df_deduped
    # Null safety — replace missing MW values with 0
    .withColumn("wind_mw",   F.coalesce(F.col("wind_mw"),   F.lit(0.0)))
    .withColumn("demand_mw", F.coalesce(F.col("demand_mw"), F.lit(0.0)))

    # Derived columns — core research metrics
    .withColumn(
        "surplus_mw",
        F.round(F.col("wind_mw") - F.col("demand_mw"), 4)
    )
    .withColumn(
        "wind_coverage",
        F.when(F.col("demand_mw") != 0,
               F.round(F.col("wind_mw") / F.col("demand_mw"), 6))
         .otherwise(F.lit(None))
    )
    .withColumn(
        "is_surplus_event",
        F.col("wind_mw") > F.col("demand_mw")
    )
    .withColumn("processed_at", F.lit(PROCESSED_AT).cast("timestamp"))

    # Column ordering
    .select(
        "timestamp", "country_code",
        "wind_mw", "demand_mw",
        "surplus_mw", "wind_coverage", "is_surplus_event",
        "ingested_at", "source_system", "processed_at"
    )
    .orderBy("timestamp")
)

print(f"Silver rows ready: {df_silver.count():,}")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Write to Silver (INSERT OVERWRITE)
# MAGIC
# MAGIC `INSERT OVERWRITE` preserves the table definition (comments, grants, CDF config).
# MAGIC CTAS would drop and recreate — losing all metadata.

# COMMAND ----------

# Create silver table if it doesn't already exist
# Write to silver — overwriteSchema=true handles the existing 17-col table
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

# Re-enable CDF (lost when schema is overwritten)
spark.sql(f"""
    ALTER TABLE {SILVER_TABLE}
    SET TBLPROPERTIES (
        'delta.enableChangeDataFeed'        = 'true',
        'delta.autoOptimize.optimizeWrite'  = 'true'
    )
""")

print(f" Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Validation

# COMMAND ----------

# Row count confirmation
silver_count = spark.read.table(SILVER_TABLE).count()
print(f"Silver row count: {silver_count:,}")

# Sample of derived metrics
spark.sql(f"""
    SELECT
        timestamp,
        wind_mw,
        demand_mw,
        surplus_mw,
        ROUND(wind_coverage, 4) AS wind_coverage,
        is_surplus_event
    FROM {SILVER_TABLE}
    ORDER BY wind_coverage DESC NULLS LAST
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Annual summary — quick sanity check
spark.sql(f"""
    SELECT
        YEAR(timestamp)                AS year,
        COUNT(*)                       AS intervals,
        ROUND(AVG(wind_coverage), 4)   AS avg_wind_coverage,
        SUM(CAST(is_surplus_event AS INT)) AS surplus_events
    FROM {SILVER_TABLE}
    GROUP BY YEAR(timestamp)
    ORDER BY year
""").show()

# COMMAND ----------

# CDF sanity — confirm feed is enabled
history = spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 3")
history.select("version", "timestamp", "operation").show()
