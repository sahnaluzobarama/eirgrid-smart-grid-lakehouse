# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 03 · Gold Aggregations — `silver.generation_cleaned` → `gold.surplus_annual`
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev.gold`
# MAGIC
# MAGIC ## Purpose
# MAGIC Aggregate silver-layer data into a business-ready annual summary table that
# MAGIC directly answers the project research question:
# MAGIC
# MAGIC > *How often does Irish wind generation approach or exceed national demand,
# MAGIC > and is the surplus problem worsening year over year?*
# MAGIC
# MAGIC ## Output Schema — `gold.surplus_annual`
# MAGIC
# MAGIC | Column | Description |
# MAGIC |--------|-------------|
# MAGIC | `year` | Calendar year |
# MAGIC | `total_intervals` | Count of 15-min measurement intervals |
# MAGIC | `avg_wind_coverage` | Average fraction of demand met by wind |
# MAGIC | `max_wind_coverage` | Peak wind coverage in the year |
# MAGIC | `surplus_events` | Intervals where wind > demand |
# MAGIC | `near_miss_events` | Intervals where wind_coverage > 0.9 |
# MAGIC | `near_miss_pct` | Near-miss events as % of total |
# MAGIC | `avg_demand_mw` | Average national electricity demand (MW) |
# MAGIC
# MAGIC ## Data Lineage
# MAGIC ```
# MAGIC silver.generation_cleaned → [annual GROUP BY] → gold.surplus_annual
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG      = "eirgrid_dev"
SILVER_TABLE = f"{CATALOG}.silver.generation_cleaned"
GOLD_TABLE   = f"{CATALOG}.gold.surplus_annual"

# Near-miss threshold: wind covers ≥90% of demand
NEAR_MISS_THRESHOLD = 0.9

print(f"Source : {SILVER_TABLE}")
print(f"Target : {GOLD_TABLE}")
print(f"Near-miss threshold: wind_coverage > {NEAR_MISS_THRESHOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Build Annual Aggregations

# COMMAND ----------

df_silver = spark.read.table(SILVER_TABLE)

df_gold = (
    df_silver
    .withColumn("year", F.year("timestamp"))
    .groupBy("year")
    .agg(
        F.count("*")
            .alias("total_intervals"),

        F.round(F.avg("wind_coverage"), 4)
            .alias("avg_wind_coverage"),

        F.round(F.max("wind_coverage"), 4)
            .alias("max_wind_coverage"),

        F.sum(F.when(F.col("is_surplus_event"), 1).otherwise(0))
            .alias("surplus_events"),

        F.sum(F.when(F.col("wind_coverage") > NEAR_MISS_THRESHOLD, 1).otherwise(0))
            .alias("near_miss_events"),

        F.round(
            F.sum(F.when(F.col("wind_coverage") > NEAR_MISS_THRESHOLD, 1).otherwise(0))
            * 100.0 / F.count("*"), 2
        ).alias("near_miss_pct"),

        F.round(F.avg("demand_mw"), 2)
            .alias("avg_demand_mw"),
    )
    .orderBy("year")
)

print("Preview:")
df_gold.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Write to Gold

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {GOLD_TABLE}
    USING DELTA
    COMMENT 'Annual wind coverage and near-miss surplus analysis for Ireland. Research question: how often does wind generation approach or exceed national demand, and is the trend worsening?'
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

print(f" Gold table written: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Results & Interpretation

# COMMAND ----------

results = spark.read.table(GOLD_TABLE)
results.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Findings
# MAGIC
# MAGIC | Finding | Detail |
# MAGIC |---------|--------|
# MAGIC | **Wind coverage is flat** | avg_wind_coverage ~0.33–0.36 across 2022–2026 |
# MAGIC | **Demand is growing** | avg_demand_mw increasing ~3% per year |
# MAGIC | **True surplus is very rare** | surplus_events < 5 across the entire 4-year window |
# MAGIC | **Near-miss frequency is rising** | 72 near-miss events in 2025 vs ~40 in 2022 |
# MAGIC
# MAGIC **Conclusion:** The near-miss frequency (`wind_coverage > 0.9`) is the more
# MAGIC meaningful operational metric. Raw surplus events are too rare to be statistically
# MAGIC useful; near-miss trends reveal the real curtailment pressure building on the grid.

# COMMAND ----------

# Validation — confirm expected row count (one row per year)
row_count = results.count()
assert row_count >= 4, f"Expected ≥4 years of data, got {row_count}"
print(f" Gold table validation passed: {row_count} annual rows")
