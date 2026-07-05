# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 00 · Eirgrid Smart Grid — Exploratory Data Analysis
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev.bronze`
# MAGIC
# MAGIC ## Purpose
# MAGIC Exploratory analysis of Irish grid generation data sourced from the ENTSO-E
# MAGIC Transparency Platform. This notebook answers the core research question:
# MAGIC
# MAGIC > *How often does Irish wind generation approach or exceed national demand,
# MAGIC > and is the surplus problem worsening year over year?*
# MAGIC
# MAGIC ## Contents
# MAGIC | Section | Description |
# MAGIC |---------|-------------|
# MAGIC | 1 | Dataset overview — row counts, date range, schema |
# MAGIC | 2 | Generation mix — wind vs demand time series |
# MAGIC | 3 | Wind coverage distribution |
# MAGIC | 4 | Annual trend analysis |
# MAGIC | 5 | Near-miss event frequency |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Dataset Overview

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "eirgrid_dev"
SCHEMA  = "bronze"
TABLE   = f"{CATALOG}.{SCHEMA}.grid_raw"

df = spark.read.table(TABLE)

print(f"Table  : {TABLE}")
print(f"Rows   : {df.count():,}")
print(f"Columns: {len(df.columns)}")

# COMMAND ----------

# Schema
df.printSchema()

# COMMAND ----------

# Date range covered
df.select(
    F.min("timestamp").alias("earliest"),
    F.max("timestamp").alias("latest"),
    F.countDistinct(F.to_date("timestamp")).alias("distinct_days")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Generation Mix — Wind vs Demand

# COMMAND ----------

# Monthly averages for visual trend
monthly = (
    df
    .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.round(F.avg("wind_mw"),   2).alias("avg_wind_mw"),
        F.round(F.avg("demand_mw"), 2).alias("avg_demand_mw"),
    )
    .orderBy("year_month")
)
display(monthly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Wind Coverage Distribution

# COMMAND ----------

# wind_coverage = wind_mw / demand_mw  (computed in silver; approximated here)
coverage_dist = (
    df
    .withColumn(
        "wind_coverage",
        F.when(F.col("demand_mw") != 0,
               F.col("wind_mw") / F.col("demand_mw"))
    )
    .select("wind_coverage")
    .dropna()
)

coverage_dist.describe("wind_coverage").show()

# COMMAND ----------

# Bucket distribution
buckets = (
    coverage_dist
    .withColumn("bucket", (F.col("wind_coverage") * 10).cast("int") / 10)
    .groupBy("bucket")
    .count()
    .orderBy("bucket")
)
display(buckets)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Annual Trend Analysis

# COMMAND ----------

annual = (
    df
    .withColumn("year",         F.year("timestamp"))
    .withColumn("wind_coverage", F.when(F.col("demand_mw") != 0,
                                        F.col("wind_mw") / F.col("demand_mw")))
    .groupBy("year")
    .agg(
        F.count("*")                              .alias("intervals"),
        F.round(F.avg("wind_coverage"),    4)     .alias("avg_wind_coverage"),
        F.round(F.avg("demand_mw"),        2)     .alias("avg_demand_mw"),
        F.round(F.avg("wind_mw"),          2)     .alias("avg_wind_mw"),
        F.sum(F.when(F.col("wind_coverage") > 0.9,
                     1).otherwise(0))             .alias("near_miss_events"),
    )
    .orderBy("year")
)
display(annual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Near-Miss Event Frequency
# MAGIC
# MAGIC A **near-miss event** is defined as any 15-minute interval where
# MAGIC `wind_coverage > 0.9` (wind generation exceeds 90% of national demand).
# MAGIC These intervals indicate grid stress and potential curtailment pressure.

# COMMAND ----------

near_miss_by_year = (
    df
    .withColumn("year", F.year("timestamp"))
    .withColumn("wind_coverage", F.when(F.col("demand_mw") != 0,
                                        F.col("wind_mw") / F.col("demand_mw")))
    .withColumn("near_miss", F.when(F.col("wind_coverage") > 0.9, 1).otherwise(0))
    .groupBy("year")
    .agg(
        F.count("*").alias("total_intervals"),
        F.sum("near_miss").alias("near_miss_events"),
        F.round(F.sum("near_miss") * 100.0 / F.count("*"), 2).alias("near_miss_pct"),
    )
    .orderBy("year")
)
display(near_miss_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings
# MAGIC
# MAGIC - **Wind coverage is flat** at ~0.33–0.36 per year despite ~3% annual demand growth
# MAGIC - **True surplus events** (wind > demand) are very rare — fewer than 1 in 4.5 years
# MAGIC - **Near-miss frequency** (wind_coverage > 0.9) is the more meaningful metric —
# MAGIC   72 events recorded in 2025 alone
# MAGIC - The gold layer (`eirgrid_dev.gold.surplus_annual`) captures these metrics annually
