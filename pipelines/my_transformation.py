# ── Eirgrid Smart Grid Lakehouse ─────────────────────────────────────────────
# Lakeflow Declarative Pipeline (Delta Live Tables)
#
# Project  : Eirgrid Smart Grid Medallion Lakehouse
# Author   : Sunny
# Type     : Lakeflow DLT pipeline source file
# Schedule : Triggered by eirgrid_bronze_to_gold job (02:00 Europe/Dublin)
#
# Datasets defined:
#   generation_cleaned  — streaming table (bronze → silver)
#   surplus_annual      — materialized view (silver → gold)
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql import functions as F

NEAR_MISS_THRESHOLD = 0.9


@dlt.table(
    name    = "generation_cleaned",
    comment = "Cleaned and enriched generation data. Derived: surplus_mw, wind_coverage, is_surplus_event."
)
@dlt.expect("valid_wind_mw",        "wind_mw >= 0")
@dlt.expect_or_drop("valid_demand", "demand_mw > 0")
@dlt.expect_or_fail("not_null_ts",  "timestamp IS NOT NULL")
def generation_cleaned():
    return (
        dlt.read_stream("grid_raw")
        .withColumn("wind_mw",   F.coalesce(F.col("wind_mw"),   F.lit(0.0)))
        .withColumn("demand_mw", F.coalesce(F.col("demand_mw"), F.lit(0.0)))
        .withColumn("surplus_mw",
            F.round(F.col("wind_mw") - F.col("demand_mw"), 4))
        .withColumn("wind_coverage",
            F.when(F.col("demand_mw") != 0,
                   F.round(F.col("wind_mw") / F.col("demand_mw"), 6)))
        .withColumn("is_surplus_event",
            F.col("wind_mw") > F.col("demand_mw"))
        .dropDuplicates(["timestamp", "country_code"])
    )


@dlt.table(
    name    = "surplus_annual",
    comment = "Annual wind coverage and near-miss analysis. Research question: how often does wind approach or exceed Irish national demand?"
)
def surplus_annual():
    return (
        dlt.read("generation_cleaned")
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
                * 100.0 / F.count("*"), 2)
                .alias("near_miss_pct"),
            F.round(F.avg("demand_mw"), 2)
                .alias("avg_demand_mw"),
        )
        .orderBy("year")
    )