# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 06 · Data Quality — Lakeflow EXPECT Patterns
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev.bronze`
# MAGIC
# MAGIC ## Purpose
# MAGIC Implements the three Lakeflow (Delta Live Tables) data quality expectation
# MAGIC modes in plain PySpark — useful as a standalone validation step and as
# MAGIC documentation of quality rules enforced in the production DLT pipeline.
# MAGIC
# MAGIC | Mode | DLT Decorator | Behaviour | PySpark equivalent |
# MAGIC |------|--------------|-----------|-------------------|
# MAGIC | Warn | `@dlt.expect` | Count violations, keep all rows | Filter + count, log warning |
# MAGIC | Drop | `@dlt.expect_or_drop` | Remove violating rows, continue | Filter rows out |
# MAGIC | Fail | `@dlt.expect_or_fail` | Halt pipeline on any violation | `raise Exception` |
# MAGIC
# MAGIC ## Quality Rules Applied
# MAGIC
# MAGIC | Rule | Column | Mode | Rationale |
# MAGIC |------|--------|------|-----------|
# MAGIC | `wind_mw >= 0` | `wind_mw` | Warn | Negative generation is physically impossible |
# MAGIC | `demand_mw > 0` | `demand_mw` | Drop | Zero demand means a metering gap — exclude |
# MAGIC | `timestamp IS NOT NULL` | `timestamp` | Fail | No timestamp = unresolvable row |
# MAGIC
# MAGIC ## Data Lineage
# MAGIC ```
# MAGIC bronze.grid_raw → [quality checks] → pass/warn/fail → pipeline continues or stops
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG      = "eirgrid_dev"
BRONZE_TABLE = f"{CATALOG}.bronze.grid_raw"

df_bronze = spark.read.table(BRONZE_TABLE)

print(f"Source table : {BRONZE_TABLE}")
print(f"Total rows   : {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · EXPECT (Warn) — `wind_mw >= 0`
# MAGIC
# MAGIC Equivalent to `@dlt.expect("valid_wind_mw", "wind_mw >= 0")`
# MAGIC
# MAGIC Violations are counted and logged. **All rows are kept.**
# MAGIC The pipeline continues — this is a soft quality alert.

# COMMAND ----------

wind_violations = df_bronze.filter(F.col("wind_mw") < 0).count()

if wind_violations > 0:
    print(f"  EXPECT wind_mw >= 0 : {wind_violations:,} violations detected (rows kept)")
else:
    print(f" EXPECT wind_mw >= 0 : 0 violations — all rows kept")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · EXPECT OR DROP — `demand_mw > 0`
# MAGIC
# MAGIC Equivalent to `@dlt.expect_or_drop("valid_demand_mw", "demand_mw > 0")`
# MAGIC
# MAGIC Rows with `demand_mw <= 0` are removed from the output.
# MAGIC The pipeline continues with the filtered dataset.

# COMMAND ----------

before = df_bronze.count()
df_after_drop = df_bronze.filter(F.col("demand_mw") > 0)
after  = df_after_drop.count()
dropped = before - after

print(f"EXPECT OR DROP demand_mw > 0:")
print(f"  Rows before : {before:,}")
print(f"  Rows dropped: {dropped:,}")
print(f"  Rows after  : {after:,}")

if dropped == 0:
    print(" No rows dropped — demand_mw > 0 satisfied for all rows")
else:
    print(f"{dropped:,} rows with demand_mw <= 0 removed from pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · EXPECT OR FAIL — `timestamp IS NOT NULL`
# MAGIC
# MAGIC Equivalent to `@dlt.expect_or_fail("not_null_timestamp", "timestamp IS NOT NULL")`
# MAGIC
# MAGIC If **any** row has a null timestamp, an exception is raised and the pipeline stops.
# MAGIC A null timestamp makes the row unresolvable — no deduplication or time-travel
# MAGIC is possible without it.

# COMMAND ----------

null_timestamps = df_bronze.filter(F.col("timestamp").isNull()).count()

if null_timestamps > 0:
    raise Exception(
        f"EXPECT OR FAIL: {null_timestamps:,} null timestamps detected — pipeline stopped. "
        "Investigate source data before re-running."
    )
else:
    print("EXPECT OR FAIL timestamp IS NOT NULL: 0 violations — pipeline continues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Quality Summary

# COMMAND ----------

print("=" * 60)
print("DATA QUALITY SUMMARY — eirgrid_dev.bronze.grid_raw")
print("=" * 60)
print(f"  Source rows              : {before:,}")
print(f"  EXPECT violations (warn) : {wind_violations:,}   [wind_mw < 0]")
print(f"  EXPECT OR DROP removed   : {dropped:,}   [demand_mw <= 0]")
print(f"  EXPECT OR FAIL triggered : {'No' if null_timestamps == 0 else 'YES — pipeline stopped'}")
print(f"  Rows passing all checks  : {after:,}")
print("=" * 60)

# Final assertion — pipeline only reaches here if no FAIL triggered
assert null_timestamps == 0, "Pipeline should have stopped on null timestamp check"
print("All critical quality gates passed — safe to proceed to silver transform")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Production DLT Equivalent
# MAGIC
# MAGIC In the Lakeflow pipeline (`my_transformation.py`), these same rules are
# MAGIC expressed as decorators on the `@dlt.table` function:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="generation_cleaned")
# MAGIC @dlt.expect("valid_wind_mw",      "wind_mw >= 0")
# MAGIC @dlt.expect_or_drop("valid_demand","demand_mw > 0")
# MAGIC @dlt.expect_or_fail("not_null_ts", "timestamp IS NOT NULL")
# MAGIC def generation_cleaned():
# MAGIC     return (
# MAGIC         dlt.read_stream("grid_raw")
# MAGIC         .withColumn("wind_coverage",  ...)
# MAGIC         .withColumn("surplus_mw",     ...)
# MAGIC         .withColumn("is_surplus_event", ...)
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC This notebook serves as the plain-PySpark reference implementation —
# MAGIC runnable outside the DLT runtime for ad-hoc quality checks.
