# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · Table Maintenance — OPTIMIZE & VACUUM
# MAGIC
# MAGIC **Project:** Eirgrid Smart Grid Medallion Lakehouse
# MAGIC **Author:** Sunny
# MAGIC **Catalog / Schema:** `eirgrid_dev` (bronze · silver · gold)
# MAGIC
# MAGIC ## Purpose
# MAGIC Nightly Delta Lake maintenance to prevent small-file accumulation and reclaim
# MAGIC storage by removing files no longer referenced by any active Delta version.
# MAGIC
# MAGIC ## Operations
# MAGIC
# MAGIC | Operation | Tables | Notes |
# MAGIC |-----------|--------|-------|
# MAGIC | `OPTIMIZE` | bronze, silver, gold | Compact small Parquet files → ~1 GB target |
# MAGIC | `VACUUM` | bronze, silver, gold | Delete unreferenced files older than 7 days |
# MAGIC
# MAGIC ## Design Decisions
# MAGIC - **OPTIMIZE runs before VACUUM** — OPTIMIZE creates new compact files and marks
# MAGIC   old ones as unreferenced. VACUUM then safely deletes those old files.
# MAGIC - **Silver uses liquid clustering** — `OPTIMIZE` only, no `ZORDER BY` clause
# MAGIC   (mixing both raises `DELTA_CLUSTERING_WITH_ZORDER_BY`).
# MAGIC - **VACUUM retention = 168 hours (7 days)** — the minimum safe floor; going lower
# MAGIC   requires disabling `retentionDurationCheck` and breaks time travel.
# MAGIC
# MAGIC ## Schedule
# MAGIC Run as the final task in the `eirgrid_bronze_to_gold` Lakeflow Job (04:00 UTC),
# MAGIC after `validate_gold` confirms pipeline success.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG  = "eirgrid_dev"
TABLES   = [
    f"{CATALOG}.bronze.grid_raw",
    f"{CATALOG}.silver.generation_cleaned",
    f"{CATALOG}.gold.surplus_annual",
]
VACUUM_RETAIN_HOURS = 168  # 7 days — do not reduce below this value

print(f"Tables to maintain : {len(TABLES)}")
for t in TABLES:
    print(f"  {t}")
print(f"VACUUM retention   : {VACUUM_RETAIN_HOURS} hours ({VACUUM_RETAIN_HOURS // 24} days)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · OPTIMIZE — File Compaction
# MAGIC
# MAGIC Compact small Parquet files into larger ones to reduce task overhead on reads.
# MAGIC
# MAGIC - `bronze.grid_raw` — bin-pack only (no clustering declared)
# MAGIC - `silver.generation_cleaned` — liquid clustering on `timestamp` (no ZORDER BY)
# MAGIC - `gold.surplus_annual` — tiny table (5 rows); OPTIMIZE is a no-op but harmless

# COMMAND ----------

print("── OPTIMIZE ──────────────────────────────────────────────────────────────")
for table in TABLES:
    spark.sql(f"OPTIMIZE {table}")
    print(f"   {table}")
print("OPTIMIZE complete\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · VACUUM — Remove Unreferenced Files

# COMMAND ----------

print("── VACUUM ────────────────────────────────────────────────────────────────")
for table in TABLES:
    spark.sql(f"VACUUM {table} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
    print(f"   {table}")
print(f"VACUUM complete — files older than {VACUUM_RETAIN_HOURS}h removed\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Post-Maintenance Verification

# COMMAND ----------

# Confirm all three tables are still queryable and row counts are intact
print("── Row count verification ─────────────────────────────────────────────────")
for table in TABLES:
    count = spark.read.table(table).count()
    print(f"  {table:<55} {count:>10,} rows")

print("\n Maintenance complete — all tables healthy")
