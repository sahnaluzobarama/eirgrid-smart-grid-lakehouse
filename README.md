# Eirgrid Smart Grid Lakehouse

A medallion lakehouse on Azure Databricks analysing Irish wind 
generation trends against national demand using ENTSO-E data.

## Research question
How often does Irish wind generation approach or exceed national 
demand, and is the surplus problem worsening year over year?

## Key finding
Wind coverage has remained flat at ~0.33-0.36 of demand annually 
despite new capacity, because demand is growing ~3%/year (driven 
by data centre expansion). The grid consistently approaches the 
SNSP curtailment ceiling in peak wind moments, with 72 near-miss 
events in 2025 alone.

## Architecture
Bronze (ENTSO-E API) → Silver (cleaned, Delta) → Gold (annual analytics)

## Stack
- Azure Databricks Free Edition, Spark 4.1.0
- Delta Lake, Unity Catalog, Lakeflow Pipelines
- Python, entsoe-py, ENTSO-E Transparency Platform
