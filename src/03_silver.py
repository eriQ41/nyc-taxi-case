# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver
# MAGIC
# MAGIC Turns the raw bronze tables into **clean, typed, query-ready** tables with a
# MAGIC **single unified schema** for both services:
# MAGIC
# MAGIC - `ifood.silver.yellow_trips`
# MAGIC - `ifood.silver.green_trips`
# MAGIC
# MAGIC Both share the same columns, so gold can union them for the "all taxis" question.
# MAGIC
# MAGIC ## What happens here
# MAGIC 1. **Unify the schema** — yellow's `tpep_*` and green's `lpep_*` pickup/dropoff
# MAGIC    columns are mapped to common `pickup_datetime` / `dropoff_datetime`, and a
# MAGIC    `service_type` column tags each row (`yellow` / `green`).
# MAGIC 2. **Type** — `vendorid`→int, `passenger_count`→int, timestamps→`timestamp_ntz`
# MAGIC    (kept tz-naive so the *local* pickup hour is preserved for Q2), `total_amount`→double.
# MAGIC 3. **Clean (structural)** — keep only trips whose pickup falls inside the project
# MAGIC    scope (2023-01..05); TLC files carry a few stray out-of-range timestamps that
# MAGIC    would corrupt month-based aggregations. Drop exact duplicate rows.
# MAGIC
# MAGIC ## What we deliberately do NOT filter here
# MAGIC Business filters that belong to a *specific question* are applied at analysis time,
# MAGIC not blanket-removed in silver:
# MAGIC - `passenger_count` NULL/0 → excluded only in Q2 (a per-passenger mean);
# MAGIC - `total_amount` < 0 (refunds) → excluded only in Q1 (revenue received).
# MAGIC
# MAGIC Keeping them in silver means silver stays a faithful, reusable base; see
# MAGIC `docs/ARCHITECTURE.md`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F

# Scope bounds derived from config: [first day of min month, first day after max month)
SCOPE_LO = date(YEAR, min(MONTHS), 1)
SCOPE_HI = date(YEAR + 1, 1, 1) if max(MONTHS) == 12 else date(YEAR, max(MONTHS) + 1, 1)
print(f"Keeping pickups in [{SCOPE_LO}, {SCOPE_HI})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build helper
# MAGIC
# MAGIC One function for both services — the only difference is the pickup/dropoff column
# MAGIC names, which come from `SERVICES` in `00_config` (lower-cased to match bronze).

# COMMAND ----------

def build_silver(service: str) -> str:
    source = fqn(BRONZE_SCHEMA, f"{service}_tripdata")
    target = fqn(SILVER_SCHEMA, f"{service}_trips")

    pickup_col = SERVICES[service]["pickup_col"].lower()
    dropoff_col = SERVICES[service]["dropoff_col"].lower()

    bronze = spark.table(source)
    raw_count = bronze.count()

    typed = bronze.select(
        F.lit(service).alias("service_type"),
        F.col("vendorid").cast("int").alias("vendorid"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col(pickup_col).cast("timestamp_ntz").alias("pickup_datetime"),
        F.col(dropoff_col).cast("timestamp_ntz").alias("dropoff_datetime"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )

    cleaned = (
        typed
        .filter(
            (F.col("pickup_datetime") >= F.lit(SCOPE_LO))
            & (F.col("pickup_datetime") < F.lit(SCOPE_HI))
        )
        .dropDuplicates()
    )

    cleaned.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(target)

    kept = spark.table(target).count()
    print(f"{service}: {raw_count:,} bronze -> {kept:,} silver "
          f"({raw_count - kept:,} dropped: out-of-scope + duplicates)")
    return target

# COMMAND ----------

silver_tables = {service: build_silver(service) for service in SERVICES}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC Confirm the date range is inside scope and look at how many rows the *analysis*
# MAGIC filters will touch (NULL/0 passengers, negative totals) — informs the queries.

# COMMAND ----------

for service, table in silver_tables.items():
    print(f"=== {service} :: {table} ===")
    df = spark.table(table)
    df.select(
        F.min("pickup_datetime").alias("min_pickup"),
        F.max("pickup_datetime").alias("max_pickup"),
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") == 0), 1)
              .otherwise(0)).alias("passengers_null_or_zero"),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("total_amount_negative"),
    ).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview unified schema

# COMMAND ----------

display(spark.table(silver_tables["green"]).limit(5))
