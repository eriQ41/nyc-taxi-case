# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze
# MAGIC
# MAGIC Reads the raw Parquet files from the landing Volume with **PySpark** and writes
# MAGIC them as **Delta tables**, one per service:
# MAGIC
# MAGIC - `ifood.bronze.yellow_tripdata`
# MAGIC - `ifood.bronze.green_tripdata`
# MAGIC
# MAGIC Bronze stays as close to the source as possible. We only:
# MAGIC - add ingestion metadata (`_source_file`, `_ingested_at`) so every row is traceable;
# MAGIC - **conform schema drift across the monthly files** (see below).
# MAGIC
# MAGIC ## Why we can't just `mergeSchema`
# MAGIC The TLC monthly files are inconsistent: the *same* column has different types in
# MAGIC different months (e.g. `VendorID` INT vs BIGINT, `passenger_count` BIGINT vs
# MAGIC DOUBLE), and one month spells it `Airport_fee` instead of `airport_fee`. Parquet's
# MAGIC `mergeSchema` fails on those conflicts (`CANNOT_MERGE_SCHEMAS`).
# MAGIC
# MAGIC So we read **each month individually** and conform every column to a single type:
# MAGIC - column names are lower-cased (fixes `Airport_fee` vs `airport_fee`);
# MAGIC - numeric types are **promoted to the widest** seen across months (INT→BIGINT,
# MAGIC   BIGINT→DOUBLE), so no value is lost — only widened.
# MAGIC
# MAGIC The load is a **full overwrite** (idempotent): re-running rebuilds bronze from
# MAGIC whatever is in landing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema-conforming union
# MAGIC
# MAGIC Aligns a list of monthly DataFrames into one schema before unioning.

# COMMAND ----------

# Numeric type widening order: a column seen as several of these collapses to the widest.
_NUMERIC_RANK = {
    "tinyint": 1, "smallint": 2, "int": 3, "integer": 3,
    "bigint": 4, "long": 4, "float": 5, "double": 6,
}


def _promoted_type(types: set) -> str:
    """Pick one type for a column given all the types it has across files."""
    non_null = {t for t in types if t not in ("void", "null")}
    if not non_null:
        return "string"
    if len(non_null) == 1:
        return next(iter(non_null))
    if all(t in _NUMERIC_RANK for t in non_null):
        return max(non_null, key=lambda t: _NUMERIC_RANK[t])
    return "string"  # genuinely mixed, non-numeric -> safest common type


def conform_union(dfs: list) -> DataFrame:
    # 1) lower-case all column names so casing differences collapse
    dfs = [df.toDF(*[c.lower() for c in df.columns]) for df in dfs]

    # 2) decide one target type per column across all files
    col_types: dict = {}
    for df in dfs:
        for field in df.schema.fields:
            col_types.setdefault(field.name, set()).add(field.dataType.simpleString())
    target = {c: _promoted_type(ts) for c, ts in col_types.items()}
    columns = list(target.keys())

    # 3) cast every df to that target schema (missing column -> typed NULL), then union
    def conform(df: DataFrame) -> DataFrame:
        present = set(df.columns)
        return df.select([
            (F.col(c) if c in present else F.lit(None)).cast(target[c]).alias(c)
            for c in columns
        ])

    return reduce(lambda a, b: a.unionByName(b), [conform(df) for df in dfs])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest helper

# COMMAND ----------

def ingest_bronze(service: str) -> str:
    target = fqn(BRONZE_SCHEMA, f"{service}_tripdata")

    monthly = [
        spark.read.parquet(landing_file_path(service, month))
        .withColumn("_source_file", F.lit(file_name(service, month)))
        for month in MONTHS
    ]

    df = conform_union(monthly).withColumn("_ingested_at", F.current_timestamp())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    count = spark.table(target).count()
    print(f"{service}: wrote {count:,} rows -> {target}")
    return target

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run for each service

# COMMAND ----------

bronze_tables = {service: ingest_bronze(service) for service in SERVICES}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC Row counts per source file — confirms all 5 months landed in each table.

# COMMAND ----------

for service, table in bronze_tables.items():
    print(f"=== {service} :: {table} ===")
    display(
        spark.table(table)
        .groupBy("_source_file")
        .count()
        .orderBy("_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview the mandated columns (yellow)
# MAGIC
# MAGIC Note: bronze names are lower-cased, so `VendorID` is `vendorid` here. The original
# MAGIC casing required by the case is restored in the gold consumption layer (`04_gold`).

# COMMAND ----------

display(
    spark.table(bronze_tables["yellow"]).select(
        "vendorid",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ).limit(10)
)
