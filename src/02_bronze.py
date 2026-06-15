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
# MAGIC Bronze is **1:1 with the source** — no cleaning, no renaming, no type changes.
# MAGIC We only add ingestion metadata so every row is traceable back to its file:
# MAGIC
# MAGIC - `_source_file` — the Parquet file the row came from
# MAGIC - `_ingested_at` — when this bronze load ran
# MAGIC
# MAGIC The load is a **full overwrite** (idempotent): re-running rebuilds bronze from
# MAGIC whatever is currently in landing, so there are no duplicates.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest helper
# MAGIC
# MAGIC Reads every month of a service in one pass (`mergeSchema` tolerates the small
# MAGIC column/type differences TLC introduces across monthly files), tags each row with
# MAGIC its source file, and writes the Delta table.

# COMMAND ----------

def ingest_bronze(service: str) -> str:
    source_dir = f"{landing_volume_path()}/{service}"
    target = fqn(BRONZE_SCHEMA, f"{service}_tripdata")

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(source_dir)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

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
# MAGIC Row counts per source file — confirms all 5 months landed in each table and gives
# MAGIC a first feel for the volume.

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
# MAGIC Quick look at the columns the case requires, straight from raw — types are still
# MAGIC whatever the source used; they get standardized in silver (`03_silver`).

# COMMAND ----------

display(
    spark.table(bronze_tables["yellow"]).select(
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ).limit(10)
)
