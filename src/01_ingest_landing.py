# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Validate Landing Zone
# MAGIC
# MAGIC Confirms the raw NYC TLC Parquet files (yellow + green, Jan–May 2023) are present
# MAGIC in the Unity Catalog Volume `ifood.bronze.landing` before the pipeline proceeds.
# MAGIC
# MAGIC ## Why this notebook does NOT download
# MAGIC
# MAGIC Databricks **Free Edition** serverless compute has **no outbound internet**, so a
# MAGIC download from inside the notebook is impossible (it fails with
# MAGIC `Temporary failure in name resolution`). The landing zone is therefore populated
# MAGIC **from outside** Databricks:
# MAGIC
# MAGIC 1. On a machine with internet: `python src/download_landing_local.py`
# MAGIC 2. Upload to the Volume (Databricks CLI `--upload`, or Catalog Explorer → Upload).
# MAGIC
# MAGIC See the README for the full step. This notebook's only job is to **validate** that
# MAGIC those files arrived — that check gates the rest of the pipeline (`02_bronze` onward).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check every expected file in the Volume

# COMMAND ----------

landing_status = []
for service in SERVICES:
    for month in MONTHS:
        path = landing_file_path(service, month)
        present = os.path.exists(path) and os.path.getsize(path) > 0
        size_mb = round(os.path.getsize(path) / 1e6, 1) if present else None
        landing_status.append(
            (service, month, file_name(service, month), present, size_mb)
        )

landing_schema = StructType([
    StructField("service", StringType()),
    StructField("month", IntegerType()),
    StructField("file", StringType()),
    StructField("present", BooleanType()),
    StructField("size_mb", DoubleType()),
])
display(spark.createDataFrame(landing_status, schema=landing_schema)
        .orderBy("service", "month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: fail if anything is missing

# COMMAND ----------

missing = [f"{svc}/{fn}" for svc, _m, fn, present, _sz in landing_status if not present]
assert not missing, (
    f"{len(missing)} file(s) missing from the landing Volume: {missing}\n"
    "Populate the landing zone first — see the README:\n"
    "  1) python src/download_landing_local.py\n"
    "  2) upload to /Volumes/ifood/bronze/landing (databricks fs cp --upload, or the UI)"
)
print(f"Landing zone validated: {len(landing_status)} files present in "
      f"{landing_volume_path()}")
