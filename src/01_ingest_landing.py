# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest → Landing Zone
# MAGIC
# MAGIC Gets the original NYC TLC Parquet files (yellow + green, Jan–May 2023) into the
# MAGIC Unity Catalog Volume `ifood.bronze.landing` **untouched**. This is our immutable
# MAGIC raw zone — every later layer is reproducible from these files.
# MAGIC
# MAGIC ## Two ways the landing zone gets populated
# MAGIC
# MAGIC 1. **Download from the notebook** (this notebook, `requests`) — works on any
# MAGIC    compute that has **outbound internet**.
# MAGIC 2. **Local download + upload** — **required on Databricks Free Edition**, whose
# MAGIC    serverless compute has **no internet egress** (a direct download fails with
# MAGIC    `Temporary failure in name resolution`). In that case populate the Volume from
# MAGIC    outside (see the README: PowerShell downloader + upload via Catalog Explorer or
# MAGIC    the Databricks CLI), then re-run this notebook.
# MAGIC
# MAGIC Either way, the **final cell validates the Volume contents** — that check, not the
# MAGIC download, is what gates the rest of the pipeline. So this notebook is safe to run
# MAGIC on Free Edition: the download attempt simply reports "no internet" and validation
# MAGIC confirms the manually-uploaded files are present.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
import requests
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best-effort download
# MAGIC
# MAGIC Idempotent (skips files already present). On a no-egress workspace every file
# MAGIC reports a connection error — that is expected and not fatal; see validation below.

# COMMAND ----------

def try_download_to_landing(service: str, month: int, *, overwrite: bool = False) -> dict:
    """Stream one TLC file into the landing Volume. Never raises — returns a status."""
    dest = landing_file_path(service, month)
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    if not overwrite and os.path.exists(dest) and os.path.getsize(dest) > 0:
        return {"service": service, "month": month, "status": "skipped (exists)",
                "size_mb": round(os.path.getsize(dest) / 1e6, 1)}

    try:
        with requests.get(file_url(service, month), stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB
                    f.write(chunk)
        return {"service": service, "month": month, "status": "downloaded",
                "size_mb": round(os.path.getsize(dest) / 1e6, 1)}
    except Exception as e:  # noqa: BLE001 - egress may be blocked; keep going, validate later
        kind = "no internet (populate Volume manually)" if "name resolution" in str(e) \
            else f"FAILED: {type(e).__name__}"
        return {"service": service, "month": month, "status": kind, "size_mb": None}

# COMMAND ----------

download_results = [
    try_download_to_landing(service, month)
    for service in SERVICES
    for month in MONTHS
]

download_schema = StructType([
    StructField("service", StringType()),
    StructField("month", IntegerType()),
    StructField("status", StringType()),
    StructField("size_mb", DoubleType()),
])
display(
    spark.createDataFrame(
        [(r["service"], r["month"], r["status"], r["size_mb"]) for r in download_results],
        schema=download_schema,
    ).orderBy("service", "month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the landing zone (authoritative gate)
# MAGIC
# MAGIC Reads what is actually in the Volume — independent of how it got there. The
# MAGIC pipeline only proceeds if every expected file is present and non-empty.

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

missing = [f"{svc}/{fn}" for svc, _m, fn, present, _sz in landing_status if not present]
assert not missing, (
    f"{len(missing)} file(s) missing from the landing Volume: {missing}\n"
    "Populate the landing zone (README → local download + upload to the Volume) and re-run."
)
print(f"Landing zone validated: {len(landing_status)} files present in "
      f"{landing_volume_path()}")
