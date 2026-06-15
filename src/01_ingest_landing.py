# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest → Landing Zone
# MAGIC
# MAGIC Downloads the original NYC TLC Parquet files (yellow + green, Jan–May 2023) from
# MAGIC the public CloudFront endpoint and stores them **untouched** in the Unity Catalog
# MAGIC Volume `ifood.bronze.landing`. This is our immutable raw zone — every later layer
# MAGIC is reproducible from these files.
# MAGIC
# MAGIC The download is **idempotent**: a file that already exists with a non-zero size is
# MAGIC skipped, so re-running is cheap and safe.
# MAGIC
# MAGIC > **Why download with Python instead of `spark.read(url)`?** Spark cannot read an
# MAGIC > HTTP URL directly. We stream the bytes into the Volume with `requests`, then read
# MAGIC > the local Parquet from the Volume in the bronze step.
# MAGIC >
# MAGIC > **If outbound internet is blocked** in your workspace, this step will fail to
# MAGIC > connect. Fallback: manually upload the same files into
# MAGIC > `/Volumes/ifood/bronze/landing/<service>/` (Catalog Explorer → the Volume →
# MAGIC > Upload), then continue from `02_bronze`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import os
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download helper

# COMMAND ----------

def download_to_landing(service: str, month: int, *, overwrite: bool = False) -> dict:
    """Stream one TLC file into the landing Volume. Returns a status dict."""
    url = file_url(service, month)
    dest = landing_file_path(service, month)
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    if not overwrite and os.path.exists(dest) and os.path.getsize(dest) > 0:
        return {"service": service, "month": month, "status": "skipped (exists)",
                "size_mb": round(os.path.getsize(dest) / 1e6, 1), "path": dest}

    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB
                f.write(chunk)

    return {"service": service, "month": month, "status": "downloaded",
            "size_mb": round(os.path.getsize(dest) / 1e6, 1), "path": dest}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run ingestion for every service × month in scope

# COMMAND ----------

results = []
for service in SERVICES:
    for month in MONTHS:
        try:
            results.append(download_to_landing(service, month))
        except Exception as e:  # noqa: BLE001 - surface per-file failures, keep going
            results.append({"service": service, "month": month,
                            "status": f"FAILED: {e}", "size_mb": None,
                            "path": landing_file_path(service, month)})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC One row per file: whether it was downloaded/skipped/failed and its size.

# COMMAND ----------

summary_df = spark.createDataFrame(results)
display(summary_df.orderBy("service", "month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the landing zone contents

# COMMAND ----------

for service in SERVICES:
    print(f"--- {service} ---")
    display(dbutils.fs.ls(f"{landing_volume_path()}/{service}"))

# COMMAND ----------

# Fail loudly if anything didn't land, so we don't silently proceed with gaps.
failures = [r for r in results if str(r["status"]).startswith("FAILED")]
assert not failures, f"{len(failures)} file(s) failed to download: {failures}"
print(f"Landing zone ready: {len(results) - len(failures)} files in "
      f"{landing_volume_path()}")
