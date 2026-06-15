# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuration
# MAGIC
# MAGIC Central place for **all** environment-specific values: catalog/schema/volume
# MAGIC names, the data scope (which services and months), the source URLs, and the
# MAGIC data-cleaning thresholds.
# MAGIC
# MAGIC Other notebooks pull these in with `%run ./00_config`. Keeping everything here
# MAGIC is what makes the project reproducible: a new user only changes values in this
# MAGIC one file (if they want different names) — nothing else, and **no secrets**.
# MAGIC
# MAGIC Running this notebook also **bootstraps** Unity Catalog (creates the catalog,
# MAGIC schemas, and landing Volume if they don't exist), so the rest of the pipeline
# MAGIC has somewhere to write.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Names (Unity Catalog)
# MAGIC
# MAGIC `catalog` → `schema` (one per medallion layer) → tables / volume.
# MAGIC If your workspace doesn't allow creating a new catalog, set `CATALOG` to an
# MAGIC existing one (e.g. `"workspace"`); everything else still works.

# COMMAND ----------

CATALOG = "ifood"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LANDING_VOLUME = "landing"  # UC Volume holding the raw downloaded files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scope & source
# MAGIC
# MAGIC NYC TLC publishes monthly Parquet files. We ingest **yellow + green** taxis for
# MAGIC **Jan–May 2023**. Green is included so we can answer Q2 ("all taxis of the fleet").
# MAGIC The `pickup_col` / `dropoff_col` differ by service (`tpep_*` vs `lpep_*`) and are
# MAGIC normalized to common names in the silver layer.

# COMMAND ----------

YEAR = 2023
MONTHS = [1, 2, 3, 4, 5]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

SERVICES = {
    "yellow": {
        "file_prefix": "yellow_tripdata",
        "pickup_col": "tpep_pickup_datetime",
        "dropoff_col": "tpep_dropoff_datetime",
    },
    "green": {
        "file_prefix": "green_tripdata",
        "pickup_col": "lpep_pickup_datetime",
        "dropoff_col": "lpep_dropoff_datetime",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning thresholds (applied in silver)
# MAGIC
# MAGIC Documented, auditable rules. See `docs/ARCHITECTURE.md` for the rationale.

# COMMAND ----------

MIN_TOTAL_AMOUNT = 0.0   # drop negative fares (refunds/errors) from the revenue average
MIN_PASSENGER_COUNT = 1  # drop NULL/0-passenger trips from the passenger average

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path / name helpers

# COMMAND ----------

def landing_volume_path() -> str:
    """Filesystem path of the landing Volume, e.g. /Volumes/ifood/bronze/landing."""
    return f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{LANDING_VOLUME}"


def file_name(service: str, month: int) -> str:
    """e.g. yellow_tripdata_2023-01.parquet"""
    return f"{SERVICES[service]['file_prefix']}_{YEAR}-{month:02d}.parquet"


def file_url(service: str, month: int) -> str:
    """Public TLC CloudFront URL for one service/month file."""
    return f"{BASE_URL}/{file_name(service, month)}"


def landing_file_path(service: str, month: int) -> str:
    """Destination path inside the landing Volume (one subfolder per service)."""
    return f"{landing_volume_path()}/{service}/{file_name(service, month)}"


def fqn(schema: str, table: str) -> str:
    """Fully-qualified table name, e.g. ifood.bronze.yellow_tripdata."""
    return f"{CATALOG}.{schema}.{table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap Unity Catalog
# MAGIC
# MAGIC Idempotent: safe to run repeatedly. Creates the catalog, the three medallion
# MAGIC schemas, and the landing Volume if they're missing.

# COMMAND ----------

def bootstrap_catalog() -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{LANDING_VOLUME}"
    )
    print(f"Bootstrapped catalog '{CATALOG}' with schemas "
          f"[{BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}] and volume "
          f"'{BRONZE_SCHEMA}.{LANDING_VOLUME}'.")


# Run bootstrap when this notebook is executed directly or via %run.
bootstrap_catalog()

# COMMAND ----------

# MAGIC %md
# MAGIC Config loaded. Downstream notebooks now have access to all names, helpers, and a
# MAGIC ready Unity Catalog.
