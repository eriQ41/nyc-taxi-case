# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold (consumption layer)
# MAGIC
# MAGIC The layer end users query with SQL. Two tables:
# MAGIC
# MAGIC - **`ifood.gold.yellow_trips_consumption`** — the **case-mandated** model. Exposes
# MAGIC   exactly the required columns, with their **original names/casing**:
# MAGIC   `VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime`.
# MAGIC   (Silver lower-cased and renamed them; here we restore the contract the case asks for.)
# MAGIC - **`ifood.gold.taxi_trips`** — yellow **+** green unified, used for Q2 ("all taxis
# MAGIC   of the fleet"). Carries `service_type` plus the common trip columns.
# MAGIC
# MAGIC Both are Delta tables registered in Unity Catalog, so they're immediately queryable
# MAGIC from the SQL editor or any notebook.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Yellow consumption table (mandated columns)

# COMMAND ----------

yellow_consumption = fqn(GOLD_SCHEMA, "yellow_trips_consumption")

(
    spark.table(fqn(SILVER_SCHEMA, "yellow_trips"))
    .select(
        F.col("vendorid").alias("VendorID"),
        F.col("passenger_count"),
        F.col("total_amount"),
        F.col("pickup_datetime").alias("tpep_pickup_datetime"),
        F.col("dropoff_datetime").alias("tpep_dropoff_datetime"),
    )
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(yellow_consumption)
)

print(f"{yellow_consumption}: {spark.table(yellow_consumption).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified taxi table (yellow + green) for "all taxis"

# COMMAND ----------

taxi_trips = fqn(GOLD_SCHEMA, "taxi_trips")

silver_dfs = [spark.table(fqn(SILVER_SCHEMA, f"{service}_trips")) for service in SERVICES]
unified: DataFrame = reduce(lambda a, b: a.unionByName(b), silver_dfs)

(
    unified.select(
        "service_type",
        F.col("vendorid").alias("VendorID"),
        "passenger_count",
        "pickup_datetime",
        "dropoff_datetime",
        "total_amount",
    )
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(taxi_trips)
)

print(f"{taxi_trips}: {spark.table(taxi_trips).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

print("=== yellow_trips_consumption schema ===")
spark.table(yellow_consumption).printSchema()
display(spark.table(yellow_consumption).limit(5))

# COMMAND ----------

print("=== taxi_trips: row split by service ===")
display(spark.table(taxi_trips).groupBy("service_type").count().orderBy("service_type"))
