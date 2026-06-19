# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Analysis
# MAGIC
# MAGIC Context and justification for the two answers. We look at the data quality issues
# MAGIC that drive the analysis filters, then compute Q1 and Q2 in **PySpark** (a
# MAGIC cross-check of the SQL in `q1_*.sql` / `q2_*.sql`).

# COMMAND ----------

# MAGIC %run ../src/00_config

# COMMAND ----------

from pyspark.sql import functions as F

yellow = spark.table(fqn(GOLD_SCHEMA, "yellow_trips_consumption"))
taxi = spark.table(fqn(GOLD_SCHEMA, "taxi_trips"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. How big is each layer?

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'bronze.yellow' AS tbl, count(*) AS rows FROM {fqn(BRONZE_SCHEMA, 'yellow_tripdata')}
    UNION ALL SELECT 'bronze.green', count(*) FROM {fqn(BRONZE_SCHEMA, 'green_tripdata')}
    UNION ALL SELECT 'silver.yellow', count(*) FROM {fqn(SILVER_SCHEMA, 'yellow_trips')}
    UNION ALL SELECT 'silver.green', count(*) FROM {fqn(SILVER_SCHEMA, 'green_trips')}
    UNION ALL SELECT 'gold.taxi_trips', count(*) FROM {fqn(GOLD_SCHEMA, 'taxi_trips')}
    ORDER BY tbl
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Why the `total_amount >= 0` filter (Q1)
# MAGIC
# MAGIC TLC data carries negative totals (refunds / adjustments) and some zeros. They are a
# MAGIC small share but would drag a revenue *average* down, so Q1 excludes negatives.

# COMMAND ----------

display(yellow.select(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("negative"),
    F.sum(F.when(F.col("total_amount") == 0, 1).otherwise(0)).alias("zero"),
    F.round(F.min("total_amount"), 2).alias("min"),
    F.round(F.max("total_amount"), 2).alias("max"),
    F.round(F.avg("total_amount"), 2).alias("avg_unfiltered"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Why the `passenger_count >= 1` filter (Q2)
# MAGIC
# MAGIC Some trips report 0 or NULL passengers (metering errors). A *per-passenger* mean
# MAGIC should ignore them.

# COMMAND ----------

display(taxi.filter((F.year("pickup_datetime") == 2023) & (F.month("pickup_datetime") == 5))
        .select(
            F.count("*").alias("may_rows"),
            F.sum(F.when(F.col("passenger_count").isNull(), 1).otherwise(0)).alias("passenger_null"),
            F.sum(F.when(F.col("passenger_count") == 0, 1).otherwise(0)).alias("passenger_zero"),
        ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — average monthly `total_amount` (yellow)
# MAGIC
# MAGIC Cross-check of `q1_avg_total_amount_monthly.sql`. Bar chart by month.

# COMMAND ----------

q1 = (
    yellow.filter(F.col("total_amount") >= 0)
    .groupBy(F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("month"))
    .agg(F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
         F.count("*").alias("trips"))
    .orderBy("month")
)
display(q1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — average `passenger_count` by hour, May, all taxis
# MAGIC
# MAGIC Cross-check of `q2_avg_passengers_by_hour_may.sql`. Line chart over the 24 hours.

# COMMAND ----------

q2 = (
    taxi
    .filter((F.year("pickup_datetime") == 2023) & (F.month("pickup_datetime") == 5)
            & (F.col("passenger_count") >= 1))
    .groupBy(F.hour("pickup_datetime").alias("hour_of_day"))
    .agg(F.round(F.avg("passenger_count"), 3).alias("avg_passengers"),
         F.count("*").alias("trips"))
    .orderBy("hour_of_day")
)
display(q2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Yellow vs green contribution to Q2
# MAGIC
# MAGIC Green is a tiny fraction of the fleet — useful to know how much it moves the mean.

# COMMAND ----------

display(
    taxi
    .filter((F.year("pickup_datetime") == 2023) & (F.month("pickup_datetime") == 5)
            & (F.col("passenger_count") >= 1))
    .groupBy("service_type")
    .agg(F.round(F.avg("passenger_count"), 3).alias("avg_passengers"),
         F.count("*").alias("trips"))
    .orderBy("service_type")
)
