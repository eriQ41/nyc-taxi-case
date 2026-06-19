# Architecture & Technical Decisions

This document explains *what* we build, *why*, and the assumptions behind the analyses.
It exists so the reviewer can follow the reasoning, not just the code.

## 1. Platform

- **Databricks Free Edition** (the successor to the now-retired Community Edition,
  which was deprecated in 2025). Free Edition runs on serverless compute and ships
  with **Unity Catalog enabled by default** — so it is the right environment to
  exercise the modern Databricks stack.
- **Metadata technology: Unity Catalog.** The case leaves the metadata tech to our
  discretion. Because Free Edition provides a UC metastore automatically, we lean
  into it instead of fighting it: a single catalog with one schema per medallion
  layer, and a **UC Volume** for the raw landing zone (the modern replacement for
  DBFS paths).

> Naming note (2025 Databricks rebrand): *DLT → Lakeflow Declarative Pipelines*,
> *Workflows/Jobs → Lakeflow Jobs*. We orchestrate with plain notebooks/scripts so
> the solution is portable and reviewable, and note where Lakeflow Jobs would slot in.

## 2. Data source

NYC TLC Trip Record Data, published as monthly **Parquet** files (CSV was retired in 2022):

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-MM.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-MM.parquet
```

Scope: **Jan–May 2023** (`MM` = 01..05), for both **yellow** and **green** taxis.

## 3. Medallion architecture

```
  TLC CloudFront (.parquet)
            │  (download)
            ▼
  landing  ──►  UC Volume  ifood.bronze.landing   (immutable raw files)
            │  (PySpark read)
            ▼
  bronze   ──►  Delta tables, raw schema + ingest metadata
            │   ifood.bronze.yellow_tripdata / green_tripdata
            │  (PySpark: type, clean, dedupe, conform)
            ▼
  silver   ──►  Delta tables, typed & validated, unified schema
            │   ifood.silver.yellow_trips / green_trips
            │  (PySpark: curate consumption models)
            ▼
  gold     ──►  ifood.gold.yellow_trips_consumption   (mandated columns)
                ifood.gold.taxi_trips                  (yellow+green unified, for Q2)
```

- **Landing:** original files, untouched, in a Volume. Idempotent re-download.
- **Bronze:** essentially 1:1 with source, plus `_source_file` and `_ingested_at`. The
  only structural change is **conforming schema drift** across the monthly files: the
  TLC files give the same column different types (e.g. `VendorID` INT vs BIGINT,
  `passenger_count` BIGINT vs DOUBLE) and inconsistent casing (`Airport_fee` vs
  `airport_fee`), which breaks Parquet `mergeSchema`. Bronze reads each month
  individually, lower-cases column names, and **promotes numeric types to the widest**
  seen (values widened, never lost) so all months load into one Delta table. The
  case-mandated `VendorID` casing is restored in gold.
- **Silver:** correct types, validation/cleaning (below), a `service_type` column,
  and a normalized `pickup_datetime` / `dropoff_datetime` (green's `lpep_*` and yellow's
  `tpep_*` mapped to common names) so the two can be unioned.
- **Gold:**
  - `yellow_trips_consumption` — exposes the **case-mandated** columns
    `VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime`.
  - `taxi_trips` — yellow + green unified, used to answer Q2 ("all taxis").

**PySpark requirement** is satisfied in the bronze→silver→gold transforms.
**Consumption language:** SQL over the gold Delta tables.

## 4. Data cleaning decisions (silver)

These are the EDA-driven choices; each is defensible and documented:

- **Date sanity:** every monthly file contains a handful of records with timestamps
  outside the file's month (TLC artifact). We **derive the month from
  `pickup_datetime` and keep only 2023-01..2023-05**, so month-based aggregations are correct.
- **`passenger_count`:** drop `NULL` and `0` for Q2's average (a 0-passenger trip is a
  metering error and would bias a *per-passenger* mean). Kept elsewhere.
- **`total_amount`:** drop negative values (refunds / chargebacks recorded as negative)
  for the revenue average, since they don't represent fare received.
- **Dedupe:** drop exact duplicate rows defensively.

All thresholds are centralized so they're easy to audit and change.

## 5. Analyses

**Q1 — average monthly `total_amount`, yellow fleet.**
Interpreted as `AVG(total_amount)` grouped by pickup month (one figure per month, Jan–May),
plus the overall per-trip average for completeness. Source: `gold.yellow_trips_consumption`.

**Q2 — average `passenger_count` per hour of day, May, all taxis.**
`AVG(passenger_count)` grouped by `HOUR(pickup_datetime)` over May 2023, across
**yellow + green**. Source: `gold.taxi_trips`.

## 6. Assumptions

- "All taxis of the fleet" (Q2) = yellow + green; FHV/HVFHV are for-hire vehicles, not
  taxis, and HVFHV has no `passenger_count`.
- "Average total in a month" (Q1) = mean of `total_amount` per trip, grouped by month.
- Out-of-range and negative records are excluded as described above.
