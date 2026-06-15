# iFood Case — NYC Taxi Data Architecture

End-to-end ingestion, modeling, and analysis of NYC TLC taxi trip data on
**Databricks Free Edition** using **PySpark**, the **medallion architecture**
(bronze → silver → gold), **Unity Catalog**, and **Delta Lake**.

> Full reasoning and technical justifications: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## What this solves

The case asks to (1) ingest NYC taxi data for **Jan–May 2023** into a data lake,
(2) expose it for SQL consumption, and (3) answer two analytical questions.

| Question | Answer source |
|----------|---------------|
| Q1 — Average monthly `total_amount` across all **yellow** taxis | `analysis/q1_avg_total_amount_monthly.sql` |
| Q2 — Average `passenger_count` per hour of day in **May**, all taxis (yellow + green) | `analysis/q2_avg_passengers_by_hour_may.sql` |

## Repository structure

```
.
├─ src/                         # Pipeline (PySpark)
│  ├─ 00_config.py              # Catalog/schema/volume names, scope, cleaning thresholds
│  ├─ 01_ingest_landing.py      # Download TLC parquet → UC Volume (landing)
│  ├─ 02_bronze.py              # Landing → bronze Delta (raw + ingest metadata)
│  ├─ 03_silver.py              # Bronze → silver (typed, cleaned, unified)
│  └─ 04_gold.py                # Silver → gold (consumption tables)
├─ analysis/                    # Answers to the two questions (SQL + notebook)
│  ├─ q1_avg_total_amount_monthly.sql
│  ├─ q2_avg_passengers_by_hour_may.sql
│  └─ exploratory_analysis.py   # EDA notebook
├─ docs/
│  └─ ARCHITECTURE.md           # Decisions, data model, assumptions
├─ README.md
└─ requirements.txt
```

## Architecture at a glance

```
TLC CloudFront (.parquet)
   → landing (UC Volume)  → bronze (Delta)  → silver (typed/clean)  → gold (consumption)
                                                                          → SQL analyses
```

Data model: catalog `ifood`, schemas `bronze` / `silver` / `gold`, raw files in the
`ifood.bronze.landing` Volume. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## How to run (Databricks Free Edition)

1. Sign up / log in at the Databricks Free Edition workspace.
2. Import this repo via **Repos → Add Repo** (Git URL) or upload the `src/` and
   `analysis/` files as notebooks.
3. Attach to **serverless** compute.
4. Run the `src/` notebooks **in order**:
   1. `00_config` — defines names and scope (imported by the others)
   2. `01_ingest_landing` — downloads Jan–May 2023 yellow + green parquet to the Volume
   3. `02_bronze` → `03_silver` → `04_gold`
5. Run the `analysis/` SQL/notebooks against the gold tables.

> Optional: chain the `src/` notebooks as a **Lakeflow Job** (formerly Workflows) for
> one-click, ordered execution.

## Results

> Filled in after running the pipeline.

- **Q1 — Average monthly `total_amount` (yellow):** _TBD_
- **Q2 — Average `passenger_count` by hour (May, yellow + green):** _TBD_

## Tech & choices (short version)

- **Databricks Free Edition** — Community Edition was retired in 2025; Free Edition is
  its successor and includes Unity Catalog by default.
- **Unity Catalog + Volumes** as metadata/storage tech (the case leaves this to our choice).
- **PySpark** for ingestion and all transforms; **SQL** for consumption.
- **Delta Lake** for ACID, schema enforcement, time travel.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full rationale, cleaning
rules, and assumptions.
