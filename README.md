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
│  ├─ 01_ingest_landing.py      # Validate the landing Volume has all expected files
│  ├─ download_landing_local.py # LOCAL: download TLC parquet + upload to the Volume
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
2. Bring this repo in via **Workspace → Create → Git folder** (paste the Git URL;
   needs a GitHub PAT under Settings → Git integration), or upload the `src/` and
   `analysis/` files as notebooks.
3. Open a notebook and attach **serverless** compute via **Connect → Serverless**
   (top-right). Free Edition has no clusters to create — serverless is the only option.
4. Run `src/00_config` → **Run all**. It bootstraps Unity Catalog: catalog `ifood`,
   schemas `bronze/silver/gold`, and the `landing` Volume.
5. **Populate the landing zone.** Free Edition serverless has **no outbound internet**,
   so the files are downloaded on your machine and pushed to the Volume (raw data lives
   in the lake, not in git — `data/` is git-ignored):
   - **One command** (download + upload via the Databricks CLI; needs a one-time
     `databricks configure` with host + PAT):
     ```
     python src/download_landing_local.py --upload
     ```
   - **Or in two steps:** `python src/download_landing_local.py` then upload the
     `data/landing/{yellow,green}/` files via Catalog Explorer → `ifood` → `bronze` →
     Volumes → `landing` → Upload.
6. Run `src/01_ingest_landing` → **Run all**. It **validates** that all expected files
   are present in the Volume (the gate for the rest of the pipeline) — it does not
   download anything (serverless has no internet; population is the step above).
7. Run `src/02_bronze` → `03_silver` → `04_gold` in order.
8. Run the `analysis/` SQL/notebooks against the gold tables.

> Optional: chain the `src/` notebooks as a **Lakeflow Job** (formerly Workflows) for
> one-click, ordered execution.

## Results

Produced by the queries in [`analysis/`](analysis) over the gold tables.

### Q1 — Average monthly `total_amount` (yellow taxis, `total_amount >= 0`)

| Month   | Avg total_amount (USD) | Trips      |
|---------|-----------------------:|-----------:|
| 2023-01 |                  27.44 | 3,041,486 |
| 2023-02 |                  27.34 | 2,889,071 |
| 2023-03 |                  28.27 | 3,373,870 |
| 2023-04 |                  28.76 | 3,258,463 |
| 2023-05 |                  29.46 | 3,481,848 |

The average fare per trip rose steadily from **~$27.4 (Jan)** to **~$29.5 (May)**, about
**+7%** over five months.

### Q2 — Average `passenger_count` by hour of day (May 2023, all taxis = yellow + green, `passenger_count >= 1`)

Passengers-per-trip is **highest late night / early morning** (00–04h ≈ 1.40–1.45,
group & social trips) and **lowest during the morning commute** (05–08h ≈ 1.26–1.29,
solo commuters), then climbs back through the afternoon and evening. Full 24-hour curve:

| Hour | Avg | Hour | Avg | Hour | Avg | Hour | Avg |
|-----:|----:|-----:|----:|-----:|----:|-----:|----:|
| 00 | 1.426 | 06 | 1.261 | 12 | 1.374 | 18 | 1.381 |
| 01 | 1.435 | 07 | 1.281 | 13 | 1.383 | 19 | 1.390 |
| 02 | 1.453 | 08 | 1.293 | 14 | 1.388 | 20 | 1.399 |
| 03 | 1.448 | 09 | 1.311 | 15 | 1.399 | 21 | 1.418 |
| 04 | 1.403 | 10 | 1.346 | 16 | 1.396 | 22 | 1.426 |
| 05 | 1.284 | 11 | 1.361 | 17 | 1.387 | 23 | 1.421 |

> Green is ~2% of the fleet (339,620 vs 16,186,145 yellow trips), so the curve is
> dominated by yellow; see `analysis/exploratory_analysis.py` for the split.

## Tech & choices (short version)

- **Databricks Free Edition** — Community Edition was retired in 2025; Free Edition is
  its successor and includes Unity Catalog by default.
- **Unity Catalog + Volumes** as metadata/storage tech (the case leaves this to our choice).
- **PySpark** for ingestion and all transforms; **SQL** for consumption.
- **Delta Lake** for ACID, schema enforcement, time travel.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full rationale, cleaning
rules, and assumptions.
