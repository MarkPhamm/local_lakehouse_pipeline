# Dagster Learning Guide

How Dagster orchestrates the lakehouse pipeline.

## What is Dagster?

Dagster is an **orchestrator** — it doesn't process data itself, it tells other
tools (Trino, dbt, Python scripts) **when** and **in what order** to run.

## Project Structure

```text
lakehouse_pipeline/
├── __init__.py
├── definitions.py              ← entry point: registers assets, jobs, resources
├── assets/
│   ├── ingestion.py            ← 3 assets: schemas, download file, insert into Iceberg
│   └── dbt_assets.py           ← wraps dbt models as Dagster assets
└── resources/
    └── trino_resource.py       ← Trino connection wrapper
```

## Key Concepts

### Assets

An **asset** is a piece of data that your pipeline produces. Each asset is a
Python function decorated with `@asset`. Dagster tracks dependencies between
assets and runs them in the right order.

Our assets:

```text
iceberg_schemas          ← creates schemas + raw table (one-time setup)
       │
raw_taxi_file            ← downloads NYC taxi parquet to local disk
       │
iceberg_raw_yellow_trips ← reads parquet, batch-inserts into Iceberg via Trino
       │
stg_yellow_trips         ← dbt: cleans raw data (silver layer)
       │
  ┌────┴────┐
  │         │
fct_daily   fct_hourly   ← dbt: aggregates (gold layer)
_trips      _revenue
```

### Resources

A **resource** is a shared connection or client that assets can use. We have:

- `trino_resource` — wraps the `trino` Python client to run SQL
- `dbt` — wraps `dbt-trino` CLI to run dbt models

### Jobs

A **job** is a named selection of assets to materialize together. We have:

- `lakehouse_full_pipeline` — materializes everything in dependency order

## How to Run

### Start Dagster

```bash
cd ~/personal/project/local_lakehouse_pipeline
uv run dagster dev -p 3001
```

Open `http://localhost:3001` in your browser.

### Materialize Everything (Full Pipeline)

**Option 1: Via the Jobs page**

1. Click **Jobs** in the sidebar
2. Click `lakehouse_full_pipeline`
3. Click **Launch Run**

This runs everything in order: schemas → download → insert → dbt silver → dbt gold.

**Option 2: Via the Assets page**

1. Click **Assets** in the top nav
2. Click **Materialize all** (top right)

### Materialize a Single Asset

1. Click **Assets** in the top nav
2. Click on the asset you want (e.g., `raw_taxi_file`)
3. Click **Materialize** on that asset's page

### Materialize Only dbt Models

**All dbt models:**

1. Click **Assets** in the top nav
2. Select the 3 dbt assets (`stg_yellow_trips`, `fct_daily_trips`, `fct_hourly_revenue`)
3. Click **Materialize selected**

**A single dbt model (e.g., only `stg_yellow_trips`):**

1. Click **Assets** in the top nav
2. Click on `stg_yellow_trips`
3. Click **Materialize**

This will only rebuild the silver staging table without touching gold.

**Note:** If you materialize a gold model (`fct_daily_trips`) without materializing
its upstream (`stg_yellow_trips`) first, Dagster will warn you that upstream data
may be stale, but it will still run — it uses whatever data is already in the
silver table.

### Run from the CLI (without UI)

```bash
# Materialize everything
uv run dagster asset materialize --select '*' -m lakehouse_pipeline.definitions

# Materialize a single asset
uv run dagster asset materialize --select 'raw_taxi_file' -m lakehouse_pipeline.definitions

# Materialize dbt assets only
uv run dagster asset materialize --select 'stg_yellow_trips fct_daily_trips fct_hourly_revenue' -m lakehouse_pipeline.definitions
```

## What Happens When You Launch a Run

1. Dagster reads the asset dependency graph
2. It topologically sorts the assets (so upstream runs before downstream)
3. For each asset, it calls the Python function
4. Each function's return value and metadata are logged in the Dagster UI
5. If any asset fails, downstream assets are skipped

## Checking Results

After a run completes:

- Click **Runs** in the sidebar to see run history
- Click a run to see logs, timing, and metadata per asset
- Click an individual asset to see its metadata (e.g., `rows_inserted: 10000`)
- Check Trino to verify data: `SELECT COUNT(*) FROM iceberg.raw.yellow_trips;`
