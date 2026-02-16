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

## Dagster vs Airflow

### The Core Difference

**Airflow** thinks in **tasks** — "run this, then run that."
**Dagster** thinks in **assets** — "this data should exist, here's how to produce it."

In Airflow you define a DAG of operations. In Dagster you define data assets and
their dependencies — the execution order is inferred automatically.

### Hello World: Side by Side

**Airflow — a task that prints hello:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    print("Hello, world!")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
```

You define a DAG, add tasks to it, and set a schedule. The focus is on the
*operation* (printing hello). To chain tasks:

```python
download_task = PythonOperator(task_id="download", python_callable=download_fn)
transform_task = PythonOperator(task_id="transform", python_callable=transform_fn)
load_task = PythonOperator(task_id="load", python_callable=load_fn)

download_task >> transform_task >> load_task
```

Dependencies are explicit arrows between tasks. Data passing between tasks uses
XCom (a sidecar key-value store), which is clunky for large datasets.

**Dagster — an asset that produces data:**

```python
from dagster import asset


@asset
def hello_world():
    return "Hello, world!"
```

That's it. The function *is* the asset. To chain assets:

```python
@asset
def raw_data():
    return download()


@asset
def clean_data(raw_data):
    return transform(raw_data)


@asset
def report(clean_data):
    return aggregate(clean_data)
```

Dependencies are inferred from function parameters — `clean_data` takes `raw_data`
as input, so Dagster knows to run `raw_data` first. No explicit wiring needed.

### Feature Comparison

| | **Dagster** | **Airflow** |
|---|---|---|
| **Core unit** | Asset (data output) | Task (operation) |
| **Dependencies** | Inferred from function params | Explicit `>>` operator |
| **Data passing** | Native — assets return values | XCom (key-value, awkward for large data) |
| **Data lineage** | Built-in — UI shows asset graph | Manual — must build yourself |
| **Local dev** | `dagster dev` — instant hot-reload UI | Need scheduler + webserver + DB |
| **Testing** | Assets are plain functions — easy to unit test | Tasks coupled to DAG context — harder |
| **Resources** | Typed, swappable (swap real DB for mock in tests) | Connections/hooks, less structured |
| **Backfills** | Native — "rematerialize for these partitions" | Possible but clunky |
| **Scheduling** | Schedules, sensors, or manual | Primarily cron-based DAGs |
| **dbt integration** | First-class (`dagster-dbt`) | Via `BashOperator` or `cosmos` |

### When to Use Which

**Dagster** is better for:
- Data pipelines and analytics engineering
- dbt-centric workflows
- When you care about *what data exists* and its lineage
- Teams that want easy local development and testing

**Airflow** is better for:
- General-purpose orchestration (triggering APIs, sending emails, coordinating services)
- Organizations with existing Airflow infrastructure
- When you need a massive ecosystem of pre-built operators (AWS, GCP, Slack, etc.)
