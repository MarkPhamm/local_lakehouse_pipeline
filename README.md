# Local Lakehouse Pipeline

Building a local lakehouse with Dagster, MinIO, Iceberg, Trino, and dbt to learn Apache Iceberg from scratch.

## Architecture

```text
       Dagster (local, `dagster dev`)
      /            |               \
     v             v                v
 Download      Upload to       dbt-trino
 NYC taxi      MinIO            transforms
 parquet       (bronze)        (silver/gold)
                  \                /
                   v              v
                  Trino (Docker :8080)
                     |
              Iceberg REST Catalog (Docker :8181)
                     |
               ┌─────┴─────┐
           Postgres       MinIO
           (catalog       (data files
            metadata)      :9000/:9001)
```

**Data flow:** Download parquet → upload to MinIO → batch INSERT into Iceberg via Trino → dbt transforms silver/gold layers → snapshot report shows time travel

## Iceberg Architecture

Iceberg uses a layered metadata tree to track data:

```text
┌─────────────────────────────────────────────────────┐
│                    ICEBERG CATALOG                   │
│  (REST, Hive, JDBC, Nessie...)                      │
│  Points to → current metadata file for each table   │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │   METADATA FILE (.json) │
          │  • Table schema         │
          │  • Partition spec       │
          │  • Snapshots list       │
          │  • Current snapshot ID  │
          └────────────┬────────────┘
                       │ (each snapshot points to)
          ┌────────────▼────────────┐
          │  MANIFEST LIST (.avro)  │
          │  • List of manifests    │
          │  • Partition summaries  │
          │  • Added/deleted counts │
          └────────────┬────────────┘
                       │ (each entry points to)
          ┌────────────▼────────────┐
          │  MANIFEST FILE (.avro)  │
          │  • List of data files   │
          │  • Per-file stats       │
          │  • Column min/max       │
          │  • Row counts           │
          └────────────┬────────────┘
                       │ (each entry points to)
          ┌────────────▼────────────┐
          │  DATA FILES (.parquet)  │
          │  Your actual data       │
          └─────────────────────────┘
```

- **Catalog** — Entry point. Maps table names to the current metadata file location.
- **Metadata File** (JSON) — Table definition: schema, partition spec, snapshots list. Each write creates a new immutable metadata file (this is how time-travel works).
- **Manifest List** (Avro) — One per snapshot. Lists manifest files with summary stats so query engines can skip irrelevant manifests.
- **Manifest File** (Avro) — Tracks a subset of data files with per-file column-level stats (min, max, null count). This is where query planning magic happens.
- **Data Files** (Parquet) — The actual data in object storage (MinIO).

Every write is atomic: new data files → new manifests → new manifest list → new metadata file → atomic catalog pointer swap.

## Stack

| Service | Runs | Image/Tool |
|---------|------|------------|
| MinIO | Docker | `minio/minio` |
| Postgres | Docker | `postgres:16` (Iceberg catalog backing store) |
| Iceberg REST Catalog | Docker | `tabulario/iceberg-rest` |
| Trino | Docker | `trinodb/trino:latest` |
| Dagster | Local | `dagster dev` |
| dbt | Local | `dbt-trino` |

## Project Structure

```
local_lakehouse_pipeline/
├── docker-compose.yml
├── pyproject.toml
├── docker/
│   └── trino/
│       └── catalog/
│           └── iceberg.properties       # Trino → Iceberg REST + MinIO S3
├── lakehouse_pipeline/                  # Dagster project
│   ├── __init__.py
│   ├── definitions.py                   # Dagster Definitions entry point
│   ├── constants.py
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── iceberg_setup.py             # Create schemas + raw table
│   │   ├── ingestion.py                 # Download, upload, INSERT into Iceberg
│   │   ├── dbt_assets.py                # @dbt_assets wrapping dbt project
│   │   └── iceberg_snapshots.py         # Snapshot report + time travel demo
│   └── resources/
│       ├── __init__.py
│       ├── minio_resource.py            # boto3 S3 wrapper
│       └── trino_resource.py            # trino Python client wrapper
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml                     # Trino connection
    └── models/
        ├── sources.yml                  # Points to iceberg.raw.yellow_trips
        ├── staging/
        │   ├── _staging__models.yml
        │   └── stg_yellow_trips.sql     # Silver: clean + type-cast + derived cols
        └── marts/
            ├── _marts__models.yml
            ├── fct_daily_trips.sql       # Gold: daily aggregations
            ├── fct_hourly_revenue.sql    # Gold: hourly revenue
            └── fct_trip_distance_stats.sql # Gold: route statistics
```

## Implementation Steps

### Step 1: Docker Infrastructure

- `docker-compose.yml` with 5 services: MinIO, mc (init), Postgres, Iceberg REST catalog, Trino
- `docker/trino/catalog/iceberg.properties` with native S3 config
- Shared `lakehouse_net` Docker network

### Step 2: Python Project Setup

- `pyproject.toml` with deps: dagster, dagster-webserver, dagster-dbt, dbt-trino, boto3, trino, pyarrow, requests, pandas
- `lakehouse_pipeline/` package with resources (MinioResource, TrinoResource) and constants

### Step 3: Dagster Ingestion Assets

- `iceberg_schemas` — creates raw/silver/gold schemas + `iceberg.raw.yellow_trips` table
- `raw_taxi_parquet` — downloads monthly parquet from NYC TLC (monthly partitioned)
- `minio_taxi_parquet` — uploads to `s3://warehouse/raw_data/yellow_trips/`
- `iceberg_raw_yellow_trips` — reads parquet with PyArrow, batch INSERTs into Iceberg via Trino (each partition = new snapshot)

### Step 4: dbt Project

- `profiles.yml` connecting to Trino's `iceberg` catalog
- `stg_yellow_trips.sql` — silver layer: clean, type-cast, add derived columns
- 3 gold mart models: `fct_daily_trips`, `fct_hourly_revenue`, `fct_trip_distance_stats`

### Step 5: Dagster-dbt Integration

- `@dbt_assets` with custom `DagsterDbtTranslator` linking dbt sources → Dagster assets
- Full asset lineage visible in Dagster UI

### Step 6: Iceberg Snapshot Exploration

- `iceberg_snapshot_report` asset queries snapshot metadata tables
- Time travel comparison: `FOR VERSION AS OF <snapshot_id>`
- Shows row count growth across snapshots

## Usage

```bash
# Start infrastructure
docker compose up -d

# Install Python deps
uv venv && uv pip install -e .

# Start Dagster
dagster dev

# In Dagster UI (localhost:3000):
# 1. Materialize iceberg_schemas
# 2. Materialize Jan 2024 partition (ingestion assets) → first snapshot
# 3. Materialize Feb 2024 partition → second snapshot
# 4. Materialize dbt assets → silver + gold tables
# 5. Materialize snapshot report → see time travel in action

# Query Trino directly
docker exec -it trino trino
trino> SELECT * FROM iceberg.raw."yellow_trips$snapshots";
```
