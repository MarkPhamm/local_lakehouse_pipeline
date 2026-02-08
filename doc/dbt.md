# dbt + Iceberg Learning Guide

How dbt interacts with Iceberg via Trino.

## Project Structure

```text
dbt_trino/
├── dbt_project.yml           ← project config
├── profiles.yml              ← Trino connection (localhost:8085, catalog=iceberg)
├── macros/
│   └── generate_schema_name.sql  ← override so schemas are exact (not prefixed)
└── models/
    ├── sources.yml           ← points to iceberg.raw.yellow_trips
    ├── staging/
    │   └── stg_yellow_trips.sql    ← silver: clean + derived columns
    └── marts/
        ├── fct_daily_trips.sql     ← gold: daily aggregations
        └── fct_hourly_revenue.sql  ← gold: hourly revenue
```

## How to run

```bash
cd dbt_trino
uv run dbt debug --profiles-dir .     # test connection
uv run dbt run --profiles-dir .       # build all models
```

## Data flow

```text
iceberg.raw.yellow_trips (10,000 rows, loaded by Python script)
        │
        ▼ (clean, filter, add derived columns)
iceberg.silver.stg_yellow_trips (9,737 rows)
        │
        ├──▶ iceberg.gold.fct_daily_trips (aggregated by day)
        └──▶ iceberg.gold.fct_hourly_revenue (aggregated by hour)
```

## profiles.yml explained

```yaml
lakehouse_transfor:
  target: dev
  outputs:
    dev:
      type: trino
      method: none        # no auth — Trino has no password
      user: admin
      host: localhost
      port: 8085          # our remapped Trino port
      database: iceberg   # maps to the Trino CATALOG name
      schema: silver      # default schema for models
      threads: 1
```

- `database` in dbt-trino = Trino **catalog** name (not a database in the traditional sense)
- `schema` = default schema; individual models override it with `schema='gold'` etc.

## sources.yml explained

```yaml
sources:
  - name: raw
    database: iceberg     # Trino catalog
    schema: raw           # Iceberg namespace
    tables:
      - name: yellow_trips
```

This tells dbt: "when a model references `{{ source('raw', 'yellow_trips') }}`,
it means `iceberg.raw.yellow_trips` in Trino."

## Model config explained

Each model has a config block at the top:

```sql
{{ config(
    materialized='table',
    schema='silver',
    properties={
      "format": "'PARQUET'"
    }
) }}
```

- `materialized='table'` — dbt runs `CREATE TABLE AS SELECT` (not a view)
- `schema='silver'` — creates the table in `iceberg.silver` schema
- `properties={"format": "'PARQUET'"}` — tells Iceberg to store data as Parquet

## generate_schema_name macro

By default dbt-trino concatenates `<default_schema>_<custom_schema>`, so you get
`silver_silver` and `silver_gold` instead of just `silver` and `gold`.

The macro in `macros/generate_schema_name.sql` overrides this:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
```

This says: if the model specifies a custom schema, use it exactly as-is.

## What happens when dbt runs

For `materialized='table'`, dbt generates and runs:

```sql
CREATE TABLE iceberg.silver.stg_yellow_trips AS (
    SELECT ... FROM iceberg.raw.yellow_trips WHERE ...
)
```

That single `CREATE TABLE AS SELECT` (CTAS) is one atomic Iceberg operation.

### Tracing the Iceberg layers for one model

When `stg_yellow_trips` runs (9,737 rows):

**1. Data file (.parquet)**

Trino reads from `iceberg.raw.yellow_trips`, applies the WHERE filters, and writes
the result as a **single parquet file**:

```text
s3://warehouse/silver/stg_yellow_trips-<uuid>/data/00000-<uuid>.parquet
```

One file, 9,737 rows. Unlike our insert script that created 20 small parquet files
(one per batch), CTAS writes everything in one shot.

**2. Manifest file (.avro)**

A new manifest file that says: "this snapshot added one data file (the parquet above),
containing 9,737 rows, with column-level stats (min/max for each column)."

```text
s3://warehouse/silver/stg_yellow_trips-<uuid>/metadata/<uuid>-m0.avro
```

**3. Manifest list (.avro)**

A new manifest list pointing to the single manifest file:

```text
s3://warehouse/silver/stg_yellow_trips-<uuid>/metadata/snap-<snapshot_id>-<uuid>.avro
```

**4. Metadata file (.json)**

A new metadata JSON with the table schema, snapshot list (just 1 snapshot for a new
table), and pointer to the manifest list:

```text
s3://warehouse/silver/stg_yellow_trips-<uuid>/metadata/00000-<uuid>.metadata.json
```

**5. Catalog (REST → Postgres)**

The REST catalog registers `silver.stg_yellow_trips` in Postgres, pointing to that
metadata JSON.

### All 3 models — file layout in MinIO

```text
dbt run creates 3 new Iceberg tables:

silver/stg_yellow_trips-<uuid>/
├── metadata/
│   ├── 00000-xxx.metadata.json     ← metadata file (schema + 1 snapshot)
│   ├── snap-xxx.avro               ← manifest list (points to 1 manifest)
│   └── xxx-m0.avro                 ← manifest file (points to 1 data file)
└── data/
    └── 00000-xxx.parquet           ← 9,737 rows

gold/fct_daily_trips-<uuid>/
├── metadata/
│   ├── 00000-xxx.metadata.json
│   ├── snap-xxx.avro
│   └── xxx-m0.avro
└── data/
    └── 00000-xxx.parquet           ← 2 rows

gold/fct_hourly_revenue-<uuid>/
├── metadata/
│   ├── 00000-xxx.metadata.json
│   ├── snap-xxx.avro
│   └── xxx-m0.avro
└── data/
    └── 00000-xxx.parquet           ← 4 rows
```

### Key difference: insert script vs dbt CTAS

| | Insert script | dbt CTAS |
|---|---|---|
| **Parquet files** | 20 files (500 rows each) | 1 file (all rows) |
| **Snapshots** | 20 snapshots | 1 snapshot |
| **Why** | 20 separate INSERT statements | 1 CREATE TABLE AS SELECT |

Real pipelines prefer bulk operations — fewer snapshots, fewer small files.

### Verify in Trino

```sql
-- Only 1 snapshot for the silver table (from CTAS)
SELECT snapshot_id, operation, committed_at
FROM iceberg.silver."stg_yellow_trips$snapshots";

-- Compare to 21 snapshots for the raw table (from batch inserts)
SELECT COUNT(*) FROM iceberg.raw."yellow_trips$snapshots";
```
