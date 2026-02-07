# Iceberg Learning Guide

Step-by-step commands to interact with Iceberg via Trino.

## How to connect to Trino

```bash
docker exec -it trino trino
```

This drops you into an interactive SQL shell. Trino is the query engine — it doesn't
store data itself, it talks to **catalogs** (like Iceberg) to read/write data.

## Step 1: Create Schemas

A schema (aka namespace) is a logical grouping of tables inside a catalog.

We create 3 schemas following the **medallion architecture**:

- `raw` — data as-is from the source (no transformation)
- `silver` — cleaned, typed, filtered
- `gold` — aggregated, business-ready metrics

### Commands

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
```

### What does `iceberg.raw` mean?

```
iceberg.raw
───┬─── ─┬─
   │     └── schema/namespace name (grouping of tables)
   └── Trino catalog name (defined by iceberg.properties)
```

- `iceberg` = the Trino catalog. Configured in `docker/trino/catalog/iceberg.properties`.
  It tells Trino: "use the Iceberg REST catalog at http://rest:8181, store files in MinIO at s3://warehouse/"
- `raw` = the schema/namespace inside that catalog

### What happens under the hood?

1. Trino sends a REST request to the Iceberg REST catalog: `POST /v1/namespaces` with body `{"namespace": ["raw"]}`
2. The REST catalog writes a row to the **Postgres** database recording that namespace `raw` exists
3. When tables are later created in this schema, their data files go to `s3://warehouse/raw/<table_name>/`

### Verify schemas were created

```sql
SHOW SCHEMAS FROM iceberg;
```

You should see `raw`, `silver`, `gold` (plus a default `information_schema`).

## Step 2: Create the Raw Yellow Trips Table

Now we create the Iceberg table that will hold NYC yellow taxi trip data.

### Command

```sql
CREATE TABLE iceberg.raw.yellow_trips (
    VendorID               INTEGER,
    tpep_pickup_datetime   TIMESTAMP(6),
    tpep_dropoff_datetime  TIMESTAMP(6),
    passenger_count        DOUBLE,
    trip_distance          DOUBLE,
    RatecodeID             DOUBLE,
    store_and_fwd_flag     VARCHAR,
    PULocationID           INTEGER,
    DOLocationID           INTEGER,
    payment_type           INTEGER,
    fare_amount            DOUBLE,
    extra                  DOUBLE,
    mta_tax                DOUBLE,
    tip_amount             DOUBLE,
    tolls_amount           DOUBLE,
    improvement_surcharge  DOUBLE,
    total_amount           DOUBLE,
    congestion_surcharge   DOUBLE,
    airport_fee            DOUBLE
) WITH (format = 'PARQUET');
```

### Breaking it down

- **`iceberg.raw.yellow_trips`** = `catalog.schema.table`
- **`WITH (format = 'PARQUET')`** — tells Iceberg to store data files as Parquet
  (columnar format, great for analytics). Other options: ORC, AVRO
- **`TIMESTAMP(6)`** — microsecond precision, matches the source parquet files
- Some columns are `DOUBLE` instead of `INTEGER` (like `passenger_count`) because
  the source data has nulls and NYC TLC stored them as doubles

### What happens under the hood?

1. **Trino → REST Catalog**: `POST /v1/namespaces/raw/tables` with the table schema
2. **REST Catalog → Postgres**: records that table `raw.yellow_trips` exists
3. **REST Catalog → MinIO**: creates the first **metadata file** (JSON) at
   `s3://warehouse/raw/yellow_trips/metadata/v1.metadata.json`
   This file contains: table schema, partition spec, an empty snapshot list
4. No data files yet — the table is empty

### What files appear in MinIO?

After CREATE TABLE, check MinIO at `http://localhost:9001` (admin/password).
Browse to: `warehouse / raw / yellow_trips-<uuid> / metadata /`

You'll see two files:

**`00000-<uuid>.metadata.json`** (~3.9 KiB) — **The Metadata File**

This is the top of Iceberg's metadata tree. Contains:

- Table schema (all 19 columns and their types)
- Partition spec (none — we didn't partition the table)
- List of snapshots (one snapshot: the empty table creation)
- The current snapshot ID
- Table properties (format = PARQUET, etc.)

Every time you modify the table (INSERT, DELETE, ALTER), Iceberg writes a **new**
metadata file (`00001-...`, `00002-...`, etc.). The old ones stay on disk — that's
how time travel works. The catalog pointer simply moves to the latest one.

**`snap-<snapshot_id>-<uuid>.avro`** (~4.2 KiB) — **The Manifest List**

This is the snapshot's manifest list (Avro format). Even though the table is empty,
Iceberg created a snapshot for the CREATE TABLE operation. This file lists which
manifest files belong to this snapshot (currently none — no data files to track).

Mapping to the Iceberg architecture:

```text
Catalog (REST + Postgres)
  → points to: 00000-...metadata.json        ← the metadata file
    → snapshot points to: snap-...avro        ← the manifest list
      → (no manifest files yet)
        → (no data files yet — table is empty)
```

After we INSERT rows, new files will appear:

- A **new metadata file** (`00001-...metadata.json`) with an updated snapshot list
- A **new manifest list** (`snap-...avro`) for the new snapshot
- A **manifest file** (`.avro`) tracking which data files were added
- **Data files** (`.parquet`) containing the actual rows

### Verify

```sql
SHOW TABLES FROM iceberg.raw;
-- Should show: yellow_trips

DESCRIBE iceberg.raw.yellow_trips;
-- Shows all columns and their types

SELECT COUNT(*) FROM iceberg.raw.yellow_trips;
-- Should be 0 — no data yet
```

## Step 3: Insert Data into Iceberg

We use a Python script (`scripts/insert_taxi_data.py`) that:

1. Reads the NYC taxi parquet file with PyArrow
2. Samples 10,000 rows
3. Batch-inserts 500 rows at a time via the `trino` Python client

### Run it

```bash
uv run python3 scripts/insert_taxi_data.py
```

### What happens on each INSERT statement?

Every `INSERT INTO yellow_trips VALUES (500 rows)` triggers this chain:

```text
1. Write 500 rows  → new .parquet file in MinIO       (the actual data)
2. Write manifest  → new .avro file                    (tracks which .parquet was added)
3. Write manifest list → new .avro file                (lists all manifests for this snapshot)
4. Update metadata → .json file                        (adds new snapshot to the list)
5. Update catalog  → Postgres row points to latest .json
```

Old files are **never modified or deleted**. Iceberg is append-only at the metadata
level. This is what makes time travel possible.

### Each INSERT = 1 snapshot = 1 new parquet file

Our script runs 20 INSERT statements (10,000 rows / 500 per batch).
Each one creates:

- 1 new `.parquet` data file (containing 500 rows)
- 1 new snapshot

So after running the script you have **20 parquet data files** and **21 snapshots**
(including the initial empty CREATE TABLE snapshot).

In a real pipeline, you'd want to insert all rows in a **single INSERT** to get
one clean snapshot instead of 20.

### How to see snapshots (versions)

```sql
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.raw."yellow_trips$snapshots"
ORDER BY committed_at;
```

- `snapshot_id` — unique ID for this version
- `parent_id` — which snapshot came before (forms a linked list)
- `operation` — what happened (`append` for INSERT)
- `committed_at` — when it was committed

The snapshots form a chain:

```text
CREATE TABLE → snapshot 2850... (parent: none)
  └─ INSERT 500  → snapshot 1240... (parent: 2850...)
       └─ INSERT 500  → snapshot 1853... (parent: 1240...)
            └─ INSERT 500  → snapshot 6628... (parent: 1853...)
                 └─ ...and so on
```

The catalog always points to the **latest** snapshot. When you query with
`FOR VERSION AS OF <id>`, Trino reads an older snapshot instead.

### Time travel

```sql
-- Go back to the first snapshot (CREATE TABLE) — 0 rows
SELECT COUNT(*) FROM iceberg.raw.yellow_trips
  FOR VERSION AS OF 2850245846613192555;

-- After first batch — 500 rows
SELECT COUNT(*) FROM iceberg.raw.yellow_trips
  FOR VERSION AS OF 1240604982178618395;

-- Current (latest snapshot) — 10,000 rows
SELECT COUNT(*) FROM iceberg.raw.yellow_trips;
```

### Why are there only ~3 metadata JSON files, not 21?

You'd expect 21 metadata files (one per snapshot), but Iceberg has an optimization.

When doing rapid writes, the engine doesn't always write a brand new metadata JSON
for every commit. It can **append the new snapshot to an existing metadata file** and
do an atomic update. So instead of 21 files, you get ~3:

1. First `.metadata.json` — from CREATE TABLE (1 snapshot)
2. An intermediate one — after some inserts
3. The latest one — contains the **full list of all 21 snapshots**

Only the **latest** metadata JSON has the complete picture. The older ones are kept
for safety, but the catalog pointer has moved to the latest.

You can verify this by downloading the latest `.metadata.json` from MinIO and
opening it. Inside you'll see all snapshots listed:

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "current-snapshot-id": 3840134531142459162,
  "snapshots": [
    {"snapshot-id": 2850245846613192555, "operation": "append", ...},
    {"snapshot-id": 1240604982178618395, "operation": "append", ...},
    ...all 21 snapshots listed here
  ]
}
```
