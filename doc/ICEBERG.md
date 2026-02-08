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

## Deep Dive: Iceberg Metadata Layers Explained

Iceberg has 5 layers, top to bottom. Each layer serves a specific purpose.

### Layer 0: Catalog (REST + Postgres)

The **entry point** for everything. The only mutable layer — everything below it
is immutable files in S3.

It answers one question: **"Given a table name, where is the current metadata file?"**

In our setup:

- **REST catalog** (`tabulario/iceberg-rest` on port 8181) — the API layer
- **Postgres** — the backing store that holds the actual pointer

When Trino asks for `iceberg.raw.yellow_trips`:

```text
Trino: "Hey REST catalog, where is raw.yellow_trips?"
REST catalog: *queries Postgres*
Postgres: "Current metadata is at s3://warehouse/raw/yellow_trips-xxx/metadata/00002-xxx.metadata.json"
REST catalog: *returns that location to Trino*
Trino: *reads the JSON file from MinIO and follows the chain down*
```

This is why the catalog is critical. There might be 3 metadata JSON files in MinIO
(from different writes), but only the catalog knows **which one is current**. When a
new write happens, the catalog atomically updates its pointer to the new metadata file.

Without a catalog, nobody knows which metadata file to read. The catalog is the
single source of truth for "what is the latest version of this table?"

### Layer 1: Metadata File (.json)

The **table's master record**. One current file per table (old versions stay for time travel).

Contains:

- Table schema (column names, types)
- Partition spec (how the table is partitioned, if at all)
- A **list of all snapshots** (every version of the table)
- Which snapshot is **current**

```json
{
  "format-version": 2,
  "table-uuid": "abc-123",
  "current-snapshot-id": 999,
  "schemas": [{"fields": [{"name": "vendor_id", "type": "int"}, ...]}],
  "snapshots": [
    {"snapshot-id": 111, "manifest-list": "s3://warehouse/.../snap-111.avro"},
    {"snapshot-id": 999, "manifest-list": "s3://warehouse/.../snap-999.avro"}
  ]
}
```

**Analogy:** The table of contents of a book. It tells you every chapter (snapshot)
that exists and where to find each one.

### Layer 2: Manifest List (.avro)

One per snapshot. Lists **which manifest files** belong to that snapshot.

For a simple table with one insert, it points to 1 manifest. For a table with
years of daily inserts, it might point to hundreds.

Also stores **summary stats** per manifest (partition ranges, record counts), so
the query engine can skip entire manifests without opening them.

```text
snap-999.avro contains:
  - manifest-1.avro  (has data for Jan 2024, 5000 rows)
  - manifest-2.avro  (has data for Feb 2024, 3000 rows)
```

**Analogy:** A chapter's table of contents. If you're looking for March data,
you can skip manifest-1 and manifest-2 without reading them.

### Layer 3: Manifest File (.avro)

Tracks a group of **actual data files**. For each data file it stores:

- File path in S3 (`s3://warehouse/.../00000.parquet`)
- Row count
- File size
- **Column-level stats**: min value, max value, null count per column

```text
manifest-1.avro contains:
  - 00000.parquet  (500 rows, trip_distance min=0.5, max=25.3)
  - 00001.parquet  (500 rows, trip_distance min=0.1, max=18.7)
```

**Analogy:** The index at the back of a book. If you're looking for trips > 30 miles,
you see max is 25.3 — you **skip this entire file** without reading it.

This is Iceberg's secret weapon for performance — **file pruning** based on column stats.

### Layer 4: Data File (.parquet)

The actual rows. A standard Parquet file sitting in MinIO. Nothing Iceberg-specific
about the file itself — it's just Parquet.

### How a query uses all 4 layers

When you run `SELECT * FROM stg_yellow_trips WHERE trip_distance > 20`:

```text
Step 1: Catalog (Postgres)
        "stg_yellow_trips metadata is at 00000-xxx.metadata.json"

Step 2: Read metadata file (.json)
        "current snapshot is 999, manifest list is at snap-999.avro"

Step 3: Read manifest list (.avro)
        "this snapshot has 1 manifest: manifest-1.avro"

Step 4: Read manifest file (.avro)
        "manifest-1 has 1 data file: 00000.parquet"
        Check column stats: trip_distance max = 25.3
        → max > 20, so we MUST read this file
        (if max was < 20, we'd SKIP it entirely — zero I/O)

Step 5: Read data file (.parquet)
        Open 00000.parquet and return matching rows
```

For the raw table with 20 parquet files, Trino reads the manifest, checks stats
for each file, and **skips files** where no rows could match the query. This is
how Iceberg queries stay fast even with thousands of files.
