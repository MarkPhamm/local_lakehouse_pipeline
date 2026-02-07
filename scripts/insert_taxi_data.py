"""
Insert NYC taxi data into Iceberg via Trino.

This script:
1. Reads a parquet file with PyArrow
2. Samples N rows (to keep it fast)
3. Inserts them into iceberg.raw.yellow_trips via Trino's Python client

Each time you run this script, Iceberg creates a NEW snapshot.
Run it multiple times to see snapshots accumulate.
"""

import sys
import pyarrow.parquet as pq
import trino


# -- Config --
TRINO_HOST = "localhost"
TRINO_PORT = 8085
PARQUET_FILE = "data/yellow_tripdata_2024-01.parquet"
SAMPLE_SIZE = 10_000  # rows to insert per run
BATCH_SIZE = 500      # rows per INSERT statement


def format_value(val):
    """Convert a Python value to a Trino SQL literal."""
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return "NULL"
    if isinstance(val, str):
        return f"'{val}'"
    if hasattr(val, "isoformat"):  # datetime/timestamp
        return f"TIMESTAMP '{val}'"
    return str(val)


def main():
    # 1. Read parquet and sample
    print(f"Reading {PARQUET_FILE}...")
    table = pq.read_table(PARQUET_FILE)
    print(f"  Total rows in file: {table.num_rows:,}")

    table = table.slice(0, SAMPLE_SIZE)
    df = table.to_pandas()

    # Fix column name: source has 'Airport_fee', our table has 'airport_fee'
    df.columns = [c.lower() for c in df.columns]

    print(f"  Sampled {len(df):,} rows")

    # 2. Connect to Trino
    print(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="raw",
    )
    cursor = conn.cursor()

    # 3. Batch insert
    columns = ", ".join(df.columns)
    total_inserted = 0

    print(f"Inserting {len(df):,} rows in batches of {BATCH_SIZE}...")
    for start in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[start : start + BATCH_SIZE]

        rows = []
        for _, row in batch.iterrows():
            vals = ", ".join(format_value(v) for v in row.values)
            rows.append(f"({vals})")

        sql = f"INSERT INTO yellow_trips ({columns}) VALUES {', '.join(rows)}"
        cursor.execute(sql)
        cursor.fetchall()  # consume result

        total_inserted += len(batch)
        print(f"  Inserted {total_inserted:,} / {len(df):,} rows", end="\r")

    print(f"\nDone! Inserted {total_inserted:,} rows.")

    # 4. Verify
    cursor.execute("SELECT COUNT(*) FROM yellow_trips")
    count = cursor.fetchone()[0]
    print(f"Total rows in iceberg.raw.yellow_trips: {count:,}")

    # 5. Show snapshots
    cursor.execute(
        'SELECT snapshot_id, operation, committed_at '
        'FROM iceberg.raw."yellow_trips$snapshots" '
        'ORDER BY committed_at'
    )
    snapshots = cursor.fetchall()
    print(f"\nIceberg snapshots ({len(snapshots)} total):")
    for snap_id, op, ts in snapshots:
        print(f"  {snap_id} | {op:>8} | {ts}")


if __name__ == "__main__":
    main()
