import requests
import pyarrow.parquet as pq
from pathlib import Path

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from lakehouse_pipeline.resources.trino_resource import TrinoResource


TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).parent.parent.parent / "data"
SAMPLE_SIZE = 10_000
BATCH_SIZE = 500


def _format_value(val):
    if val is None or (isinstance(val, float) and str(val) == "nan"):
        return "NULL"
    if isinstance(val, str):
        return f"'{val}'"
    if hasattr(val, "isoformat"):
        return f"TIMESTAMP '{val}'"
    return str(val)


@asset(group_name="setup")
def iceberg_schemas(trino_resource: TrinoResource) -> Output[None]:
    """Create Iceberg schemas and the raw yellow_trips table."""
    statements = [
        "CREATE SCHEMA IF NOT EXISTS iceberg.raw",
        "CREATE SCHEMA IF NOT EXISTS iceberg.silver",
        "CREATE SCHEMA IF NOT EXISTS iceberg.gold",
        """
        CREATE TABLE IF NOT EXISTS iceberg.raw.yellow_trips (
            vendorid               INTEGER,
            tpep_pickup_datetime   TIMESTAMP(6),
            tpep_dropoff_datetime  TIMESTAMP(6),
            passenger_count        DOUBLE,
            trip_distance          DOUBLE,
            ratecodeid             DOUBLE,
            store_and_fwd_flag     VARCHAR,
            pulocationid           INTEGER,
            dolocationid           INTEGER,
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
        ) WITH (format = 'PARQUET')
        """,
    ]
    for stmt in statements:
        trino_resource.execute(stmt)

    return Output(None, metadata={"status": MetadataValue.text("schemas + table created")})


@asset(group_name="ingestion")
def raw_taxi_file(context: AssetExecutionContext) -> Output[str]:
    """Download NYC yellow taxi parquet for Jan 2024."""
    DATA_DIR.mkdir(exist_ok=True)
    filename = "yellow_tripdata_2024-01.parquet"
    local_path = DATA_DIR / filename

    if not local_path.exists():
        url = f"{TAXI_BASE_URL}/{filename}"
        context.log.info(f"Downloading {url}")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

    num_rows = pq.ParquetFile(local_path).metadata.num_rows
    context.log.info(f"File ready: {filename} ({num_rows:,} rows)")

    return Output(
        str(local_path),
        metadata={
            "filename": MetadataValue.text(filename),
            "total_rows": MetadataValue.int(num_rows),
        },
    )


@asset(group_name="ingestion", deps=[iceberg_schemas])
def iceberg_raw_yellow_trips(
    context: AssetExecutionContext,
    raw_taxi_file: str,
    trino_resource: TrinoResource,
) -> Output[None]:
    """Read parquet and batch-insert into Iceberg via Trino."""
    table = pq.read_table(raw_taxi_file)
    table = table.slice(0, SAMPLE_SIZE)
    df = table.to_pandas()
    df.columns = [c.lower() for c in df.columns]

    conn = trino_resource.get_connection(schema="raw")
    cursor = conn.cursor()

    columns = ", ".join(df.columns)
    total = 0

    for start in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[start : start + BATCH_SIZE]
        rows = []
        for _, row in batch.iterrows():
            vals = ", ".join(_format_value(v) for v in row.values)
            rows.append(f"({vals})")
        cursor.execute(f"INSERT INTO yellow_trips ({columns}) VALUES {', '.join(rows)}")
        cursor.fetchall()
        total += len(batch)
        context.log.info(f"Inserted {total:,} / {len(df):,}")

    # Get snapshot count
    cursor.execute('SELECT COUNT(*) FROM iceberg.raw."yellow_trips$snapshots"')
    snap_count = cursor.fetchone()[0]

    return Output(
        None,
        metadata={
            "rows_inserted": MetadataValue.int(total),
            "total_snapshots": MetadataValue.int(snap_count),
        },
    )
