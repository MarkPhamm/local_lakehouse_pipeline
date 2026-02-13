from dagster import Definitions, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource

from lakehouse_pipeline.assets.ingestion import (
    iceberg_schemas,
    raw_taxi_file,
    iceberg_raw_yellow_trips,
)
from lakehouse_pipeline.assets.dbt_assets import lakehouse_dbt_assets, dbt_project
from lakehouse_pipeline.resources.trino_resource import TrinoResource


# Job that materializes everything in dependency order
lakehouse_full_pipeline = define_asset_job(
    name="lakehouse_full_pipeline",
    selection=AssetSelection.all(),
    description="Run the full pipeline: setup → ingest → dbt transforms",
)


defs = Definitions(
    assets=[
        iceberg_schemas,
        raw_taxi_file,
        iceberg_raw_yellow_trips,
        lakehouse_dbt_assets,
    ],
    jobs=[lakehouse_full_pipeline],
    resources={
        "trino_resource": TrinoResource(),
        "dbt": DbtCliResource(
            project_dir=str(dbt_project.project_dir),
            profiles_dir=str(dbt_project.project_dir),
        ),
    },
)
