from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets


dbt_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbt_trino").resolve(),
)
dbt_project.prepare_if_dev()


class CustomDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "transforms"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=CustomDbtTranslator())
def lakehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
