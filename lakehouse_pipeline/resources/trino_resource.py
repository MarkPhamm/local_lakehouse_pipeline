import trino
from dagster import ConfigurableResource


class TrinoResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8085
    user: str = "admin"
    catalog: str = "iceberg"
    schema: str = "raw"

    def get_connection(self, schema: str | None = None):
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=schema or self.schema,
        )

    def execute(self, sql: str, schema: str | None = None) -> list:
        conn = self.get_connection(schema)
        cursor = conn.cursor()
        cursor.execute(sql)
        try:
            return cursor.fetchall()
        except Exception:
            return []
