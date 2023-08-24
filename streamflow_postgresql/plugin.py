from streamflow.ext.plugin import StreamFlowPlugin

from streamflow_postgresql.database import PostgreSQLDatabase


class PostgreSQLStreamFlowPlugin(StreamFlowPlugin):
    def register(self) -> None:
        self.register_database("unito.postgresql", PostgreSQLDatabase)
