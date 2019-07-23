from typing import Sequence

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient

from query import Query


class BaseTable:
    client: KustoClient
    database: str

    def __init__(self, client: KustoClient, database: str) -> None:
        self.client = client
        self.database = database

    def execute(self, query: Query) -> KustoResponseDataSet:
        raise NotImplementedError()


class SingleTable(BaseTable):
    table: str

    def __init__(self, client: KustoClient, database: str, table: str) -> None:
        super().__init__(client, database)
        if '*' in table:
            raise ValueError("SingleTable does not support wildcards. Instead use UnionTable")
        self.table = table

    def execute(self, query: Query) -> KustoResponseDataSet:
        return self.client.execute(self.database, self.table + query.compile_all())


class UnionTable(BaseTable):
    tables: Sequence[str]

    def __init__(self, client: KustoClient, database: str, tables: Sequence[str]) -> None:
        super().__init__(client, database)
        self.tables = tables

    def execute(self, query: Query) -> KustoResponseDataSet:
        return self.client.execute(self.database, 'union {}{}'.format(
            ', '.join(self.table),
            query.compile_all()
        ))
