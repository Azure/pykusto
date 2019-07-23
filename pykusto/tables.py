from typing import Sequence, Union

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient

from pykusto.query import Query
from pykusto.utils import logger


class Table:
    client: KustoClient
    database: str
    table: str

    def __init__(self, client: KustoClient, database: str, tables: Union[str, Sequence[str]]) -> None:
        self.client = client
        self.database = database
        if isinstance(tables, Sequence):
            self.table = ', '.join(tables)
        else:
            self.table = tables
        if '*' in self.table or ',' in self.table:
            self.table = 'union ' + self.table

    def execute(self, query: Query) -> KustoResponseDataSet:
        query = self.table + query.render()
        logger.debug("Running query: " + query)
        return self.client.execute(self.database, query)
