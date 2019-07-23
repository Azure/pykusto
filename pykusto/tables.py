from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient

from pykusto.query import Query
from pykusto.utils import logger


class Table:
    client: KustoClient
    database: str
    table: str

    def __init__(self, client: KustoClient, database: str, tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        self.client = client
        self.database = database
        if isinstance(tables, (List, Tuple)):
            self.table = ', '.join(tables)
        else:
            self.table = tables
        if '*' in self.table or ',' in self.table:
            self.table = 'union ' + self.table

    def execute(self, query: Query) -> KustoResponseDataSet:
        rendered_query = self.table + query.render()
        logger.debug("Running query: " + rendered_query)
        return self.client.execute(self.database, rendered_query)
