from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.request import KustoClient

from pykusto.query import Query
from pykusto.utils import logger, KQL


class Table:
    client: KustoClient
    database: str
    _table: KQL

    def __init__(self, client: KustoClient, database: str, tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        self.client = client
        self.database = database
        if isinstance(tables, (List, Tuple)):
            self._table = KQL(', '.join(tables))
        else:
            self._table = KQL(tables)
        if '*' in self._table or ',' in self._table:
            self._table = KQL('union ' + self._table)

    def execute(self, query: Query) -> KustoResponseDataSet:
        rendered_query = self._table + query.render()
        logger.debug("Running query: " + rendered_query)
        return self.client.execute(self.database, rendered_query)

    def execute_to_dataframe(self, query: Query):
        res = self.execute(query)
        return dataframe_from_result_table(res.primary_results[0])
