from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient

from pykusto.utils import KQL


class Table:
    client: KustoClient
    database: str
    table: KQL

    def __init__(self, client: KustoClient, database: str, tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        self.client = client
        self.database = database
        if isinstance(tables, (List, Tuple)):
            self.table = KQL(', '.join(tables))
        else:
            self.table = KQL(tables)
        if '*' in self.table or ',' in self.table:
            self.table = KQL('union ' + self.table)

    def execute(self, rendered_query: str) -> KustoResponseDataSet:
        return self.client.execute(self.database, rendered_query)

