from typing import List, Tuple

from azure.kusto.data.request import KustoClient, ClientRequestProperties

from pykusto.query import Query
from pykusto.tables import Table
from test.test_base import TestBase


# noinspection PyMissingConstructor
class MockKustoClient(KustoClient):
    executions: List[Tuple[str, str, ClientRequestProperties]]

    def __init__(self):
        self.executions = []

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None):
        self.executions.append((database, rendered_query, properties))


class TestTable(TestBase):
    def test_execute(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', 'test_table')
        table.execute(Query().take(5))
        self.assertEqual(
            mock_kusto_client.executions,
            [('test_db', 'test_table | take 5', None)]
        )
