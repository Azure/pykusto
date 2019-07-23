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
    def test_single_table(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', 'test_table')
        Query(table).take(5).execute()
        self.assertEqual(
            mock_kusto_client.executions,
            [('test_db', 'test_table | take 5', None)]
        )

    def test_union_table(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', ('test_table1', 'test_table2'))
        Query(table).take(5).execute()
        self.assertEqual(
            mock_kusto_client.executions,
            [('test_db', 'union test_table1, test_table2 | take 5', None)]
        )

    def test_union_table_with_wildcard(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', 'test_table_*')
        Query(table).take(5).execute()
        self.assertEqual(
            mock_kusto_client.executions,
            [('test_db', 'union test_table_* | take 5', None)]
        )
