from typing import List, Tuple
from unittest.mock import patch
from urllib.parse import urljoin

from azure.kusto.data.request import KustoClient, ClientRequestProperties

from pykusto.expressions import column_generator as col
from pykusto.client import PyKustoClient
from pykusto.query import Query
from test.test_base import TestBase


# noinspection PyMissingConstructor
class MockKustoClient(KustoClient):
    executions: List[Tuple[str, str, ClientRequestProperties]]

    def __init__(self, cluster="https://test_cluster.kusto.windows.net"):
        self.executions = []
        self._query_endpoint = urljoin(cluster, "/v2/rest/query")

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None):
        self.executions.append((database, rendered_query, properties))


class TestTable(TestBase):
    def test_single_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        Query(table).take(5).execute()
        self.assertEqual(
            [('test_db', 'test_table | take 5', None)],
            mock_kusto_client.executions
        )

    def test_execute_no_table(self):
        self.assertRaises(
            RuntimeError("No table supplied"),
            Query().take(5).execute
        )

    def test_execute_already_bound(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']

        self.assertRaises(
            RuntimeError("This table is already bound to a query"),
            Query(table).take(5).execute,
            table
        )

    def test_single_table_on_execute(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        Query().take(5).execute(table)
        self.assertEqual(
            [('test_db', 'test_table | take 5', None)],
            mock_kusto_client.executions,
        )

    def test_union_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db'].get_tables('test_table1', 'test_table2')
        Query(table).take(5).execute()
        self.assertEqual(
            [('test_db', 'union test_table1, test_table2 | take 5', None)],
            mock_kusto_client.executions,
        )

    def test_union_table_with_wildcard(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table_*']
        Query(table).take(5).execute()
        self.assertEqual(
            [('test_db', 'union test_table_* | take 5', None)],
            mock_kusto_client.executions,
        )

    def test_default_authentication(self):
        mock_kusto_client = MockKustoClient()
        with patch('pykusto.client.PyKustoClient._get_client_for_cluster', lambda s, cluster: mock_kusto_client):
            table = PyKustoClient('https://help.kusto.windows.net/')['test_db']['test_table']
            Query().take(5).execute(table)
        self.assertIs(
            mock_kusto_client,
            table.database.client._client,
        )
        self.assertEqual(
            [('test_db', 'test_table | take 5', None)],
            mock_kusto_client.executions,
        )

    def test_cross_cluster_join(self):
        mock_kusto_client_1 = MockKustoClient("https://one.kusto.windows.net")
        mock_kusto_client_2 = MockKustoClient("https://two.kusto.windows.net")

        table1 = PyKustoClient(mock_kusto_client_1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(mock_kusto_client_2)['test_db_2']['test_table_2']
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [('test_db_1', 'test_table_1 | take 5 | join  (cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2") | take 6) on foo', None)],
            mock_kusto_client_1.executions,
        )

    def test_cross_cluster_join_with_union(self):
        mock_kusto_client_1 = MockKustoClient("https://one.kusto.windows.net")
        mock_kusto_client_2 = MockKustoClient("https://two.kusto.windows.net")

        table1 = PyKustoClient(mock_kusto_client_1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(mock_kusto_client_2)['test_db_2'].get_tables('test_table_2_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [('test_db_1',
              'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*") | take 6) on foo',
              None)],
            mock_kusto_client_1.executions,
        )

    def test_cross_cluster_join_with_union_2(self):
        mock_kusto_client_1 = MockKustoClient("https://one.kusto.windows.net")
        mock_kusto_client_2 = MockKustoClient("https://two.kusto.windows.net")

        table1 = PyKustoClient(mock_kusto_client_1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(mock_kusto_client_2)['test_db_2'].get_tables('test_table_2_*', 'test_table_3_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [('test_db_1',
              'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*"), cluster("two.kusto.windows.net").database("test_db_2").table("test_table_3_*") | take 6) on foo',
              None)],
            mock_kusto_client_1.executions,
        )
