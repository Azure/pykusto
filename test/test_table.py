from typing import List, Tuple
from unittest.mock import patch

from azure.kusto.data.request import KustoClient, ClientRequestProperties

from pykusto.client import PyKustoClient
from pykusto.query import Query
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
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        Query(table).take(5).execute()
        self.assertEqual(
            [('test_db', 'test_table | take 5', None)],
            mock_kusto_client.executions
        )

    def test_execute_no_table(self):
        self.assertRaises(
            RuntimeError,
            Query().take(5).execute
        )

    def test_execute_already_bound(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']

        self.assertRaises(
            RuntimeError,
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
