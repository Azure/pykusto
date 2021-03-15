import logging
from typing import Type
from unittest.mock import patch

from azure.kusto.data import KustoClient
from azure.kusto.data.exceptions import KustoError

from pykusto import PyKustoClient, column_generator as col, Query, KustoServiceError, RetryConfig, NO_RETRIES
# noinspection PyProtectedMember
from pykusto._src.logger import _logger
from test.test_base import TestBase, MockKustoClient, RecordedQuery, mock_response


class TestClient(TestBase):
    def test_single_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['mock_table']
        Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'mock_table | take 5')],
            mock_kusto_client.recorded_queries
        )

    def test_execute_no_table(self):
        self.assertRaises(
            RuntimeError("No table supplied"),
            Query().take(5).execute
        )

    def test_execute_already_bound(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['mock_table']

        self.assertRaises(
            RuntimeError("This table is already bound to a query"),
            Query(table).take(5).execute,
            table
        )

    def test_single_table_on_execute(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['mock_table']
        Query().take(5).execute(table)
        self.assertEqual(
            [RecordedQuery('test_db', 'mock_table | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_get_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db'].get_table('mock_table')
        Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'mock_table | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_union_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db'].get_table('test_table1', 'test_table2')
        Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'union test_table1, test_table2 | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_union_table_with_wildcard(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table_*']
        Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'union test_table_* | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_default_authentication(self):
        mock_kusto_client = MockKustoClient()
        with patch('pykusto._src.client.PyKustoClient._get_client_for_cluster', lambda s, cluster: mock_kusto_client):
            table = PyKustoClient('https://help.kusto.windows.net/')['test_db']['mock_table']
            Query().take(5).execute(table)
        self.assertIs(
            mock_kusto_client,
            table._Table__database._Database__client._PyKustoClient__client,
        )
        self.assertEqual(
            [RecordedQuery('test_db', 'mock_table | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_client_instances(self):
        with patch('pykusto._src.client.PyKustoClient._get_client_for_cluster', MockKustoClient):
            client_1 = PyKustoClient('https://help.kusto.windows.net/')
            client_2 = PyKustoClient('https://help.kusto.windows.net/')

        self.assertIsNot(
            client_1._PyKustoClient__client,
            client_2._PyKustoClient__client,
        )

    def test_client_instances_cached(self):
        with patch('pykusto._src.client.PyKustoClient._get_client_for_cluster', MockKustoClient):
            client_1 = PyKustoClient('https://help.kusto.windows.net/', use_global_cache=True)
            client_2 = PyKustoClient('https://help.kusto.windows.net/', use_global_cache=True)

        self.assertIs(
            client_1._PyKustoClient__client,
            client_2._PyKustoClient__client,
        )

    def test_client_instances_cached_distinct(self):
        with patch('pykusto._src.client.PyKustoClient._get_client_for_cluster', MockKustoClient):
            client_1 = PyKustoClient('https://help1.kusto.windows.net/', use_global_cache=True)
            client_2 = PyKustoClient('https://help2.kusto.windows.net/', use_global_cache=True)

        self.assertIsNot(
            client_1._PyKustoClient__client,
            client_2._PyKustoClient__client,
        )

    def test_cross_cluster_join(self):
        client1 = MockKustoClient("https://one.kusto.windows.net")
        client2 = MockKustoClient("https://two.kusto.windows.net")
        table1 = PyKustoClient(client1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(client2)['test_db_2']['test_table_2']
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [RecordedQuery('test_db_1', 'test_table_1 | take 5 | join  (cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2") | take 6) on foo')],
            client1.recorded_queries,
        )

    def test_cross_cluster_join_with_union(self):
        client1 = MockKustoClient("https://one.kusto.windows.net")
        client2 = MockKustoClient("https://two.kusto.windows.net")
        table1 = PyKustoClient(client1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(client2)['test_db_2'].get_table('test_table_2_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [RecordedQuery('test_db_1', 'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*") | take 6) on foo')],
            client1.recorded_queries,
        )

    def test_cross_cluster_join_with_union_2(self):
        client1 = MockKustoClient("https://one.kusto.windows.net")
        client2 = MockKustoClient("https://two.kusto.windows.net")
        table1 = PyKustoClient(client1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(client2)['test_db_2'].get_table('test_table_2_*', 'test_table_3_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [RecordedQuery(
                'test_db_1',
                'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*"), '
                'cluster("two.kusto.windows.net").database("test_db_2").table("test_table_3_*") | take 6) on foo',
            )],
            client1.recorded_queries,
        )

    def test_client_for_cluster_with_azure_cli_auth(self):
        with patch('pykusto._src.client._get_azure_cli_auth_token', lambda: "MOCK_TOKEN"), self.assertLogs(_logger, logging.INFO) as cm:
            client = PyKustoClient('https://help.kusto.windows.net', fetch_by_default=False)
            self.assertIsInstance(client._PyKustoClient__client, KustoClient)
            self.assertEqual('https://help.kusto.windows.net', client.get_cluster_name())
        self.assertEqual([], cm.output)

    def test_client_for_cluster_fallback_to_aad_device_auth(self):
        with patch('pykusto._src.client._get_azure_cli_auth_token', lambda: None), self.assertLogs(_logger, logging.INFO) as cm:
            client = PyKustoClient('https://help.kusto.windows.net', fetch_by_default=False)
            self.assertIsInstance(client._PyKustoClient__client, KustoClient)
            self.assertEqual('https://help.kusto.windows.net', client.get_cluster_name())
        self.assertEqual(
            ['INFO:pykusto:Failed to get Azure CLI token, falling back to AAD device authentication'],
            cm.output
        )

    @staticmethod
    def unreliable_mock_kusto_client(number_of_failures: int, exception_type: Type[Exception] = KustoServiceError):
        TestClient.attempt = 1

        def main_response():
            if TestClient.attempt <= number_of_failures:
                TestClient.attempt += 1
                raise exception_type("Mock exception for test", None)
            return mock_response(tuple())()

        return MockKustoClient(main_response=main_response)

    def retries_base(self, number_of_retries: int):
        mock_kusto_client = self.unreliable_mock_kusto_client(number_of_retries - 1)
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False, retry_config=RetryConfig(number_of_retries, sleep_time=0.1, jitter=0))['test_db']['mock_table']
        with self.assertLogs(_logger, logging.INFO) as cm:
            Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'mock_table | take 5')] * number_of_retries,
            mock_kusto_client.recorded_queries
        )
        self.assertEqual(
            [
                f"INFO:pykusto:Attempt number {i} out of {number_of_retries} failed, "
                f"previous sleep time was 0.1 seconds. Exception: (KustoServiceError(...), 'Mock exception for test')"
                for i in range(1, number_of_retries)
            ],
            cm.output
        )

    def test_2_retry(self):
        self.retries_base(2)

    def test_3_retries(self):
        self.retries_base(3)

    def test_missing_retries(self):
        mock_kusto_client = self.unreliable_mock_kusto_client(1)
        table = PyKustoClient(
            mock_kusto_client, fetch_by_default=False, retry_config=NO_RETRIES
        )['test_db']['mock_table']
        self.assertRaises(
            KustoServiceError("Mock exception for test", None),
            lambda: Query(table).take(5).execute(),
        )

    def test_non_transient_exception(self):
        mock_kusto_client = self.unreliable_mock_kusto_client(1, KustoError)
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False, retry_config=RetryConfig(2, sleep_time=0.1, jitter=0))['test_db']['mock_table']
        self.assertRaises(
            KustoError("Mock exception for test", None),
            lambda: Query(table).take(5).execute(),
        )
