from concurrent.futures import Future
from typing import List, Tuple, Callable
from unittest.mock import patch
from urllib.parse import urljoin

from azure.kusto.data.request import KustoClient, ClientRequestProperties

from pykusto.client import PyKustoClient
from pykusto.expressions import column_generator as col, StringColumn, NumberColumn, AnyTypeColumn, BooleanColumn
from pykusto.query import Query
from pykusto.type_utils import KustoType
from test.test_base import TestBase, mock_columns_response, mock_tables_response, mock_databases_response


# noinspection PyMissingConstructor
class MockKustoClient(KustoClient):
    executions: List[Tuple[str, str, ClientRequestProperties]]
    columns_response: Callable
    tables_response: Callable
    databases_response: Callable

    def __init__(
            self,
            cluster="https://test_cluster.kusto.windows.net",
            columns_response: Callable = mock_columns_response([]),
            tables_response: Callable = mock_tables_response([]),
            databases_response: Callable = mock_databases_response([]),
    ):
        self.executions = []
        self._query_endpoint = urljoin(cluster, "/v2/rest/query")
        self.columns_response = columns_response
        self.tables_response = tables_response
        self.databases_response = databases_response

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None):
        if rendered_query == '.show database schema | project TableName, ColumnName, ColumnType | limit 10000':
            return self.tables_response()
        if rendered_query.startswith('.show table '):
            return self.columns_response()
        if rendered_query.startswith('.show databases schema '):
            return self.databases_response()
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

    def test_column_retrieve(self):
        mock_kusto_client = MockKustoClient(columns_response=mock_columns_response([('foo', KustoType.STRING), ('bar', KustoType.INT)]))
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        table.refresh()
        table.wait_for_items()  # Avoid race condition
        self.assertEqual(StringColumn, type(table.foo))
        self.assertEqual(NumberColumn, type(table.bar))
        self.assertEqual(AnyTypeColumn, type(table['baz']))

    def test_column_retrieve_brackets(self):
        mock_kusto_client = MockKustoClient(columns_response=mock_columns_response([('foo', KustoType.STRING), ('bar', KustoType.INT)]))
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        table.refresh()
        table.wait_for_items()  # Avoid race condition
        self.assertEqual(StringColumn, type(table['foo']))
        self.assertEqual(NumberColumn, type(table['bar']))
        self.assertEqual(AnyTypeColumn, type(table['baz']))

    def test_column_retrieve_slow(self):
        mock_response_future = Future()
        try:
            mock_kusto_client = MockKustoClient(columns_response=lambda: mock_response_future.result())
            table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
            self.assertEqual(AnyTypeColumn, type(table['foo']))
            self.assertEqual(AnyTypeColumn, type(table['bar']))
            self.assertEqual(AnyTypeColumn, type(table['baz']))
        finally:
            mock_response_future.set_result(mock_columns_response([])())

    def test_table_retrieve(self):
        mock_kusto_client = MockKustoClient(tables_response=mock_tables_response([('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]))
        db = PyKustoClient(mock_kusto_client)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        table = db.test_table
        self.assertEqual(StringColumn, type(table.foo))
        self.assertEqual(NumberColumn, type(table.bar))
        self.assertEqual(AnyTypeColumn, type(table['baz']))
        self.assertEqual(AnyTypeColumn, type(db['other_table']['foo']))

    def test_table_retrieve_error(self):
        mock_kusto_client = MockKustoClient(tables_response=mock_tables_response([('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]))
        db = PyKustoClient(mock_kusto_client)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        self.assertRaises(
            AttributeError("PyKustoClient(test_cluster.kusto.windows.net).Database(test_db) has no attribute 'test_table_1'"),
            lambda: db.test_table_1
        )

    def test_two_tables_retrieve(self):
        mock_kusto_client = MockKustoClient(tables_response=mock_tables_response([
            ('test_table_1', [('foo', KustoType.STRING), ('bar', KustoType.INT)]),
            ('test_table_2', [('baz', KustoType.BOOL)])
        ]))
        db = PyKustoClient(mock_kusto_client)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        self.assertEqual(StringColumn, type(db.test_table_1.foo))
        self.assertEqual(NumberColumn, type(db.test_table_1.bar))
        self.assertEqual(BooleanColumn, type(db.test_table_2['baz']))
        self.assertEqual(AnyTypeColumn, type(db['other_table']['foo']))

    def test_database_retrieve(self):
        mock_kusto_client = MockKustoClient(databases_response=mock_databases_response([('test_db', [('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])])]))
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        table = client.test_db.test_table
        self.assertEqual(StringColumn, type(table.foo))
        self.assertEqual(NumberColumn, type(table.bar))
        self.assertEqual(AnyTypeColumn, type(table['baz']))
        self.assertEqual(AnyTypeColumn, type(client.test_db['other_table']['foo']))
