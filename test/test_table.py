from concurrent.futures import Future
from unittest.mock import patch

from azure.kusto.data.request import KustoClient

from pykusto.client import PyKustoClient, Database
from pykusto.expressions import column_generator as col, StringColumn, NumberColumn, AnyTypeColumn, BooleanColumn
from pykusto.query import Query
from pykusto.type_utils import KustoType
from test.test_base import TestBase, mock_columns_response, mock_tables_response, mock_databases_response, \
    MockKustoClient, RecordedQuery


class TestTable(TestBase):
    def test_single_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
        Query(table).take(5).execute()
        self.assertEqual(
            [RecordedQuery('test_db', 'test_table | take 5')],
            mock_kusto_client.recorded_queries
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
            [RecordedQuery('test_db', 'test_table | take 5')],
            mock_kusto_client.recorded_queries,
        )

    def test_union_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db'].get_tables('test_table1', 'test_table2')
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
        with patch('pykusto.client.PyKustoClient._get_client_for_cluster', lambda s, cluster: mock_kusto_client):
            table = PyKustoClient('https://help.kusto.windows.net/')['test_db']['test_table']
            Query().take(5).execute(table)
        self.assertIs(
            mock_kusto_client,
            table.database.client._client,
        )
        self.assertEqual(
            [RecordedQuery('test_db', 'test_table | take 5')],
            mock_kusto_client.recorded_queries,
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
        table2 = PyKustoClient(client2)['test_db_2'].get_tables('test_table_2_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [RecordedQuery('test_db_1', 'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*") | take 6) on foo')],
            client1.recorded_queries,
        )

    def test_cross_cluster_join_with_union_2(self):
        client1 = MockKustoClient("https://one.kusto.windows.net")
        client2 = MockKustoClient("https://two.kusto.windows.net")
        table1 = PyKustoClient(client1)['test_db_1']['test_table_1']
        table2 = PyKustoClient(client2)['test_db_2'].get_tables('test_table_2_*', 'test_table_3_*')
        Query(table1).take(5).join(Query(table2).take(6)).on(col.foo).execute()
        self.assertEqual(
            [RecordedQuery(
                'test_db_1',
                'test_table_1 | take 5 | join  (union cluster("two.kusto.windows.net").database("test_db_2").table("test_table_2_*"), '
                'cluster("two.kusto.windows.net").database("test_db_2").table("test_table_3_*") | take 6) on foo',
            )],
            client1.recorded_queries,
        )

    def test_column_retrieve(self):
        mock_kusto_client = MockKustoClient(
            columns_response=mock_columns_response([('foo', KustoType.STRING), ('bar', KustoType.INT)]),
            record_metadata=True,
        )
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['test_table']
        table.refresh()
        table.wait_for_items()  # Avoid race condition
        self.assertEqual(
            [RecordedQuery('test_db', '.show table test_table | project AttributeName, AttributeType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertIsInstance(table.foo, StringColumn)
        self.assertIsInstance(table.bar, NumberColumn)
        self.assertIsInstance(table['baz'], AnyTypeColumn)

    def test_column_retrieve_brackets(self):
        mock_kusto_client = MockKustoClient(
            columns_response=mock_columns_response([('foo', KustoType.STRING), ('bar', KustoType.INT)]),
            record_metadata=True,
        )
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['test_table']
        table.refresh()
        table.wait_for_items()  # Avoid race condition
        self.assertEqual(
            [RecordedQuery('test_db', '.show table test_table | project AttributeName, AttributeType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertIsInstance(table['foo'], StringColumn)
        self.assertIsInstance(table['bar'], NumberColumn)
        self.assertIsInstance(table['baz'], AnyTypeColumn)

    def test_column_retrieve_slow(self):
        mock_response_future = Future()
        try:
            mock_kusto_client = MockKustoClient(columns_response=lambda: mock_response_future.result(), record_metadata=True)
            table = PyKustoClient(mock_kusto_client)['test_db']['test_table']
            self.assertIsInstance(table['foo'], AnyTypeColumn)
            self.assertIsInstance(table['bar'], AnyTypeColumn)
            self.assertIsInstance(table['baz'], AnyTypeColumn)
        finally:
            mock_response_future.set_result(mock_columns_response([])())

    def test_table_retrieve(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        table = db.test_table
        self.assertIsInstance(table.foo, StringColumn)
        self.assertIsInstance(table.bar, NumberColumn)
        self.assertIsInstance(table['baz'], AnyTypeColumn)
        self.assertIsInstance(db['other_table']['foo'], AnyTypeColumn)

    def test_table_retrieve_error(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertRaises(
            AttributeError("PyKustoClient(test_cluster.kusto.windows.net).Database(test_db) has no attribute 'test_table_1'"),
            lambda: db.test_table_1
        )

    def test_two_tables_retrieve(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', KustoType.STRING), ('bar', KustoType.INT)]),
                ('test_table_2', [('baz', KustoType.BOOL)])
            ]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.refresh()
        db.wait_for_items()  # Avoid race condition
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertIsInstance(db.test_table_1.foo, StringColumn)
        self.assertIsInstance(db.test_table_1.bar, NumberColumn)
        self.assertIsInstance(db.test_table_2['baz'], BooleanColumn)
        self.assertIsInstance(db['other_table']['foo'], AnyTypeColumn)

    def test_database_retrieve(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])])]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        self.assertEqual(
            [RecordedQuery('', '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')],
            mock_kusto_client.recorded_queries,
        )
        table = client.test_db.test_table
        self.assertIsInstance(table.foo, StringColumn)
        self.assertIsInstance(table.bar, NumberColumn)
        self.assertIsInstance(table['baz'], AnyTypeColumn)
        self.assertIsInstance(client.test_db['other_table']['foo'], AnyTypeColumn)

    def test_empty_database(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([
                ('test_db', [('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]),
                ('', [('test_table1', [('foo1', KustoType.STRING), ('bar1', KustoType.INT)])])
            ]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        self.assertEqual(
            [RecordedQuery('', '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertIsInstance(client.test_db.test_table.foo, StringColumn)

    def test_client_databases(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])])]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        self.assertEqual(
            [RecordedQuery('', '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')],
            mock_kusto_client.recorded_queries,
        )
        db = client.get_database('test_db')
        self.assertIsInstance(db, Database)
        self.assertEqual('test_db', db.name)
        self.assertEqual(('test_db',), client.show_databases())
        self.assertEqual(('test_table',), client.test_db.show_tables())
        self.assertEqual(('foo', 'bar'), client.test_db.test_table.show_columns())
        self.assertTrue({'foo', 'bar'} < set(dir(client.test_db.test_table)))
        self.assertEqual('PyKustoClient(test_cluster.kusto.windows.net).Database(test_db).Table(test_table)', repr(client.test_db.test_table))

    def test_client_for_cluster(self):
        client = PyKustoClient('https://help.kusto.windows.net', fetch_by_default=False)
        self.assertIsInstance(client._client, KustoClient)
        self.assertEqual('https://help.kusto.windows.net', client._cluster_name)
