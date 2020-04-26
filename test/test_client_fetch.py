from concurrent.futures import Future
from threading import Thread

from pykusto.client import PyKustoClient, Database
from pykusto.expressions import StringColumn, NumberColumn, AnyTypeColumn, BooleanColumn
from pykusto.query import Query
from pykusto.type_utils import KustoType
from test.test_base import TestBase, MockKustoClient, mock_columns_response, RecordedQuery, mock_tables_response, mock_getschema_response, mock_databases_response


class TestClientFetch(TestBase):
    def test_column_fetch(self):
        mock_kusto_client = MockKustoClient(
            columns_response=mock_columns_response([('foo', KustoType.STRING), ('bar', KustoType.INT)]),
            record_metadata=True,
        )
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['test_table']
        table.blocking_refresh()
        # Fetch query
        self.assertEqual(
            [RecordedQuery('test_db', '.show table test_table | project AttributeName, AttributeType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        # Dot notation
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        # Bracket notation
        self.assertEqual(type(table['foo']), StringColumn)
        self.assertEqual(type(table['bar']), NumberColumn)
        self.assertEqual(type(table['baz']), AnyTypeColumn)

    def test_column_fetch_slow(self):
        mock_response_future = Future()
        mock_response_future.executed = False

        # noinspection PyUnusedLocal
        def upon_execute(query):  # Parameter required since function is passed as Callable[[RecordedQuery], None]
            mock_response_future.result()
            mock_response_future.executed = True

        try:
            mock_kusto_client = MockKustoClient(upon_execute=upon_execute, record_metadata=True)
            table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['test_table']
            table.refresh()
            self.assertEqual(type(table['foo']), AnyTypeColumn)
            self.assertEqual(type(table['bar']), AnyTypeColumn)
            self.assertEqual(type(table['baz']), AnyTypeColumn)
            # Make sure above lines were called while the fetch query was still waiting
            assert not mock_response_future.executed
        finally:
            # Return the fetch
            mock_response_future.set_result(None)

        table.wait_for_items()
        # Make sure the fetch query was indeed called
        assert mock_response_future.executed

    def test_query_before_fetch_returned(self):
        mock_response_future = Future()
        mock_response_future.returned_queries = []
        mock_response_future.called = False
        mock_response_future.executed = False

        def upon_execute(query):
            if not mock_response_future.called:
                mock_response_future.called = True
                mock_response_future.result()
                mock_response_future.executed = True
            mock_response_future.returned_queries.append(query)

        try:
            mock_kusto_client = MockKustoClient(upon_execute=upon_execute, record_metadata=True)
            table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['test_table']
            table.refresh()

            # Executing a query in a separate thread, because it is supposed to block until the fetch returns
            query_thread = Thread(target=Query(table).take(5).execute)
            query_thread.start()

            # Make sure above lines were called while the fetch query was still waiting
            assert not mock_response_future.executed
        finally:
            # Return the fetch
            mock_response_future.set_result(None)

        table.wait_for_items()
        query_thread.join()
        # Make sure the fetch query was indeed called
        assert mock_response_future.executed
        # Before the fix the order of returned query was reversed
        self.assertEqual(
            [
                RecordedQuery('test_db', '.show table test_table | project AttributeName, AttributeType | limit 10000'),
                RecordedQuery('test_db', 'test_table | take 5'),
            ],
            mock_response_future.returned_queries,
        )

    def test_table_fetch(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([('test_table', [('foo', KustoType.STRING), ('bar', KustoType.INT)])]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.blocking_refresh()
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        table = db.test_table
        # Table columns
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table['baz']), AnyTypeColumn)
        # Bracket notation
        self.assertEqual(type(db['other_table']['foo']), AnyTypeColumn)
        # Dot notation error
        self.assertRaises(
            AttributeError("PyKustoClient(test_cluster.kusto.windows.net).Database(test_db) has no attribute 'test_table_1'"),
            lambda: db.test_table_1
        )

    def test_two_tables_fetch(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', KustoType.STRING), ('bar', KustoType.INT)]),
                ('test_table_2', [('baz', KustoType.BOOL)])
            ]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.blocking_refresh()
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        # Table columns
        self.assertEqual(type(db.test_table_1.foo), StringColumn)
        self.assertEqual(type(db.test_table_1.bar), NumberColumn)
        self.assertEqual(type(db.test_table_2['baz']), BooleanColumn)
        self.assertEqual(type(db['other_table']['foo']), AnyTypeColumn)
        # Union
        table = db.get_table('test_table_1', 'test_table_2')
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table.baz), BooleanColumn)
        # Wildcard
        table = db.get_table('test_table_*')
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table.baz), BooleanColumn)

    def test_union_column_name_conflict(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', KustoType.STRING), ('bar', KustoType.INT)]),
                ('test_table_2', [('foo', KustoType.BOOL)])
            ]),
            getschema_response=mock_getschema_response([
                ('foo_string', KustoType.STRING), ('bar', KustoType.INT), ('foo_bool', KustoType.BOOL)
            ]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.blocking_refresh()
        table = db.get_table('test_table_*')
        table.blocking_refresh()  # To trigger name conflict resolution
        self.assertEqual(
            [
                # First trying the usual fetch
                RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000'),
                # Fallback for name conflict resolution
                RecordedQuery('test_db', 'union test_table_* | getschema | project ColumnName, DataType | limit 10000')
            ],
            mock_kusto_client.recorded_queries,
        )
        self.assertEqual(type(table.foo_string), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table.foo_bool), BooleanColumn)

    def test_union_wildcard_one_table(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', KustoType.STRING), ('bar', KustoType.INT)]),
                ('other_table_2', [('baz', KustoType.BOOL)])
            ]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.blocking_refresh()
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        table = db.get_table('test_table_*')
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table['baz']), AnyTypeColumn)

    def test_database_fetch(self):
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
        # Table columns
        table = client.test_db.test_table
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), NumberColumn)
        self.assertEqual(type(table['baz']), AnyTypeColumn)
        self.assertEqual(type(client.test_db['other_table']['foo']), AnyTypeColumn)
        # Various utility methods
        db = client.get_database('test_db')
        self.assertEqual(type(db), Database)
        self.assertEqual('test_db', db.get_name())
        self.assertEqual(('test_db',), tuple(client.get_databases_names()))
        self.assertEqual(('test_table', 'other_table'), tuple(client.test_db.get_table_names()))
        self.assertEqual(('foo', 'bar', 'baz'), tuple(client.test_db.test_table.get_columns_names()))
        self.assertTrue({'foo', 'bar'} < set(dir(client.test_db.test_table)))
        self.assertEqual('PyKustoClient(test_cluster.kusto.windows.net).Database(test_db).Table(test_table)', repr(client.test_db.test_table))

    def test_autocomplete_with_dot(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('test_table', [('foo', KustoType.STRING), ('bar.baz', KustoType.INT)])])]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        # Table columns
        table = client.test_db.test_table
        self.assertEqual(type(table.foo), StringColumn)
        self.assertEqual(type(table.bar), AnyTypeColumn)
        self.assertEqual(type(table['bar.baz']), NumberColumn)
        autocomplete_list = set(dir(client.test_db.test_table))
        self.assertIn('foo', autocomplete_list)
        self.assertNotIn('bar.baz', autocomplete_list)

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
        self.assertEqual(type(client.test_db.test_table.foo), StringColumn)

    def test_client_not_fetched(self):
        client = PyKustoClient(MockKustoClient(), fetch_by_default=False)
        self.assertEqual(frozenset(), set(client.get_databases_names()))
        self.assertEqual(frozenset(), set(client.get_databases()))
