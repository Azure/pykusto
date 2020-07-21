from concurrent.futures import Future
from threading import Thread, Lock

from pykusto import PyKustoClient, Query
# noinspection PyProtectedMember
from pykusto._src.client import _Database
# noinspection PyProtectedMember
from pykusto._src.expressions import _StringColumn, _NumberColumn, _AnyTypeColumn, _BooleanColumn
# noinspection PyProtectedMember
from pykusto._src.type_utils import _KustoType
from test.test_base import TestBase, MockKustoClient, mock_columns_response, RecordedQuery, mock_tables_response, mock_getschema_response, mock_databases_response


class TestClientFetch(TestBase):
    def test_column_fetch(self):
        mock_kusto_client = MockKustoClient(
            columns_response=mock_columns_response([('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
            record_metadata=True,
        )
        table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['mock_table']
        table.blocking_refresh()
        # Fetch query
        self.assertEqual(
            [RecordedQuery('test_db', '.show table mock_table | project AttributeName, AttributeType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        # Dot notation
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        # Bracket notation
        self.assertEqual(type(table['foo']), _StringColumn)
        self.assertEqual(type(table['bar']), _NumberColumn)
        self.assertEqual(type(table['baz']), _AnyTypeColumn)

    def test_column_fetch_slow(self):
        mock_response_future = Future()
        mock_response_future.executed = False

        # noinspection PyUnusedLocal
        def upon_execute(query):  # Parameter required since function is passed as Callable[[RecordedQuery], None]
            mock_response_future.result()
            mock_response_future.executed = True

        try:
            mock_kusto_client = MockKustoClient(upon_execute=upon_execute, record_metadata=True)
            table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['mock_table']
            table.refresh()
            self.assertEqual(type(table['foo']), _AnyTypeColumn)
            self.assertEqual(type(table['bar']), _AnyTypeColumn)
            self.assertEqual(type(table['baz']), _AnyTypeColumn)
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
        future_called_lock = Lock()

        def upon_execute(query):
            with future_called_lock:
                if mock_response_future.called:
                    first_run = False
                else:
                    mock_response_future.called = True
                    first_run = True
            if first_run:
                mock_response_future.result()
                mock_response_future.executed = True
            mock_response_future.returned_queries.append(query)

        try:
            mock_kusto_client = MockKustoClient(upon_execute=upon_execute, record_metadata=True)
            table = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']['mock_table']
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
                RecordedQuery('test_db', '.show table mock_table | project AttributeName, AttributeType | limit 10000'),
                RecordedQuery('test_db', 'mock_table | take 5'),
            ],
            mock_response_future.returned_queries,
        )

    def test_table_fetch(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([('mock_table', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)])]),
            record_metadata=True,
        )
        db = PyKustoClient(mock_kusto_client, fetch_by_default=False)['test_db']
        db.blocking_refresh()
        self.assertEqual(
            [RecordedQuery('test_db', '.show database schema | project TableName, ColumnName, ColumnType | limit 10000')],
            mock_kusto_client.recorded_queries,
        )
        table = db.mock_table
        # Table columns
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table['baz']), _AnyTypeColumn)
        # Bracket notation
        self.assertEqual(type(db['other_table']['foo']), _AnyTypeColumn)
        # Dot notation error
        self.assertRaises(
            AttributeError("PyKustoClient(test_cluster.kusto.windows.net).Database(test_db) has no attribute 'test_table_1'"),
            lambda: db.test_table_1
        )

    def test_two_tables_fetch(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
                ('test_table_2', [('baz', _KustoType.BOOL)])
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
        self.assertEqual(type(db.test_table_1.foo), _StringColumn)
        self.assertEqual(type(db.test_table_1.bar), _NumberColumn)
        self.assertEqual(type(db.test_table_2['baz']), _BooleanColumn)
        self.assertEqual(type(db['other_table']['foo']), _AnyTypeColumn)
        # Union
        table = db.get_table('test_table_1', 'test_table_2')
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table.baz), _BooleanColumn)
        # Wildcard
        table = db.get_table('test_table_*')
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table.baz), _BooleanColumn)

    def test_union_column_name_conflict(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
                ('test_table_2', [('foo', _KustoType.BOOL)])
            ]),
            getschema_response=mock_getschema_response([
                ('foo_string', _KustoType.STRING), ('bar', _KustoType.INT), ('foo_bool', _KustoType.BOOL)
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
        self.assertEqual(type(table.foo_string), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table.foo_bool), _BooleanColumn)

    def test_union_wildcard_one_table(self):
        mock_kusto_client = MockKustoClient(
            tables_response=mock_tables_response([
                ('test_table_1', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
                ('other_table_2', [('baz', _KustoType.BOOL)])
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
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table['baz']), _AnyTypeColumn)

    def test_database_fetch(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('mock_table', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)])])]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        self.assertEqual(
            [RecordedQuery('', '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')],
            mock_kusto_client.recorded_queries,
        )
        # Table columns
        table = client.test_db.mock_table
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _NumberColumn)
        self.assertEqual(type(table['baz']), _AnyTypeColumn)
        self.assertEqual(type(client.test_db['other_table']['foo']), _AnyTypeColumn)
        # Various utility methods
        db = client.get_database('test_db')
        self.assertEqual(type(db), _Database)
        self.assertEqual('test_db', db.get_name())
        self.assertEqual(('test_db',), tuple(client.get_databases_names()))
        self.assertEqual(('mock_table', 'other_table'), tuple(client.test_db.get_table_names()))
        self.assertEqual(('foo', 'bar', 'baz'), tuple(client.test_db.mock_table.get_columns_names()))
        self.assertTrue({'foo', 'bar'} < set(dir(client.test_db.mock_table)))
        self.assertEqual('PyKustoClient(test_cluster.kusto.windows.net).Database(test_db).Table(mock_table)', repr(client.test_db.mock_table))

    def test_autocomplete_with_dot(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('mock_table', [('foo', _KustoType.STRING), ('bar.baz', _KustoType.INT)])])]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        # Table columns
        table = client.test_db.mock_table
        self.assertEqual(type(table.foo), _StringColumn)
        self.assertEqual(type(table.bar), _AnyTypeColumn)
        self.assertEqual(type(table['bar.baz']), _NumberColumn)
        autocomplete_list = set(dir(client.test_db.mock_table))
        self.assertIn('foo', autocomplete_list)
        self.assertNotIn('bar.baz', autocomplete_list)

    def test_empty_database(self):
        mock_kusto_client = MockKustoClient(
            databases_response=mock_databases_response([
                ('test_db', [('mock_table', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)])]),
                ('', [('test_table1', [('foo1', _KustoType.STRING), ('bar1', _KustoType.INT)])])
            ]),
            record_metadata=True,
        )
        client = PyKustoClient(mock_kusto_client)
        client.wait_for_items()
        self.assertEqual(
            [RecordedQuery('', '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')],
            mock_kusto_client.recorded_queries,
        )
        self.assertEqual(type(client.test_db.mock_table.foo), _StringColumn)

    def test_client_not_fetched(self):
        client = PyKustoClient(MockKustoClient(), fetch_by_default=False)
        self.assertEqual(frozenset(), set(client.get_databases_names()))
        self.assertEqual(frozenset(), set(client.get_databases()))
