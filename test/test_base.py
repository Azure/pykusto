import json
import logging
import sys
from concurrent.futures import Future
from threading import Event
from typing import Callable, Tuple, Any, List, Optional, Union
from unittest import TestCase
# noinspection PyProtectedMember
from unittest.case import _AssertLogsContext
from urllib.parse import urljoin

from azure.kusto.data import KustoClient, ClientRequestProperties
# noinspection PyProtectedMember
from azure.kusto.data._models import KustoResultTable, KustoResultRow
from azure.kusto.data.response import KustoResponseDataSet

# noinspection PyProtectedMember
from pykusto._src.client import _Table
# noinspection PyProtectedMember
from pykusto._src.expressions import _NumberColumn, _BooleanColumn, _ArrayColumn, _MappingColumn, _StringColumn, _DatetimeColumn, _TimespanColumn, _DynamicColumn
# noinspection PyProtectedMember
from pykusto._src.type_utils import _KustoType

# Naming this variable "test_table" triggers the following bug: https://github.com/pytest-dev/pytest/issues/7378
# noinspection PyTypeChecker
mock_table = _Table(
    None, "mock_table",
    (
        _NumberColumn('numField'), _NumberColumn('numField2'), _NumberColumn('numField3'), _NumberColumn('numField4'), _NumberColumn('numField5'), _NumberColumn('numField6'),
        _BooleanColumn('boolField'), _ArrayColumn('arrayField'), _ArrayColumn('arrayField2'), _ArrayColumn('arrayField3'), _MappingColumn('mapField'), _StringColumn('stringField'),
        _StringColumn('stringField2'), _DatetimeColumn('dateField'), _DatetimeColumn('dateField2'), _DatetimeColumn('dateField3'), _TimespanColumn('timespanField'),
        _DynamicColumn('dynamicField')
    )
)


class TestBase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG,
            format='%(asctime)s %(levelname)5s %(message)s'
        )

    def setUp(self) -> None:
        test_logger.info("Running test: " + self._testMethodName)

    def assertRaises(self, expected_exception: BaseException, test_callable: Callable, *args, **kwargs):
        """
        This method overrides the one in `unittest.case.TestCase`.

        Instead of providing it with an exception type, you provide it with an exception instance that contains
        the expected message.
        """
        expected_exception_type = type(expected_exception)
        expected_exception_message = str(expected_exception)
        with super().assertRaises(expected_exception_type) as cm:
            test_callable(*args, **kwargs)
        self.assertEqual(
            expected_exception_message,
            str(cm.exception)
        )

    def assertLogs(self, logger_to_watch=None, level=None) -> 'CustomAssertLogsContext':
        """
        This method overrides the one in `unittest.case.TestCase`, and has the same behavior, except for not causing a failure when there are no log messages.
        The point is to allow asserting there are no logs.
        Get rid of this once this is resolved: https://github.com/python/cpython/pull/18067
        """
        # noinspection PyArgumentList
        return CustomAssertLogsContext(self, logger_to_watch, level)

    @staticmethod
    def raise_mock_exception():
        raise Exception("Mock exception")


class CustomAssertLogsContext(_AssertLogsContext):
    # noinspection PyUnresolvedReferences
    def __exit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        # Fool the original exit method to think there is at least one record, to avoid causing a failure
        self.watcher.records.append("DUMMY")
        result = super().__exit__(exc_type, exc_val, exc_tb)
        self.watcher.records.pop()
        return result


# noinspection PyMissingConstructor
class MockKustoResultTable(KustoResultTable):
    def __init__(self, rows: Tuple[Any, ...], columns: Tuple[str, ...]):
        self.rows = tuple(KustoResultRow(columns, row) for row in rows)
        self.columns = tuple(type('Column', (object,), {'column_name': col, 'column_type': ''}) for col in columns)


# noinspection PyTypeChecker
def mock_response(rows: Tuple[Any, ...], columns: Tuple[str, ...] = tuple()) -> Callable[[], KustoResponseDataSet]:
    return lambda: type(
        'MockKustoResponseDataSet',
        (KustoResponseDataSet,),
        {'primary_results': (MockKustoResultTable(rows, columns),)}
    )


def mock_columns_response(columns: List[Tuple[str, _KustoType]] = tuple()) -> Callable[[], KustoResponseDataSet]:
    return mock_response(tuple((c_name, c_type.internal_name) for c_name, c_type in columns), ('ColumnName', 'ColumnType'))


def mock_tables_response(tables: List[Tuple[str, List[Tuple[str, _KustoType]]]] = tuple()) -> Callable[[], KustoResponseDataSet]:
    return mock_response(
        tuple((t_name, c_name, c_type.dot_net_name) for t_name, columns in tables for c_name, c_type in columns),
        ('TableName', 'ColumnName', 'ColumnType')
    )


def mock_databases_response(databases: List[Tuple[str, List[Tuple[str, List[Tuple[str, _KustoType]]]]]] = tuple()) -> Callable[[], KustoResponseDataSet]:
    return mock_response(
        tuple(
            (d_name, t_name, c_name, c_type.dot_net_name)
            for d_name, tables in databases
            for t_name, columns in tables
            for c_name, c_type in columns
        ),
        ('DatabaseName', 'TableName', 'ColumnName', 'ColumnType')
    )


def mock_getschema_response(columns: List[Tuple[str, _KustoType]] = tuple()) -> Callable[[], KustoResponseDataSet]:
    return mock_response(tuple((c_name, c_type.dot_net_name) for c_name, c_type in columns), ('ColumnName', 'DataType'))


class RecordedQuery:
    database: str
    query: str
    properties: ClientRequestProperties

    def __init__(self, database: str, query: str, properties: ClientRequestProperties = None):
        self.database = database
        self.query = query
        self.properties = properties

    def __repr__(self) -> str:
        return json.dumps({'database': self.database, 'query': self.query, 'properties': self.properties})

    def __eq__(self, o: 'RecordedQuery') -> bool:
        if not isinstance(o, RecordedQuery):
            return False
        return self.database == o.database and self.query == o.query and self.properties == o.properties


# noinspection PyMissingConstructor
class MockKustoClient(KustoClient):
    recorded_queries: List[RecordedQuery]
    columns_response: Callable[[], KustoResponseDataSet]
    tables_response: Callable[[], KustoResponseDataSet]
    databases_response: Callable[[], KustoResponseDataSet]
    getschema_response: Callable[[], KustoResponseDataSet]
    main_response: Callable[[], KustoResponseDataSet]
    record_metadata: bool
    block: bool
    query_future: Union[None, Future]
    blocked_event: Event

    def __init__(
            self,
            cluster="https://test_cluster.kusto.windows.net",
            columns_response: Callable[[], KustoResponseDataSet] = mock_columns_response([('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
            tables_response: Callable[[], KustoResponseDataSet] = mock_tables_response([
                ('mock_table', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)]),
                ('mock_table_2', [('baz', _KustoType.BOOL)]),
            ]),
            databases_response: Callable[[], KustoResponseDataSet] = mock_databases_response(
                [('test_db', [('mock_table', [('foo', _KustoType.STRING), ('bar', _KustoType.INT)])])]
            ),
            getschema_response: Callable[[], KustoResponseDataSet] = mock_getschema_response([]),
            main_response: Callable[[], KustoResponseDataSet] = mock_response(tuple()),
            record_metadata: bool = False,
            block: bool = False,
    ):
        self.recorded_queries = []
        self._query_endpoint = urljoin(cluster, "/v2/rest/query")
        self.columns_response = columns_response
        self.tables_response = tables_response
        self.databases_response = databases_response
        self.getschema_response = getschema_response
        self.main_response = main_response
        self.record_metadata = record_metadata
        if block:
            self.block = True
            self.query_future = Future()
        else:
            self.block = False
            self.query_future = None
        self.blocked_event = Event()

    def release(self):
        assert self.blocked()
        if self.query_future is not None:
            self.query_future.set_result(None)

    def blocked(self):
        return self.blocked_event.is_set()

    def wait_until_blocked(self):
        self.blocked_event.wait()

    def do_not_block_next_requests(self):
        self.block = False

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        recorded_query = RecordedQuery(database, rendered_query, properties)
        if self.block:
            self.blocked_event.set()
            self.query_future.result()
            self.blocked_event.clear()
        metadata_query = True
        if rendered_query == '.show database schema | project TableName, ColumnName, ColumnType | limit 10000':
            response = self.tables_response
        elif rendered_query.startswith('.show table '):
            response = self.columns_response
        elif rendered_query.startswith('.show databases schema '):
            response = self.databases_response
        elif rendered_query.endswith(' | getschema | project ColumnName, DataType | limit 10000'):
            response = self.getschema_response
        else:
            metadata_query = False
            response = self.main_response
        if self.record_metadata or not metadata_query:
            self.recorded_queries.append(recorded_query)
        return response()



test_logger = logging.getLogger("pykusto_test")
