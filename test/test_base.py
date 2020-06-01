import json
import logging
import sys
from typing import Callable, Tuple, Any, List, Optional
from unittest import TestCase
# noinspection PyProtectedMember
from unittest.case import _AssertLogsContext
from urllib.parse import urljoin

from azure.kusto.data import KustoClient, ClientRequestProperties
# noinspection PyProtectedMember
from azure.kusto.data._models import KustoResultTable, KustoResultRow
from azure.kusto.data.response import KustoResponseDataSet

from pykusto.client import Table
from pykusto.expressions import NumberColumn, BooleanColumn, ArrayColumn, MappingColumn, StringColumn, DatetimeColumn, TimespanColumn, DynamicColumn
from pykusto.logger import logger
from pykusto.type_utils import KustoType

# noinspection PyTypeChecker
test_table = Table(
    None, "test_table",
    (
        NumberColumn('numField'), NumberColumn('numField2'), NumberColumn('numField3'), NumberColumn('numField4'), NumberColumn('numField5'), NumberColumn('numField6'),
        BooleanColumn('boolField'), ArrayColumn('arrayField'), ArrayColumn('arrayField2'), ArrayColumn('arrayField3'), MappingColumn('mapField'), StringColumn('stringField'),
        StringColumn('stringField2'), DatetimeColumn('dateField'), DatetimeColumn('dateField2'), DatetimeColumn('dateField3'), TimespanColumn('timespanField'),
        DynamicColumn('dynamicField')
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
        logger.info("Running test: " + self._testMethodName)

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
def mock_response(rows: Tuple[Any, ...], columns: Tuple[str, ...] = tuple()) -> KustoResponseDataSet:
    return type(
        'MockKustoResponseDataSet',
        (KustoResponseDataSet,),
        {'primary_results': (MockKustoResultTable(rows, columns),)}
    )


def mock_columns_response(columns: List[Tuple[str, KustoType]] = tuple()) -> KustoResponseDataSet:
    return mock_response(tuple((c_name, c_type.internal_name) for c_name, c_type in columns), ('ColumnName', 'ColumnType'))


def mock_tables_response(tables: List[Tuple[str, List[Tuple[str, KustoType]]]] = tuple()) -> KustoResponseDataSet:
    return mock_response(
        tuple((t_name, c_name, c_type.dot_net_name) for t_name, columns in tables for c_name, c_type in columns),
        ('TableName', 'ColumnName', 'ColumnType')
    )


def mock_databases_response(databases: List[Tuple[str, List[Tuple[str, List[Tuple[str, KustoType]]]]]] = tuple()) -> KustoResponseDataSet:
    return mock_response(
        tuple(
            (d_name, t_name, c_name, c_type.dot_net_name)
            for d_name, tables in databases
            for t_name, columns in tables
            for c_name, c_type in columns
        ),
        ('DatabaseName', 'TableName', 'ColumnName', 'ColumnType')
    )


def mock_getschema_response(columns: List[Tuple[str, KustoType]] = tuple()) -> KustoResponseDataSet:
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
    columns_response: KustoResponseDataSet
    tables_response: KustoResponseDataSet
    databases_response: KustoResponseDataSet
    getschema_response: KustoResponseDataSet
    main_response: KustoResponseDataSet
    upon_execute: Callable[[RecordedQuery], None]
    record_metadata: bool

    def __init__(
            self,
            cluster="https://test_cluster.kusto.windows.net",
            columns_response: KustoResponseDataSet = mock_columns_response([]),
            tables_response: KustoResponseDataSet = mock_tables_response([]),
            databases_response: KustoResponseDataSet = mock_databases_response([]),
            getschema_response: KustoResponseDataSet = mock_getschema_response([]),
            main_response: KustoResponseDataSet = mock_response(tuple()),
            upon_execute: Callable[[RecordedQuery], None] = None,
            record_metadata: bool = False
    ):
        self.recorded_queries = []
        self._query_endpoint = urljoin(cluster, "/v2/rest/query")
        self.columns_response = columns_response
        self.tables_response = tables_response
        self.databases_response = databases_response
        self.getschema_response = getschema_response
        self.main_response = main_response
        self.upon_execute = upon_execute
        self.record_metadata = record_metadata

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        recorded_query = RecordedQuery(database, rendered_query, properties)
        if self.upon_execute is not None:
            self.upon_execute(recorded_query)
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
        return response
