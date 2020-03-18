import logging
import sys
from typing import Callable, Tuple, Any, List
from unittest import TestCase
from urllib.parse import urljoin

from azure.kusto.data._models import KustoResultTable, KustoResultRow
from azure.kusto.data.request import KustoClient, ClientRequestProperties

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
    def setUpClass(cls):
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG,
            format='%(asctime)s %(levelname)5s %(message)s'
        )

    def setUp(self):
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


# noinspection PyMissingConstructor
class MockKustoResultTable(KustoResultTable):
    def __init__(self, rows: Tuple[Any, ...], columns: Tuple[str, ...]):
        self.rows = tuple(KustoResultRow(columns, row) for row in rows)
        self.columns = tuple(type('Column', (object,), {'column_name': col, 'column_type': ''}) for col in columns)


def mock_response(rows: Tuple[Any, ...], columns: Tuple[str, ...] = tuple()):
    return type(
        'KustoResponseDataSet',
        (object,),
        {'primary_results': (MockKustoResultTable(rows, columns),)}
    )


def mock_columns_response(columns: List[Tuple[str, KustoType]] = tuple()) -> Callable:
    return lambda: mock_response(tuple((c_name, c_type.internal_name) for c_name, c_type in columns), ('ColumnName', 'ColumnType'))


def mock_tables_response(tables: List[Tuple[str, List[Tuple[str, KustoType]]]] = tuple()) -> Callable:
    return lambda: mock_response(
        tuple((t_name, c_name, c_type.dot_net_name) for t_name, columns in tables for c_name, c_type in columns),
        ('TableName', 'ColumnName', 'ColumnType')
    )


def mock_databases_response(databases: List[Tuple[str, List[Tuple[str, List[Tuple[str, KustoType]]]]]] = tuple()) -> Callable:
    return lambda: mock_response(
        tuple(
            (d_name, t_name, c_name, c_type.dot_net_name)
            for d_name, tables in databases
            for t_name, columns in tables
            for c_name, c_type in columns
        ),
        ('DatabaseName', 'TableName', 'ColumnName', 'ColumnType')
    )


# noinspection PyMissingConstructor
class MockKustoClient(KustoClient):
    executions: List[Tuple[str, str, ClientRequestProperties]]
    columns_response: Callable
    tables_response: Callable
    databases_response: Callable
    main_response: Callable

    def __init__(
            self,
            cluster="https://test_cluster.kusto.windows.net",
            columns_response: Callable = mock_columns_response([]),
            tables_response: Callable = mock_tables_response([]),
            databases_response: Callable = mock_databases_response([]),
            main_response: Callable = mock_response(tuple()),
    ):
        self.executions = []
        self._query_endpoint = urljoin(cluster, "/v2/rest/query")
        self.columns_response = columns_response
        self.tables_response = tables_response
        self.databases_response = databases_response
        self.main_response = main_response

    def execute(self, database: str, rendered_query: str, properties: ClientRequestProperties = None):
        if rendered_query == '.show database schema | project TableName, ColumnName, ColumnType | limit 10000':
            return self.tables_response()
        if rendered_query.startswith('.show table '):
            return self.columns_response()
        if rendered_query.startswith('.show databases schema '):
            return self.databases_response()
        self.executions.append((database, rendered_query, properties))
        return self.main_response()
