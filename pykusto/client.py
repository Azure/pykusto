from collections import defaultdict
from concurrent.futures import Future, wait
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import chain
from threading import Lock
from typing import Union, List, Tuple, Dict, Iterable
from urllib.parse import urlparse

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from pykusto.expressions import BaseColumn, AnyTypeColumn
from pykusto.kql_converters import KQL
from pykusto.type_utils import INTERNAL_NAME_TO_TYPE, column, DOT_NAME_TO_TYPE

POOL = ThreadPoolExecutor(max_workers=4)


class PyKustoClient:
    """
    Handle to a Kusto cluster
    """
    _client: KustoClient
    _cluster_name: str
    _databases: Union[None, Dict[str, 'Database']]
    _databases_future: Union[None, Future]
    _lock: Lock

    def __init__(self, client_or_cluster: Union[str, KustoClient]) -> None:
        """
        Create a new handle to Kusto cluster

        :param client_or_cluster: Either a KustoClient object, or a cluster name. In case a cluster name is given,
            a KustoClient is generated with AAD device authentication
        """
        self._lock = Lock()
        self._databases_future = None
        if isinstance(client_or_cluster, KustoClient):
            self._client = client_or_cluster
            # noinspection PyProtectedMember
            self._cluster_name = urlparse(client_or_cluster._query_endpoint).netloc  # TODO neater way
        else:
            self._client = self._get_client_for_cluster(client_or_cluster)
            self._cluster_name = client_or_cluster
        self._databases = None
        self.refresh()

    def get_database(self, name: str) -> 'Database':
        if self._databases is None:
            return Database(self, name)
        resolved_database = self._databases.get(name)
        if resolved_database is None:
            return Database(self, name)
        return resolved_database

    def __getattr__(self, name: str) -> 'Database':
        return self.get_database(name)

    def __getitem__(self, name: str) -> 'Database':
        return self.get_database(name)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self._databases is None else self._tables.keys()))

    def refresh(self):
        self._databases_future = POOL.submit(self._get_databases)
        self._databases_future.add_done_callback(self._set_databases)

    # For use mainly in tests
    def wait_for_databases(self):
        if self._databases_future is not None:
            wait((self._databases_future,))

    def execute(self, database: str, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self._client.execute(database, query, properties)

    def show_databases(self) -> Tuple[str, ...]:
        res: KustoResponseDataSet = self.execute('', KQL('.show databases'))
        return tuple(r[0] for r in res.primary_results[0].rows)

    def get_cluster_name(self) -> str:
        return self._cluster_name

    @staticmethod
    def _get_client_for_cluster(cluster: str) -> KustoClient:
        return KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))

    def _set_databases(self, databases_future: Future):
        self._databases = databases_future.result()

    def _get_databases(self) -> Dict[str, 'Database']:
        with self._lock:
            res: KustoResponseDataSet = self.execute('', KQL('.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000'))
            database_to_table_to_columns = defaultdict(lambda: defaultdict(list))
            for database_name, table_name, column_name, column_type in res.primary_results[0].rows:
                if is_empty(database_name) or is_empty(table_name) or is_empty(column_name):
                    continue
                database_to_table_to_columns[database_name][table_name].append(column.registry[DOT_NAME_TO_TYPE[column_type]](column_name))
            return {
                database_name: Database(self, database_name, {table_name: tuple(columns) for table_name, columns in table_to_columns.items()})
                for database_name, table_to_columns in database_to_table_to_columns.items()
            }


class Database:
    """
    Handle to a Kusto database
    """
    client: PyKustoClient
    name: str
    _tables: Union[None, Dict[str, 'Table']]
    _tables_future: Union[None, Future]
    _lock: Lock

    def __init__(self, client: PyKustoClient, name: str, tables: Dict[str, Tuple[BaseColumn]] = None) -> None:
        self._lock = Lock()
        self.client = client
        self.name = name
        self._tables_future = None
        if tables is None:
            self._tables = None
            self.refresh()
        else:
            self._tables = {table_name: Table(self, table_name, columns) for table_name, columns in tables.items()}

    def get_table(self, name: str) -> 'Table':
        if self._tables is None:
            return self.get_tables(name)
        resolved_table = self._tables.get(name)
        if resolved_table is None:
            return self.get_tables(name)
        return resolved_table

    def __getattr__(self, name: str) -> 'Table':
        return self.get_table(name)

    def __getitem__(self, name: str) -> 'Table':
        return self.get_table(name)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self._tables is None else self._tables.keys()))

    def refresh(self):
        self._tables_future = POOL.submit(self._get_tables)
        self._tables_future.add_done_callback(self._set_tables)

    # For use mainly in tests
    def wait_for_tables(self):
        if self._tables_future is not None:
            wait((self._tables_future,))

    def execute(self, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self.client.execute(self.name, query, properties)

    def show_tables(self) -> Tuple[str, ...]:
        res: KustoResponseDataSet = self.execute(KQL('.show tables'))
        return tuple(r[0] for r in res.primary_results[0].rows)

    def get_tables(self, *tables: str):
        return Table(self, tables)

    def _set_tables(self, tables_future: Future):
        self._tables = tables_future.result()

    def _get_tables(self) -> Dict[str, 'Table']:
        with self._lock:
            res: KustoResponseDataSet = self.execute(KQL('.show database schema | project TableName, ColumnName, ColumnType | limit 10000'))
            table_to_columns = defaultdict(list)
            for table_name, column_name, column_type in res.primary_results[0].rows:
                if is_empty(table_name) or is_empty(column_name):
                    continue
                table_to_columns[table_name].append(column.registry[DOT_NAME_TO_TYPE[column_type]](column_name))
            return {table_name: Table(self, table_name, tuple(columns)) for table_name, columns in table_to_columns.items()}


class Table:
    """
    Handle to a Kusto table
    """
    database: Database
    tables: Tuple[str, ...]
    _columns: Union[None, Dict[str, BaseColumn]]
    _columns_future: Union[None, Future]
    _lock: Lock

    def __init__(self, database: Database, tables: Union[str, List[str], Tuple[str, ...]], columns: Tuple[BaseColumn, ...] = None) -> None:
        """
        Create a new handle to a Kusto table

        :param database: Database object
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        """
        self._lock = Lock()
        self._columns_future = None
        self.database = database
        self.tables = (tables,) if isinstance(tables, str) else tuple(tables)
        if columns is None:
            self._columns = None
            self.refresh()
        else:
            self._columns = {c.get_name(): c for c in columns}

    def get_column(self, name: str) -> BaseColumn:
        if self._columns is None:
            return AnyTypeColumn(name)
        resolved_column = self._columns.get(name)
        if resolved_column is None:
            return AnyTypeColumn(name)
        return resolved_column

    def __getattr__(self, name: str) -> BaseColumn:
        return self.get_column(name)

    def __getitem__(self, name: str) -> BaseColumn:
        return self.get_column(name)

    def __dir__(self) -> Iterable[str]:
        return sorted(chain(super().__dir__(), tuple() if self._columns is None else self._columns.keys()))

    def refresh(self):
        self._columns_future = POOL.submit(self._get_columns)
        self._columns_future.add_done_callback(self._set_columns)

    # For use mainly in tests
    def wait_for_columns(self):
        if self._columns_future is not None:
            wait((self._columns_future,))

    def get_table(self) -> KQL:
        result = KQL(', '.join(self.tables))
        if '*' in result or ',' in result:
            result = KQL('union ' + result)
        return result

    def get_full_table(self) -> KQL:
        assert len(self.tables) > 0
        if len(self.tables) == 1 and not any('*' in t for t in self.tables):
            return self._format_full_table_name(self.tables[0])
        else:
            return KQL("union " + ", ".join(self._format_full_table_name(t) for t in self.tables))

    def _format_full_table_name(self, table):
        table_format_str = 'cluster("{}").database("{}").table("{}")'
        return KQL(
            table_format_str.format(self.database.client.get_cluster_name(), self.database.name, table))

    def execute(self, rendered_query: KQL) -> KustoResponseDataSet:
        return self.database.execute(rendered_query)

    def _set_columns(self, columns_future: Future):
        self._columns = columns_future.result()

    def _get_columns(self) -> Dict[str, BaseColumn]:
        with self._lock:
            # TODO: Handle unions
            res: KustoResponseDataSet = self.execute(KQL('.show table {} | project AttributeName, AttributeType | limit 10000'.format(self.get_table())))
            return {
                column_name: column.registry[INTERNAL_NAME_TO_TYPE[column_type]](column_name)
                for column_name, column_type in res.primary_results[0].rows
            }


# There has to be code somewhere that already does this, but I didn't find it
def is_empty(s: str) -> bool:
    return s is None or len(s.strip()) == 0
