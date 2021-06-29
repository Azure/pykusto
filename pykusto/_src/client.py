from collections import defaultdict
from fnmatch import fnmatch
from threading import Lock
from typing import Union, List, Tuple, Dict, Generator, Optional, Set, Type, Callable, Iterable
from urllib.parse import urlparse

import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSet
from redo import retrier

from .expressions import BaseColumn, _AnyTypeColumn
from .item_fetcher import _ItemFetcher
from .kql_converters import KQL
from .logger import _logger
from .type_utils import _INTERNAL_NAME_TO_TYPE, _typed_column, _DOT_NAME_TO_TYPE


class RetryConfig:
    def __init__(
            self, attempts: int = 5, sleep_time: float = 60, max_sleep_time: float = 300, sleep_scale: float = 1.5, jitter: float = 1,
            retry_exceptions: Tuple[Type[Exception], ...] = (KustoServiceError,)
    ) -> None:
        """
        All time parameters are in seconds
        """
        self.attempts = attempts
        self.sleep_time = sleep_time
        self.max_sleep_time = max_sleep_time
        self.sleep_scale = sleep_scale
        self.jitter = jitter
        self.retry_exceptions = retry_exceptions

    def retry(self, action: Callable):
        attempt = 1
        for sleep_time in retrier(attempts=self.attempts, sleeptime=self.sleep_time, max_sleeptime=self.max_sleep_time, sleepscale=self.sleep_scale, jitter=self.jitter):
            try:
                return action()
            except Exception as e:
                for exception_to_check in self.retry_exceptions:
                    if isinstance(e, exception_to_check):
                        if attempt == self.attempts:
                            _logger.warning(f"Reached maximum number of attempts ({self.attempts}), raising exception")
                            raise
                        _logger.info(
                            f"Attempt number {attempt} out of {self.attempts} failed, "
                            f"previous sleep time was {sleep_time} seconds. Exception: {e.__class__.__name__}('{str(e)}')"
                        )
                        break
                else:
                    raise
            attempt += 1


NO_RETRIES = RetryConfig(1)


class KustoResponse:
    __response: KustoResponseDataSet

    def __init__(self, response: KustoResponseDataSet):
        self.__response = response

    def get_rows(self) -> List[Iterable]:
        return self.__response.primary_results[0].rows

    @staticmethod
    def is_row_valid(row: Iterable) -> bool:
        for field in row:
            if field is None or (isinstance(field, str) and len(field.strip()) == 0):
                return False
        return True

    def get_valid_rows(self) -> Generator[Tuple, None, None]:
        for row in self.get_rows():
            if self.is_row_valid(row):
                yield tuple(row)

    def to_dataframe(self) -> pd.DataFrame:
        return dataframe_from_result_table(self.__response.primary_results[0])


class PyKustoClient(_ItemFetcher):
    """
    Handle to a Kusto cluster.
    Uses :class:`ItemFetcher` to fetch and cache the full cluster schema, including all databases, tables, columns and
    their types.
    """
    __client: KustoClient
    __cluster_name: str
    __first_execution: bool
    __first_execution_lock: Lock
    __retry_config: RetryConfig
    __auth_method: Callable[[str], KustoConnectionStringBuilder]

    __global_client_cache: Dict[str, KustoClient] = {}
    __global_cache_lock: Lock = Lock()

    def __init__(
            self, client_or_cluster: Union[str, KustoClient], fetch_by_default: bool = True, use_global_cache: bool = False,
            retry_config: RetryConfig = NO_RETRIES,
            auth_method: Optional[Callable[[str], KustoConnectionStringBuilder]] = KustoConnectionStringBuilder.with_az_cli_authentication,
    ) -> None:
        """
        Create a new handle to a Kusto cluster. The value of "fetch_by_default" is used for current instance, and also passed on to database instances.

        :param client_or_cluster: Either a KustoClient instance, or a cluster URL. In case a cluster URL is provided, a KustoClient is generated using the provided auth_method.
        :param use_global_cache: If true, share a global client cache between all instances. Provided for convenience during development, not recommended for general use.
        :param retry_config: An instance of RetryConfig which instructs the client how to perform retries in case of failure. The default is NO_RETRIES.
        :param auth_method: A method that returns a KustoConnectionStringBuilder for authentication. The default is 'KustoConnectionStringBuilder.with_az_cli_authentication'.
        A popular alternative is 'KustoConnectionStringBuilder.with_aad_device_authentication'
        """
        super().__init__(None, fetch_by_default)
        self.__first_execution = True
        self.__first_execution_lock = Lock()
        self.__retry_config = retry_config
        self.__auth_method = auth_method
        self._internal_init(client_or_cluster, use_global_cache)
        self._refresh_if_needed()

    def _internal_init(self, client_or_cluster: Union[str, KustoClient], use_global_cache: bool):
        if isinstance(client_or_cluster, KustoClient):
            self.__client = client_or_cluster
            # noinspection PyProtectedMember
            self.__cluster_name = urlparse(client_or_cluster._query_endpoint).netloc
            assert not use_global_cache, "Global cache not supported when providing your own client instance"
        else:
            self.__cluster_name = client_or_cluster
            self.__client = (self._cached_get_client_for_cluster if use_global_cache else self._get_client_for_cluster)()

    def __repr__(self) -> str:
        return f'PyKustoClient({self.__cluster_name})'

    def to_query_format(self) -> KQL:
        return KQL(f'cluster("{self.__cluster_name}")')

    def _new_item(self, name: str) -> 'Database':
        # "fetch_by_default" set to false because often a database generated this way is not represented by an actual
        # Kusto database
        return Database(self, name, fetch_by_default=False)

    def get_database(self, name: str) -> 'Database':
        return self[name]

    def execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        # The first execution usually triggers an authentication flow. We block all subsequent executions to prevent redundant authentications.
        # Remove the below block once this is resolved: https://github.com/Azure/azure-kusto-python/issues/208
        with self.__first_execution_lock:
            if self.__first_execution:
                self.__first_execution = False
                return self._internal_execute(database, query, properties, retry_config)
        return self._internal_execute(database, query, properties, retry_config)

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        resolved_retry_config = self.__retry_config if retry_config is None else retry_config
        return KustoResponse(resolved_retry_config.retry(lambda: self.__client.execute(database, query, properties)))

    def get_databases_names(self) -> Generator[str, None, None]:
        yield from self._get_item_names()

    def get_databases(self) -> Generator['Database', None, None]:
        yield from self._get_items()

    def get_cluster_name(self) -> str:
        return self.__cluster_name

    def _get_client_for_cluster(self) -> KustoClient:
        return KustoClient(self.__auth_method(self.__cluster_name))

    def _cached_get_client_for_cluster(self) -> KustoClient:
        """
        Provided for convenience during development, not recommended for general use.
        """
        with PyKustoClient.__global_cache_lock:
            client = PyKustoClient.__global_client_cache.get(self.__cluster_name)
            if client is None:
                client = self._get_client_for_cluster()
                PyKustoClient.__global_client_cache[self.__cluster_name] = client
                assert len(PyKustoClient.__global_client_cache) <= 1024, "Global client cache cannot exceed size of 1024"

        return client

    def _internal_get_items(self) -> Dict[str, 'Database']:
        # Retrieves database names, table names, column names and types for all databases. A database name is required
        # by the "execute" method, but is ignored for this query
        res: KustoResponse = self.execute(
            '', KQL('.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000')
        )
        database_to_table_to_columns = defaultdict(lambda: defaultdict(list))
        for database_name, table_name, column_name, column_type in res.get_valid_rows():
            database_to_table_to_columns[database_name][table_name].append(
                _typed_column.registry[_DOT_NAME_TO_TYPE[column_type]](column_name)
            )
        return {
            # Database instances are provided with all table and column data, preventing them from generating more
            # queries. However the "fetch_by_default" behavior is passed on to them for future actions.
            database_name: Database(
                self, database_name,
                {table_name: tuple(columns) for table_name, columns in table_to_columns.items()},
                fetch_by_default=self._fetch_by_default
            )
            for database_name, table_to_columns in database_to_table_to_columns.items()
        }


class Database(_ItemFetcher):
    """
    Handle to a Kusto database.
    Uses :class:`ItemFetcher` to fetch and cache the full database schema, including all tables, columns and their
    types.
    """
    __client: PyKustoClient
    __name: str

    def __init__(
            self, client: PyKustoClient, name: str, tables: Dict[str, Tuple[BaseColumn]] = None,
            fetch_by_default: bool = True
    ) -> None:
        """
        Create a new handle to Kusto database. The value of "fetch_by_default" is used for current instance, and also
        passed on to database instances.

        :param client: The associated PyKustoClient instance
        :param name: Database name
        :param tables: A mapping from table names to the columns of each table. If this is None and "fetch_by_default"
            is true then they will be fetched in the constructor.
        """
        super().__init__(
            # Providing the items to ItemFetcher prevents further queries until the "refresh" method is explicitly
            # called
            None if tables is None else {
                table_name: Table(self, table_name, columns, fetch_by_default=fetch_by_default)
                for table_name, columns in tables.items()
            },
            fetch_by_default
        )
        self.__client = client
        self.__name = name
        self._refresh_if_needed()

    def __repr__(self) -> str:
        return f'{self.__client}.Database({self.__name})'

    def to_query_format(self) -> KQL:
        return KQL(f'{self.__client.to_query_format()}.database("{self.__name}")')

    def get_name(self) -> str:
        return self.__name

    def _new_item(self, name: str) -> 'Table':
        # "fetch_by_default" set to false because often a table generated this way is not represented by an actual
        # Kusto table
        return Table(self, name, fetch_by_default=False)

    def execute(self, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        return self.__client.execute(self.__name, query, properties, retry_config)

    def get_table_names(self) -> Generator[str, None, None]:
        yield from self._get_item_names()

    def get_table(self, *tables: str) -> 'Table':
        assert len(tables) > 0
        if not Table.static_is_union(*tables):
            return self[tables[0]]
        columns: Optional[Tuple[BaseColumn, ...]] = None
        if self._items_fetched():
            resolved_tables: Set[Table] = set()
            for table_pattern in tables:
                if '*' in table_pattern:
                    resolved_tables.update(table for table in self._get_items() if fnmatch(table.get_name(), table_pattern))
                else:
                    resolved_tables.add(self[table_pattern])
            if len(resolved_tables) == 1:
                return next(iter(resolved_tables))
            columns = self.__try_to_resolve_union_columns(*resolved_tables)
        return Table(self, tables, columns, fetch_by_default=self._fetch_by_default)

    @staticmethod
    def __try_to_resolve_union_columns(*resolved_tables: 'Table') -> Optional[Tuple[BaseColumn, ...]]:
        column_by_name: Dict[str, BaseColumn] = {}
        for table in resolved_tables:
            for column in table.get_columns():
                existing_column = column_by_name.setdefault(column.get_name(), column)
                if type(column) is not type(existing_column):
                    return None  # Fallback to Kusto query for column name conflict resolution
        return tuple(column_by_name.values())

    def _internal_get_items(self) -> Dict[str, 'Table']:
        # Retrieves table names, column names and types for this database only (the database name is added in the
        # "execute" method)
        res: KustoResponse = self.execute(
            KQL('.show database schema | project TableName, ColumnName, ColumnType | limit 10000')
        )
        table_to_columns = defaultdict(list)
        for table_name, column_name, column_type in res.get_valid_rows():
            table_to_columns[table_name].append(_typed_column.registry[_DOT_NAME_TO_TYPE[column_type]](column_name))
        # Table instances are provided with all column data, preventing them from generating more queries. However the
        # "fetch_by_default" behavior is
        # passed on to them for future actions.
        return {
            table_name: Table(self, table_name, tuple(columns), fetch_by_default=self._fetch_by_default)
            for table_name, columns in table_to_columns.items()
        }


class Table(_ItemFetcher):
    """
    Handle to a Kusto table.
    Uses :class:`ItemFetcher` to fetch and cache the table schema of columns and their types.
    """
    __database: Database
    __tables: Tuple[str, ...]

    def __init__(
            self, database: Database, tables: Union[str, List[str], Tuple[str, ...]],
            columns: Tuple[BaseColumn, ...] = None, fetch_by_default: bool = True
    ) -> None:
        """
        Create a new handle to a Kusto table.

        :param database: The associated Database instance
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        :param columns: Table columns. If this is None and "ItemFetcher" is true then they will be fetched in the
            constructor.
        """
        super().__init__(
            None if columns is None else {c.get_name(): c for c in columns},
            fetch_by_default
        )
        self.__database = database
        self.__tables = (tables,) if isinstance(tables, str) else tuple(tables)
        assert len(self.__tables) > 0
        self._refresh_if_needed()

    def __repr__(self) -> str:
        return f'{self.__database}.Table({", ".join(self.__tables)})'

    def _new_item(self, name: str) -> BaseColumn:
        return _AnyTypeColumn(name)

    def __getattr__(self, name: str) -> BaseColumn:
        """
        Convenience function for obtaining a column using dot notation.
        In contrast with the overridden method from the :class:`ItemFetcher` class, a new column is generated if needed,
        since new columns can be created on the fly in the course of the query (e.g. using 'extend'), and there is no
        fear of undesired erroneous queries sent to Kusto.

        :param name: Name of column
        :return: The column with the given name
        """
        return self[name]

    @staticmethod
    def static_is_union(*table_names: str) -> bool:
        return len(table_names) > 1 or '*' in table_names[0]

    def is_union(self) -> bool:
        return self.static_is_union(*self.__tables)

    def get_name(self) -> str:
        assert not self.is_union()
        return self.__tables[0]

    def to_query_format(self, fully_qualified: bool = False) -> KQL:
        if fully_qualified:
            table_names = tuple(f'{self.__database.to_query_format()}.table("{table}")' for table in self.__tables)
        else:
            table_names = self.__tables
        if self.is_union():
            return KQL('union ' + ', '.join(table_names))
        return KQL(table_names[0])

    def execute(self, query: KQL, retry_config: RetryConfig = None) -> KustoResponse:
        return self.__database.execute(query, retry_config=retry_config)

    def get_columns_names(self) -> Generator[str, None, None]:
        yield from self._get_item_names()

    def get_columns(self) -> Generator[BaseColumn, None, None]:
        yield from self._get_items()

    def _internal_get_items(self) -> Dict[str, BaseColumn]:
        if not self.is_union():
            # Retrieves column names and types for this table only
            res: KustoResponse = self.execute(
                KQL(f'.show table {self.get_name()} | project AttributeName, AttributeType | limit 10000')
            )
            return {
                column_name: _typed_column.registry[_INTERNAL_NAME_TO_TYPE[column_type]](column_name)
                for column_name, column_type in res.get_valid_rows()
            }
        # Get Kusto to figure out the schema of the union, especially useful for column name conflict resolution
        res: KustoResponse = self.execute(
            KQL(f'{self.to_query_format()} | getschema | project ColumnName, DataType | limit 10000')
        )
        return {
            column_name: _typed_column.registry[_DOT_NAME_TO_TYPE[column_type]](column_name)
            for column_name, column_type in res.get_valid_rows()
        }
