from collections import defaultdict
from typing import Union, List, Tuple, Dict, Any
from urllib.parse import urlparse

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from pykusto.expressions import BaseColumn, AnyTypeColumn
from pykusto.kql_converters import KQL
from pykusto.retriever import Retriever
from pykusto.type_utils import INTERNAL_NAME_TO_TYPE, typed_column, DOT_NAME_TO_TYPE


class PyKustoClient(Retriever):
    """
    Handle to a Kusto cluster.
    Uses :class:`Retriever` to retrieve the full cluster schema, including all databases, tables, columns and their types.
    """
    _client: KustoClient
    _cluster_name: str

    def __init__(self, client_or_cluster: Union[str, KustoClient], retrieve_by_default: bool = True) -> None:
        """
        Create a new handle to Kusto cluster. The value of "retrieve_by_default" is used for current instance, and also passed on to database instances.

        :param client_or_cluster: Either a KustoClient object, or a cluster name. In case a cluster name is given,
            a KustoClient is generated with AAD device authentication
        """
        self._set_client(client_or_cluster)
        super().__init__(None, retrieve_by_default)

    def _set_client(self, client_or_cluster):
        if isinstance(client_or_cluster, KustoClient):
            self._client = client_or_cluster
            # noinspection PyProtectedMember
            self._cluster_name = urlparse(client_or_cluster._query_endpoint).netloc  # TODO neater way
        else:
            self._client = self._get_client_for_cluster(client_or_cluster)
            self._cluster_name = client_or_cluster

    def __repr__(self) -> str:
        return f'PyKustoClient({self._cluster_name})'

    def _new_item(self, name: str) -> 'Database':
        # "retrieve_by_default" set to false because often a database generated this way is not represented by an actual Kusto database
        return Database(self, name, retrieve_by_default=False)

    def get_database(self, name: str) -> 'Database':
        return self[name]

    def execute(self, database: str, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self._client.execute(database, query, properties)

    def show_databases(self) -> Tuple[str, ...]:
        return tuple(self._items.keys())

    def get_cluster_name(self) -> str:
        return self._cluster_name

    @staticmethod
    def _get_client_for_cluster(cluster: str) -> KustoClient:
        return KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))

    def _internal_get_items(self) -> Dict[str, 'Database']:
        res: KustoResponseDataSet = self.execute('', KQL('.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000'))
        database_to_table_to_columns = defaultdict(lambda: defaultdict(list))
        for database_name, table_name, column_name, column_type in res.primary_results[0].rows:
            if is_empty(database_name) or is_empty(table_name) or is_empty(column_name):
                continue
            database_to_table_to_columns[database_name][table_name].append(typed_column.registry[DOT_NAME_TO_TYPE[column_type]](column_name))
        return {
            # Database instances are provided with all table and column data, preventing them from generating more queries. However the "retrieve_by_default" behavior is
            # passed on to them for future actions.
            database_name: Database(
                self, database_name, {table_name: tuple(columns) for table_name, columns in table_to_columns.items()}, retrieve_by_default=self._retrieve_by_default
            )
            for database_name, table_to_columns in database_to_table_to_columns.items()
        }


class Database(Retriever):
    """
    Handle to a Kusto database.
    Uses :class:`Retriever` to retrieve the full database schema, including all tables, columns and their types.
    """
    client: PyKustoClient
    name: str

    def __init__(self, client: PyKustoClient, name: str, tables: Dict[str, Tuple[BaseColumn]] = None, retrieve_by_default: bool = True) -> None:
        """
        Create a new handle to Kusto database. The value of "retrieve_by_default" is used for current instance, and also passed on to database instances.

        :param client: The associated PyKustoClient instance
        :param name: Database name
        :param tables: A mapping from table names to the columns of each table. If this is None and "retrieve_by_default" is true then they will be retrieved in the constructor.
        """
        super().__init__(
            # Providing the items to Retriever prevents further queries until the "refresh" method is explicitly called
            None if tables is None else {table_name: Table(self, table_name, columns, retrieve_by_default=retrieve_by_default) for table_name, columns in tables.items()},
            retrieve_by_default
        )
        self.client = client
        self.name = name

    def __repr__(self) -> str:
        return f'{self.client}.Database({self.name})'

    def _new_item(self, name: str) -> 'Table':
        # "retrieve_by_default" set to false because often a table generated this way is not represented by an actual Kusto table
        return Table(self, name, retrieve_by_default=False)

    def execute(self, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self.client.execute(self.name, query, properties)

    def show_tables(self) -> Tuple[str, ...]:
        return tuple(self._items.keys())

    def get_tables(self, *tables: str):
        return Table(self, tables, retrieve_by_default=self._retrieve_by_default)

    def _internal_get_items(self) -> Dict[str, 'Table']:
        res: KustoResponseDataSet = self.execute(KQL('.show database schema | project TableName, ColumnName, ColumnType | limit 10000'))
        table_to_columns = defaultdict(list)
        for table_name, column_name, column_type in res.primary_results[0].rows:
            if is_empty(table_name) or is_empty(column_name):
                continue
            table_to_columns[table_name].append(typed_column.registry[DOT_NAME_TO_TYPE[column_type]](column_name))
        # Table instances are provided with all column data, preventing them from generating more queries. However the "retrieve_by_default" behavior is
        # passed on to them for future actions.
        return {table_name: Table(self, table_name, tuple(columns), retrieve_by_default=self._retrieve_by_default) for table_name, columns in table_to_columns.items()}


class Table(Retriever):
    """
    Handle to a Kusto table.
    Uses :class:`Retriever` to retrieve the table schema of columns and their types.
    """
    database: Database
    tables: Tuple[str, ...]

    def __init__(self, database: Database, tables: Union[str, List[str], Tuple[str, ...]], columns: Tuple[BaseColumn, ...] = None, retrieve_by_default: bool = True) -> None:
        """
        Create a new handle to a Kusto table.

        :param database: The associated Database instance
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        :param columns: Table columns. If this is None and "retrieve_by_default" is true then they will be retrieved in the constructor.
        """
        super().__init__(
            None if columns is None else {c.get_name(): c for c in columns},
            retrieve_by_default
        )
        self.database = database
        self.tables = (tables,) if isinstance(tables, str) else tuple(tables)

    def __repr__(self) -> str:
        return f'{self.database}.Table({self.get_table()})'

    def _new_item(self, name: str) -> BaseColumn:
        return AnyTypeColumn(name)

    def __getattr__(self, name: str) -> Any:
        """
        Convenience function for retrieving a column using dot notation.
        In contrast with the overridden method from the :class:`Retriever` class, a new column is generated if needed, since new columns can be created on the fly in the course of
        the query (e.g. using 'extend'), and there is no fear of undesired erroneous queries sent to Kusto.

        :param name: Name of column
        :return: The retrieved column
        """
        return self[name]

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

    def _internal_get_items(self) -> Dict[str, Any]:
        # TODO: Handle unions
        res: KustoResponseDataSet = self.execute(KQL('.show table {} | project AttributeName, AttributeType | limit 10000'.format(self.get_table())))
        return {
            column_name: typed_column.registry[INTERNAL_NAME_TO_TYPE[column_type]](column_name)
            for column_name, column_type in res.primary_results[0].rows
        }


# There has to be code somewhere that already does this, but I didn't find it
def is_empty(s: str) -> bool:
    return s is None or len(s.strip()) == 0
