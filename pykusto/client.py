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
    Handle to a Kusto cluster
    """
    _client: KustoClient
    _cluster_name: str

    def __init__(self, client_or_cluster: Union[str, KustoClient], retrieve_by_default: bool = True) -> None:
        """
        Create a new handle to Kusto cluster

        :param client_or_cluster: Either a KustoClient object, or a cluster name. In case a cluster name is given,
            a KustoClient is generated with AAD device authentication
        """
        if isinstance(client_or_cluster, KustoClient):
            self._client = client_or_cluster
            # noinspection PyProtectedMember
            self._cluster_name = urlparse(client_or_cluster._query_endpoint).netloc  # TODO neater way
        else:
            self._client = self._get_client_for_cluster(client_or_cluster)
            self._cluster_name = client_or_cluster
        self._items = None
        super().__init__(retrieve_by_default)

    def __repr__(self) -> str:
        return f'PyKustoClient({self._cluster_name})'

    def _new_item(self, name: str) -> 'Database':
        return Database(self, name, retrieve_by_default=self._retrieve_by_default)

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
            database_name: Database(
                self, database_name, {table_name: tuple(columns) for table_name, columns in table_to_columns.items()}, retrieve_by_default=self._retrieve_by_default
            )
            for database_name, table_to_columns in database_to_table_to_columns.items()
        }


class Database(Retriever):
    """
    Handle to a Kusto database
    """
    client: PyKustoClient
    name: str

    def __init__(self, client: PyKustoClient, name: str, tables: Dict[str, Tuple[BaseColumn]] = None, retrieve_by_default: bool = True) -> None:
        self.client = client
        self.name = name
        if tables is None:
            self._items = None
        else:
            self._items = {table_name: Table(self, table_name, columns, retrieve_by_default=retrieve_by_default) for table_name, columns in tables.items()}
        super().__init__(retrieve_by_default)

    def __repr__(self) -> str:
        return f'{self.client}.Database({self.name})'

    def _new_item(self, name: str) -> 'Table':
        return self.get_tables(name)

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
        return {table_name: Table(self, table_name, tuple(columns), retrieve_by_default=self._retrieve_by_default) for table_name, columns in table_to_columns.items()}


class Table(Retriever):
    """
    Handle to a Kusto table
    """
    database: Database
    tables: Tuple[str, ...]

    def __init__(self, database: Database, tables: Union[str, List[str], Tuple[str, ...]], columns: Tuple[BaseColumn, ...] = None, retrieve_by_default: bool = True) -> None:
        """
        Create a new handle to a Kusto table

        :param database: Database object
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        """
        self.database = database
        self.tables = (tables,) if isinstance(tables, str) else tuple(tables)
        if columns is None:
            self._items = None
        else:
            self._items = {c.get_name(): c for c in columns}
        super().__init__(retrieve_by_default)

    def __repr__(self) -> str:
        return f'{self.database}.Table({self.get_table()})'

    def _new_item(self, name: str) -> BaseColumn:
        return AnyTypeColumn(name)

    # For columns we allow creating new ones with dot notation, since new columns can be generated on the fly with 'extend'
    def __getattr__(self, name: str) -> Any:
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
