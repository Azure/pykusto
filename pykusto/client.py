from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from pykusto.utils import KQL


class PyKustoClient:
    """
    Handle to a Kusto cluster
    """
    _client: KustoClient

    def __init__(self, client_or_cluster: Union[str, KustoClient]) -> None:
        """
        Create a new handle to Kusto cluster

        :param client_or_cluster: Either a KustoClient object, or a cluster name. In case a cluster name is given,
            a KustoClient is generated with AAD device authentication
        """
        if isinstance(client_or_cluster, KustoClient):
            self._client = client_or_cluster
        else:
            self._client = self._get_client_for_cluster(client_or_cluster)

    def execute(self, database: str, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self._client.execute(database, query, properties)

    def show_databases(self) -> Tuple[str, ...]:
        res: KustoResponseDataSet = self.execute('', KQL('.show databases'))
        return tuple(r[0] for r in res.primary_results[0].rows)

    def __getitem__(self, database_name: str) -> 'Database':
        return Database(self, database_name)

    @staticmethod
    def _get_client_for_cluster(cluster: str) -> KustoClient:
        return KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))


class Database:
    """
    Handle to a Kusto database
    """
    client: PyKustoClient
    name: str

    def __init__(self, client: PyKustoClient, name: str) -> None:
        self.client = client
        self.name = name

    def execute(self, query: KQL, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        return self.client.execute(self.name, query, properties)

    def show_tables(self) -> Tuple[str, ...]:
        res: KustoResponseDataSet = self.execute(KQL('.show tables'))
        return tuple(r[0] for r in res.primary_results[0].rows)

    def get_tables(self, *tables: str):
        return Table(self, tables)

    def __getitem__(self, table_name: str) -> 'Table':
        return self.get_tables(table_name)


class Table:
    """
    Handle to a Kusto table
    """
    database: Database
    table: KQL

    def __init__(self, database: Database, tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        """
        Create a new handle to a Kusto table

        :param database: Database object
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        """

        self.database = database

        if isinstance(tables, (List, Tuple)):
            self.table = KQL(', '.join(tables))
        else:
            self.table = KQL(tables)
        if '*' in self.table or ',' in self.table:
            self.table = KQL('union ' + self.table)

    def execute(self, rendered_query: KQL) -> KustoResponseDataSet:
        return self.database.execute(rendered_query)

    def show_columns(self) -> Tuple[Tuple[str, str], ...]:
        res: KustoResponseDataSet = self.execute(KQL('.show table {}'.format(self.table)))
        return tuple(
            (
                r[0],  # Column name
                r[1],  # Column type
            )
            for r in res.primary_results[0].rows
        )
