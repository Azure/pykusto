from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder

from pykusto.utils import KQL


class Table:
    """
    Handle to a Kusto table
    """
    client: KustoClient
    database: str
    table: KQL

    def __init__(self, client_or_cluster: Union[str, KustoClient], database: str,
                 tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        """
        Create a new handle to a Kusto table

        :param client_or_cluster: Either a KustoClient object, or a cluster name. In case a cluster name is given,
            a KustoClient is generated with aad device authentication
        :param database: Database name
        :param tables: Either a single table name, or a list of tables. If more than one table is given OR the table
            name contains a wildcard, the Kusto 'union' statement will be used.
        """
        if isinstance(client_or_cluster, KustoClient):
            self.client = client_or_cluster
        else:
            self.client = self._get_client_for_cluster(client_or_cluster)

        self.database = database

        if isinstance(tables, (List, Tuple)):
            self.table = KQL(', '.join(tables))
        else:
            self.table = KQL(tables)
        if '*' in self.table or ',' in self.table:
            self.table = KQL('union ' + self.table)

    def execute(self, rendered_query: str) -> KustoResponseDataSet:
        return self.client.execute(self.database, rendered_query)

    @staticmethod
    def _get_client_for_cluster(cluster: str) -> KustoClient:
        return KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))
