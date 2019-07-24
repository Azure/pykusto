from typing import Union, List, Tuple

# noinspection PyProtectedMember
from azure.kusto.data._response import KustoResponseDataSet
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder

from pykusto.utils import KQL


class Table:
    client: KustoClient
    database: str
    table: KQL

    def __init__(self, client_or_cluster: Union[str, KustoClient], database: str,
                 tables: Union[str, List[str], Tuple[str, ...]]) -> None:
        if isinstance(client_or_cluster, KustoClient):
            self.client = client_or_cluster
        else:
            self.client = self.get_client_for_cluster(client_or_cluster)

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
    def get_client_for_cluster(cluster: str) -> KustoClient:
        return KustoClient(KustoConnectionStringBuilder.with_aad_device_authentication(cluster))
