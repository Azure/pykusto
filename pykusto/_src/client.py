from multiprocessing import Lock
from typing import Iterable, Callable, Dict, Union, Optional
from urllib.parse import urlparse

import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSet

from .client_base import KustoResponseBase, PyKustoClientBase, RetryConfig, NO_RETRIES, ClientRequestProperties
from .kql_converters import KQL


class KustoResponse(KustoResponseBase):
    __response: KustoResponseDataSet

    def __init__(self, response: KustoResponseDataSet):
        self.__response = response

    def get_rows(self) -> Iterable[Iterable]:
        return self.__response.primary_results[0].rows

    def to_dataframe(self) -> pd.DataFrame:
        return dataframe_from_result_table(self.__response.primary_results[0])


class PyKustoClient(PyKustoClientBase):
    """
    Handle to a Kusto cluster.
    Uses :class:`ItemFetcher` to fetch and cache the full cluster schema, including all databases, tables, columns and
    their types.
    """
    __client: KustoClient
    __auth_method: Callable[[str], KustoConnectionStringBuilder]

    # Static members
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
        self.__auth_method = auth_method
        client_resolved = False
        if isinstance(client_or_cluster, KustoClient):
            self.__client = client_or_cluster
            client_resolved = True
            # noinspection PyProtectedMember
            cluster_name = urlparse(client_or_cluster._query_endpoint).netloc
            assert not use_global_cache, "Global cache not supported when providing your own client instance"
        else:
            cluster_name = client_or_cluster
        super().__init__(cluster_name, fetch_by_default, retry_config)
        if not client_resolved:
            self.__client = (self._cached_get_client_for_cluster if use_global_cache else self._get_client_for_cluster)()

    def __repr__(self) -> str:
        return f"PyKustoClient('{self._cluster_name}')"

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        resolved_retry_config = self._retry_config if retry_config is None else retry_config
        if resolved_retry_config is not None:
            resolved_retry_config = resolved_retry_config.retry_on(KustoServiceError)
        return KustoResponse(resolved_retry_config.retry(lambda: self.__client.execute(database, query, properties)))

    def _get_client_for_cluster(self) -> KustoClient:
        return KustoClient(self.__auth_method(self._cluster_name))

    def _cached_get_client_for_cluster(self) -> KustoClient:
        """
        Provided for convenience during development, not recommended for general use.
        """
        with PyKustoClient.__global_cache_lock:
            client = PyKustoClient.__global_client_cache.get(self._cluster_name)
            if client is None:
                client = self._get_client_for_cluster()
                PyKustoClient.__global_client_cache[self._cluster_name] = client
                assert len(PyKustoClient.__global_client_cache) <= 1024, "Global client cache cannot exceed size of 1024"

        return client
