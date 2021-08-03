from typing import Iterable, Generator, Tuple

import pandas as pd
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSet

from pykusto._src.client_base import KustoResponseBase, PyKustoClientBase


class KustoResponse(KustoResponseBase):
    __response: KustoResponseDataSet

    def __init__(self, response: KustoResponseDataSet):
        self.__response = response

    def get_rows(self) -> Iterable[Iterable]:
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


class PyKustoClient(PyKustoClientBase):
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
        return f"PyKustoClient('{self.__cluster_name}')"

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        resolved_retry_config = self.__retry_config if retry_config is None else retry_config
        return KustoResponse(resolved_retry_config.retry(lambda: self.__client.execute(database, query, properties)))

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