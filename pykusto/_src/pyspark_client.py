from importlib.util import find_spec
from itertools import chain
from typing import List, Iterable, Dict, Callable, Union, Tuple

import pandas as pd
from azure.kusto.data import ClientRequestProperties, KustoClient

from pykusto import PyKustoClient, NO_RETRIES, KustoResponse, KQL, RetryConfig


class DataframeBasedKustoResponse(KustoResponse):
    __dataframe: pd.DataFrame

    # noinspection PyMissingConstructor
    def __init__(self, dataframe: pd.DataFrame):
        self.__dataframe = dataframe

    def get_rows(self) -> List[Iterable]:
        # noinspection PyTypeChecker
        return self.__dataframe.values.tolist()

    def to_dataframe(self) -> pd.DataFrame:
        return self.__dataframe


class PySparkKustoClient(PyKustoClient):
    def __init__(self, cluster: str, linked_service: str = None, fetch_by_default: bool = True, use_global_cache: bool = False) -> None:
        self.__linked_service = linked_service
        super().__init__(cluster, fetch_by_default, use_global_cache, NO_RETRIES, None)

    def _internal_init(self, client_or_cluster: Union[str, KustoClient], use_global_cache: bool):
        assert isinstance(client_or_cluster, str), "PySparkKustoClient must be initialized with a cluster name"
        self.__cluster_name = client_or_cluster
        self.__options: Dict[str, str] = {}
        self.__option_producers: Dict[str, Callable[[], str]] = {}
        self.__kusto_session, spark_context = self.get_spark_session_and_context()

        if self.__linked_service is None:
            # noinspection PyProtectedMember
            device_auth = spark_context._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(self.__cluster_name, "common")
            print(device_auth.getDeviceCodeMessage())
            self.__format = 'com.microsoft.kusto.spark.datasource'
            self.option('kustoCluster', self.__cluster_name)
            self.option('accessToken', device_auth.acquireToken)
        else:
            self.__format = 'com.microsoft.kusto.spark.synapse.datasource'
            self.option('spark.synapse.linkedService', self.__linked_service)

    # noinspection PyUnresolvedReferences,PyPackageRequirements
    @staticmethod
    def get_spark_session_and_context() -> Tuple['pyspark.sql.session.SparkSession', 'pyspark.context.SparkContext']:  # noqa: F821  # pragma: no cover
        if find_spec('pyspark') is None:
            raise RuntimeError("pyspark package not found. PySparkKustoClient can only be used inside a PySpark notebook")
        from pyspark.sql import SparkSession
        from pyspark.context import SparkContext
        return SparkSession.builder.appName("kustoPySpark").getOrCreate(), SparkContext.getOrCreate()

    def option(self, key: str, value: Union[str, Callable[[], str]]) -> 'PySparkKustoClient':
        if isinstance(value, str):
            self.__options[key] = value
        else:
            self.__option_producers[key] = value
        return self

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        resolved_options = chain(
            self.__options.items(),
            ((key, value_producer()) for key, value_producer in self.__option_producers.items()),
            (('kustoDatabase', database), ('kustoQuery', query))
        )
        kusto_read_session = self.__kusto_session.read.format(self.__format)
        for key, value in resolved_options:
            kusto_read_session = kusto_read_session.option(key, value)
        return DataframeBasedKustoResponse(kusto_read_session.load())
