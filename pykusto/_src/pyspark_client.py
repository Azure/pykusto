from importlib.util import find_spec
from typing import List, Iterable, Dict, Callable, Union

import pandas as pd
from azure.kusto.data import ClientRequestProperties

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
        if find_spec('pyspark') is None:
            raise RuntimeError("pyspark package not found. PySparkKustoClient can only be used inside a PySpark notebook")

        super().__init__(cluster, fetch_by_default, use_global_cache, NO_RETRIES, None)  # TODO: postpone call to "_refresh_if_needed"
        self.__options: Dict[str, str] = {}
        self.__option_producers: Dict[str, Callable[[], str]] = {}
        self.__kusto_session = self.get_spark_session()

        if linked_service is None:
            # noinspection PyProtectedMember
            self.__device_auth = self.get_spark_context()._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(self.__cluster_name, "common")
            print(self.__device_auth.getDeviceCodeMessage())
            self.__format = 'com.microsoft.kusto.spark.datasource'
            self.option('kustoCluster', self.__cluster_nam)
            self.option('accessToken', self.__device_auth.acquireToken)
        else:
            self.__format = 'com.microsoft.kusto.spark.synapse.datasource'
            self.option('spark.synapse.linkedService', linked_service)

    # noinspection PyUnresolvedReferences
    @staticmethod
    def get_spark_session() -> 'pyspark.sql.session.SparkSession':
        # noinspection PyPackageRequirements
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("kustoPySpark").getOrCreate()

    # noinspection PyUnresolvedReferences
    @staticmethod
    def get_spark_context() -> 'pyspark.context.SparkContext':
        # noinspection PyPackageRequirements
        from pyspark.context import SparkContext
        return SparkContext.getOrCreate()

    def option(self, key: str, value: Union[str, Callable[[], str]]) -> 'PySparkKustoClient':
        if isinstance(value, str):
            self.__options[key] = value
        else:
            self.__option_producers[key] = value
        return self

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        kusto_read_session = self.__kusto_session.read.format(self.__format).option('kustoDatabase', database).option('kustoQuery', KQL)
        for key, value in self.__options.items():
            kusto_read_session = kusto_read_session.option(key, value)
        for key, value_producer in self.__option_producers.items():
            kusto_read_session = kusto_read_session.option(key, value_producer())
        return DataframeBasedKustoResponse(kusto_read_session.load())
