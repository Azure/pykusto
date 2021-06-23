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
        super().__init__(cluster, fetch_by_default, use_global_cache, NO_RETRIES, None)  # TODO: postpone call to "_refresh_if_needed"

        pyspark_installed = find_spec('pyspark') is not None
        spark_context = globals().get('sc', None)
        spark_context_type = type(spark_context).__module__ + '.' + type(spark_context).__qualname__
        if not (pyspark_installed and spark_context is not None and spark_context_type == 'pyspark.context.SparkContext'):
            raise RuntimeError(
                f"PyKustoClientForSpark can only be used inside a PySpark notebook: "
                f"pyspark_installed={pyspark_installed}, spark_context={spark_context}, spark_context_type={spark_context_type}"
            )

        self.__options: Dict[str, str] = {}
        self.__option_producers: Dict[str, Callable[[], str]] = {}

        if linked_service is None:
            # noinspection PyUnresolvedReferences,PyPackageRequirements
            from pyspark.sql import SparkSession
            self.__kusto_session = SparkSession.builder.appName("kustoPySpark").getOrCreate()
            # noinspection PyProtectedMember
            self.__device_auth = spark_context._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(self.__cluster_name, "common")
            print(self.__device_auth.getDeviceCodeMessage())
            self.option('kustoCluster', self.__cluster_nam)
            self.option('accessToken', self.__device_auth.acquireToken)
        else:
            self.option('spark.synapse.linkedService', linked_service)

    def option(self, key: str, value: Union[str, Callable[[], str]]) -> 'PySparkKustoClient':
        if isinstance(value, str):
            self.__options[key] = value
        else:
            self.__option_producers[key] = value
        return self

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        kusto_read_session = self.__kusto_session.read.format('com.microsoft.kusto.spark.datasource').option('kustoDatabase', database).option('kustoQuery', KQL)
        for key, value in self.__options.items():
            kusto_read_session = kusto_read_session.option(key, value)
        for key, value_producer in self.__option_producers.items():
            kusto_read_session = kusto_read_session.option(key, value_producer())
        return DataframeBasedKustoResponse(kusto_read_session.load())
