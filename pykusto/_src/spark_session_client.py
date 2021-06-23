from importlib.util import find_spec
from typing import List, Iterable

import pandas as pd
from azure.kusto.data import ClientRequestProperties

from pykusto import PyKustoClient, NO_RETRIES, KustoResponse, KQL, RetryConfig

global sc


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


class PyKustoClientForSpark(PyKustoClient):
    def __init__(self, cluster: str, fetch_by_default: bool = True, use_global_cache: bool = False) -> None:
        super().__init__(cluster, fetch_by_default, use_global_cache, NO_RETRIES, None)

        pyspark_installed = find_spec('pyspark') is not None
        spark_context_available = 'sc' in globals()
        spark_context_type = type(sc).__module__ + '.' + type(sc).__qualname__
        if not (pyspark_installed and spark_context_available and spark_context_type == 'pyspark.context.SparkContext'):
            raise RuntimeError(
                f"PyKustoClientForSpark can only be used inside a PySpark notebook: "
                f"pyspark_installed={pyspark_installed}, spark_context_available={spark_context_available}, spark_context_type={spark_context_type}"
            )

        # Code taken from: https://github.com/Azure/azure-kusto-spark/blob/master/samples/src/main/python/pyKusto.py
        # noinspection PyUnresolvedReferences,PyPackageRequirements
        from pyspark.sql import SparkSession
        self.__kusto_session = SparkSession.builder.appName("kustoPySpark").getOrCreate()
        self.__device_auth = sc._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(self.__cluster_name, "common")
        print(self.__device_auth.getDeviceCodeMessage())

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        return DataframeBasedKustoResponse(
            self.__kusto_session.read
                .format("com.microsoft.kusto.spark.datasource")
                .option("kustoCluster", self.__cluster_name)
                .option("kustoDatabase", database)
                .option("kustoQuery", KQL)
                .option("accessToken", self.__device_auth.acquireToken())
                .load()
        )

