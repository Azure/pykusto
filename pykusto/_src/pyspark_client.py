from importlib.util import find_spec
from typing import Dict, Callable, Union, Tuple

import numpy as np
import pandas as pd
from azure.kusto.data import ClientRequestProperties, KustoClient

from pykusto import PyKustoClient, NO_RETRIES, KustoResponse, KQL, RetryConfig
from .logger import _logger


class DataframeBasedKustoResponse(KustoResponse):
    """
    In PySpark Kusto results are returned as dataframes. We wrap the dataframe with this object for compatibility with :class:`PyKustoClient`.
    """
    __dataframe: pd.DataFrame

    # noinspection PyMissingConstructor
    def __init__(self, dataframe: pd.DataFrame):
        self.__dataframe = dataframe

    def get_rows(self) -> np.ndarray:
        return self.__dataframe.to_numpy()

    def to_dataframe(self) -> pd.DataFrame:
        return self.__dataframe


class PySparkKustoClient(PyKustoClient):
    """
    Handle to a Kusto cluster, to be used inside a PySpark notebook.
    """
    def __init__(self, cluster: str, linked_service: str = None, fetch_by_default: bool = True) -> None:
        """
        Create a new handle to a Kusto cluster. The value of "fetch_by_default" is used for current instance, and also passed on to database instances.

        :param cluster: a cluster URL.
        :param linked_service: If provided, the connection to Kusto will be made via a pre-configured link (used only for Synapse). Otherwise, device authentication will be used
        (tested only for Synapse, but should work for any PySpark notebook).
        """
        self.__linked_service = linked_service
        super().__init__(cluster, fetch_by_default, False, NO_RETRIES, None)

    def _internal_init(self, client_or_cluster: Union[str, KustoClient], use_global_cache: bool):
        assert isinstance(client_or_cluster, str), "PySparkKustoClient must be initialized with a cluster name"
        self.__cluster_name = client_or_cluster
        self.__options: Dict[str, Callable[[], str]] = {}
        self.__kusto_session, self.__spark_context = self.__get_spark_session_and_context()

        if self.__linked_service is None:
            # Connect via device authentication
            self.refresh_device_auth()
            self.__format = 'com.microsoft.kusto.spark.datasource'
            self.option('kustoCluster', self.__cluster_name)
        else:
            # Connect via pre-configured link
            self.__format = 'com.microsoft.kusto.spark.synapse.datasource'
            self.option('spark.synapse.linkedService', self.__linked_service)

    def refresh_device_auth(self) -> None:
        """
        Run device authentication sequence, called in the client constructor. Call this method again if you need to re-authenticate.
        """
        assert self.__linked_service is None, "Device authentication can be used only when a linked_service was not provided to the client constructor"
        # noinspection PyProtectedMember
        device_auth = self.__spark_context._jvm.com.microsoft.kusto.spark.authentication.DeviceAuthentication(self.__cluster_name, "common")
        _logger.info(device_auth.getDeviceCodeMessage())
        self.option('accessToken', device_auth.acquireToken)

    # noinspection PyUnresolvedReferences,PyPackageRequirements
    @staticmethod
    def __get_spark_session_and_context() -> Tuple['pyspark.sql.session.SparkSession', 'pyspark.context.SparkContext']:  # noqa: F821  # pragma: no cover
        if find_spec('pyspark') is None:
            raise RuntimeError("pyspark package not found. PySparkKustoClient can only be used inside a PySpark notebook")
        from pyspark.sql import SparkSession
        from pyspark.context import SparkContext
        return SparkSession.builder.appName("kustoPySpark").getOrCreate(), SparkContext.getOrCreate()

    def option(self, key: str, value: Union[str, Callable[[], str]]) -> 'PySparkKustoClient':
        """
        Add an option to the underlying DataFrameReader. All authentication related options are already handled by this class, but use this method if you need any other options.
        :param key: The option key.
        :param value: Either an option value, or a callable to generate the option value.
        :return: This instance for chained calls.
        """
        if isinstance(value, str):
            self.__options[key] = lambda: value
        else:
            self.__options[key] = value
        return self

    def clear_option(self, key: str) -> 'PySparkKustoClient':
        """
        Clear an option from the underlying DataFrameReader.
        :param key: The option key to clear.
        :return: This instance for chained calls.
        """
        self.__options.pop(key, None)
        return self

    def get_options(self) -> Dict[str, str]:
        """
        Get the options set for the underlying DataFrameReader.
        """
        return {key: value_producer() for key, value_producer in self.__options.items()}

    def _internal_execute(self, database: str, query: KQL, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponse:
        resolved_options = self.get_options()
        resolved_options['kustoDatabase'] = database
        resolved_options['kustoQuery'] = query
        kusto_read_session = self.__kusto_session.read.format(self.__format)
        for key, value in resolved_options.items():
            kusto_read_session = kusto_read_session.option(key, value)
        return DataframeBasedKustoResponse(kusto_read_session.load())
