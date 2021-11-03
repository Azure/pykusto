from unittest.mock import patch

import pandas as pd

from pykusto import Query, PySparkKustoClient, ClientRequestProperties
# noinspection PyProtectedMember
from pykusto._src.expressions import _StringColumn, _NumberColumn
# noinspection PyProtectedMember
from test.test_base import TestBase, nested_attribute_dict, captured_stdout


class MockDataFrameReader:
    def __init__(self, dataframe_to_return: pd.DataFrame) -> None:
        self.recorded_format = None
        self.recorded_options = {}
        self.dataframe_to_return = dataframe_to_return

    def format(self, the_format: str) -> 'MockDataFrameReader':
        assert self.recorded_format is None, "Trying to set format twice"
        self.recorded_format = the_format
        return self

    def option(self, key: str, value: str) -> 'MockDataFrameReader':
        assert key not in self.recorded_options, f"Trying to set option '{key}' twice"
        self.recorded_options[key] = value
        return self

    def load(self) -> pd.DataFrame:
        return self.dataframe_to_return


class MockSparkSession:
    def __init__(self, dataframe_to_return: pd.DataFrame) -> None:
        self.read = MockDataFrameReader(dataframe_to_return)


# noinspection PyPep8Naming,PyMethodMayBeStatic
class MockDeviceAuthentication:
    def __init__(self, mock_token: str):
        self.mock_token = mock_token

    def getDeviceCodeMessage(self):
        return "To sign in, use a lubricated goat to open the pod bay doors."

    def acquireToken(self):
        return self.mock_token


class MockSparkContext:
    def __init__(self, mock_token: str):
        self.mock_token = mock_token
        self._jvm = nested_attribute_dict('com.microsoft.kusto.spark.authentication.DeviceAuthentication', lambda s, n, c: MockDeviceAuthentication(self.mock_token))


class TestClient(TestBase):
    def test_linked_service(self):
        rows = (['foo', 10], ['bar', 20], ['baz', 30])
        columns = ('stringField', 'numField')
        expected_df = pd.DataFrame(rows, columns=columns)
        mock_spark_session = MockSparkSession(expected_df)

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, None)):
            client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto', fetch_by_default=False)

        table = client['test_db']['mock_table']
        actual_df = Query(table).take(5).to_dataframe()
        self.assertTrue(expected_df.equals(actual_df))

        self.assertEqual('com.microsoft.kusto.spark.synapse.datasource', mock_spark_session.read.recorded_format)
        self.assertEqual(
            {
                'spark.synapse.linkedService': 'MockLinkedKusto',
                'kustoDatabase': 'test_db',
                'kustoQuery': 'mock_table | take 5',
            },
            mock_spark_session.read.recorded_options,
        )

    def test_linked_service_with_request_properties(self):
        rows = (['foo', 10], ['bar', 20], ['baz', 30])
        columns = ('stringField', 'numField')
        expected_df = pd.DataFrame(rows, columns=columns)
        mock_spark_session = MockSparkSession(expected_df)

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, None)):
            client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto', fetch_by_default=False)

        properties = ClientRequestProperties()
        properties.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, False)
        properties.set_parameter('xIntValue', 11)

        table = client['test_db']['mock_table']
        actual_df = Query(table).take(5).to_dataframe(properties=properties)
        self.assertTrue(expected_df.equals(actual_df))

        self.assertEqual('com.microsoft.kusto.spark.synapse.datasource', mock_spark_session.read.recorded_format)
        self.assertEqual(
            {
                'spark.synapse.linkedService': 'MockLinkedKusto',
                'clientRequestPropertiesJson': '{"Options": {"deferpartialqueryfailures": false}, "Parameters": {"xIntValue": 11}}',
                'kustoDatabase': 'test_db',
                'kustoQuery': 'mock_table | take 5',
            },
            mock_spark_session.read.recorded_options,
        )

    def test_linked_service_with_extra_options(self):
        rows = (['foo', 10], ['bar', 20], ['baz', 30])
        columns = ('stringField', 'numField')
        expected_df = pd.DataFrame(rows, columns=columns)
        mock_spark_session = MockSparkSession(expected_df)

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, None)):
            client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto', fetch_by_default=False)

        client.option('alsoMake', 'coffee')
        client.option('performanceLevel', 'awesome')
        client.clear_option('alsoMake')
        table = client['test_db']['mock_table']
        actual_df = Query(table).take(5).to_dataframe()
        self.assertTrue(expected_df.equals(actual_df))

        self.assertEqual('com.microsoft.kusto.spark.synapse.datasource', mock_spark_session.read.recorded_format)
        self.assertEqual(
            {
                'spark.synapse.linkedService': 'MockLinkedKusto',
                'kustoDatabase': 'test_db',
                'kustoQuery': 'mock_table | take 5',
                'performanceLevel': 'awesome',
            },
            mock_spark_session.read.recorded_options,
        )

    def test_device_auth(self):
        rows = (['foo', 10], ['bar', 20], ['baz', 30])
        columns = ('stringField', 'numField')
        expected_df = pd.DataFrame(rows, columns=columns)
        mock_spark_session = MockSparkSession(expected_df)
        mock_spark_context = MockSparkContext('MOCK_TOKEN')

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, mock_spark_context)), \
                captured_stdout() as out:
            client = PySparkKustoClient('https://help.kusto.windows.net/', fetch_by_default=False)

        self.assertEqual("To sign in, use a lubricated goat to open the pod bay doors.", out.getvalue().strip())
        table = client['test_db']['mock_table']
        actual_df = Query(table).take(5).to_dataframe()
        self.assertTrue(expected_df.equals(actual_df))

        self.assertEqual('com.microsoft.kusto.spark.datasource', mock_spark_session.read.recorded_format)
        self.assertEqual(
            {
                'kustoCluster': 'https://help.kusto.windows.net/',
                'accessToken': 'MOCK_TOKEN',
                'kustoDatabase': 'test_db',
                'kustoQuery': 'mock_table | take 5',
            },
            mock_spark_session.read.recorded_options,
        )

    def test_repr(self):
        mock_spark_session = MockSparkSession(pd.DataFrame())
        mock_spark_context = MockSparkContext('MOCK_TOKEN')

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, mock_spark_context)):
            device_auth_client = PySparkKustoClient('https://help.kusto.windows.net/', fetch_by_default=False)
            linked_service_client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto', fetch_by_default=False)

        self.assertEqual("PySparkKustoClient('https://help.kusto.windows.net/')", repr(device_auth_client))
        self.assertEqual("PySparkKustoClient('https://help.kusto.windows.net/', 'MockLinkedKusto')", repr(linked_service_client))

    def test_linked_service_with_fetch(self):
        rows = (
            ['test_db', 'mock_table', 'stringField', 'System.String'],
            ['test_db', 'mock_table', 'numField', 'System.Int32'],
        )
        columns = ('DatabaseName', 'TableName', 'ColumnName', 'ColumnType')
        expected_df = pd.DataFrame(rows, columns=columns)
        mock_spark_session = MockSparkSession(expected_df)

        with patch('pykusto._src.pyspark_client.PySparkKustoClient._PySparkKustoClient__get_spark_session_and_context', lambda s: (mock_spark_session, None)):
            client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto')
            client.wait_for_items()

        self.assertType(client.test_db.mock_table.stringField, _StringColumn)
        self.assertType(client.test_db.mock_table.numField, _NumberColumn)

        self.assertEqual('com.microsoft.kusto.spark.synapse.datasource', mock_spark_session.read.recorded_format)
        self.assertEqual(
            {
                'spark.synapse.linkedService': 'MockLinkedKusto',
                'kustoDatabase': '',
                'kustoQuery': '.show databases schema | project DatabaseName, TableName, ColumnName, ColumnType | limit 100000',
            },
            mock_spark_session.read.recorded_options,
        )
