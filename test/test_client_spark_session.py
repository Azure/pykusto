from unittest.mock import patch

import pandas as pd

from pykusto import Query, PySparkKustoClient
from test.test_base import TestBase


class TestClient(TestBase):
    def test_sanity(self):
        mock_spark_context = type('SparkContext', tuple(), {})
        with patch('pykusto._src.pyspark_client.PySparkKustoClient.get_spark_context', lambda s: mock_spark_context):
            client = PySparkKustoClient('https://help.kusto.windows.net/', linked_service='MockLinkedKusto', fetch_by_default=False)

        table = client['test_db']['mock_table']
        rows = []
        columns = []
        actual_df = Query(table).take(5).to_dataframe()
        expected_df = pd.DataFrame(rows, columns=columns)
        self.assertTrue(expected_df.equals(actual_df))
