import pandas as pd

from pykusto import PyKustoClient, Query
from test.test_base import TestBase


class TestClient(TestBase):
    def test_sanity(self):
        client = PyKustoClient.from_spark_session('https://help.kusto.windows.net/')
        table = client['test_db']['mock_table']
        rows = []
        columns = []
        actual_df = Query(table).take(5).to_dataframe()
        expected_df = pd.DataFrame(rows, columns=columns)
        self.assertTrue(expected_df.equals(actual_df))
