from pykusto.column import column_generator as col
from pykusto.query import Query
from test.test_base import TestBase


class TestExpressions(TestBase):
    def test_contains(self):
        self.assertEqual(
            ' | where foo contains "bar"',
            Query().where(col.foo.contains('bar')).render(),
        )

    def test_array_access(self):
        self.assertEqual(
            ' | where (arr[3]) == "bar"',
            Query().where(col.arr[3] == 'bar').render(),
        )

    def test_array_access_expression_index(self):
        self.assertEqual(
            ' | where (arr[(foo * 2)]) == "bar"',
            Query().where(col.arr[col.foo * 2] == 'bar').render(),
        )

    def test_mapping_access(self):
        self.assertEqual(
            ' | where (dict["key"]) == "bar"',
            Query().where(col.dict['key'] == 'bar').render(),
        )

    def test_mapping_access_expression_index(self):
        self.assertEqual(
            ' | where (dict[foo]) == "bar"',
            Query().where(col.dict[col.foo] == 'bar').render(),
        )
