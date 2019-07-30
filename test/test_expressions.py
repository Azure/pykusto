from datetime import timedelta

from pykusto.expressions import column_generator as col
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

    def test_assign_to(self):
        self.assertEqual(
            " | extend ['foo.bar'] = (shoo * 2)",
            Query().extend((col.shoo * 2).assign_to(col.foo.bar)).render(),
        )

    def test_between_timespan(self):
        self.assertEqual(
            " | where foo between (time(0.0:0:0.0) .. time(0.3:0:0.0))",
            Query().where(col.foo.between(timedelta(0), timedelta(hours=3))).render(),
        )

    def test_is_empty(self):
        self.assertEqual(
            'isempty(foo)',
            col.foo.is_empty().kql,
        )

    def test_method_does_not_exist(self):
        self.assertRaises(
            AttributeError,
            col.foo.non_existant_method,
        )

    def test_column_generator(self):
        self.assertEqual(
            " | project ['foo.bar']",
            Query().project(col.foo.bar).render(),
        )

    def test_column_generator_2(self):
        self.assertEqual(
            " | project ['foo.bar']",
            Query().project(col['foo.bar']).render(),
        )
