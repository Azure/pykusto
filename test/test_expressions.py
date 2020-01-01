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
        self.assertEqual(
            ' | where foo contains_cs "bar"',
            Query().where(col.foo.contains('bar', True)).render(),
        )

    def test_not_contains(self):
        self.assertEqual(
            ' | where foo !contains "bar"',
            Query().where(col.foo.not_contains('bar')).render(),
        )
        self.assertEqual(
            ' | where foo !contains_cs "bar"',
            Query().where(col.foo.not_contains('bar', True)).render(),
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

    def test_array_access_yields_any_expression(self):
        self.assertEqual(
            ' | where (cos(arr[3])) < 1',
            Query().where(col.arr[3].cos() < 1).render(),
        )

    def test_mapping_access(self):
        self.assertEqual(
            ' | where (dict["key"]) == "bar"',
            Query().where(col.dict['key'] == 'bar').render(),
        )

    def test_mapping_access_attribute(self):
        self.assertEqual(
            ' | where (dict.key) == "bar"',
            Query().where(col.dict.key == 'bar').render(),
        )

    def test_mapping_access_expression_index(self):
        self.assertEqual(
            ' | where (dict[foo]) == "bar"',
            Query().where(col.dict[col.foo] == 'bar').render(),
        )

    def test_mapping_access_yields_any_expression(self):
        self.assertEqual(
            ' | where (dict["key"]) contains "substr"',
            Query().where(col.dict['key'].contains("substr")).render(),
        )

    def test_dynamic(self):
        self.assertEqual(
            ' | where (dict["foo"][0].bar[1][2][(tolower(bar))]) > time(1.0:0:0.0)',
            Query().where(col.dict['foo'][0].bar[1][2][col.bar.lower()] > timedelta(1)).render(),
        )

    def test_assign_to(self):
        self.assertEqual(
            " | extend bar = (foo * 2)",
            Query().extend((col.foo * 2).assign_to(col.bar)).render(),
        )
        self.assertEqual(
            " | extend foo = (shoo * 2)",
            Query().extend(foo=(col.shoo * 2)).render(),
        )

    def test_extend_const(self):
        self.assertEqual(
            " | extend foo = (5), bar = (\"bar\"), other_col = other",
            Query().extend(foo=5, bar="bar", other_col=col.other).render(),
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

    def test_column_generator(self):
        self.assertEqual(
            " | project ['foo.bar']",
            Query().project(col['foo.bar']).render(),
        )

    def test_is_in(self):
        self.assertEqual(
            " | where foo in (\"A\", \"B\", \"C\")",
            Query().where(col.foo.is_in(["A", "B", "C"])).render()
        )
        self.assertEqual(
            " | where foo in (\"[\", \"[[\", \"]\")",
            Query().where(col.foo.is_in(['[', "[[", "]"])).render()
        )

    def test_has(self):
        self.assertEqual(
            " | where foo has \"test\"",
            Query().where(col.foo.has("test")).render()
        )