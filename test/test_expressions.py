from datetime import timedelta, datetime

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

    def test_not_equals(self):
        self.assertEqual(
            ' | where foo != "bar"',
            Query().where(col.foo != 'bar').render(),
        )

    def test_repr(self):
        self.assertEqual(
            'foo == "bar"',
            repr(col.foo == 'bar')
        )

    def test_to_bool(self):
        self.assertEqual(
            ' | extend boolFoo = tobool(foo)',
            Query().extend(boolFoo=col.foo.to_bool()).render(),
        )

    def test_to_int(self):
        self.assertEqual(
            ' | extend intFoo = toint(foo)',
            Query().extend(intFoo=col.foo.to_int()).render(),
        )

    def test_to_long(self):
        self.assertEqual(
            ' | extend longFoo = tolong(foo)',
            Query().extend(longFoo=col.foo.to_long()).render(),
        )

    def test_and(self):
        self.assertEqual(
            ' | where foo and (bar contains "hello")',
            Query().where(col.foo & col.bar.contains("hello")).render(),
        )

    def test_or(self):
        self.assertEqual(
            ' | where foo or (bar contains "hello")',
            Query().where(col.foo | col.bar.contains("hello")).render(),
        )

    def test_not(self):
        self.assertEqual(
            ' | where not(bar contains "hello")',
            Query().where(~col.bar.contains("hello")).render(),
        )

    def test_ge(self):
        self.assertEqual(
            ' | where foo >= 10',
            Query().where(col.foo >= 10).render(),
        )

    def test_div(self):
        self.assertEqual(
            ' | extend foo = bar / 2',
            Query().extend(foo=col.bar / 2).render(),
        )

    def test_mod(self):
        self.assertEqual(
            ' | extend foo = bar % 2',
            Query().extend(foo=col.bar % 2).render(),
        )

    def test_negation(self):
        self.assertEqual(
            ' | extend foo = -bar',
            Query().extend(foo=-col.bar).render(),
        )

    def test_abs(self):
        self.assertEqual(
            ' | extend foo = abs(bar)',
            Query().extend(foo=abs(col.bar)).render(),
        )

    def test_str_equals(self):
        self.assertEqual(
            ' | where foo =~ bar',
            Query().where(col.foo.equals(col.bar)).render(),
        )

    def test_str_not_equals(self):
        self.assertEqual(
            ' | where foo !~ bar',
            Query().where(col.foo.not_equals(col.bar)).render(),
        )

    def test_str_matches(self):
        self.assertEqual(
            ' | where foo matches regex "[a-z]+"',
            Query().where(col.foo.matches("[a-z]+")).render(),
        )

    def test_str_starts_with(self):
        self.assertEqual(
            ' | where foo startswith "hello"',
            Query().where(col.foo.startswith("hello")).render(),
        )

    def test_str_ends_with(self):
        self.assertEqual(
            ' | where foo endswith "hello"',
            Query().where(col.foo.endswith("hello")).render(),
        )

    def test_le_date(self):
        self.assertEqual(
            ' | where foo <= datetime(2000-01-01 00:00:00.000000)',
            Query().where(col.foo <= datetime(2000, 1, 1)).render(),
        )

    def test_lt_date(self):
        self.assertEqual(
            ' | where foo < datetime(2000-01-01 00:00:00.000000)',
            Query().where(col.foo < datetime(2000, 1, 1)).render(),
        )

    def test_ge_date(self):
        self.assertEqual(
            ' | where foo >= datetime(2000-01-01 00:00:00.000000)',
            Query().where(col.foo >= datetime(2000, 1, 1)).render(),
        )

    def test_gt_date(self):
        self.assertEqual(
            ' | where foo > datetime(2000-01-01 00:00:00.000000)',
            Query().where(col.foo > datetime(2000, 1, 1)).render(),
        )

    def test_add_timespan(self):
        self.assertEqual(
            ' | extend foo = bar + time(0.1:0:0.0)',
            Query().extend(foo=col.bar + timedelta(hours=1)).render(),
        )

    def test_sub_timespan(self):
        self.assertEqual(
            ' | extend foo = bar - time(0.1:0:0.0)',
            Query().extend(foo=col.bar - timedelta(hours=1)).render(),
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
            " | extend bar = foo * 2",
            Query().extend((col.foo * 2).assign_to(col.bar)).render(),
        )
        self.assertEqual(
            " | extend foo = shoo * 2",
            Query().extend(foo=(col.shoo * 2)).render(),
        )

    def test_extend_const(self):
        self.assertEqual(
            ' | extend foo = 5, bar = "bar", other_col = other',
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
            ' | where foo in ("A", "B", "C")',
            Query().where(col.foo.is_in(["A", "B", "C"])).render()
        )
        self.assertEqual(
            ' | where foo in ("[", "[[", "]")',
            Query().where(col.foo.is_in(['[', "[[", "]"])).render()
        )
        self.assertRaises(
            NotImplementedError("'in' not supported. Instead use '.is_in()'"),
            lambda: Query().where(col.foo in col.bar).render()
        )

    def test_has(self):
        self.assertEqual(
            ' | where foo has "test"',
            Query().where(col.foo.has("test")).render()
        )
