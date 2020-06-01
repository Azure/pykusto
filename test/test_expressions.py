from datetime import timedelta, datetime

from pykusto.expressions import column_generator as col, AnyTypeColumn
from pykusto.functions import Functions as f
from pykusto.query import Query
from test.test_base import TestBase, test_table as t


class TestExpressions(TestBase):
    def test_contains(self):
        self.assertEqual(
            ' | where stringField contains "bar"',
            Query().where(t.stringField.contains('bar')).render(),
        )
        self.assertEqual(
            ' | where stringField contains_cs "bar"',
            Query().where(t.stringField.contains('bar', True)).render(),
        )

    def test_not_contains(self):
        self.assertEqual(
            ' | where stringField !contains "bar"',
            Query().where(t.stringField.not_contains('bar')).render(),
        )
        self.assertEqual(
            ' | where stringField !contains_cs "bar"',
            Query().where(t.stringField.not_contains('bar', True)).render(),
        )

    def test_array_access(self):
        self.assertEqual(
            ' | where (arrayField[3]) == "bar"',
            Query().where(t.arrayField[3] == 'bar').render(),
        )

    def test_array_contains(self):
        self.assertEqual(
            ' | where true in arrayField',
            Query().where(t.arrayField.array_contains(True)).render(),
        )

    def test_not_equals(self):
        self.assertEqual(
            ' | where stringField != "bar"',
            Query().where(t.stringField != 'bar').render(),
        )

    def test_repr(self):
        self.assertEqual(
            'StringColumn(stringField)',
            repr(t.stringField)
        )
        self.assertEqual(
            'stringField == "bar"',
            repr(t.stringField == 'bar')
        )

    def test_to_bool(self):
        self.assertEqual(
            ' | extend boolFoo = tobool(stringField)',
            Query().extend(boolFoo=t.stringField.to_bool()).render(),
        )

    def test_to_int(self):
        self.assertEqual(
            ' | extend intFoo = toint(stringField)',
            Query().extend(intFoo=t.stringField.to_int()).render(),
        )

    def test_to_long(self):
        self.assertEqual(
            ' | extend longFoo = tolong(stringField)',
            Query().extend(longFoo=t.stringField.to_long()).render(),
        )

    def test_and(self):
        self.assertEqual(
            ' | where boolField and (stringField contains "hello")',
            Query().where(t.boolField & t.stringField.contains("hello")).render(),
        )

    def test_swapped_and(self):
        self.assertEqual(
            ' | where 1 and boolField',
            Query().where(1 & t.boolField).render(),
        )

    def test_or(self):
        self.assertEqual(
            ' | where boolField or (stringField contains "hello")',
            Query().where(t.boolField | t.stringField.contains("hello")).render(),
        )

    def test_swapped_or(self):
        self.assertEqual(
            ' | where 0 or boolField',
            Query().where(0 | t.boolField).render(),
        )

    def test_not(self):
        self.assertEqual(
            ' | where not(stringField contains "hello")',
            Query().where(~t.stringField.contains("hello")).render(),
        )

    def test_ge(self):
        self.assertEqual(
            ' | where numField >= 10',
            Query().where(t.numField >= 10).render(),
        )

    def test_div(self):
        self.assertEqual(
            ' | extend foo = numField / 2',
            Query().extend(foo=t.numField / 2).render(),
        )

    def test_swapped_div(self):
        self.assertEqual(
            ' | extend foo = 2 / numField',
            Query().extend(foo=2 / t.numField).render(),
        )

    def test_mod(self):
        self.assertEqual(
            ' | extend foo = numField % 2',
            Query().extend(foo=t.numField % 2).render(),
        )

    def test_swapped_mod(self):
        self.assertEqual(
            ' | extend foo = 2 % numField',
            Query().extend(foo=2 % t.numField).render(),
        )

    def test_negation(self):
        self.assertEqual(
            ' | extend foo = -numField',
            Query().extend(foo=-t.numField).render(),
        )

    def test_abs(self):
        self.assertEqual(
            ' | extend foo = abs(numField)',
            Query().extend(foo=abs(t.numField)).render(),
        )

    def test_between(self):
        self.assertEqual(
            ' | where numField between (numField2 .. 100)',
            Query().where(t.numField.between(t.numField2, 100)).render(),
        )

    def test_str_equals(self):
        self.assertEqual(
            ' | where stringField =~ stringField2',
            Query().where(t.stringField.equals(t.stringField2)).render(),
        )

    def test_str_not_equals(self):
        self.assertEqual(
            ' | where stringField !~ stringField2',
            Query().where(t.stringField.not_equals(t.stringField2)).render(),
        )

    def test_str_matches(self):
        self.assertEqual(
            ' | where stringField matches regex "[a-z]+"',
            Query().where(t.stringField.matches("[a-z]+")).render(),
        )

    def test_str_starts_with(self):
        self.assertEqual(
            ' | where stringField startswith "hello"',
            Query().where(t.stringField.startswith("hello")).render(),
        )

    def test_str_ends_with(self):
        self.assertEqual(
            ' | where stringField endswith "hello"',
            Query().where(t.stringField.endswith("hello")).render(),
        )

    def test_le_date(self):
        self.assertEqual(
            'test_table | where dateField <= datetime(2000-01-01 00:00:00.000000)',
            Query(t).where(t.dateField <= datetime(2000, 1, 1)).render(),
        )

    def test_lt_date(self):
        self.assertEqual(
            ' | where dateField < datetime(2000-01-01 00:00:00.000000)',
            Query().where(t.dateField < datetime(2000, 1, 1)).render(),
        )

    def test_ge_date(self):
        self.assertEqual(
            ' | where dateField >= datetime(2000-01-01 00:00:00.000000)',
            Query().where(t.dateField >= datetime(2000, 1, 1)).render(),
        )

    def test_gt_date(self):
        self.assertEqual(
            ' | where dateField > datetime(2000-01-01 00:00:00.000000)',
            Query().where(t.dateField > datetime(2000, 1, 1)).render(),
        )

    def test_add_timespan_to_date(self):
        self.assertEqual(
            ' | extend foo = dateField + time(0.1:0:0.0)',
            Query().extend(foo=t.dateField + timedelta(hours=1)).render(),
        )

    def test_add_timespan_to_timespan(self):
        self.assertEqual(
            ' | extend foo = timespanField + time(0.1:0:0.0)',
            Query().extend(foo=t.timespanField + timedelta(hours=1)).render(),
        )

    def test_add_swapped_timespan_to_timespan(self):
        self.assertEqual(
            ' | extend foo = time(0.1:0:0.0) + timespanField',
            Query().extend(foo=timedelta(hours=1) + t.timespanField).render(),
        )

    def test_subtract_timespan_from_timespan(self):
        self.assertEqual(
            ' | extend foo = timespanField - time(0.1:0:0.0)',
            Query().extend(foo=t.timespanField - timedelta(hours=1)).render(),
        )

    def test_swapped_subtract_timespan_from_timespan(self):
        self.assertEqual(
            ' | extend foo = time(0.1:0:0.0) - timespanField',
            Query().extend(foo=timedelta(hours=1) - t.timespanField).render(),
        )

    def test_sub_timespan(self):
        self.assertEqual(
            ' | extend foo = dateField - time(0.1:0:0.0)',
            Query().extend(foo=t.dateField - timedelta(hours=1)).render(),
        )

    def test_sub_datetime(self):
        self.assertEqual(
            ' | extend foo = dateField - datetime(2020-01-01 00:00:00.000000)',
            Query().extend(foo=t.dateField - datetime(2020, 1, 1)).render(),
        )

    def test_sub_from_datetime(self):
        self.assertEqual(
            ' | extend foo = datetime(2020-01-01 00:00:00.000000) - dateField',
            Query().extend(foo=datetime(2020, 1, 1) - t.dateField).render(),
        )

    def test_sub_from_number(self):
        self.assertEqual(
            ' | extend foo = 3 - numField',
            Query().extend(foo=3 - t.numField).render(),
        )

    def test_sub_date_unknown_type(self):
        self.assertEqual(
            ' | extend foo = dateField - (case(boolField, bar, baz))',
            Query().extend(foo=t.dateField - f.case(t.boolField, col.bar, col.baz)).render(),
        )

    def test_sub_date_unknown_column(self):
        self.assertEqual(
            ' | extend foo = dateField - bar',
            Query().extend(foo=t.dateField - col.bar).render(),
        )

    def test_sub_unknown_type_number(self):
        self.assertEqual(
            ' | extend foo = cos(bar - numField)',
            Query().extend(foo=(col.bar - t.numField).cos()).render(),
        )

    def test_sub_unknown_type_datetime(self):
        self.assertEqual(
            ' | extend foo = ago(bar - dateField)',
            Query().extend(foo=(col.bar - t.dateField).ago()).render(),
        )

    def test_sub_unknown_type_timespan(self):
        self.assertEqual(
            ' | extend foo = bar - timespanField',
            Query().extend(foo=col.bar - t.timespanField).render(),
        )

    def test_bin_auto(self):
        self.assertEqual(
            ' | extend foo = bin_auto(dateField)',
            Query().extend(foo=t.dateField.bin_auto()).render(),
        )

    def test_array_access_expression_index(self):
        self.assertEqual(
            ' | where (arrayField[numField * 2]) == "bar"',
            Query().where(t.arrayField[t.numField * 2] == 'bar').render(),
        )

    def test_array_access_yields_any_expression(self):
        self.assertEqual(
            ' | where (cos(arrayField[3])) < 1',
            Query().where(t.arrayField[3].cos() < 1).render(),
        )

    def test_mapping_access(self):
        self.assertEqual(
            ' | where (mapField["key"]) == "bar"',
            Query().where(t.mapField['key'] == 'bar').render(),
        )

    def test_mapping_access_attribute(self):
        self.assertEqual(
            ' | where (mapField.key) == "bar"',
            Query().where(t.mapField.key == 'bar').render(),
        )

    def test_mapping_access_expression_index(self):
        self.assertEqual(
            ' | where (mapField[stringField]) == "bar"',
            Query().where(t.mapField[t.stringField] == 'bar').render(),
        )

    def test_mapping_access_yields_any_expression(self):
        self.assertEqual(
            ' | where (mapField["key"]) contains "substr"',
            Query().where(t.mapField['key'].contains("substr")).render(),
        )

    def test_dynamic(self):
        self.assertEqual(
            ' | where (mapField["foo"][0].bar[1][2][(tolower(stringField))]) > time(1.0:0:0.0)',
            Query().where(t.mapField['foo'][0].bar[1][2][t.stringField.lower()] > timedelta(1)).render(),
        )

    def test_assign_to(self):
        self.assertEqual(
            " | extend numFieldNew = numField * 2",
            Query().extend((t.numField * 2).assign_to(col.numFieldNew)).render(),
        )
        self.assertEqual(
            " | extend foo = numField * 2",
            Query().extend(foo=(t.numField * 2)).render(),
        )

    def test_extend_const(self):
        self.assertEqual(
            ' | extend foo = 5, bar = "bar", other_col = stringField',
            Query().extend(foo=5, bar="bar", other_col=t.stringField).render(),
        )

    def test_between_date(self):
        self.assertEqual(
            " | where dateField between (datetime(2020-01-01 00:00:00.000000) .. datetime(2020-01-31 00:00:00.000000))",
            Query().where(t.dateField.between(datetime(2020, 1, 1), datetime(2020, 1, 31))).render(),
        )

    def test_between_timespan(self):
        self.assertEqual(
            " | where timespanField between (time(0.0:0:0.0) .. time(0.3:0:0.0))",
            Query().where(t.timespanField.between(timedelta(0), timedelta(hours=3))).render(),
        )

    def test_is_empty(self):
        self.assertEqual(
            'isempty(stringField)',
            t.stringField.is_empty().kql,
        )

    def test_column_with_dot(self):
        self.assertEqual(
            " | project ['foo.bar']",
            Query().project(t['foo.bar']).render(),
        )

    def test_is_in(self):
        self.assertEqual(
            ' | where stringField in ("A", "B", "C")',
            Query().where(t.stringField.is_in(["A", "B", "C"])).render()
        )
        self.assertEqual(
            ' | where stringField in ("[", "[[", "]")',
            Query().where(t.stringField.is_in(['[', "[[", "]"])).render()
        )
        self.assertRaises(
            NotImplementedError("'in' not supported. Instead use '.is_in()'"),
            lambda: t.stringField in t.stringField2
        )

    def test_has(self):
        self.assertEqual(
            ' | where stringField has "test"',
            Query().where(t.stringField.has("test")).render()
        )

    def test_column_generator(self):
        field1 = col.foo
        field2 = col['foo.bar']
        self.assertIsInstance(field1, AnyTypeColumn)
        self.assertIsInstance(field2, AnyTypeColumn)
        self.assertEqual('foo', field1.get_name())
        self.assertEqual('foo.bar', field2.get_name())

    def test_column_name_quoting(self):
        self.assertEqual(
            ' | where [\'title\'] has "test"',
            Query().where(t.title.has("test")).render()
        )
        self.assertEqual(
            ' | where [\'stringField\'] has "test"',
            Query().where(col.of('stringField').has("test")).render()
        )

    def test_multiply_number_column(self):
        self.assertEqual(
            ' | where (todouble(100 * numberField)) > 0.2',
            Query().where(f.to_double(100 * t.numberField) > 0.2).render(),
        )

    def test_add_number_column(self):
        self.assertEqual(
            ' | where (todouble(100 + numberField)) > 0.2',
            Query().where(f.to_double(100 + t.numberField) > 0.2).render(),
        )

    def test_multiply_number_expression(self):
        self.assertEqual(
            ' | where (100 * (todouble(numberField))) > 0.2',
            Query().where(100 * f.to_double(t.numberField) > 0.2).render(),
        )

    def test_column_with_digits(self):
        self.assertEqual(
            " | where (['100'] * (todouble(numberField))) > 0.2",
            Query().where(col['100'] * f.to_double(t.numberField) > 0.2).render(),
        )
