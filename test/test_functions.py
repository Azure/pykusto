import logging
from datetime import datetime, timedelta

from pykusto.expressions import column_generator as col
from pykusto.functions import Functions as f
from pykusto.logger import logger
from pykusto.query import Query
from test.test_base import TestBase
from test.test_base import test_table as t


class TestFunction(TestBase):
    def test_acos(self):
        self.assertEqual(
            " | where (acos(numField)) > 4",
            Query().where(f.acos(t.numField) > 4).render()
        )

    def test_ago(self):
        self.assertEqual(
            " | where dateField > (ago(time(4.0:0:0.0)))",
            Query().where(t.dateField > f.ago(timedelta(4))).render()
        )
        self.assertEqual(
            " | where dateField > (ago(timespanField))",
            Query().where(t.dateField > f.ago(t.timespanField)).render()
        )

    def test_array_length(self):
        self.assertEqual(
            " | where (array_length(arrayField)) > 4",
            Query().where(f.array_length(t.arrayField) > 4).render()
        )

    def test_bag_keys(self):
        self.assertEqual(
            " | where (array_length(bag_keys(mapField))) > 4",
            Query().where(f.bag_keys(t.mapField).array_length() > 4).render()
        )

    def test_case(self):
        self.assertEqual(
            ' | extend bucket = case(numField <= 3, "Small", numField <= 10, "Medium", "Large")',
            Query().extend(bucket=f.case(t.numField <= 3, "Small", t.numField <= 10, 'Medium', 'Large')).render()
        )

    def test_ceiling(self):
        self.assertEqual(
            " | where (ceiling(numField)) > 4",
            Query().where(f.ceiling(t.numField) > 4).render()
        )

    def test_cos(self):
        self.assertEqual(
            " | where (cos(numField)) > 4",
            Query().where(f.cos(t.numField) > 4).render()
        )

    def test_endofday(self):
        self.assertEqual(
            " | where (endofday(dateField)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_day(t.dateField) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofday(dateField, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_day(t.dateField, 2) > datetime(2019, 7, 23)).render()
        )

    def test_endofmonth(self):
        self.assertEqual(
            " | where (endofmonth(dateField)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_month(t.dateField) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofmonth(dateField, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_month(t.dateField, 2) > datetime(2019, 7, 23)).render()
        )

    def test_endofweek(self):
        self.assertEqual(
            " | where (endofweek(dateField)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_week(t.dateField) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofweek(dateField, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_week(t.dateField, 2) > datetime(2019, 7, 23)).render()
        )

    def test_endofyear(self):
        self.assertEqual(
            " | where (endofyear(dateField)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_year(t.dateField) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofyear(dateField, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.end_of_year(t.dateField, 2) > datetime(2019, 7, 23)).render()
        )

    def test_exp(self):
        self.assertEqual(
            " | where (exp(numField)) > 4",
            Query().where(f.exp(t.numField) > 4).render()
        )

    def test_exp10(self):
        self.assertEqual(
            " | where (exp10(numField)) > 4",
            Query().where(f.exp10(t.numField) > 4).render()
        )

    def test_exp2(self):
        self.assertEqual(
            " | where (exp2(numField)) > 4",
            Query().where(f.exp2(t.numField) > 4).render()
        )

    def test_floor(self):
        self.assertEqual(
            " | where (floor(dateField, time(0.12:0:0.0))) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.floor(t.dateField, timedelta(0.5)) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (floor(numField, 0.1)) > 3",
            Query().where(f.floor(t.numField, 0.1) > 3).render()
        )
        self.assertEqual(
            " | where (floor(numField, 0.1)) > 3",
            Query().where(t.numField.floor(0.1) > 3).render()
        )

    def test_format_datetime(self):
        self.assertEqual(
            ' | where (format_datetime(dateField, "yy-MM-dd [HH:mm:ss]")) == "2019-07-23 00:00:00"',
            Query().where(f.format_datetime(t.dateField, 'yy-MM-dd [HH:mm:ss]') == '2019-07-23 00:00:00').render()
        )

    # noinspection SpellCheckingInspection
    def test_format_timespan(self):
        self.assertEqual(
            ' | where (format_timespan(timespanField, "h:m:s.fffffff")) == "2:3:4.1234500"',
            Query().where(f.format_timespan(t.timespanField, 'h:m:s.fffffff') == "2:3:4.1234500").render()
        )

    def test_getmonth(self):
        self.assertEqual(
            " | where (getmonth(dateField)) > 3",
            Query().where(f.get_month(t.dateField) > 3).render()
        )

    def test_gettype(self):
        self.assertEqual(
            ' | where (gettype(dateField)) == "datetime"',
            Query().where(f.get_type(t.dateField) == 'datetime').render()
        )

    def test_getyear(self):
        self.assertEqual(
            " | where (getyear(dateField)) > 2019",
            Query().where(f.get_year(t.dateField) > 2019).render()
        )

    def test_hourofday(self):
        self.assertEqual(
            " | where (hourofday(dateField)) == 3",
            Query().where(f.hour_of_day(t.dateField) == 3).render()
        )

    def test_hash(self):
        self.assertEqual(
            " | where (hash(stringField)) == 3",
            Query().where(f.hash(t.stringField) == 3).render()
        )

    def test_hash_sha256(self):
        self.assertEqual(
            " | where (hash_sha256(stringField)) == 3",
            Query().where(f.hash_sha256(t.stringField) == 3).render()
        )

    def test_isempty(self):
        self.assertEqual(
            " | where isempty(stringField)",
            Query().where(f.is_empty(t.stringField)).render()
        )

    def test_isfinite(self):
        self.assertEqual(
            " | where isfinite(numField)",
            Query().where(f.is_finite(t.numField)).render()
        )

    def test_isinf(self):
        self.assertEqual(
            " | where isinf(numField)",
            Query().where(f.is_inf(t.numField)).render()
        )

    def test_isnan(self):
        self.assertEqual(
            " | where isnan(numField)",
            Query().where(f.is_nan(t.numField)).render()
        )

    def test_isnotempty(self):
        self.assertEqual(
            " | where isnotempty(stringField)",
            Query().where(f.is_not_empty(t.stringField)).render()
        )

    def test_isnotnull(self):
        self.assertEqual(
            " | where isnotnull(stringField)",
            Query().where(f.is_not_null(t.stringField)).render()
        )

    def test_isnull(self):
        self.assertEqual(
            " | where isnull(stringField)",
            Query().where(f.is_null(t.stringField)).render()
        )

    def test_isutf8(self):
        self.assertEqual(
            " | where isutf8(stringField)",
            Query().where(f.is_utf8(t.stringField)).render()
        )

    def test_log(self):
        self.assertEqual(
            " | where (log(numField)) < 3",
            Query().where(f.log(t.numField) < 3).render()
        )

    def test_log10(self):
        self.assertEqual(
            " | where (log10(numField)) < 3",
            Query().where(f.log10(t.numField) < 3).render(),
        )

    def test_log2(self):
        self.assertEqual(
            " | where (log2(numField)) < 3",
            Query().where(f.log2(t.numField) < 3).render()
        )

    def test_loggamma(self):
        self.assertEqual(
            " | where (loggamma(numField)) < 3",
            Query().where(f.log_gamma(t.numField) < 3).render()
        )

    def test_make_datetime(self):
        self.assertEqual(
            " | where (make_datetime(numField, numField2, numField3, 0, 0, 0)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.make_datetime(t.numField, t.numField2, t.numField3) > datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (make_datetime(numField, numField2, numField3, numField4, 0, 0)) > datetime(2019-07-23 05:00:00.000000)",
            Query().where(f.make_datetime(t.numField, t.numField2, t.numField3, t.numField4) > datetime(2019, 7, 23, 5)).render()
        )
        self.assertEqual(
            " | where (make_datetime(numField, numField2, numField3, numField4, numField5, 0)) > datetime(2019-07-23 05:29:00.000000)",
            Query().where(
                f.make_datetime(t.numField, t.numField2, t.numField3, t.numField4, t.numField5) > datetime(2019, 7, 23, 5, 29)).render()
        )
        self.assertEqual(
            " | where (make_datetime(numField, numField2, numField3, numField4, numField5, numField6)) > datetime(2019-07-23 05:29:15.000000)",
            Query().where(
                f.make_datetime(t.numField, t.numField2, t.numField3, t.numField4, t.numField5, t.numField6) > datetime(2019, 7, 23, 5, 29, 15)).render()
        )

    def test_now(self):
        self.assertEqual(
            " | where dateField < (now())",
            Query().where(t.dateField < f.now()).render()
        )
        self.assertEqual(
            " | where dateField < (now(time(-3.0:0:0.0)))",
            Query().where(t.dateField < f.now(timedelta(-3))).render()
        )

    def test_parse_json_to_string(self):
        self.assertEqual(
            ' | where (tostring(parse_json(stringField))) contains "ABC"',
            Query().where(f.parse_json(t.stringField).to_string().contains('ABC')).render()
        )

    def test_parse_json_brackets(self):
        self.assertEqual(
            ' | where (tostring(parse_json(stringField)["bar"])) contains "ABC"',
            Query().where(f.parse_json(t.stringField)['bar'].to_string().contains('ABC')).render()
        )

    def test_parse_json_dot(self):
        self.assertEqual(
            ' | where (tostring(parse_json(stringField).bar)) contains "ABC"',
            Query().where(f.parse_json(t.stringField).bar.to_string().contains('ABC')).render()
        )

    def test_parse_json_number_expression(self):
        self.assertEqual(
            ' | where (todouble(parse_json(stringField).bar)) > 4',
            Query().where(f.to_double(f.parse_json(t.stringField).bar) > 4).render()
        )

    def test_parse_json_array(self):
        self.assertEqual(
            ' | where (parse_json(stringField)[2]) == 3',
            Query().where(f.parse_json(t.stringField)[2] == 3).render()
        )

    def test_parse_json_nesting(self):
        self.assertEqual(
            ' | where (parse_json(stringField)["a"].b[2]) contains "bar"',
            Query().where(f.parse_json(t.stringField)['a'].b[2].contains('bar')).render())

    def test_pow(self):
        self.assertEqual(
            " | where (pow(numField, numField2)) > 3",
            Query().where(f.pow(t.numField, t.numField2) > 3).render()
        )

    def test_round(self):
        self.assertEqual(
            " | where (round(numField, numField2)) == 3",
            Query().where(f.round(t.numField, t.numField2) == 3).render()
        )

    def test_sign(self):
        self.assertEqual(
            " | where (sign(numField)) == 1",
            Query().where(f.sign(t.numField) == 1).render()
        )

    def test_sqrt(self):
        self.assertEqual(
            " | where (sqrt(numField)) > 1",
            Query().where(f.sqrt(t.numField) > 1).render()
        )

    def test_startofday(self):
        self.assertEqual(
            " | where (startofday(dateField)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.start_of_day(t.dateField) > datetime(2019, 7, 23)).render()
        )

    def test_startofmonth(self):
        self.assertEqual(
            " | where (startofmonth(dateField)) > datetime(2019-07-01 00:00:00.000000)",
            Query().where(f.start_of_month(t.dateField) > datetime(2019, 7, 1)).render()
        )

    def test_startofweek(self):
        self.assertEqual(
            " | where (startofweek(dateField)) > datetime(2019-07-08 00:00:00.000000)",
            Query().where(f.start_of_week(t.dateField) > datetime(2019, 7, 8)).render()
        )

    def test_startofyear(self):
        self.assertEqual(
            " | where (startofyear(dateField)) > datetime(2019-01-01 00:00:00.000000)",
            Query().where(f.start_of_year(t.dateField) > datetime(2019, 1, 1)).render()
        )

    def test_strcat(self):
        self.assertEqual(
            ' | extend strcat("hello", ",", stringField, "!")',
            Query().extend(f.strcat("hello", ',', t.stringField, '!')).render()
        )
        self.assertEqual(
            ' | extend strcat(stringField, "!")',
            Query().extend(f.strcat(t.stringField, '!')).render()
        )

    def test_strcat_one_argument(self):
        self.assertRaises(
            ValueError("strcat requires at least two arguments"),
            lambda: f.strcat(t.stringField)
        )

    def test_strcat_delim(self):
        self.assertEqual(
            ' | extend strcat_delim("-", "hello", ",", stringField, "!")',
            Query().extend(f.strcat_delim('-', "hello", ',', t.stringField, '!')).render()
        )
        self.assertEqual(
            ' | extend strcat_delim("-", ",", stringField)',
            Query().extend(f.strcat_delim('-', ',', t.stringField)).render()
        )

    def test_strcat_array(self):
        self.assertEqual(
            ' | where (strcat_array(arrayField, ",")) == "A,B,C"',
            Query().where(f.strcat_array(t.arrayField, ',') == 'A,B,C').render()
        )
        self.assertEqual(
            ' | where (strcat_array(dynamic(["A", "B", "C"]), ",")) == "A,B,C"',
            Query().where(f.strcat_array(['A', 'B', 'C'], ',') == 'A,B,C').render()
        )

    def test_strcmp(self):
        self.assertEqual(
            " | where (strcmp(stringField, stringField2)) == 1",
            Query().where(f.strcmp(t.stringField, t.stringField2) == 1).render()
        )

    def test_string_size(self):
        self.assertEqual(
            " | where (string_size(stringField)) == 1",
            Query().where(f.string_size(t.stringField) == 1).render()
        )

    def test_strlen(self):
        self.assertEqual(
            " | where (strlen(stringField)) == 1",
            Query().where(f.strlen(t.stringField) == 1).render()
        )

    def test_strrep(self):
        self.assertEqual(
            ' | where (strrep(stringField, numField)) == "ABC"',
            Query().where(f.strrep(t.stringField, t.numField) == 'ABC').render()
        )
        self.assertEqual(
            ' | where (strrep(stringField, numField, ",")) == "ABC,ABC"',
            Query().where(f.strrep(t.stringField, t.numField, ',') == 'ABC,ABC').render()
        )
        self.assertEqual(
            ' | where (strrep(stringField, numField, stringField2)) == "ABC,ABC"',
            Query().where(f.strrep(t.stringField, t.numField, t.stringField2) == 'ABC,ABC').render()
        )

    def test_substring(self):
        self.assertEqual(
            ' | where (substring(stringField, numField)) == "ABC"',
            Query().where(f.substring(t.stringField, t.numField) == 'ABC').render()
        )
        self.assertEqual(
            ' | where (substring(stringField, numField, 4)) == "ABC,ABC"',
            Query().where(f.substring(t.stringField, t.numField, 4) == 'ABC,ABC').render()
        )

    def test_split(self):
        self.assertEqual(
            ' | extend foo = split(stringField, "_", 3)',
            Query().extend(foo=f.split(t.stringField, "_", 3)).render()
        )
        self.assertEqual(
            ' | extend foo = split(stringField, "_")[3]',
            Query().extend(foo=f.split(t.stringField, "_")[3]).render()
        )
        self.assertEqual(
            ' | extend foo = split("1_2", "_")[3]',
            Query().extend(foo=f.split("1_2", "_")[3]).render()
        )

    def test_count_of(self):
        self.assertEqual(
            ' | where (countof(stringField, "abc", "normal")) == 2',
            Query().where(f.count_of(t.stringField, "abc") == 2).render()
        )

    def test_tobool(self):
        self.assertEqual(
            " | where tobool(stringField)",
            Query().where(f.to_bool(t.stringField)).render()
        )

    def test_todouble(self):
        self.assertEqual(
            " | where (todouble(stringField)) > 0.2",
            Query().where(f.to_double(t.stringField) > 0.2).render()
        )
        
    def test_todecimal(self):
        self.assertEqual(
            " | where (todecimal(stringField)) > 0.2",
            Query().where(f.to_decimal(t.stringField) > 0.2).render()
        )

    def test_toint(self):
        self.assertEqual(
            " | where (toint(stringField)) > 1",
            Query().where(f.to_int(t.stringField) > 1).render()
        )

    def test_tolong(self):
        self.assertEqual(
            " | where (tolong(stringField)) > 2222222222",
            Query().where(f.to_long(t.stringField) > 2222222222).render()
        )

    def test_todatetime(self):
        self.assertEqual(
            " | extend foo = todatetime(stringField)",
            Query().extend(foo=f.to_datetime(t.stringField)).render()
        )

    def test_tolower(self):
        self.assertEqual(
            ' | where (tolower(stringField)) == "foo"',
            Query().where(f.to_lower(t.stringField) == "foo").render()
        )

    def test_toreal(self):
        self.assertEqual(
            " | where (toreal(stringField)) > 0.2",
            Query().where(f.to_real(t.stringField) > 0.2).render()
        )
        
    def test_toupper(self):
        self.assertEqual(
            ' | where (toupper(stringField)) == "FOO"',
            Query().where(f.to_upper(t.stringField) == "FOO").render()
        )

    def test_tohex(self):
        self.assertEqual(
            ' | where (tohex(256)) == "100"',
            Query().where(f.to_hex(256) == "100").render()
        )

    # ------------------------------------------------------
    # Aggregation Functions
    # ------------------------------------------------------

    def test_any(self):
        self.assertEqual(
            " | summarize any(stringField, numField, boolField)",
            Query().summarize(f.any(t.stringField, t.numField, t.boolField)).render()
        )

    def test_any_if(self):
        self.assertEqual(
            " | summarize anyif(stringField, boolField)",
            Query().summarize(f.any_if(t.stringField, t.boolField)).render()
        )

    def test_aggregation_assign_to(self):
        self.assertEqual(
            " | summarize foo = any(stringField)",
            Query().summarize(f.any(t.stringField).assign_to(col.foo)).render()
        )

    def test_aggregation_by_assign_to(self):
        self.assertEqual(
            " | summarize any(stringField) by foo = boolField",
            Query().summarize(f.any(t.stringField)).by(t.boolField.assign_to(col.foo)).render()
        )

    def test_aggregation_assign_to_multiple(self):
        self.assertRaises(
            ValueError("Aggregations cannot be assigned to multiple columns"),
            lambda: f.any(t.stringField).assign_to(col.foo, col.bar)
        )

    def test_arg_max(self):
        self.assertEqual(
            " | summarize arg_max(stringField, numField, boolField)",
            Query().summarize(f.arg_max(t.stringField, t.numField, t.boolField)).render()
        )

    def test_arg_min(self):
        self.assertEqual(
            " | summarize arg_min(stringField, numField, boolField)",
            Query().summarize(f.arg_min(t.stringField, t.numField, t.boolField)).render()
        )

    def test_avg(self):
        self.assertEqual(
            " | summarize avg(numField)",
            Query().summarize(f.avg(t.numField)).render()
        )
        self.assertEqual(
            " | summarize avg(numField) - 5",
            Query().summarize(f.avg(t.numField) - 5).render()
        )

    def test_avgif(self):
        self.assertEqual(
            " | summarize avgif(numField, boolField)",
            Query().summarize(f.avg_if(t.numField, t.boolField)).render()
        )

    def test_bin(self):
        self.assertEqual(
            " | summarize avg(numField) by bin(numField2, 0.1)",
            Query().summarize(f.avg(t.numField)).by(f.bin(t.numField2, 0.1)).render()
        )
        self.assertEqual(
            " | summarize avg(numField2) by bin(dateField, time(0.12:0:0.0))",
            Query().summarize(f.avg(t.numField2)).by(f.bin(t.dateField, timedelta(0.5))).render()
        )
        self.assertEqual(
            " | summarize avg(numField2) by bin(timespanField, time(0.12:0:0.0))",
            Query().summarize(f.avg(t.numField2)).by(f.bin(t.timespanField, timedelta(0.5))).render()
        )

    def test_bin_at(self):
        self.assertEqual(
            " | summarize avg(numField) by bin_at(numField2, 0.1, 1)",
            Query().summarize(f.avg(t.numField)).by(f.bin_at(t.numField2, 0.1, 1)).render()
        )
        self.assertEqual(
            " | summarize avg(numField) by bin_at(dateField, time(0.12:0:0.0), time(0.2:24:0.0))",
            Query().summarize(f.avg(t.numField)).by(f.bin_at(t.dateField, timedelta(0.5), timedelta(0.1))).render()
        )
        self.assertEqual(
            " | summarize avg(numField) by bin_at(dateField, time(0.12:0:0.0), datetime(2019-07-08 00:00:00.000000))",
            Query().summarize(f.avg(t.numField)).by(f.bin_at(t.dateField, timedelta(0.5), datetime(2019, 7, 8))).render()
        )
        self.assertEqual(
            " | summarize avg(numField) by bin_at(timespanField, 0.1, 1)",
            Query().summarize(f.avg(t.numField)).by(f.bin_at(t.timespanField, 0.1, 1)).render()
        )

    def test_bin_auto(self):
        self.assertEqual(
            " | summarize avg(numField) by bin_auto(numField)",
            Query().summarize(f.avg(t.numField)).by(f.bin_auto(t.numField)).render()
        )
        self.assertEqual(
            " | summarize avg(numField) by bin_auto(timespanField)",
            Query().summarize(f.avg(t.numField)).by(f.bin_auto(t.timespanField)).render()
        )

    def test_count(self):
        self.assertEqual(
            " | summarize count()",
            Query().summarize(f.count()).render()
        )
        self.assertEqual(
            " | summarize count(stringField)",
            Query().summarize(f.count(t.stringField)).render()
        )

    def test_countif(self):
        self.assertEqual(
            " | summarize countif(numField == 1)",
            Query().summarize(f.count_if(t.numField == 1)).render()
        )

    def test_dcount(self):
        self.assertEqual(
            " | summarize dcount(stringField)",
            Query().summarize(f.dcount(t.stringField)).render()
        )
        acc = 0.1
        self.assertEqual(
            " | summarize dcount(numField, 0.1)",
            Query().summarize(f.dcount(t.numField, acc)).render()
        )

    def test_dcountif(self):
        self.assertEqual(
            " | summarize dcountif(stringField, boolField, 0)",
            Query().summarize(f.dcount_if(t.stringField, t.boolField)).render()
        )

    def test_make_bag(self):
        self.assertEqual(
            " | summarize make_bag(stringField)",
            Query().summarize(f.make_bag(t.stringField)).render()
        )
        self.assertEqual(
            " | summarize make_bag(stringField, numField)",
            Query().summarize(f.make_bag(t.stringField, t.numField)).render()
        )

    def test_make_list(self):
        self.assertEqual(
            " | summarize make_list(stringField)",
            Query().summarize(f.make_list(t.stringField)).render()
        )
        self.assertEqual(
            " | summarize make_list(stringField, numField)",
            Query().summarize(f.make_list(t.stringField, t.numField)).render()
        )

    def test_make_set(self):
        self.assertEqual(
            " | summarize make_set(stringField)",
            Query().summarize(f.make_set(t.stringField)).render()
        )
        self.assertEqual(
            " | summarize make_set(stringField, numField)",
            Query().summarize(f.make_set(t.stringField, t.numField)).render()
        )

    def test_max(self):
        self.assertEqual(
            " | summarize max(numField)",
            Query().summarize(f.max(t.numField)).render()
        )

    def test_min(self):
        self.assertEqual(
            " | summarize min(numField)",
            Query().summarize(f.min(t.numField)).render()
        )

    def test_max_if(self):
        self.assertEqual(
            " | summarize maxif(numField, boolField)",
            Query().summarize(f.max_if(t.numField, t.boolField)).render()
        )

    def test_min_if(self):
        self.assertEqual(
            " | summarize minif(numField, boolField)",
            Query().summarize(f.min_if(t.numField, t.boolField)).render()
        )

    def test_percentile(self):
        self.assertEqual(
            " | summarize percentiles(numField, 5)",
            Query().summarize(f.percentile(t.numField, 5)).render()
        )

    def test_percentiles(self):
        self.assertEqual(
            " | summarize percentiles(numField, 5, 50, 95)",
            Query().summarize(f.percentiles(t.numField, 5, 50, 95)).render()
        )

    def test_percentiles_array(self):
        self.assertEqual(
            " | summarize percentiles_array(numField, 5, 50, 95)",
            Query().summarize(f.percentiles_array(t.numField, 5, 50, 95)).render()
        )

    def test_stdev(self):
        self.assertEqual(
            " | summarize stdev(numField)",
            Query().summarize(f.stdev(t.numField)).render()
        )

    def test_stdevif(self):
        self.assertEqual(
            " | summarize stdevif(numField, boolField)",
            Query().summarize(f.stdevif(t.numField, t.boolField)).render()
        )

    def test_stdevp(self):
        self.assertEqual(
            " | summarize stdevp(numField)",
            Query().summarize(f.stdevp(t.numField)).render()
        )

    def test_sum(self):
        self.assertEqual(
            " | summarize sum(numField)",
            Query().summarize(f.sum(t.numField)).render()
        )

    def test_sumif(self):
        self.assertEqual(
            " | summarize sumif(numField, boolField)",
            Query().summarize(f.sum_if(t.numField, t.boolField)).render()
        )

    def test_variance(self):
        self.assertEqual(
            " | summarize variance(numField)",
            Query().summarize(f.variance(t.numField)).render()
        )

    def test_varianceif(self):
        self.assertEqual(
            " | summarize varianceif(numField, boolField)",
            Query().summarize(f.variance_if(t.numField, t.boolField)).render()
        )

    def test_variancep(self):
        self.assertEqual(
            " | summarize variancep(numField)",
            Query().summarize(f.variancep(t.numField)).render()
        )

    def test_nesting(self):
        self.assertEqual(
            " | summarize active_days = dcount(bin(dateField, time(1.0:0:0.0)))",
            Query().summarize(active_days=f.dcount(f.bin(t.dateField, timedelta(1)))).render()
        )

    def test_iff(self):
        self.assertEqual(
            " | project foo = iff(dateField > (ago(time(2.0:0:0.0))), time(3.0:0:0.0), time(4.0:0:0.0))",
            Query().project(foo=f.iff(t.dateField > f.ago(timedelta(2)), timedelta(3), timedelta(4))).render()
        )

    def test_iff_expression_return_type(self):
        self.assertEqual(
            " | project foo = iff(dateField > (ago(time(2.0:0:0.0))), array_length(arrayField), array_length(arrayField2))",
            Query().project(foo=f.iff(t.dateField > f.ago(timedelta(2)), f.array_length(t.arrayField), f.array_length(t.arrayField2))).render()
        )

    def test_iff_different_types(self):
        with self.assertLogs(logger, logging.WARN) as cm:
            self.assertEqual(
                ' | project foo = iff(dateField > (ago(time(2.0:0:0.0))), time(3.0:0:0.0), "hello")',
                Query().project(foo=f.iff(t.dateField > f.ago(timedelta(2)), timedelta(3), "hello")).render()
            )
        self.assertEqual(
            ['WARNING:pykusto:The second and third arguments must be of the same type, but they are: timespan and string. '
             'If this is a mistake, please report it at https://github.com/Azure/pykusto/issues'],
            cm.output
        )

    def test_iff_related_types(self):
        self.assertEqual(
            " | project foo = iff(dateField > (ago(time(2.0:0:0.0))), 2, array_length(arrayField))",
            Query().project(foo=f.iff(t.dateField > f.ago(timedelta(2)), 2, f.array_length(t.arrayField))).render()
        )

    def test_iff_ambiguous_type(self):
        with self.assertLogs(logger, logging.WARN) as cm:
            self.assertEqual(
                " | project foo = iff(boolField, time(3.0:0:0.0), foo - bar)",
                Query().project(foo=f.iff(t.boolField, timedelta(3), col.foo - col.bar)).render()
            )
        self.assertEqual([], cm.output)

    def test_iif(self):
        # iif is just an alias to iff
        self.assertEqual(
            " | project foo = iff(dateField > (ago(time(2.0:0:0.0))), time(3.0:0:0.0), time(4.0:0:0.0))",
            Query().project(foo=f.iif(t.dateField > f.ago(timedelta(2)), timedelta(3), timedelta(4))).render()
        )

    def test_pack(self):
        self.assertEqual(
            ' | extend foo = pack("bar", numField, "baz", stringField)',
            Query().extend(foo=f.pack(bar=t.numField, baz=t.stringField)).render()
        )

    def test_pack_array(self):
        self.assertEqual(
            ' | extend foo = pack_array(numField, stringField)',
            Query().extend(foo=f.pack_array(t.numField, t.stringField)).render()
        )

    def test_set_has_element(self):
        self.assertEqual(
            ' | extend foo = set_has_element(arrayField, numField)',
            Query().extend(foo=f.set_has_element(t.arrayField, t.numField)).render()
        )

    def test_set_difference(self):
        self.assertEqual(
            ' | extend foo = set_difference(arrayField, arrayField2)',
            Query().extend(foo=f.set_difference(t.arrayField, t.arrayField2)).render()
        )

    def test_set_intersect(self):
        self.assertEqual(
            ' | extend foo = set_intersect(arrayField, arrayField2)',
            Query().extend(foo=f.set_intersect(t.arrayField, t.arrayField2)).render()
        )

    def test_set_union(self):
        self.assertEqual(
            ' | extend foo = set_union(arrayField, arrayField2)',
            Query().extend(foo=f.set_union(t.arrayField, t.arrayField2)).render()
        )

    def test_array_concat(self):
        self.assertEqual(
            ' | extend foo = array_concat(arrayField, arrayField2)',
            Query().extend(foo=f.array_concat(t.arrayField, t.arrayField2)).render()
        )

    def test_array_iif(self):
        self.assertEqual(
            ' | extend foo = array_iif(arrayField, arrayField2, arrayField3)',
            Query().extend(foo=f.array_iif(t.arrayField, t.arrayField2, t.arrayField3)).render()
        )

    def test_array_index_of(self):
        self.assertEqual(
            ' | extend foo = array_index_of(arrayField, numField)',
            Query().extend(foo=f.array_index_of(t.arrayField, t.numField)).render()
        )

    def test_array_rotate_left(self):
        self.assertEqual(
            ' | extend foo = array_rotate_left(arrayField, numField)',
            Query().extend(foo=f.array_rotate_left(t.arrayField, t.numField)).render()
        )

    def test_array_rotate_right(self):
        self.assertEqual(
            ' | extend foo = array_rotate_right(arrayField, numField)',
            Query().extend(foo=f.array_rotate_right(t.arrayField, t.numField)).render()
        )

    def test_array_shift_left(self):
        self.assertEqual(
            ' | extend foo = array_shift_left(arrayField, numField)',
            Query().extend(foo=f.array_shift_left(t.arrayField, t.numField)).render()
        )

    def test_array_shift_right(self):
        self.assertEqual(
            ' | extend foo = array_shift_right(arrayField, numField, numField2)',
            Query().extend(foo=f.array_shift_right(t.arrayField, t.numField, t.numField2)).render()
        )

    def test_array_slice(self):
        self.assertEqual(
            ' | extend foo = array_slice(arrayField, numField, numField2)',
            Query().extend(foo=f.array_slice(t.arrayField, t.numField, t.numField2)).render()
        )

    def test_array_split(self):
        self.assertEqual(
            ' | extend foo = array_split(arrayField, numField)',
            Query().extend(foo=f.array_split(t.arrayField, t.numField)).render()
        )

    def test_ingestion_time(self):
        self.assertEqual(
            ' | extend ingestionTime = ingestion_time()',
            Query().extend(ingestionTime=f.ingestion_time()).render()
        )
