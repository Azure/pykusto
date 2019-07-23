import datetime

from pykusto import functions as f
from pykusto.column import column_generator as col
from pykusto.query import Query
from test.test_base import TestBase


# TODO bin, bin_at, bin_auto, dcount_hll, floor, iif

class TestFunction(TestBase):
    def test_acos(self):
        self.assertEqual(
            Query().where(f.acos(col.foo) > 4).render(),
            " | where (acos(foo)) > 4")

    def test_ago(self):
        self.assertEqual(
            Query().where(col.foo > f.ago(datetime.timedelta(4))).render(),
            " | where foo > (ago(time(4.0:0:0.0)))")

    def test_array_length(self):
        self.assertEqual(
            Query().where(f.array_length(col.foo) > 4).render(),
            " | where (array_length(foo)) > 4")

    def test_ceiling(self):
        self.assertEqual(
            Query().where(f.ceiling(col.foo) > 4).render(),
            " | where (ceiling(foo)) > 4")

    def test_endofday(self):
        self.assertEqual(
            Query().where(f.endofday(col.foo) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofday(foo)) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.endofday(col.foo, 2) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofday(foo, 2)) > datetime(2019-07-23 00:00:00.000000)")

    def test_endofmonth(self):
        self.assertEqual(
            Query().where(f.endofmonth(col.foo) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofmonth(foo)) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.endofmonth(col.foo, 2) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofmonth(foo, 2)) > datetime(2019-07-23 00:00:00.000000)")

    def test_endofweek(self):
        self.assertEqual(
            Query().where(f.endofweek(col.foo) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofweek(foo)) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.endofweek(col.foo, 2) > datetime.datetime(2019, 7, 23)).render(),
            " | where (endofweek(foo, 2)) > datetime(2019-07-23 00:00:00.000000)")

    def test_exp(self):
        self.assertEqual(
            Query().where(f.exp(col.foo) > 4).render(),
            " | where (exp(foo)) > 4")

    def test_exp10(self):
        self.assertEqual(
            Query().where(f.exp10(col.foo) > 4).render(),
            " | where (exp10(foo)) > 4")

    def test_exp2(self):
        self.assertEqual(
            Query().where(f.exp2(col.foo) > 4).render(),
            " | where (exp2(foo)) > 4")

    def test_format_datetime(self):
        self.assertEqual(
            Query().where(f.format_datetime(col.foo, 'yy-MM-dd [HH:mm:ss]') == '2019-07-23 00:00:00').render(),
            " | where (format_datetime(foo, 'yy-MM-dd [HH:mm:ss]')) == \"2019-07-23 00:00:00\"")

    def test_format_timespan(self):
        self.assertEqual(
            Query().where(f.format_timespan(col.foo, 'h:m:s.fffffff') == "2:3:4.1234500").render(),
            " | where (format_timespan(foo, 'h:m:s.fffffff')) == \"2:3:4.1234500\"")

    def test_getmonth(self):
        self.assertEqual(
            Query().where(f.getmonth(col.foo) > 3).render(),
            " | where (getmonth(foo)) > 3")

    def test_gettype(self):
        self.assertEqual(
            Query().where(f.gettype(col.foo) == 'datetime').render(),
            " | where (gettype(foo)) == \"datetime\"")

    def test_getyear(self):
        self.assertEqual(
            Query().where(f.getyear(col.foo) > 2019).render(),
            " | where (getyear(foo)) > 2019")

    def test_hourofday(self):
        self.assertEqual(
            Query().where(f.hourofday(col.foo) == 3).render(),
            " | where (hourofday(foo)) == 3")

    def test_isempty(self):
        self.assertEqual(
            Query().where(f.isempty(col.foo)).render(),
            " | where isempty(foo)")

    def test_isfinite(self):
        self.assertEqual(
            Query().where(f.isfinite(col.foo)).render(),
            " | where isfinite(foo)")

    def test_isinf(self):
        self.assertEqual(
            Query().where(f.isinf(col.foo)).render(),
            " | where isinf(foo)")

    def test_isnan(self):
        self.assertEqual(
            Query().where(f.isnan(col.foo)).render(),
            " | where isnan(foo)")

    def test_isnotempty(self):
        self.assertEqual(
            Query().where(f.isnotempty(col.foo)).render(),
            " | where isnotempty(foo)")

    def test_isnotnull(self):
        self.assertEqual(
            Query().where(f.isnotnull(col.foo)).render(),
            " | where isnotnull(foo)")

    def test_isnull(self):
        self.assertEqual(
            Query().where(f.isnull(col.foo)).render(),
            " | where isnull(foo)")

    def test_isutf8(self):
        self.assertEqual(
            Query().where(f.isutf8(col.foo)).render(),
            " | where isutf8(foo)")

    def test_log(self):
        self.assertEqual(
            Query().where(f.log(col.foo) < 3).render(),
            " | where (log(foo)) < 3")

    def test_log10(self):
        self.assertEqual(
            Query().where(f.log10(col.foo) < 3).render(),
            " | where (log10(foo)) < 3")

    def test_log2(self):
        self.assertEqual(
            Query().where(f.log2(col.foo) < 3).render(),
            " | where (log2(foo)) < 3")

    def test_loggamma(self):
        self.assertEqual(
            Query().where(f.loggamma(col.foo) < 3).render(),
            " | where (loggamma(foo)) < 3")

    def test_make_datetime(self):
        self.assertEqual(
            Query().where(f.make_datetime(col.y, col.m, col.d) > datetime.datetime(2019, 7, 23)).render(),
            " | where (make_datetime(y, m, d)) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.make_datetime(col.y, col.m, col.d, col.h) > datetime.datetime(2019, 7, 23, 5)).render(),
            " | where (make_datetime(y, m, d, h, 0)) > datetime(2019-07-23 05:00:00.000000)")
        self.assertEqual(
            Query().where(
                f.make_datetime(col.y, col.m, col.d, col.h, col.min) > datetime.datetime(2019, 7, 23, 5, 29)).render(),
            " | where (make_datetime(y, m, d, h, min)) > datetime(2019-07-23 05:29:00.000000)")
        self.assertEqual(
            Query().where(
                f.make_datetime(col.y, col.m, col.d, col.h, col.min, col.sec) > datetime.datetime(2019, 7, 23, 5, 29,
                                                                                                  15)).render(),
            " | where (make_datetime(y, m, d, h, min, sec)) > datetime(2019-07-23 05:29:15.000000)")

    def test_now(self):
        self.assertEqual(
            Query().where(col.foo < f.now()).render(),
            " | where foo < (now())")
        self.assertEqual(
            Query().where(col.foo < f.now(datetime.timedelta(-3))).render(),
            " | where foo < (now(time(-3.0:0:0.0)))")
