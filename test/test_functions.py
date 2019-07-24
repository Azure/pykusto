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
        self.assertEqual(
            Query().where(col.foo > f.ago(col.timespan)).render(),
            " | where foo > (ago(timespan))")

    def test_array_length(self):
        self.assertEqual(
            Query().where(f.array_length(col.foo) > 4).render(),
            " | where (array_length(foo)) > 4")

    def test_bag_keys(self):
        self.assertEqual(
            Query().where(f.bag_keys(col.foo).array_length() > 4).render(),
            " | where (array_length(bag_keys(foo))) > 4")

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

    def test_floor(self):
        self.assertEqual(
            Query().where(f.floor(col.foo, datetime.timedelta(0.5)) > datetime.datetime(2019, 7, 23)).render(),
            " | where (floor(foo, time(0.12:0:0.0))) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.floor(col.foo, 0.1) > 3).render(),
            " | where (floor(foo, 0.1)) > 3")
        self.assertEqual(
            Query().where(col.foo.floor(0.1) > 3).render(),
            " | where (floor(foo, 0.1)) > 3")

    def test_format_datetime(self):
        self.assertEqual(
            Query().where(f.format_datetime(col.foo, 'yy-MM-dd [HH:mm:ss]') == '2019-07-23 00:00:00').render(),
            " | where (format_datetime(foo, \"yy-MM-dd [HH:mm:ss]\")) == \"2019-07-23 00:00:00\"")

    def test_format_timespan(self):
        self.assertEqual(
            Query().where(f.format_timespan(col.foo, 'h:m:s.fffffff') == "2:3:4.1234500").render(),
            " | where (format_timespan(foo, \"h:m:s.fffffff\")) == \"2:3:4.1234500\"")

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
            " | where (make_datetime(y, m, d, 0, 0, 0)) > datetime(2019-07-23 00:00:00.000000)")
        self.assertEqual(
            Query().where(f.make_datetime(col.y, col.m, col.d, col.h) > datetime.datetime(2019, 7, 23, 5)).render(),
            " | where (make_datetime(y, m, d, h, 0, 0)) > datetime(2019-07-23 05:00:00.000000)")
        self.assertEqual(
            Query().where(
                f.make_datetime(col.y, col.m, col.d, col.h, col.min) > datetime.datetime(2019, 7, 23, 5, 29)).render(),
            " | where (make_datetime(y, m, d, h, min, 0)) > datetime(2019-07-23 05:29:00.000000)")
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

    def test_pow(self):
        self.assertEqual(
            Query().where(f.pow(col.foo, col.bar) > 3).render(),
            " | where (pow(foo, bar)) > 3")

    def test_round(self):
        self.assertEqual(
            Query().where(f.round(col.foo, col.bar) == 3).render(),
            " | where (round(foo, bar)) == 3")

    def test_sign(self):
        self.assertEqual(
            Query().where(f.sign(col.foo) == 1).render(),
            " | where (sign(foo)) == 1")

    def test_sqrt(self):
        self.assertEqual(
            Query().where(f.sqrt(col.foo) > 1).render(),
            " | where (sqrt(foo)) > 1")

    def test_startofday(self):
        self.assertEqual(
            Query().where(f.startofday(col.foo) > datetime.datetime(2019, 7, 23)).render(),
            " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)")

    def test_startofmonth(self):
        self.assertEqual(
            Query().where(f.startofmonth(col.foo) > datetime.datetime(2019, 7, 1)).render(),
            " | where (startofmonth(foo)) > datetime(2019-07-01 00:00:00.000000)")

    def test_startofweek(self):
        self.assertEqual(
            Query().where(f.startofweek(col.foo) > datetime.datetime(2019, 7, 8)).render(),
            " | where (startofweek(foo)) > datetime(2019-07-08 00:00:00.000000)")

    def test_startofyear(self):
        self.assertEqual(
            Query().where(f.startofyear(col.foo) > datetime.datetime(2019, 1, 1)).render(),
            " | where (startofyear(foo)) > datetime(2019-01-01 00:00:00.000000)")

    def test_strcat_array(self):
        self.assertEqual(
            Query().where(f.strcat_array(col.foo, ',') == 'A,B,C').render(),
            " | where (strcat_array(foo, \",\")) == \"A,B,C\"")
        self.assertEqual(
            Query().where(f.strcat_array(['A', 'B', 'C'], ',') == 'A,B,C').render(),
            " | where (strcat_array(['A', 'B', 'C'], \",\")) == \"A,B,C\"")

    def test_strcmp(self):
        self.assertEqual(
            Query().where(f.strcmp(col.foo, col.bar) == 1).render(),
            " | where (strcmp(foo, bar)) == 1")

    def test_string_size(self):
        self.assertEqual(
            Query().where(f.string_size(col.foo) == 1).render(),
            " | where (string_size(foo)) == 1")

    def test_strlen(self):
        self.assertEqual(
            Query().where(f.strlen(col.foo) == 1).render(),
            " | where (strlen(foo)) == 1")

    def test_strrep(self):
        self.assertEqual(
            Query().where(f.strrep(col.foo, col.bar) == 'ABCABC').render(),
            " | where (strrep(foo, bar)) == \"ABCABC\"")
        self.assertEqual(
            Query().where(f.strrep(col.foo, col.bar, ',') == 'ABC,ABC').render(),
            " | where (strrep(foo, bar, \",\")) == \"ABC,ABC\"")
        self.assertEqual(
            Query().where(f.strrep(col.foo, col.bar, col.fam) == 'ABC,ABC').render(),
            " | where (strrep(foo, bar, fam)) == \"ABC,ABC\"")

    def test_substring(self):
        self.assertEqual(
            Query().where(f.substring(col.foo, col.bar) == 'ABCABC').render(),
            " | where (substring(foo, bar)) == \"ABCABC\"")
        self.assertEqual(
            Query().where(f.substring(col.foo, col.bar, 4) == 'ABC,ABC').render(),
            " | where (substring(foo, bar, 4)) == \"ABC,ABC\"")

    def test_tobool(self):
        self.assertEqual(
            Query().where(f.tobool(col.foo)).render(),
            " | where tobool(foo)")

    def test_toboolean(self):
        self.assertEqual(
            Query().where(f.toboolean(col.foo)).render(),
            " | where toboolean(foo)")

    # def test_todatetime(self):
    #     self.assertEqual(
    #         Query().where(f.todatetime(col.foo) > datetime.datetime(2019, 7, 23)).render(),
    #         " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)")
    #     self.assertEqual(
    #         Query().where(f.todatetime('') > datetime.datetime(2019, 7, 23)).render(),
    #         " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)")
    # def todatetime(expr: StringType) -> DatetimeExpression:
    #     return DatetimeExpression(KQL('todatetime({})'.format(expr)))

    # ------------------------------------------------------
    # Aggregative Functions
    # ------------------------------------------------------

    def test_any(self):
        self.assertEqual(
            Query().summarize(f.any(col.foo, col.bar, col.fam)).render(),
            " | summarize any(foo, bar, fam)")

    def test_arg_max(self):
        self.assertEqual(
            Query().summarize(f.arg_max(col.foo, col.bar, col.fam)).render(),
            " | summarize arg_max(foo, bar, fam)")

    def test_arg_min(self):
        self.assertEqual(
            Query().summarize(f.arg_min(col.foo, col.bar, col.fam)).render(),
            " | summarize arg_min(foo, bar, fam)")

    def test_avg(self):
        self.assertEqual(
            Query().summarize(f.avg(col.foo)).render(),
            " | summarize avg(foo)")
        # self.assertEqual( TODO: fix
        #     Query().summarize(f.avg(col.foo) - 5).render(),
        #     " | summarize avg(foo)-5")

    def test_avgif(self):
        self.assertEqual(
            Query().summarize(f.avgif(col.foo, col.bar)).render(),
            " | summarize avgif(foo, bar)")

    def test_count(self):
        self.assertEqual(
            Query().summarize(f.count()).render(),
            " | summarize count()")
        self.assertEqual(
            Query().summarize(f.count(col.foo)).render(),
            " | summarize count(foo)")

    def test_countif(self):
        self.assertEqual(
            Query().summarize(f.countif(col.foo == 1)).render(),
            " | summarize countif(foo == 1)")

    def test_dcount(self):
        self.assertEqual(
            Query().summarize(f.dcount(col.foo)).render(),
            " | summarize dcount(foo)")
        acc = 0.1
        self.assertEqual(
            Query().summarize(f.dcount(col.foo, acc)).render(),
            " | summarize dcount(foo, 0.1)")

    def test_hll(self):
        self.assertEqual(
            Query().summarize(f.hll(col.foo)).render(),
            " | summarize hll(foo)")
        acc = 0.1
        self.assertEqual(
            Query().summarize(f.hll(col.foo, acc)).render(),
            " | summarize hll(foo, 0.1)")

    def test_hll_merge(self):
        self.assertEqual(
            Query().summarize(f.hll_merge(col.foo)).render(),
            " | summarize hll_merge(foo)")

    def test_make_bag(self):
        self.assertEqual(
            Query().summarize(f.make_bag(col.foo)).render(),
            " | summarize make_bag(foo)")

    def test_make_list(self):
        self.assertEqual(
            Query().summarize(f.make_list(col.foo)).render(),
            " | summarize make_list(foo)")

    def test_make_set(self):
        self.assertEqual(
            Query().summarize(f.make_set(col.foo)).render(),
            " | summarize make_set(foo)")

    def test_max(self):
        self.assertEqual(
            Query().summarize(f.max(col.foo)).render(),
            " | summarize max(foo)")

    def test_min(self):
        self.assertEqual(
            Query().summarize(f.min(col.foo)).render(),
            " | summarize min(foo)")

    def test_stdev(self):
        self.assertEqual(
            Query().summarize(f.stdev(col.foo)).render(),
            " | summarize stdev(foo)")

    def test_stdevif(self):
        self.assertEqual(
            Query().summarize(f.stdevif(col.foo, col.bar)).render(),
            " | summarize stdevif(foo, bar)")

    def test_stdevp(self):
        self.assertEqual(
            Query().summarize(f.stdevp(col.foo)).render(),
            " | summarize stdevp(foo)")

    def test_sum(self):
        self.assertEqual(
            Query().summarize(f.sum(col.foo)).render(),
            " | summarize sum(foo)")

    def test_sumif(self):
        self.assertEqual(
            Query().summarize(f.sumif(col.foo, col.bar)).render(),
            " | summarize sumif(foo, bar)")

    def test_variance(self):
        self.assertEqual(
            Query().summarize(f.variance(col.foo)).render(),
            " | summarize variance(foo)")

    def test_varianceif(self):
        self.assertEqual(
            Query().summarize(f.varianceif(col.foo, col.bar)).render(),
            " | summarize varianceif(foo, bar)")

    def test_variancep(self):
        self.assertEqual(
            Query().summarize(f.variancep(col.foo)).render(),
            " | summarize variancep(foo)")
