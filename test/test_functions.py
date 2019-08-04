import datetime

from pykusto import functions as f
from pykusto.expressions import column_generator as col
from pykusto.query import Query
from test.test_base import TestBase


# TODO dcount_hll, iif

class TestFunction(TestBase):
    def test_acos(self):
        self.assertEqual(
            " | where (acos(foo)) > 4",
            Query().where(f.acos(col.foo) > 4).render()
        )

    def test_ago(self):
        self.assertEqual(
            " | where foo > (ago(time(4.0:0:0.0)))",
            Query().where(col.foo > f.ago(datetime.timedelta(4))).render()
        )
        self.assertEqual(
            " | where foo > (ago(timespan))",
            Query().where(col.foo > f.ago(col.timespan)).render()
        )

    def test_array_length(self):
        self.assertEqual(
            " | where (array_length(foo)) > 4",
            Query().where(f.array_length(col.foo) > 4).render()
        )

    def test_bag_keys(self):
        self.assertEqual(
            " | where (array_length(bag_keys(foo))) > 4",
            Query().where(f.bag_keys(col.foo).array_length() > 4).render()
        )

    def test_case(self):
        self.assertEqual(
            " | extend bucket = case((foo <= 3), \"Small\", (foo <= 10), \"Medium\", \"Large\")",
            Query().extend(bucket=f.case(col.foo <= 3, "Small", col.foo <= 10, 'Medium', 'Large')).render()
        )

    def test_ceiling(self):
        self.assertEqual(
            " | where (ceiling(foo)) > 4",
            Query().where(f.ceiling(col.foo) > 4).render()
        )

    def test_cos(self):
        self.assertEqual(
            " | where (cos(foo)) > 4",
            Query().where(f.cos(col.foo) > 4).render()
        )

    def test_endofday(self):
        self.assertEqual(
            " | where (endofday(foo)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofday(col.foo) > datetime.datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofday(foo, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofday(col.foo, 2) > datetime.datetime(2019, 7, 23)).render()
        )

    def test_endofmonth(self):
        self.assertEqual(
            " | where (endofmonth(foo)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofmonth(col.foo) > datetime.datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofmonth(foo, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofmonth(col.foo, 2) > datetime.datetime(2019, 7, 23)).render()
        )

    def test_endofweek(self):
        self.assertEqual(
            " | where (endofweek(foo)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofweek(col.foo) > datetime.datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (endofweek(foo, 2)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.endofweek(col.foo, 2) > datetime.datetime(2019, 7, 23)).render()
        )

    def test_exp(self):
        self.assertEqual(
            " | where (exp(foo)) > 4",
            Query().where(f.exp(col.foo) > 4).render()
        )

    def test_exp10(self):
        self.assertEqual(
            " | where (exp10(foo)) > 4",
            Query().where(f.exp10(col.foo) > 4).render()
        )

    def test_exp2(self):
        self.assertEqual(
            " | where (exp2(foo)) > 4",
            Query().where(f.exp2(col.foo) > 4).render()
        )

    def test_floor(self):
        self.assertEqual(
            " | where (floor(foo, time(0.12:0:0.0))) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.floor(col.foo, datetime.timedelta(0.5)) > datetime.datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (floor(foo, 0.1)) > 3",
            Query().where(f.floor(col.foo, 0.1) > 3).render()
        )
        self.assertEqual(
            " | where (floor(foo, 0.1)) > 3",
            Query().where(col.foo.floor(0.1) > 3).render()
        )

    def test_format_datetime(self):
        self.assertEqual(
            " | where (format_datetime(foo, \"yy-MM-dd [HH:mm:ss]\")) == \"2019-07-23 00:00:00\"",
            Query().where(f.format_datetime(col.foo, 'yy-MM-dd [HH:mm:ss]') == '2019-07-23 00:00:00').render()
        )

    def test_format_timespan(self):
        self.assertEqual(
            " | where (format_timespan(foo, \"h:m:s.fffffff\")) == \"2:3:4.1234500\"",
            Query().where(f.format_timespan(col.foo, 'h:m:s.fffffff') == "2:3:4.1234500").render()
        )

    def test_getmonth(self):
        self.assertEqual(
            " | where (getmonth(foo)) > 3",
            Query().where(f.getmonth(col.foo) > 3).render()
        )

    def test_gettype(self):
        self.assertEqual(
            " | where (gettype(foo)) == \"datetime\"",
            Query().where(f.gettype(col.foo) == 'datetime').render()
        )

    def test_getyear(self):
        self.assertEqual(
            " | where (getyear(foo)) > 2019",
            Query().where(f.getyear(col.foo) > 2019).render()
        )

    def test_hourofday(self):
        self.assertEqual(
            " | where (hourofday(foo)) == 3",
            Query().where(f.hourofday(col.foo) == 3).render()
        )

    def test_hash(self):
        self.assertEqual(
            " | where (hash(foo)) == 3",
            Query().where(f.hash(col.foo) == 3).render()
        )

    def test_hash_sha256(self):
        self.assertEqual(
            " | where (hash_sha256(foo)) == 3",
            Query().where(f.hash_sha256(col.foo) == 3).render()
        )

    def test_isempty(self):
        self.assertEqual(
            " | where isempty(foo)",
            Query().where(f.isempty(col.foo)).render()
        )

    def test_isfinite(self):
        self.assertEqual(
            " | where isfinite(foo)",
            Query().where(f.isfinite(col.foo)).render()
        )

    def test_isinf(self):
        self.assertEqual(
            " | where isinf(foo)",
            Query().where(f.isinf(col.foo)).render()
        )

    def test_isnan(self):
        self.assertEqual(
            " | where isnan(foo)",
            Query().where(f.isnan(col.foo)).render()
        )

    def test_isnotempty(self):
        self.assertEqual(
            " | where isnotempty(foo)",
            Query().where(f.isnotempty(col.foo)).render()
        )

    def test_isnotnull(self):
        self.assertEqual(
            " | where isnotnull(foo)",
            Query().where(f.isnotnull(col.foo)).render()
        )

    def test_isnull(self):
        self.assertEqual(
            " | where isnull(foo)",
            Query().where(f.isnull(col.foo)).render()
        )

    def test_isutf8(self):
        self.assertEqual(
            " | where isutf8(foo)",
            Query().where(f.isutf8(col.foo)).render()
        )

    def test_log(self):
        self.assertEqual(
            " | where (log(foo)) < 3",
            Query().where(f.log(col.foo) < 3).render()
        )

    def test_log10(self):
        self.assertEqual(
            " | where (log10(foo)) < 3",
            Query().where(f.log10(col.foo) < 3).render(),
        )

    def test_log2(self):
        self.assertEqual(
            " | where (log2(foo)) < 3",
            Query().where(f.log2(col.foo) < 3).render()
        )

    def test_loggamma(self):
        self.assertEqual(
            " | where (loggamma(foo)) < 3",
            Query().where(f.loggamma(col.foo) < 3).render()
        )

    def test_make_datetime(self):
        self.assertEqual(
            " | where (make_datetime(y, m, d, 0, 0, 0)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.make_datetime(col.y, col.m, col.d) > datetime.datetime(2019, 7, 23)).render()
        )
        self.assertEqual(
            " | where (make_datetime(y, m, d, h, 0, 0)) > datetime(2019-07-23 05:00:00.000000)",
            Query().where(f.make_datetime(col.y, col.m, col.d, col.h) > datetime.datetime(2019, 7, 23, 5)).render()
        )
        self.assertEqual(
            " | where (make_datetime(y, m, d, h, min, 0)) > datetime(2019-07-23 05:29:00.000000)",
            Query().where(
                f.make_datetime(col.y, col.m, col.d, col.h, col.min) > datetime.datetime(2019, 7, 23, 5, 29)).render()
        )
        self.assertEqual(
            " | where (make_datetime(y, m, d, h, min, sec)) > datetime(2019-07-23 05:29:15.000000)",
            Query().where(
                f.make_datetime(col.y, col.m, col.d, col.h, col.min, col.sec) > datetime.datetime(2019, 7, 23, 5, 29,
                                                                                                  15)).render()
        )

    def test_now(self):
        self.assertEqual(
            " | where foo < (now())",
            Query().where(col.foo < f.now()).render()
        )
        self.assertEqual(
            " | where foo < (now(time(-3.0:0:0.0)))",
            Query().where(col.foo < f.now(datetime.timedelta(-3))).render()
        )

    def test_pow(self):
        self.assertEqual(
            " | where (pow(foo, bar)) > 3",
            Query().where(f.pow(col.foo, col.bar) > 3).render()
        )

    def test_round(self):
        self.assertEqual(
            " | where (round(foo, bar)) == 3",
            Query().where(f.round(col.foo, col.bar) == 3).render()
        )

    def test_sign(self):
        self.assertEqual(
            " | where (sign(foo)) == 1",
            Query().where(f.sign(col.foo) == 1).render()
        )

    def test_sqrt(self):
        self.assertEqual(
            " | where (sqrt(foo)) > 1",
            Query().where(f.sqrt(col.foo) > 1).render()
        )

    def test_startofday(self):
        self.assertEqual(
            " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)",
            Query().where(f.startofday(col.foo) > datetime.datetime(2019, 7, 23)).render()
        )

    def test_startofmonth(self):
        self.assertEqual(
            " | where (startofmonth(foo)) > datetime(2019-07-01 00:00:00.000000)",
            Query().where(f.startofmonth(col.foo) > datetime.datetime(2019, 7, 1)).render()
        )

    def test_startofweek(self):
        self.assertEqual(
            " | where (startofweek(foo)) > datetime(2019-07-08 00:00:00.000000)",
            Query().where(f.startofweek(col.foo) > datetime.datetime(2019, 7, 8)).render()
        )

    def test_startofyear(self):
        self.assertEqual(
            " | where (startofyear(foo)) > datetime(2019-01-01 00:00:00.000000)",
            Query().where(f.startofyear(col.foo) > datetime.datetime(2019, 1, 1)).render()
        )

    def test_strcat(self):
        self.assertEqual(
            " | extend (strcat(\"hello\", \",\", foo, \"!\"))",
            Query().extend(f.strcat("hello", ',', col.foo, '!')).render()
        )

    def test_strcat_delim(self):
        self.assertEqual(
            " | extend (strcat_delim(\"-\", \"hello\", \",\", foo, \"!\"))",
            Query().extend(f.strcat_delim('-', "hello", ',', col.foo, '!')).render()
        )

    def test_strcat_array(self):
        self.assertEqual(
            " | where (strcat_array(foo, \",\")) == \"A,B,C\"",
            Query().where(f.strcat_array(col.foo, ',') == 'A,B,C').render()
        )
        self.assertEqual(
            " | where (strcat_array(['A', 'B', 'C'], \",\")) == \"A,B,C\"",
            Query().where(f.strcat_array(['A', 'B', 'C'], ',') == 'A,B,C').render()
        )

    def test_strcmp(self):
        self.assertEqual(
            " | where (strcmp(foo, bar)) == 1",
            Query().where(f.strcmp(col.foo, col.bar) == 1).render()
        )

    def test_string_size(self):
        self.assertEqual(
            " | where (string_size(foo)) == 1",
            Query().where(f.string_size(col.foo) == 1).render()
        )

    def test_strlen(self):
        self.assertEqual(
            " | where (strlen(foo)) == 1",
            Query().where(f.strlen(col.foo) == 1).render()
        )

    def test_strrep(self):
        self.assertEqual(
            " | where (strrep(foo, bar)) == \"ABCABC\"",
            Query().where(f.strrep(col.foo, col.bar) == 'ABCABC').render()
        )
        self.assertEqual(
            " | where (strrep(foo, bar, \",\")) == \"ABC,ABC\"",
            Query().where(f.strrep(col.foo, col.bar, ',') == 'ABC,ABC').render()
        )
        self.assertEqual(
            " | where (strrep(foo, bar, fam)) == \"ABC,ABC\"",
            Query().where(f.strrep(col.foo, col.bar, col.fam) == 'ABC,ABC').render()
        )

    def test_substring(self):
        self.assertEqual(
            " | where (substring(foo, bar)) == \"ABCABC\"",
            Query().where(f.substring(col.foo, col.bar) == 'ABCABC').render()
        )
        self.assertEqual(
            " | where (substring(foo, bar, 4)) == \"ABC,ABC\"",
            Query().where(f.substring(col.foo, col.bar, 4) == 'ABC,ABC').render()
        )

    def test_tobool(self):
        self.assertEqual(
            " | where tobool(foo)",
            Query().where(f.tobool(col.foo)).render()
        )

    def test_toboolean(self):
        self.assertEqual(
            " | where toboolean(foo)",
            Query().where(f.toboolean(col.foo)).render()
        )

    # def test_todatetime(self):
    #     self.assertEqual(
    #         Query().where(f.todatetime(col.foo) > datetime.datetime(2019, 7, 23)).render(),
    #         " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)")
    #     self.assertEqual(
    #         Query().where(f.todatetime('') > datetime.datetime(2019, 7, 23)).render(),
    #         " | where (startofday(foo)) > datetime(2019-07-23 00:00:00.000000)")
    # def todatetime(expr: StringType) -> DatetimeExpression:
    #     return DatetimeExpression(KQL('todatetime({})'.format(expr)))

    def test_todouble(self):
        self.assertEqual(
            " | where (todouble(foo)) > 0.2",
            Query().where(f.todouble(col.foo) > 0.2).render()
        )

    # ------------------------------------------------------
    # Aggregative Functions
    # ------------------------------------------------------

    def test_any(self):
        self.assertEqual(
            " | summarize any(foo, bar, fam)",
            Query().summarize(f.any(col.foo, col.bar, col.fam)).render()
        )

    def test_arg_max(self):
        self.assertEqual(
            " | summarize arg_max(foo, bar, fam)",
            Query().summarize(f.arg_max(col.foo, col.bar, col.fam)).render()
        )

    def test_arg_min(self):
        self.assertEqual(
            " | summarize arg_min(foo, bar, fam)",
            Query().summarize(f.arg_min(col.foo, col.bar, col.fam)).render()
        )

    def test_avg(self):
        self.assertEqual(
            " | summarize avg(foo)",
            Query().summarize(f.avg(col.foo)).render()
        )
        self.assertEqual(
            " | summarize avg(foo)-5",
            Query().summarize(f.avg(col.foo) - 5).render()
        )

    def test_avgif(self):
        self.assertEqual(
            " | summarize avgif(foo, bar)",
            Query().summarize(f.avgif(col.foo, col.bar)).render()
        )

    def test_bin(self):
        self.assertEqual(
            " | summarize avg(foo) by bin(bar, 0.1)",
            Query().summarize(f.avg(col.foo)).by(f.bin(col.bar, 0.1)).render()
        )
        self.assertEqual(
            " | summarize avg(foo) by bin(bar, time(0.12:0:0.0))",
            Query().summarize(f.avg(col.foo)).by(f.bin(col.bar, datetime.timedelta(0.5))).render()
        )

    def test_bin_at(self):
        self.assertEqual(
            " | summarize avg(foo) by bin_at(bar, 0.1, 1)",
            Query().summarize(f.avg(col.foo)).by(f.bin_at(col.bar, 0.1, 1)).render()
        )
        self.assertEqual(
            " | summarize avg(foo) by bin_at(bar, time(0.12:0:0.0), time(0.2:24:0.0))",
            Query().summarize(f.avg(col.foo)).by(f.bin_at(col.bar,
                                                          datetime.timedelta(0.5),
                                                          datetime.timedelta(0.1))).render()
        )
        self.assertEqual(
            " | summarize avg(foo) by bin_at(bar, time(0.12:0:0.0), datetime(2019-07-08 00:00:00.000000))",
            Query().summarize(f.avg(col.foo)).by(f.bin_at(col.bar,
                                                          datetime.timedelta(0.5),
                                                          datetime.datetime(2019, 7, 8))).render()
        )

    def test_bin_auto(self):
        self.assertEqual(
            " | summarize avg(foo) by bin_auto(bar)",
            Query().summarize(f.avg(col.foo)).by(f.bin_auto(col.bar)).render()
        )

    def test_count(self):
        self.assertEqual(
            " | summarize count()",
            Query().summarize(f.count()).render()
        )
        self.assertEqual(
            " | summarize count(foo)",
            Query().summarize(f.count(col.foo)).render()
        )

    def test_countif(self):
        self.assertEqual(
            " | summarize countif(foo == 1)",
            Query().summarize(f.countif(col.foo == 1)).render()
        )

    def test_dcount(self):
        self.assertEqual(
            " | summarize dcount(foo)",
            Query().summarize(f.dcount(col.foo)).render()
        )
        acc = 0.1
        self.assertEqual(
            " | summarize dcount(foo, 0.1)",
            Query().summarize(f.dcount(col.foo, acc)).render()
        )

    # def test_hll(self):
    #     self.assertEqual(
    #         " | summarize hll(foo)",
    #         Query().summarize(f.hll(col.foo)).render()
    #         )
    #     acc = 0.1
    #     self.assertEqual(
    #         " | summarize hll(foo, 0.1)",
    #         Query().summarize(f.hll(col.foo, acc)).render()
    #         )
    #
    # def test_hll_merge(self):
    #     self.assertEqual(
    #         " | summarize hll_merge(foo)",
    #         Query().summarize(f.hll_merge(col.foo)).render()
    #         )

    def test_make_bag(self):
        self.assertEqual(
            " | summarize make_bag(foo)",
            Query().summarize(f.make_bag(col.foo)).render()
        )

    def test_make_list(self):
        self.assertEqual(
            " | summarize make_list(foo)",
            Query().summarize(f.make_list(col.foo)).render()
        )

    def test_make_set(self):
        self.assertEqual(
            " | summarize make_set(foo)",
            Query().summarize(f.make_set(col.foo)).render()
        )

    def test_max(self):
        self.assertEqual(
            " | summarize max(foo)",
            Query().summarize(f.max(col.foo)).render()
        )

    def test_min(self):
        self.assertEqual(
            " | summarize min(foo)",
            Query().summarize(f.min(col.foo)).render()
        )

    def test_percentile(self):
        self.assertEqual(
            " | summarize percentiles(foo, 5)",
            Query().summarize(f.percentiles(col.foo, 5)).render()
        )

    def test_percentiles(self):
        self.assertEqual(
            " | summarize percentiles(foo, 5, 50, 95)",
            Query().summarize(f.percentiles(col.foo, 5, 50, 95)).render()
        )

    def test_stdev(self):
        self.assertEqual(
            " | summarize stdev(foo)",
            Query().summarize(f.stdev(col.foo)).render()
        )

    def test_stdevif(self):
        self.assertEqual(
            " | summarize stdevif(foo, bar)",
            Query().summarize(f.stdevif(col.foo, col.bar)).render()
        )

    def test_stdevp(self):
        self.assertEqual(
            " | summarize stdevp(foo)",
            Query().summarize(f.stdevp(col.foo)).render()
        )

    def test_sum(self):
        self.assertEqual(
            " | summarize sum(foo)",
            Query().summarize(f.sum(col.foo)).render()
        )

    def test_sumif(self):
        self.assertEqual(
            " | summarize sumif(foo, bar)",
            Query().summarize(f.sumif(col.foo, col.bar)).render()
        )

    def test_variance(self):
        self.assertEqual(
            " | summarize variance(foo)",
            Query().summarize(f.variance(col.foo)).render()
        )

    def test_varianceif(self):
        self.assertEqual(
            " | summarize varianceif(foo, bar)",
            Query().summarize(f.varianceif(col.foo, col.bar)).render()
        )

    def test_variancep(self):
        self.assertEqual(
            " | summarize variancep(foo)",
            Query().summarize(f.variancep(col.foo)).render()
        )

    def test_nesting(self):
        self.assertEqual(
            " | summarize active_days = dcount(bin(timestamp, time(1.0:0:0.0)))",
            Query().summarize(active_days=f.dcount(f.bin(col.timestamp, datetime.timedelta(1)))).render()
        )
