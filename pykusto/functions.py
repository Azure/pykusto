from pykusto import utils
from pykusto.column import Column
from pykusto.expressions import *
from pykusto.utils import KQL


# Scalar functions
def acos(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('acos({})'.format(expr)))


def ago(expr: TimespanType) -> DatetimeExpression:
    return DatetimeExpression(KQL('ago({})'.format(utils.timedelta_to_kql(expr))))


# def array_concat(): return
#
#
# def array_iif(): return


def array_length(expr: ArrayType) -> NumberExpression:
    return NumberExpression(KQL('array_length({})'.format(expr)))


# def array_slice(): return
#
#
# def array_split(): return
#
#
# def asin(): return
#
#
# def atan(self): return
#
#
# def atan2(self): return
#
#
# def base64_decode_toarray(self): return
#
#
# def base64_decode_tostring(self): return
#
#
# def base64_encode_tostring(self): return
#
#
# def bag_keys(self): return
#
#
# def beta_cdf(self): return
#
#
# def beta_inv(self): return
#
#
# def beta_pdf(self): return


def bin(expr: ExpressionType, round_to: NumberType) -> GroupExpression:
    return GroupExpression(KQL('bin({}, {})'.format(expr, round_to)))


def bin_at(expr: ExpressionType, bin_size: NumberType, fixed_point: NumberType) -> BaseExpression:
    return BaseExpression(KQL('bin_at({}, {})'.format(expr, bin_size, fixed_point)))


def bin_auto(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('bin_auto({})'.format(expr)))


# def binary_and(self): return
#
#
# def binary_not(self): return
#
#
# def binary_or(self): return
#
#
# def binary_shift_left(self): return
#
#
# def binary_shift_right(self): return
#
#
# def binary_xor(self): return


def case(): return  # TODO


def ceiling(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('ceiling({})'.format(expr)))


# def coalesce(self): return
#
#
# def column_ifexists(self): return
#
#
# def cos(self): return
#
#
# def cot(self): return

# def countof(self): return
#
#
# def current_cluster_endpoint(self): return
#
#
# def current_cursor(self): return
#
#
# def current_database(self): return
#
#
# def current_principal(self): return
#
#
# def cursor_after(self): return
#
#
# def cursor_before_or_at(self): return
#
#
# def cursor_current(self): return
#
# def datetime_add(self): return
#
#
# def datetime_part(self): return
#
#
# def datetime_diff(self): return
#
#
# def dayofmonth(self): return
#
#
# def dayofweek(self): return
#
#
# def dayofyear(self): return


def dcount_hll(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('dcount_hll({})'.format(expr)))


# def degrees(self): return


def endofday(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('endofday({})' if offset is None else 'endofday({}, {})').format(expr, offset)))


def endofmonth(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('endofmonth({})' if offset is None else 'endofmonth({}, {})').format(expr, offset)))


def endofweek(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('endofweek({})' if offset is None else 'endofweek({}, {})').format(expr, offset)))


def endofyear(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('endoftyear({})' if offset is None else 'endoftyear({}, {})').format(expr, offset)))


# def estimate_data_size(self): return


def exp(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('exp({})'.format(expr)))


def exp10(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('exp10({})'.format(expr)))


def exp2(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('exp2({})'.format(expr)))


# def extent_id(self): return
#
#
# def extent_tags(self): return
#
#
# def extract(self): return
#
#
# def extract_all(self): return
#
#
# def extractjson(self): return


def floor(expr: ExpressionType, round_to: NumberType) -> BaseExpression:
    return bin(expr, round_to)


def format_datetime(expr: DatetimeType, format_string: StringType) -> StringExpression:
    return StringExpression(KQL('format_datetime({}, \'{}\')'.format(expr, format_string)))


def format_timespan(expr: TimespanType, format_string: StringType) -> StringExpression:
    return StringExpression(KQL('format_timespan({}, \'{}\')'.format(expr, format_string)))


# def gamma(self): return


def getmonth(expr: DatetimeType) -> NumberExpression:
    return NumberExpression(KQL('getmonth({})'.format(expr)))


def gettype(expr: ExpressionType) -> StringExpression:
    return StringExpression(KQL('gettype({})'.format(expr)))


def getyear(expr: DatetimeType) -> NumberExpression:
    return NumberExpression(KQL('getyear({})'.format(expr)))


def hash(self): return  # TODO


def hash_sha256(self): return  # TODO


def hourofday(expr: DatetimeType) -> DatetimeExpression:
    return DatetimeExpression(KQL('hourofday({})'.format(expr)))


def iif(predicate: BooleanType, if_true: ExpressionType, if_false: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('iff({}, {}, {})'.format(predicate, if_true, if_false)))


#
# def indexof(self): return
#
#
# def indexof_regex(self): return
#
#
# def ingestion_time(self): return
#
#
# def isascii(self): return


def isempty(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('isempty({})'.format(expr)))


def isfinite(expr: NumberType) -> BooleanExpression:
    return BooleanExpression(KQL('isfinite({})'.format(expr)))


def isinf(expr: NumberType) -> BooleanExpression:
    return BooleanExpression(KQL('isinf({})'.format(expr)))


def isnan(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('isnan({})'.format(expr)))


def isnotempty(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('isnotempty({})'.format(expr)))


def isnotnull(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('isnotnull({})'.format(expr)))


def isnull(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('isnull({})'.format(expr)))


def isutf8(expr: StringType) -> BooleanExpression:
    return BooleanExpression(KQL('isutf8({})'.format(expr)))


def log(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('log({})'.format(expr)))


def log10(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('log10({})'.format(expr)))


def log2(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('log2({})'.format(expr)))


def loggamma(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('loggamma({})'.format(expr)))


def make_datetime(year: NumberType,
                  month: NumberType,
                  day: NumberType,
                  hour: NumberType = None,
                  minute: NumberType = None,
                  second: NumberType = None) -> DatetimeExpression:
    str = 'make_datetime({}, {}, {}'
    if hour:
        str += ', {}, 0'
    if minute:
        str = str[:-1] + '{}'
    if second:
        str += ', {}'
    str += ')'
    return DatetimeExpression(
        KQL(str.format(year, month, day, hour, minute, second)))


def make_string(self): return  # TODO


def make_timespan(self): return  # TODO


# def max_of(self): return
#
#
# def min_of(self): return


def monthofyear(self): return  # TODO


def new_guid(self): return  # TODO


def now(offset: TimespanType = None) -> StringExpression:
    if offset:
        return StringExpression(KQL('now({})'.format(utils.timedelta_to_kql(offset))))
    return StringExpression(KQL('now()'))


# def pack(self): return
#
#
# def pack_all(self): return
#
#
# def pack_array(self): return
#
#
# def pack_dictionary(self): return
#
#
# def parse_csv(self): return
#
#
# def parse_ipv4(self): return


def parse_json(expr: ExpressionType): return  # TODO


# def parse_path(self): return
#
#
# def parse_url(self): return
#
#
# def parse_urlquery(self): return
#
#
# def parse_user_agent(self): return
#
#
# def parse_version(self): return
#
#
# def parse_xml(self): return


# def percentile_tdigest(self): return
#
#
# def percentrank_tdigest(self): return


def pow(expr1: NumberType, expr2: NumberType) -> NumberExpression:
    return NumberExpression(KQL('pow({}, {})'.format(expr1, expr2)))


# def radians(self): return
#
#
# def rand(self): return
#
#
# def range(self): return
#
#
# def rank_tdigest(self): return
#
#
# def repeat(self): return


# def replace(self): return


# def reverse(self): return


def round(expr: NumberType, precision: NumberType = None) -> NumberExpression:
    return NumberExpression(KQL(
        ('round({}, {})' if precision is None else 'round({}, {})').format(expr, precision)))


# def series_add(self): return
#
#
# def series_decompose(self): return
#
#
# def series_decompose_anomalies(self): return
#
#
# def series_decompose_forecast(self): return
#
#
# def series_divide(self): return
#
#
# def series_equals(self): return
#
#
# def series_fill_backward(self): return
#
#
# def series_fill_const(self): return
#
#
# def series_fill_forward(self): return
#
#
# def series_fill_linear(self): return
#
#
# def series_fir(self): return
#
#
# def series_fit_2lines(self): return
#
#
# def series_fit_2lines_dynamic(self): return
#
#
# def series_fit_line(self): return
#
#
# def series_fit_line_dynamic(self): return
#
#
# def series_greater(self): return
#
#
# def series_greater_equals(self): return
#
#
# def series_iir(self): return
#
#
# def series_less(self): return
#
#
# def series_less_equals(self): return
#
#
# def series_multiply(self): return
#
#
# def series_not_equals(self): return
#
#
# def series_outliers(self): return
#
#
# def series_periods_detect(self): return
#
#
# def series_periods_validate(self): return
#
#
# def series_seasonal(self): return
#
#
# def series_stats(self): return
#
#
# def series_stats_dynamic(self): return
#
#
# def series_subtract(self): return
#
#
# def set_difference(self): return
#
#
# def set_intersect(self): return
#
#
# def set_union(self): return


def sign(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('sign({})'.format(expr)))


# def sin(self): return
#
#
# def split(self): return


def sqrt(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('sqrt({})'.format(expr)))


def startofday(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('startofday({})' if offset is None else 'startofday({}, {})').format(expr, offset)))


def startofmonth(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('startofmonth({})' if offset is None else 'startofmonth({}, {})').format(expr, offset)))


def startofweek(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('startofweek({})' if offset is None else 'startofweek({}, {})').format(expr, offset)))


def startofyear(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return DatetimeExpression(KQL(
        ('startofyear({})' if offset is None else 'startofyear({}, {})').format(expr, offset)))


def strcat(self): return  # TODO


def strcat_array(expr: ArrayType, delimiter: StringType) -> StringExpression:
    return StringExpression(KQL('strcat_array({}, \'{}\')'.format(expr, delimiter)))


def strcat_delim(): raise NotImplemented  # TODO


def strcmp(expr1: StringType, expr2: StringType) -> NumberExpression:
    return NumberExpression(KQL('strcmp({}, {})'.format(expr1, expr2)))


def string_size(expr: StringType) -> NumberExpression:
    return NumberExpression(KQL('string_size({})'.format(expr)))


def strlen(expr: StringType) -> NumberExpression:
    return NumberExpression(KQL('strlen({})'.format(expr)))


def strrep(expr: StringType, multiplier: NumberType, delimiter: StringType = '') -> StringExpression:
    return StringExpression(KQL('strrep({}, {}, {})'.format(expr, multiplier, delimiter)))


def substring(expr: StringType, start_index: NumberType, length: NumberType = None) -> StringExpression:
    if length:
        return StringExpression(KQL('substring({}, {}, {})'.format(expr, start_index, length)))
    return StringExpression(KQL('substring({}, {})'.format(expr, start_index)))


# def tan(self): return
#
#
# def tdigest_merge(self): return


def tobool(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('tobool({})'.format(expr)))


def toboolean(expr: ExpressionType) -> BooleanExpression:
    return tobool(expr)


def todatetime(expr: StringType) -> DatetimeExpression:
    return DatetimeExpression(KQL('todatetime({})'.format(expr)))


def todecimal(self): return  # TODO


def todouble(self): return  # TODO


def todynamic(self): return  # TODO


def toguid(self): return  # TODO


def tohex(self): return  # TODO


def toint(self): return  # TODO


def tolong(self): return  # TODO


def tolower(self): return  # TODO


def toreal(self): return  # TODO


def tostring(self): return  # TODO


def totimespan(self): return  # TODO


def toupper(self): return  # TODO


# def to_utf8(self): return
#
#
# def translate(self): return
#
#
# def treepath(self): return


def trim(self): return  # TODO


# def trim_end(self): return
#
#
# def trim_start(self): return


def url_decode(self): return  # TODO


def url_encode(self): return  # TODO


def weekofyear(self): return  # TODO


# def welch_test(self): return


def zip(self): return  # TODO


# aggregative functions
def any(*args: ExpressionType) -> GroupExpression:
    str = 'any('
    for expr in args:
        str += expr + ', '
    str += ')'
    return GroupExpression(KQL(str))


def arg_max(self):  raise NotImplemented  # TODO


def arg_min(self):  raise NotImplemented  # TODO


def avg(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('avg({})'.format(expr)))


def avgif(expr: NumberType, predicate: BooleanType) -> NumberExpression:
    return NumberExpression(KQL('avgif({}, {})'.format(expr, predicate)))


# def buildschema(self):
#     return


def count(col: Column = None):
    res = "count()" if col is None else "count({})".format(col.kql)
    return AggregationExpression(KQL(res))


def countif(predicate: BooleanType) -> NumberExpression:
    return NumberExpression(KQL('countif({})'.format(predicate)))


def dcount(expr: ExpressionType, accuracy: NumberType = 1) -> NumberExpression:
    return NumberExpression(KQL('dcount({}, {})'.format(expr, accuracy)))


def dcountif(expr: ExpressionType, predicate: BooleanType, accuracy: NumberType = 0) -> NumberExpression:
    return NumberExpression(KQL('dcountif({}, {}, {})'.format(expr, predicate, accuracy)))


def hll(expr: ExpressionType, accuracy: NumberType = 1) -> BaseExpression:
    return NumberExpression(KQL('hll({}, {})'.format(expr, accuracy)))


def hll_merge(expr: ExpressionType) -> BaseExpression:
    return NumberExpression(KQL('hll_megre({})'.format(expr)))


def make_bag(expr: ExpressionType, max_size: NumberType = None) -> ArrayExpression:
    if max_size:
        return ArrayExpression(KQL('make_bag({}, {})'.format(expr, max_size)))
    return ArrayExpression(KQL('make_bag({})'.format(expr)))


def make_list(expr: ExpressionType, max_size: NumberType = None) -> ArrayExpression:
    if max_size:
        return ArrayExpression(KQL('make_list({}, {})'.format(expr, max_size)))
    return ArrayExpression(KQL('make_list({})'.format(expr)))


def make_set(expr: ExpressionType, max_size: NumberType = None) -> ArrayExpression:
    if max_size:
        return ArrayExpression(KQL('make_set({}, {})'.format(expr, max_size)))
    return ArrayExpression(KQL('make_set({})'.format(expr)))


def max(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('max({})'.format(expr)))


def min(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('min({})'.format(expr)))


def percentiles(self):  # TODO
    return


def stdev(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('stdev({})'.format(expr)))


def stdevif(expr: ExpressionType, predicate: BooleanType) -> BaseExpression:
    return NumberExpression(KQL('stdevif({}, {})'.format(expr, predicate)))


def stdevp(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('stdevp({})'.format(expr)))


def sum(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('sum({})'.format(expr)))


def sumif(expr: ExpressionType, predicate: BooleanType) -> BaseExpression:
    return NumberExpression(KQL('sumif({}, {})'.format(expr, predicate)))


# def tdigest(self):
#     return
#
#
# def tdigest_merge(self):
#     return


def variance(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('variance({})'.format(expr)))


def varianceif(expr: ExpressionType, predicate: BooleanType) -> BaseExpression:
    return NumberExpression(KQL('varianceif({}, {})'.format(expr, predicate)))


def variancep(expr: ExpressionType) -> BaseExpression:
    return BaseExpression(KQL('variancep({})'.format(expr)))
