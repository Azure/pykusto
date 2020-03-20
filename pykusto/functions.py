import json
from typing import Union

from pykusto.expressions import AnyTypeColumn, NumberType, NumberExpression, TimespanType, \
    DatetimeExpression, TimespanExpression, ArrayType, DynamicType, DatetimeType, BaseExpression, BooleanType, \
    ExpressionType, AggregationExpression, StringType, StringExpression, BooleanExpression, \
    NumberAggregationExpression, MappingAggregationExpression, ArrayAggregationExpression, to_kql, DynamicExpression, \
    ArrayExpression, ColumnToType, BaseColumn, AnyExpression, AnyAggregationExpression, MappingExpression
from pykusto.kql_converters import KQL
from pykusto.logger import logger
from pykusto.type_utils import plain_expression, get_base_types, KustoType


# Scalar functions
def acos(expr: NumberType) -> NumberExpression:
    return expr.acos()


def ago(expr: TimespanType) -> DatetimeExpression:
    return TimespanExpression.ago(expr)


# def array_concat(): return
#
#
# def array_iif(): return


def array_length(expr: ArrayType) -> NumberExpression:
    return ArrayExpression(expr).array_length()


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
def bag_keys(expr: DynamicType):
    return expr.keys()


# def beta_cdf(self): return
#
#
# def beta_inv(self): return
#
#
# def beta_pdf(self): return


def bin(expr: Union[NumberType, DatetimeType, TimespanType],
        round_to: Union[NumberType, TimespanType]) -> BaseExpression:
    """
    Refers only to bin() as part of summarize by bin(...),
     if you wish to use it as a scalar function, use 'floor()' instead
    :param expr: A number, date, or timespan.
    :param round_to: The "bin size". A number, date or timespan that divides value.
    :return:
    """
    return expr.bin(round_to)


def bin_at(expr: Union[NumberType, DatetimeType, TimespanType],
           bin_size: Union[NumberType, TimespanType],
           fixed_point: Union[NumberType, DatetimeType, TimespanType]) -> BaseExpression:
    return expr.bin_at(bin_size, fixed_point)


def bin_auto(expr: Union[NumberType, DatetimeType, TimespanType]) -> BaseExpression:
    return expr.bin_auto()


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


def case(predicate: BooleanType, val: ExpressionType, *args: Union[BooleanType, ExpressionType]) -> AnyExpression:
    res = 'case({}, {}, {})'.format(
        to_kql(predicate), to_kql(val), ', '.join([to_kql(arg) for arg in args])
    )
    return AnyExpression(KQL(res))


def ceiling(expr: NumberType) -> NumberExpression:
    return expr.ceiling()


# def coalesce(self): return
#
#
# def column_ifexists(self): return
#
#

def cos(expr: NumberType) -> NumberExpression:
    return expr.cos()


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


# def dcount_hll(expr: ExpressionType) -> BaseExpression:
#     return BaseExpression(KQL('dcount_hll({})'.format(to_kql(expr))))


# def degrees(self): return


def endofday(expr: DatetimeExpression, offset: NumberType = None) -> DatetimeExpression:
    return expr.endofday(offset)


def endofmonth(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.endofmonth(offset)


def endofweek(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.endofweek(offset)


def endofyear(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.endofyear(offset)


# def estimate_data_size(self): return


def exp(expr: NumberType) -> NumberExpression:
    return expr.exp()


def exp10(expr: NumberType) -> NumberExpression:
    return expr.exp10()


def exp2(expr: NumberType) -> NumberExpression:
    return expr.exp2()


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


def floor(expr: Union[NumberType, DatetimeType],
          round_to: Union[NumberType, TimespanType]) -> Union[NumberExpression, DatetimeExpression]:
    return expr.floor(round_to)


def format_datetime(expr: DatetimeExpression, format_string: StringType) -> StringExpression:
    return expr.format_datetime(format_string)


def format_timespan(expr: TimespanType, format_string: StringType) -> StringExpression:
    return expr.format_timespan(format_string)


# def gamma(self): return


def getmonth(expr: DatetimeType) -> NumberExpression:
    return expr.getmonth()


def gettype(expr: ExpressionType) -> StringExpression:
    return expr.gettype()


def getyear(expr: DatetimeType) -> NumberExpression:
    return expr.getyear()


def hash(expr: ExpressionType):
    return expr.__hash__()


def hash_sha256(expr: ExpressionType):
    return expr.hash_sha256()


def hourofday(expr: DatetimeType) -> NumberExpression:
    return expr.hourofday()


def iff(predicate: BooleanType, if_true: ExpressionType, if_false: ExpressionType) -> BaseExpression:
    return_types = get_base_types(if_true)
    other_types = get_base_types(if_false)
    common_types = other_types & return_types
    if len(common_types) == 0:
        # If there is not at least one common type, then certainly the arguments are not of the same type
        logger.warning(
            "The second and third arguments must be of the same type, but they are: {} and {}. "
            "If this is a mistake, please report it at https://github.com/Azure/pykusto/issues".format(
                ", ".join(sorted(t.primary_name for t in return_types)),
                ", ".join(sorted(t.primary_name for t in other_types))
            )
        )
        expression_type = AnyExpression
    else:
        expression_type = plain_expression.registry[next(iter(common_types))]
    return expression_type(
        KQL('iff({}, {}, {})'.format(to_kql(predicate), to_kql(if_true), to_kql(if_false)))
    )


def iif(predicate: BooleanType, if_true: ExpressionType, if_false: ExpressionType) -> BaseExpression:
    return iff(predicate, if_true, if_false)


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
    return expr.is_empty()


def isfinite(expr: NumberType) -> BooleanExpression:
    return expr.isfinite()


def isinf(expr: NumberType) -> BooleanExpression:
    return expr.isinf()


def isnan(expr: NumberExpression) -> BooleanExpression:
    return expr.isnan()


def isnotempty(expr: ExpressionType) -> BooleanExpression:
    return expr.is_not_empty()


def isnotnull(expr: ExpressionType) -> BooleanExpression:
    return expr.is_not_null()


def isnull(expr: ExpressionType) -> BooleanExpression:
    return expr.is_null()


def isutf8(expr: StringType) -> BooleanExpression:
    return expr.is_utf8()


def log(expr: NumberType) -> NumberExpression:
    return expr.log()


def log10(expr: NumberType) -> NumberExpression:
    return expr.log10()


def log2(expr: NumberType) -> NumberExpression:
    return expr.log2()


def loggamma(expr: NumberType) -> NumberExpression:
    return expr.loggamma()


def make_datetime(year: NumberType,
                  month: NumberType,
                  day: NumberType,
                  hour: NumberType = None,
                  minute: NumberType = None,
                  second: NumberType = None) -> DatetimeExpression:
    res = 'make_datetime({year}, {month}, {day}, {hour}, {minute}, {second})'.format(
        year=to_kql(year),
        month=to_kql(month),
        day=to_kql(day),
        hour=to_kql(0 if hour is None else hour),
        minute=to_kql(0 if minute is None else minute),
        second=to_kql(0 if second is None else second)
    )
    return DatetimeExpression(KQL(res))


def make_string() -> StringExpression: raise NotImplemented  # TODO


def make_timespan() -> TimespanExpression: raise NotImplemented  # TODO


# def max_of(self): return
#
#
# def min_of(self): return


def monthofyear() -> NumberExpression: raise NotImplemented  # TODO


def new_guid() -> AnyExpression: raise NotImplemented  # TODO


def now(offset: TimespanType = None) -> StringExpression:
    if offset:
        return StringExpression(KQL('now({})'.format(to_kql(offset))))
    return StringExpression(KQL('now()'))


def pack(**kwargs: ExpressionType) -> MappingExpression:
    return MappingExpression(KQL('pack({})'.format(
        ', '.join('"{}", {}'.format(k, to_kql(v)) for k, v in kwargs.items())
    )))


def pack_all() -> MappingExpression: raise NotImplemented  # TODO


def pack_array(*elements: ExpressionType) -> 'ArrayExpression':
    return ArrayExpression(KQL('pack_array({})'.format(
        ', '.join(to_kql(e) for e in elements)
    )))


def pack_dictionary() -> MappingExpression: raise NotImplemented  # TODO


#
#
# def parse_csv(self): return
#
#
# def parse_ipv4(self): return


def parse_json(expr: Union[StringType, DynamicType]) -> DynamicExpression:
    return DynamicExpression(KQL('parse_json({})'.format(to_kql(expr))))


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


def percentile_tdigest() -> AnyExpression: raise NotImplemented  # TODO


def percentrank_tdigest() -> AnyExpression: raise NotImplemented  # TODO


def pow(expr1: NumberType, expr2: NumberType) -> NumberExpression:
    return NumberExpression(KQL('pow({}, {})'.format(to_kql(expr1), to_kql(expr2))))


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
    return expr.round(precision)


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
    return NumberExpression(KQL('sign({})'.format(to_kql(expr))))


# def sin(self): return
#
#
def split(string: StringType, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
    return StringExpression(to_kql(string)).split(delimiter, requested_index)


def sqrt(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL('sqrt({})'.format(to_kql(expr))))


def startofday(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.startofday(offset)


def startofmonth(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.startofmonth(offset)


def startofweek(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.startofweek(offset)


def startofyear(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
    return expr.startofyear(offset)


def strcat(*strings: StringType) -> StringExpression:
    if len(strings) < 2:
        raise ValueError("strcat requires at least two arguments")
    return StringExpression(KQL('strcat({})'.format(', '.join(to_kql(s) for s in strings))))


def to_literal_dynamic(d: DynamicType) -> KQL:
    if isinstance(d, BaseExpression):
        return d.kql
    return KQL('dynamic({})'.format(json.dumps(d)))


def strcat_array(expr: ArrayType, delimiter: StringType) -> StringExpression:
    return StringExpression(KQL('strcat_array({}, {})'.format(to_literal_dynamic(expr), to_kql(delimiter))))


def strcat_delim(delimiter: StringType, expr1: StringType, expr2: StringType, *exprs: StringType) -> StringExpression:
    res = 'strcat_delim({}, {}, {}'.format(
        to_kql(delimiter),
        to_kql(expr1),
        to_kql(expr2))
    if len(exprs) > 0:
        res = res + ', ' + ', '.join([to_kql(expr) for expr in exprs])
    return StringExpression(KQL(res + ')'))


def strcmp(expr1: StringType, expr2: StringType) -> NumberExpression:
    return NumberExpression(KQL('strcmp({}, {})'.format(to_kql(expr1), to_kql(expr2))))


def string_size(expr: StringType) -> NumberExpression:
    return StringExpression(expr).string_size()


def strlen(expr: StringType) -> NumberExpression:
    return NumberExpression(KQL('strlen({})'.format(to_kql(expr))))


def strrep(expr: StringType,
           multiplier: NumberType,
           delimiter: StringType = None) -> StringExpression:
    if delimiter is None:
        res = 'strrep({}, {})'.format(to_kql(expr), to_kql(multiplier))
    else:
        res = 'strrep({}, {}, {})'.format(to_kql(expr), to_kql(multiplier),
                                          to_kql(delimiter))
    return StringExpression(KQL((res)))


def substring(expr: StringType, start_index: NumberType, length: NumberType = None) -> StringExpression:
    return StringExpression(KQL(
        ('substring({}, {})' if length is None else 'substring({}, {}, {})').format(
            to_kql(expr), to_kql(start_index), to_kql(length)
        )
    ))


# def tan(self): return
#
#
# def tdigest_merge(self): return


def tobool(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('tobool({})'.format(to_kql(expr))))


def toboolean(expr: ExpressionType) -> BooleanExpression:
    return BooleanExpression(KQL('toboolean({})'.format(to_kql(expr))))


def todatetime(expr: StringType) -> DatetimeExpression:
    return DatetimeExpression(KQL('todatetime({})'.format(to_kql(expr))))


def todecimal(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL("todecimal({})".format(to_kql(expr))))


def todouble(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL("todouble({})".format(to_kql(expr))))


def todynamic() -> DynamicExpression: raise NotImplemented  # TODO


def toguid() -> AnyExpression: raise NotImplemented  # TODO


def tohex(expr1: NumberType, expr2: NumberType = None) -> StringExpression:
    return StringExpression(KQL(('tohex({})' if expr2 is None else 'tohex({}, {})').format(to_kql(expr1), to_kql(expr2))))


def toint(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL("toint({})".format(to_kql(expr))))


def tolong(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL("tolong({})".format(to_kql(expr))))


def tolower(expr: StringType) -> StringExpression:
    return expr.lower()


def toreal(expr: NumberType) -> NumberExpression:
    return NumberExpression(KQL("toreal({})".format(to_kql(expr))))


def tostring(expr: ExpressionType):
    return expr.to_string()


def totimespan() -> TimespanExpression: raise NotImplemented  # TODO


def toupper(expr: StringType) -> StringExpression:
    return expr.upper()


# def to_utf8(self): return
#
#
# def translate(self): return
#
#
# def treepath(self): return


def trim() -> StringExpression: raise NotImplemented  # TODO


# def trim_end(self): return
#
#
# def trim_start(self): return


def url_decode() -> StringExpression: raise NotImplemented  # TODO


def url_encode() -> StringExpression: raise NotImplemented  # TODO


def weekofyear() -> NumberExpression: raise NotImplemented  # TODO


# def welch_test(self): return


def zip() -> DynamicExpression: raise NotImplemented  # TODO


# ----------------------------------------------------
# aggregative functions
# -----------------------------------------------------

# noinspection PyShadowingBuiltins
def any(*args: ExpressionType) -> AnyAggregationExpression:
    res = 'any({})'.format(', '.join([arg.kql for arg in args]))
    return AnyAggregationExpression(KQL(res))


def arg_max(*args: ExpressionType) -> AnyAggregationExpression:
    res = 'arg_max({})'.format(', '.join([arg.kql for arg in args]))
    return AnyAggregationExpression(KQL(res))


def arg_min(*args: ExpressionType) -> AnyAggregationExpression:
    res = 'arg_min({})'.format(', '.join([arg.kql for arg in args]))
    return AnyAggregationExpression(KQL(res))


def avg(expr: ExpressionType) -> NumberAggregationExpression:
    return NumberAggregationExpression(KQL('avg({})'.format(to_kql(expr))))


def avgif(expr: ExpressionType, predicate: BooleanType) -> NumberAggregationExpression:
    return NumberAggregationExpression(KQL('avgif({}, {})'.format(to_kql(expr), to_kql(predicate))))


# def buildschema(self):
#     return


def count(col: AnyTypeColumn = None) -> NumberAggregationExpression:
    res = "count()" if col is None else "count({})".format(col.kql)
    return NumberAggregationExpression(KQL(res))


def countif(predicate: BooleanType) -> NumberAggregationExpression:
    return NumberAggregationExpression(KQL('countif({})'.format(to_kql(predicate))))


def dcount(expr: ExpressionType, accuracy: NumberType = None) -> NumberAggregationExpression:
    return NumberAggregationExpression(KQL(
        ('dcount({})' if accuracy is None else 'dcount({}, {})').format(to_kql(expr), to_kql(accuracy))
    ))


def dcountif(expr: ExpressionType, predicate: BooleanType, accuracy: NumberType = 0) -> NumberAggregationExpression:
    return NumberAggregationExpression(KQL('dcountif({}, {}, {})'.format(
        to_kql(expr), to_kql(predicate), to_kql(accuracy)
    )))


# def hll(expr: ExpressionType, accuracy: NumberType = None) -> AggregationExpression:
#     return AggregationExpression(KQL(
#         ('hll({})' if accuracy is None else 'hll({}, {})').format(expr, accuracy)
#     ))


# def hll_merge(expr: ExpressionType) -> AggregationExpression:
#     return AggregationExpression(KQL('hll_merge({})'.format(to_kql(expr))))


def make_bag(expr: ExpressionType, max_size: NumberType = None) -> MappingAggregationExpression:
    if max_size:
        return MappingAggregationExpression(KQL('make_bag({}, {})'.format(expr, max_size)))
    return MappingAggregationExpression(KQL('make_bag({})'.format(to_kql(expr))))


def make_list(expr: ExpressionType, max_size: NumberType = None) -> ArrayAggregationExpression:
    if max_size:
        return ArrayAggregationExpression(KQL('make_list({}, {})'.format(expr, max_size)))
    return ArrayAggregationExpression(KQL('make_list({})'.format(to_kql(expr))))


def make_set(expr: ExpressionType, max_size: NumberType = None) -> ArrayAggregationExpression:
    if max_size:
        return ArrayAggregationExpression(KQL('make_set({}, {})'.format(expr, max_size)))
    return ArrayAggregationExpression(KQL('make_set({})'.format(to_kql(expr))))


def max(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('max({})'.format(to_kql(expr))))


def min(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('min({})'.format(to_kql(expr))))


def percentile(expr: ExpressionType, per: NumberType) -> AggregationExpression:
    res = 'percentiles({}, {})'.format(expr, to_kql(per))
    return AnyAggregationExpression(KQL(res))


def percentiles(expr: ExpressionType, *pers: NumberType) -> AggregationExpression:
    res = 'percentiles({}, {})'.format(expr.kql, ', '.join([str(to_kql(per)) for per in pers]))
    return AnyAggregationExpression(KQL(res))


def stdev(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('stdev({})'.format(to_kql(expr))))


def stdevif(expr: ExpressionType, predicate: BooleanType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('stdevif({}, {})'.format(to_kql(expr), to_kql(predicate))))


def stdevp(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('stdevp({})'.format(to_kql(expr))))


def sum(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('sum({})'.format(to_kql(expr))))


def sumif(expr: ExpressionType, predicate: BooleanType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('sumif({}, {})'.format(to_kql(expr), to_kql(predicate))))


# def tdigest(self):
#     return
#
#
# def tdigest_merge(self):
#     return


def variance(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('variance({})'.format(to_kql(expr))))


def varianceif(expr: ExpressionType, predicate: BooleanType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('varianceif({}, {})'.format(to_kql(expr), to_kql(predicate))))


def variancep(expr: ExpressionType) -> AggregationExpression:
    return AnyAggregationExpression(KQL('variancep({})'.format(to_kql(expr))))


# Used for mv-expand
def to_type(column: BaseColumn, type_name: KustoType) -> ColumnToType:
    return ColumnToType(column, type_name)
