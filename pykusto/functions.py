import json
from itertools import chain
from typing import Union

from pykusto.enums import Kind
from pykusto.expressions import AnyTypeColumn, NumberType, NumberExpression, TimespanType, \
    DatetimeExpression, TimespanExpression, ArrayType, DynamicType, DatetimeType, BaseExpression, BooleanType, \
    ExpressionType, StringType, StringExpression, BooleanExpression, \
    NumberAggregationExpression, MappingAggregationExpression, ArrayAggregationExpression, to_kql, DynamicExpression, \
    ArrayExpression, ColumnToType, BaseColumn, AnyExpression, AnyAggregationExpression, MappingExpression
from pykusto.kql_converters import KQL
from pykusto.logger import logger
from pykusto.type_utils import plain_expression, KustoType


class Functions:
    """
    Recommended import style:\n
    `from pykusto.functions import Functions as f`
    """
    # Scalar functions

    @staticmethod
    def acos(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/acosfunction
        """
        return expr.acos()

    @staticmethod
    def ago(expr: TimespanType) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction
        """
        return TimespanExpression.ago(expr)

    @staticmethod
    def array_length(expr: ArrayType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction
        """
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
    @staticmethod
    def bag_keys(expr: DynamicType):
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bagkeysfunction
        """
        return expr.keys()

    # def beta_cdf(self): return
    #
    #
    # def beta_inv(self): return
    #
    #
    # def beta_pdf(self): return

    @staticmethod
    def bin(expr: Union[NumberType, DatetimeType, TimespanType],
            round_to: Union[NumberType, TimespanType]) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction\n
        Refers only to bin() as part of summarize by bin(...),
         if you wish to use it as a scalar function, use 'floor()' instead
        :param expr: A number, date, or timespan.
        :param round_to: The "bin size". A number, date or timespan that divides value.
        :return:
        """
        return expr.bin(round_to)

    @staticmethod
    def bin_at(expr: Union[NumberType, DatetimeType, TimespanType],
               bin_size: Union[NumberType, TimespanType],
               fixed_point: Union[NumberType, DatetimeType, TimespanType]) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return expr.bin_at(bin_size, fixed_point)

    @staticmethod
    def bin_auto(expr: Union[NumberType, DatetimeType, TimespanType]) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
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

    @staticmethod
    def case(predicate: BooleanType, val: ExpressionType, *args: Union[BooleanType, ExpressionType]) -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/casefunction
        """
        assert len(args) > 0, "case must have at least three arguments"
        return AnyExpression(KQL(f"case({to_kql(predicate)}, {to_kql(val)}, {', '.join(to_kql(arg) for arg in args)})"))

    @staticmethod
    def ceiling(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ceilingfunction
        """
        return expr.ceiling()

    # def coalesce(self): return
    #
    #
    # def column_ifexists(self): return
    #
    #

    @staticmethod
    def cos(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/cosfunction
        """
        return expr.cos()

    # def cot(self): return

    @staticmethod
    def count_of(text: StringType, search: StringType, kind: Kind = Kind.NORMAL) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoffunction
        """
        return NumberExpression(KQL(f'countof({to_kql(text)}, {to_kql(search)}, {to_kql(kind.value)})'))

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
    #     return BaseExpression(KQL(f'dcount_hll({to_kql(expr)})'))

    # def degrees(self): return

    @staticmethod
    def end_of_day(expr: DatetimeExpression, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction
        """
        return expr.end_of_day(offset)

    @staticmethod
    def end_of_month(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction
        """
        return expr.end_of_month(offset)

    @staticmethod
    def end_of_week(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction
        """
        return expr.end_of_week(offset)

    @staticmethod
    def end_of_year(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction
        """
        return expr.end_of_year(offset)

    # def estimate_data_size(self): return

    @staticmethod
    def exp(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp-function
        """
        return expr.exp()

    @staticmethod
    def exp10(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp10-function
        """
        return expr.exp10()

    @staticmethod
    def exp2(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp2-function
        """
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

    @staticmethod
    def floor(expr: Union[NumberType, DatetimeType],
              round_to: Union[NumberType, TimespanType]) -> Union[NumberExpression, DatetimeExpression]:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return expr.floor(round_to)

    @staticmethod
    def format_datetime(expr: DatetimeExpression, format_string: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction
        """
        return expr.format_datetime(format_string)

    @staticmethod
    def format_timespan(expr: TimespanType, format_string: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction
        """
        return expr.format_timespan(format_string)

    # def gamma(self): return

    @staticmethod
    def get_month(expr: DatetimeType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction
        """
        return expr.get_month()

    @staticmethod
    def get_type(expr: ExpressionType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/gettypefunction
        """
        return expr.get_type()

    @staticmethod
    def get_year(expr: DatetimeType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction
        """
        return expr.get_year()

    @staticmethod
    def hash(expr: ExpressionType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hashfunction
        """
        return expr.__hash__()

    @staticmethod
    def hash_sha256(expr: ExpressionType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sha256hashfunction
        """
        return expr.hash_sha256()

    @staticmethod
    def hour_of_day(expr: DatetimeType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction
        """
        return expr.hour_of_day()

    @staticmethod
    def iff(predicate: BooleanType, if_true: ExpressionType, if_false: ExpressionType) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ifffunction
        """
        return_types = plain_expression.get_base_types(if_true)
        other_types = plain_expression.get_base_types(if_false)
        common_types = other_types & return_types
        if len(common_types) == 0:
            # If there is not at least one common type, then certainly the arguments are not of the same type
            logger.warning(
                "The second and third arguments must be of the same type, but they are: "
                f"{', '.join(sorted(t.primary_name for t in return_types))} and {', '.join(sorted(t.primary_name for t in other_types))}. "
                "If this is a mistake, please report it at https://github.com/Azure/pykusto/issues"
            )
            expression_type = AnyExpression
        else:
            expression_type = plain_expression.registry[next(iter(common_types))]
        return expression_type(
            KQL(f'iff({to_kql(predicate)}, {to_kql(if_true)}, {to_kql(if_false)})')
        )

    @staticmethod
    def iif(predicate: BooleanType, if_true: ExpressionType, if_false: ExpressionType) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/iiffunction
        """
        return Functions.iff(predicate, if_true, if_false)

    #
    # def indexof(self): return
    #
    #
    # def indexof_regex(self): return
    #
    #
    # def isascii(self): return

    @staticmethod
    def ingestion_time() -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ingestiontimefunction
        """
        return DatetimeExpression(KQL('ingestion_time()'))

    @staticmethod
    def is_empty(expr: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isemptyfunction
        """
        return expr.is_empty()

    @staticmethod
    def is_finite(expr: NumberType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isfinitefunction
        """
        return expr.isfinite()

    @staticmethod
    def is_inf(expr: NumberType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isinffunction
        """
        return expr.is_inf()

    @staticmethod
    def is_nan(expr: NumberExpression) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction
        """
        return expr.is_nan()

    @staticmethod
    def is_not_empty(expr: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotemptyfunction
        """
        return expr.is_not_empty()

    @staticmethod
    def is_not_null(expr: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotnullfunction
        """
        return expr.is_not_null()

    @staticmethod
    def is_null(expr: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnullfunction
        """
        return expr.is_null()

    @staticmethod
    def is_utf8(expr: StringType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isutf8
        """
        return expr.is_utf8()

    @staticmethod
    def log(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log-function
        """
        return expr.log()

    @staticmethod
    def log10(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log10-function
        """
        return expr.log10()

    @staticmethod
    def log2(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log2-function
        """
        return expr.log2()

    @staticmethod
    def log_gamma(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/loggammafunction
        """
        return expr.log_gamma()

    @staticmethod
    def make_datetime(year: NumberType,
                      month: NumberType,
                      day: NumberType,
                      hour: NumberType = None,
                      minute: NumberType = None,
                      second: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-datetimefunction
        """
        res = f'make_datetime(' \
              f'{to_kql(year)}, {to_kql(month)}, {to_kql(day)}, {to_kql(0 if hour is None else hour)}, ' \
              f'{to_kql(0 if minute is None else minute)}, {to_kql(0 if second is None else second)})'
        return DatetimeExpression(KQL(res))

    @staticmethod
    def make_string() -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makestringfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def make_timespan() -> TimespanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-timespanfunction
        """
        raise NotImplemented  # pragma: no cover

    # def max_of(self): return
    #
    #
    # def min_of(self): return

    @staticmethod
    def month_of_year() -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/monthofyearfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def new_guid() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/newguidfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def now(offset: TimespanType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/nowfunction
        """
        if offset:
            return DatetimeExpression(KQL(f'now({to_kql(offset)})'))
        return DatetimeExpression(KQL('now()'))

    @staticmethod
    def pack(**kwargs: ExpressionType) -> MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packfunction
        """
        return MappingExpression(KQL(f'pack({", ".join(f"{to_kql(k)}, {to_kql(v)}" for k, v in kwargs.items())})'))

    @staticmethod
    def pack_all() -> MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packallfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def pack_array(*elements: ExpressionType) -> 'ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packarrayfunction
        """
        return ArrayExpression(KQL(f'pack_array({", ".join(to_kql(e) for e in elements)})'))

    @staticmethod
    def pack_dictionary() -> MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packdictionaryfunction
        """
        raise NotImplemented  # pragma: no cover

    #
    #
    # def parse_csv(self): return
    #
    #
    # def parse_ipv4(self): return

    @staticmethod
    def parse_json(expr: Union[StringType, DynamicType]) -> DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsejsonfunction
        """
        return DynamicExpression(KQL(f'parse_json({to_kql(expr)})'))

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

    @staticmethod
    def percentile_tdigest() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentile-tdigestfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def percentrank_tdigest() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentrank-tdigestfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def pow(expr1: NumberType, expr2: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/powfunction
        """
        return NumberExpression(KQL(f'pow({to_kql(expr1)}, {to_kql(expr2)})'))

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

    @staticmethod
    def round(expr: NumberType, precision: NumberType = None) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/roundfunction
        """
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
    # def set_intersect(self): return
    #
    #
    # def set_union(self): return

    @staticmethod
    def set_has_element(array: ArrayType, value: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sethaselementfunction
        """
        return BooleanExpression(KQL(f'set_has_element({to_kql(array)}, {to_kql(value)})'))

    @staticmethod
    def set_difference(array1: ArrayType, array2: ArrayType, *more_arrays: ArrayType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setdifferencefunction
        """
        return ArrayExpression(KQL(f'set_difference({to_kql(array1)}, {", ".join(to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def set_intersect(array1: ArrayType, array2: ArrayType, *more_arrays: ArrayType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setintersectfunction
        """
        return ArrayExpression(KQL(f'set_intersect({to_kql(array1)}, {", ".join(to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def set_union(array1: ArrayType, array2: ArrayType, *more_arrays: ArrayType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setunionfunction
        """
        return ArrayExpression(KQL(f'set_union({to_kql(array1)}, {", ".join(to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def array_concat(array1: ArrayType, array2: ArrayType, *more_arrays: ArrayType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayconcatfunction
        """
        return ArrayExpression(KQL(f'array_concat({to_kql(array1)}, {", ".join(to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def array_iif(condition_array: ArrayType, if_true: ArrayType, if_false: ArrayType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayifffunction
        """
        return ArrayExpression(KQL(f'array_iif({to_kql(condition_array)}, {to_kql(if_true)}, {to_kql(if_false)})'))

    @staticmethod
    def array_index_of(array: ArrayType, value: ExpressionType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayindexoffunction
        """
        return NumberExpression(KQL(f'array_index_of({to_kql(array)}, {to_kql(value)})'))

    @staticmethod
    def array_rotate_left(array: ArrayType, rotate_count: NumberType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_leftfunction
        """
        return ArrayExpression(KQL(f'array_rotate_left({to_kql(array)}, {to_kql(rotate_count)})'))

    @staticmethod
    def array_rotate_right(array: ArrayType, rotate_count: NumberType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_rightfunction
        """
        return ArrayExpression(KQL(f'array_rotate_right({to_kql(array)}, {to_kql(rotate_count)})'))

    @staticmethod
    def array_shift_left(array: ArrayType, shift_count: NumberType, fill_value: ExpressionType = None) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_leftfunction
        """
        return ArrayExpression(KQL(f'array_shift_left({to_kql(array)}, {to_kql(shift_count)}{"" if fill_value is None else ", " + to_kql(fill_value)})'))

    @staticmethod
    def array_shift_right(array: ArrayType, shift_count: NumberType, fill_value: ExpressionType = None) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_rightfunction
        """
        return ArrayExpression(KQL(f'array_shift_right({to_kql(array)}, {to_kql(shift_count)}{"" if fill_value is None else ", " + to_kql(fill_value)})'))

    @staticmethod
    def array_slice(array: ArrayType, start: NumberType, end: NumberType) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayslicefunction
        """
        return ArrayExpression(KQL(f'array_slice({to_kql(array)}, {to_kql(start)}, {to_kql(end)})'))

    @staticmethod
    def array_split(array: ArrayType, indices: Union[NumberType, ArrayType]) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysplitfunction
        """
        return ArrayExpression(KQL(f'array_split({to_kql(array)}, {to_kql(indices)})'))

    @staticmethod
    def sign(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/signfunction
        """
        return NumberExpression(KQL(f'sign({to_kql(expr)})'))

    # def sin(self): return
    #
    #
    @staticmethod
    def split(string: StringType, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction
        """
        return StringExpression(to_kql(string)).split(delimiter, requested_index)

    @staticmethod
    def sqrt(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqrtfunction
        """
        return NumberExpression(KQL(f'sqrt({to_kql(expr)})'))

    @staticmethod
    def start_of_day(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction
        """
        return expr.start_of_day(offset)

    @staticmethod
    def start_of_month(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction
        """
        return expr.start_of_month(offset)

    @staticmethod
    def start_of_week(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction
        """
        return expr.start_of_week(offset)

    @staticmethod
    def start_of_year(expr: DatetimeType, offset: NumberType = None) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction
        """
        return expr.start_of_year(offset)

    @staticmethod
    def strcat(*strings: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcatfunction
        """
        if len(strings) < 2:
            raise ValueError("strcat requires at least two arguments")
        return StringExpression(KQL(f"strcat({', '.join(to_kql(s) for s in strings)})"))

    @staticmethod
    def to_literal_dynamic(d: DynamicType) -> KQL:
        if isinstance(d, BaseExpression):
            return d.kql
        return KQL(f'dynamic({json.dumps(d)})')

    @staticmethod
    def strcat_array(expr: ArrayType, delimiter: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-arrayfunction
        """
        return StringExpression(KQL(f'strcat_array({Functions.to_literal_dynamic(expr)}, {to_kql(delimiter)})'))

    @staticmethod
    def strcat_delim(delimiter: StringType, expr1: StringType, expr2: StringType, *expressions: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-delimfunction
        """
        res = f'strcat_delim({to_kql(delimiter)}, {to_kql(expr1)}, {to_kql(expr2)}'
        if len(expressions) > 0:
            res = res + ', ' + ', '.join(to_kql(expr) for expr in expressions)
        return StringExpression(KQL(res + ')'))

    @staticmethod
    def strcmp(expr1: StringType, expr2: StringType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcmpfunction
        """
        return NumberExpression(KQL(f'strcmp({to_kql(expr1)}, {to_kql(expr2)})'))

    @staticmethod
    def string_size(expr: StringType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stringsizefunction
        """
        return StringExpression(expr).string_size()

    @staticmethod
    def strlen(expr: StringType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strlenfunction
        """
        return NumberExpression(KQL(f'strlen({to_kql(expr)})'))

    @staticmethod
    def strrep(expr: StringType,
               multiplier: NumberType,
               delimiter: StringType = None) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strrepfunction
        """
        if delimiter is None:
            res = f'strrep({to_kql(expr)}, {to_kql(multiplier)})'
        else:
            res = f'strrep({to_kql(expr)}, {to_kql(multiplier)}, {to_kql(delimiter)})'
        return StringExpression(KQL(res))

    @staticmethod
    def substring(expr: StringType, start_index: NumberType, length: NumberType = None) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/substringfunction
        """
        return StringExpression(KQL(
            (f'substring({to_kql(expr)}, {to_kql(start_index)})' if length is None else f'substring({to_kql(expr)}, {to_kql(start_index)}, {to_kql(length)})')
        ))

    # def tan(self): return
    #
    #
    # def tdigest_merge(self): return

    @staticmethod
    def to_bool(expr: ExpressionType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction
        """
        return BooleanExpression(KQL(f'tobool({to_kql(expr)})'))

    @staticmethod
    def to_datetime(expr: StringType) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todatetimefunction
        """
        return DatetimeExpression(KQL(f'todatetime({to_kql(expr)})'))

    @staticmethod
    def to_decimal(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todecimalfunction
        """
        return NumberExpression(KQL(f"todecimal({to_kql(expr)})"))

    @staticmethod
    def to_double(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction
        """
        return NumberExpression(KQL(f"todouble({to_kql(expr)})"))

    @staticmethod
    def to_dynamic() -> DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todynamicfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def to_guid() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toguidfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def to_hex(expr1: NumberType, expr2: NumberType = None) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tohexfunction
        """
        return StringExpression(KQL(f'tohex({to_kql(expr1)})' if expr2 is None else 'tohex({to_kql(expr1)}, {to_kql(expr2)})'))

    @staticmethod
    def to_int(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction
        """
        return NumberExpression(KQL(f"toint({to_kql(expr)})"))

    @staticmethod
    def to_long(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolongfunction
        """
        return NumberExpression(KQL(f"tolong({to_kql(expr)})"))

    @staticmethod
    def to_lower(expr: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolowerfunction
        """
        return expr.lower()

    @staticmethod
    def to_real(expr: NumberType) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction
        """
        return NumberExpression(KQL(f"toreal({to_kql(expr)})"))

    @staticmethod
    def to_string(expr: ExpressionType):
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction
        """
        return expr.to_string()

    @staticmethod
    def to_timespan() -> TimespanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/totimespanfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def to_upper(expr: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toupperfunction
        """
        return expr.upper()

    # def to_utf8(self): return
    #
    #
    # def translate(self): return
    #
    #
    # def treepath(self): return

    @staticmethod
    def trim() -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimfunction
        """
        raise NotImplemented  # pragma: no cover

    # def trim_end(self): return
    #
    #
    # def trim_start(self): return

    @staticmethod
    def url_decode() -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/urldecodefunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def url_encode() -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/urlencodefunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def week_of_year() -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/weekofyearfunction
        """
        raise NotImplemented  # pragma: no cover

    # def welch_test(self): return

    @staticmethod
    def zip() -> DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/zipfunction
        """
        raise NotImplemented  # pragma: no cover

    # ----------------------------------------------------
    # Aggregation functions
    # -----------------------------------------------------

    @staticmethod
    def any(*args: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/any-aggfunction
        """
        return AnyAggregationExpression(KQL(f"any({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def any_if(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/anyif-aggfunction
        """
        return AnyAggregationExpression(KQL(f"anyif({to_kql(expr)}, {to_kql(predicate)})"))

    @staticmethod
    def arg_max(*args: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arg-max-aggfunction
        """
        return AnyAggregationExpression(KQL(f"arg_max({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def arg_min(*args: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arg-min-aggfunction
        """
        return AnyAggregationExpression(KQL(f"arg_min({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def avg(expr: ExpressionType) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/avg-aggfunction
        """
        return NumberAggregationExpression(KQL(f'avg({to_kql(expr)})'))

    @staticmethod
    def avg_if(expr: ExpressionType, predicate: BooleanType) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/avgif-aggfunction
        """
        return NumberAggregationExpression(KQL(f'avgif({to_kql(expr)}, {to_kql(predicate)})'))

    # def buildschema(self):
    #     return

    @staticmethod
    def count(col: AnyTypeColumn = None) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/count-aggfunction
        """
        return NumberAggregationExpression(KQL("count()" if col is None else f"count({col.kql})"))

    @staticmethod
    def count_if(predicate: BooleanType) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countif-aggfunction
        """
        return NumberAggregationExpression(KQL(f'countif({to_kql(predicate)})'))

    @staticmethod
    def dcount(expr: ExpressionType, accuracy: NumberType = None) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dcount-aggfunction
        """
        return NumberAggregationExpression(KQL(f'dcount({to_kql(expr)})' if accuracy is None else f'dcount({to_kql(expr)}, {to_kql(accuracy)})'))

    @staticmethod
    def dcount_if(expr: ExpressionType, predicate: BooleanType, accuracy: NumberType = 0) -> NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dcountif-aggfunction
        """
        return NumberAggregationExpression(KQL(f'dcountif({to_kql(expr)}, {to_kql(predicate)}, {to_kql(accuracy)})'))

    @staticmethod
    def make_bag(expr: ExpressionType, max_size: NumberType = None) -> MappingAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-bag-aggfunction
        """
        if max_size:
            return MappingAggregationExpression(KQL(f'make_bag({to_kql(expr)}, {to_kql(max_size)})'))
        return MappingAggregationExpression(KQL(f'make_bag({to_kql(expr)})'))

    @staticmethod
    def make_list(expr: ExpressionType, max_size: NumberType = None) -> ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelist-aggfunction
        """
        if max_size:
            return ArrayAggregationExpression(KQL(f'make_list({to_kql(expr)}, {to_kql(max_size)})'))
        return ArrayAggregationExpression(KQL(f'make_list({to_kql(expr)})'))

    @staticmethod
    def make_set(expr: ExpressionType, max_size: NumberType = None) -> ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction
        """
        if max_size:
            return ArrayAggregationExpression(KQL(f'make_set({to_kql(expr)}, {to_kql(max_size)})'))
        return ArrayAggregationExpression(KQL(f'make_set({to_kql(expr)})'))

    @staticmethod
    def max(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction
        """
        return AnyAggregationExpression(KQL(f'max({to_kql(expr)})'))

    @staticmethod
    def min(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/min-aggfunction
        """
        return AnyAggregationExpression(KQL(f'min({to_kql(expr)})'))

    @staticmethod
    def max_if(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/maxif-aggfunction
        """
        return AnyAggregationExpression(KQL(f'maxif({to_kql(expr)}, {to_kql(predicate)})'))

    @staticmethod
    def min_if(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/minif-aggfunction
        """
        return AnyAggregationExpression(KQL(f'minif({to_kql(expr)}, {to_kql(predicate)})'))

    @staticmethod
    def percentile(expr: ExpressionType, per: NumberType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return AnyAggregationExpression(KQL(f'percentiles({to_kql(expr)}, {to_kql(per)})'))

    @staticmethod
    def percentiles(expr: ExpressionType, *pers: NumberType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return AnyAggregationExpression(KQL(f"percentiles({to_kql(expr)}, {', '.join(str(to_kql(per)) for per in pers)})"))

    @staticmethod
    def percentiles_array(expr: ExpressionType, *pers: NumberType) -> ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return ArrayAggregationExpression(KQL(f"percentiles_array({to_kql(expr)}, {', '.join(str(to_kql(per)) for per in pers)})"))

    @staticmethod
    def stdev(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdev-aggfunction
        """
        return AnyAggregationExpression(KQL(f'stdev({to_kql(expr)})'))

    @staticmethod
    def stdevif(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevif-aggfunction
        """
        return AnyAggregationExpression(KQL(f'stdevif({to_kql(expr)}, {to_kql(predicate)})'))

    @staticmethod
    def stdevp(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevp-aggfunction
        """
        return AnyAggregationExpression(KQL(f'stdevp({to_kql(expr)})'))

    @staticmethod
    def sum(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sum-aggfunction
        """
        return AnyAggregationExpression(KQL(f'sum({to_kql(expr)})'))

    @staticmethod
    def sum_if(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sumif-aggfunction
        """
        return AnyAggregationExpression(KQL(f'sumif({to_kql(expr)}, {to_kql(predicate)})'))

    # def tdigest(self):
    #     return
    #
    #
    # def tdigest_merge(self):
    #     return

    @staticmethod
    def variance(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/variance-aggfunction
        """
        return AnyAggregationExpression(KQL(f'variance({to_kql(expr)})'))

    @staticmethod
    def variance_if(expr: ExpressionType, predicate: BooleanType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/varianceif-aggfunction
        """
        return AnyAggregationExpression(KQL(f'varianceif({to_kql(expr)}, {to_kql(predicate)})'))

    @staticmethod
    def variancep(expr: ExpressionType) -> AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/variancep-aggfunction
        """
        return AnyAggregationExpression(KQL(f'variancep({to_kql(expr)})'))

    # Used for mv-expand
    @staticmethod
    def to_type(column: BaseColumn, type_name: KustoType) -> ColumnToType:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator
        """
        return ColumnToType(column, type_name)
