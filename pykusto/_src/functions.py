from itertools import chain
from typing import Union

from .enums import Kind
from .expressions import _AnyTypeColumn, _NumberType, _NumberExpression, _TimespanType, \
    _DatetimeExpression, _TimespanExpression, _ArrayType, _DynamicType, _DatetimeType, BaseExpression, _BooleanType, \
    _ExpressionType, _StringType, _StringExpression, _BooleanExpression, \
    _NumberAggregationExpression, _MappingAggregationExpression, _ArrayAggregationExpression, _to_kql, _DynamicExpression, \
    _ArrayExpression, _ColumnToType, BaseColumn, AnyExpression, _AnyAggregationExpression, _MappingExpression
from .kql_converters import KQL
from .logger import _logger
from .type_utils import _plain_expression, _KustoType


class Functions:
    """
    Recommended import style:\n
    `from pykusto import Functions as f`
    """

    # Scalar functions

    @staticmethod
    def acos(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/acosfunction
        """
        return expr.acos()

    @staticmethod
    def ago(expr: _TimespanType) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction
        """
        return _TimespanExpression.ago(expr)

    @staticmethod
    def array_length(expr: _ArrayType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction
        """
        return _ArrayExpression(expr).array_length()

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
    def bag_keys(expr: _DynamicType):
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
    def bin(expr: Union[_NumberType, _DatetimeType, _TimespanType],
            round_to: Union[_NumberType, _TimespanType]) -> BaseExpression:
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
    def bin_at(expr: Union[_NumberType, _DatetimeType, _TimespanType],
               bin_size: Union[_NumberType, _TimespanType],
               fixed_point: Union[_NumberType, _DatetimeType, _TimespanType]) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return expr.bin_at(bin_size, fixed_point)

    @staticmethod
    def bin_auto(expr: Union[_NumberType, _DatetimeType, _TimespanType]) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return expr.bin_auto()

    @staticmethod
    def all_of(*predicates: _BooleanType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression(KQL(' and '.join(_to_kql(c, True) for c in predicates)))

    @staticmethod
    def any_of(*predicates: _BooleanType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression(KQL(' or '.join(_to_kql(c, True) for c in predicates)))

    @staticmethod
    def not_of(predicate: _BooleanType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        Note that using the Python 'not' does not have the desired effect, because unfortunately its behavior cannot be overridden.
        """
        return _BooleanExpression(KQL(f'not({_to_kql(predicate)})'))

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
    def case(predicate: _BooleanType, val: _ExpressionType, *args: Union[_BooleanType, _ExpressionType]) -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/casefunction
        """
        assert len(args) > 0, "case must have at least three arguments"
        return AnyExpression(KQL(f"case({_to_kql(predicate)}, {_to_kql(val)}, {', '.join(_to_kql(arg) for arg in args)})"))

    @staticmethod
    def ceiling(expr: _NumberType) -> _NumberExpression:
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
    def cos(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/cosfunction
        """
        return expr.cos()

    # def cot(self): return

    @staticmethod
    def count_of(text: _StringType, search: _StringType, kind: Kind = Kind.NORMAL) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoffunction
        """
        return _NumberExpression(KQL(f'countof({_to_kql(text)}, {_to_kql(search)}, {_to_kql(kind.value)})'))

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
    def end_of_day(expr: _DatetimeExpression, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction
        """
        return expr.end_of_day(offset)

    @staticmethod
    def end_of_month(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction
        """
        return expr.end_of_month(offset)

    @staticmethod
    def end_of_week(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction
        """
        return expr.end_of_week(offset)

    @staticmethod
    def end_of_year(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction
        """
        return expr.end_of_year(offset)

    # def estimate_data_size(self): return

    @staticmethod
    def exp(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp-function
        """
        return expr.exp()

    @staticmethod
    def exp10(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp10-function
        """
        return expr.exp10()

    @staticmethod
    def exp2(expr: _NumberType) -> _NumberExpression:
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
    def floor(expr: Union[_NumberType, _DatetimeType],
              round_to: Union[_NumberType, _TimespanType]) -> Union[_NumberExpression, _DatetimeExpression]:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return expr.floor(round_to)

    @staticmethod
    def format_datetime(expr: _DatetimeExpression, format_string: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction
        """
        return expr.format_datetime(format_string)

    @staticmethod
    def format_timespan(expr: _TimespanType, format_string: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction
        """
        return expr.format_timespan(format_string)

    # def gamma(self): return

    @staticmethod
    def get_month(expr: _DatetimeType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction
        """
        return expr.get_month()

    @staticmethod
    def get_type(expr: _ExpressionType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/gettypefunction
        """
        return expr.get_type()

    @staticmethod
    def get_year(expr: _DatetimeType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction
        """
        return expr.get_year()

    @staticmethod
    def hash(expr: BaseExpression, mod: Union['_NumberExpression', int] = None) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hashfunction
        """
        return expr.__hash__(mod)

    @staticmethod
    def hash_sha256(expr: _ExpressionType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sha256hashfunction
        """
        return expr.hash_sha256()

    @staticmethod
    def hour_of_day(expr: _DatetimeType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction
        """
        return expr.hour_of_day()

    @staticmethod
    def iff(predicate: _BooleanType, if_true: _ExpressionType, if_false: _ExpressionType) -> BaseExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ifffunction
        """
        return_types = _plain_expression.get_base_types(if_true)
        other_types = _plain_expression.get_base_types(if_false)
        common_types = other_types & return_types
        if len(common_types) == 0:
            # If there is not at least one common type, then certainly the arguments are not of the same type
            _logger.warning(
                "The second and third arguments must be of the same type, but they are: "
                f"{', '.join(sorted(t.primary_name for t in return_types))} and {', '.join(sorted(t.primary_name for t in other_types))}. "
                "If this is a mistake, please report it at https://github.com/Azure/pykusto/issues"
            )
            expression_type = AnyExpression
        else:
            expression_type = _plain_expression.registry[next(iter(common_types))]
        return expression_type(
            KQL(f'iff({_to_kql(predicate)}, {_to_kql(if_true)}, {_to_kql(if_false)})')
        )

    @staticmethod
    def iif(predicate: _BooleanType, if_true: _ExpressionType, if_false: _ExpressionType) -> BaseExpression:
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
    def ingestion_time() -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ingestiontimefunction
        """
        return _DatetimeExpression(KQL('ingestion_time()'))

    @staticmethod
    def is_empty(expr: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isemptyfunction
        """
        return expr.is_empty()

    @staticmethod
    def is_finite(expr: _NumberType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isfinitefunction
        """
        return expr.isfinite()

    @staticmethod
    def is_inf(expr: _NumberType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isinffunction
        """
        return expr.is_inf()

    @staticmethod
    def is_nan(expr: _NumberExpression) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction
        """
        return expr.is_nan()

    @staticmethod
    def is_not_empty(expr: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotemptyfunction
        """
        return expr.is_not_empty()

    @staticmethod
    def is_not_null(expr: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotnullfunction
        """
        return expr.is_not_null()

    @staticmethod
    def is_null(expr: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnullfunction
        """
        return expr.is_null()

    @staticmethod
    def is_utf8(expr: _StringType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isutf8
        """
        return expr.is_utf8()

    @staticmethod
    def log(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log-function
        """
        return expr.log()

    @staticmethod
    def log10(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log10-function
        """
        return expr.log10()

    @staticmethod
    def log2(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log2-function
        """
        return expr.log2()

    @staticmethod
    def log_gamma(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/loggammafunction
        """
        return expr.log_gamma()

    @staticmethod
    def make_datetime(year: _NumberType,
                      month: _NumberType,
                      day: _NumberType,
                      hour: _NumberType = None,
                      minute: _NumberType = None,
                      second: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-datetimefunction
        """
        res = f'make_datetime(' \
              f'{_to_kql(year)}, {_to_kql(month)}, {_to_kql(day)}, {_to_kql(0 if hour is None else hour)}, ' \
              f'{_to_kql(0 if minute is None else minute)}, {_to_kql(0 if second is None else second)})'
        return _DatetimeExpression(KQL(res))

    @staticmethod
    def make_string(*args: Union[_NumberType, _ArrayType]) -> _StringExpression:
        """
        print str = make_string(dynamic([75, 117, 115]), 116, 111)
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makestringfunction
        """
        return _StringExpression(KQL(f"make_string({', '.join(_to_kql(arg) for arg in args)})"))

    @staticmethod
    def make_timespan() -> _TimespanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-timespanfunction
        """
        raise NotImplemented  # pragma: no cover

    # def max_of(self): return
    #
    #
    # def min_of(self): return

    @staticmethod
    def month_of_year(date: _DatetimeType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/monthofyearfunction
        """
        return _NumberExpression(KQL(f"monthofyear({_to_kql(date)})"))

    @staticmethod
    def new_guid() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/newguidfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def now(offset: _TimespanType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/nowfunction
        """
        if offset:
            return _DatetimeExpression(KQL(f'now({_to_kql(offset)})'))
        return _DatetimeExpression(KQL('now()'))

    @staticmethod
    def pack(**kwargs: _ExpressionType) -> _MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packfunction
        """
        return _MappingExpression(KQL(f'pack({", ".join(f"{_to_kql(k)}, {_to_kql(v)}" for k, v in kwargs.items())})'))

    @staticmethod
    def pack_all() -> _MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packallfunction
        """
        return _MappingExpression(KQL("pack_all()"))

    @staticmethod
    def pack_array(*elements: _ExpressionType) -> '_ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packarrayfunction
        """
        return _ArrayExpression(KQL(f'pack_array({", ".join(_to_kql(e) for e in elements)})'))

    @staticmethod
    def pack_dictionary(**kwargs: _ExpressionType) -> _MappingExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/packdictionaryfunction
        """
        return _MappingExpression(KQL(f'pack_dictionary({", ".join(f"{_to_kql(k)}, {_to_kql(v)}" for k, v in kwargs.items())})'))

    #
    #
    # def parse_csv(self): return
    #
    #
    # def parse_ipv4(self): return

    @staticmethod
    def parse_json(expr: Union[_StringType, _DynamicType]) -> _DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/parsejsonfunction
        """
        return _DynamicExpression(KQL(f'parse_json({_to_kql(expr)})'))

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
    def pow(expr1: _NumberType, expr2: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/powfunction
        """
        return _NumberExpression(KQL(f'pow({_to_kql(expr1)}, {_to_kql(expr2)})'))

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
    def round(expr: _NumberType, precision: _NumberType = None) -> _NumberExpression:
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
    def set_has_element(array: _ArrayType, value: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sethaselementfunction
        """
        return _BooleanExpression(KQL(f'set_has_element({_to_kql(array)}, {_to_kql(value)})'))

    @staticmethod
    def set_difference(array1: _ArrayType, array2: _ArrayType, *more_arrays: _ArrayType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setdifferencefunction
        """
        return _ArrayExpression(KQL(f'set_difference({_to_kql(array1)}, {", ".join(_to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def set_intersect(array1: _ArrayType, array2: _ArrayType, *more_arrays: _ArrayType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setintersectfunction
        """
        return _ArrayExpression(KQL(f'set_intersect({_to_kql(array1)}, {", ".join(_to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def set_union(array1: _ArrayType, array2: _ArrayType, *more_arrays: _ArrayType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/setunionfunction
        """
        return _ArrayExpression(KQL(f'set_union({_to_kql(array1)}, {", ".join(_to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def array_concat(array1: _ArrayType, array2: _ArrayType, *more_arrays: _ArrayType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayconcatfunction
        """
        return _ArrayExpression(KQL(f'array_concat({_to_kql(array1)}, {", ".join(_to_kql(a) for a in chain([array2], more_arrays))})'))

    @staticmethod
    def array_iif(condition_array: _ArrayType, if_true: _ArrayType, if_false: _ArrayType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayifffunction
        """
        return _ArrayExpression(KQL(f'array_iif({_to_kql(condition_array)}, {_to_kql(if_true)}, {_to_kql(if_false)})'))

    @staticmethod
    def array_index_of(array: _ArrayType, value: _ExpressionType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayindexoffunction
        """
        return _NumberExpression(KQL(f'array_index_of({_to_kql(array)}, {_to_kql(value)})'))

    @staticmethod
    def array_rotate_left(array: _ArrayType, rotate_count: _NumberType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_leftfunction
        """
        return _ArrayExpression(KQL(f'array_rotate_left({_to_kql(array)}, {_to_kql(rotate_count)})'))

    @staticmethod
    def array_rotate_right(array: _ArrayType, rotate_count: _NumberType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_rotate_rightfunction
        """
        return _ArrayExpression(KQL(f'array_rotate_right({_to_kql(array)}, {_to_kql(rotate_count)})'))

    @staticmethod
    def array_shift_left(array: _ArrayType, shift_count: _NumberType, fill_value: _ExpressionType = None) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_leftfunction
        """
        return _ArrayExpression(KQL(f'array_shift_left({_to_kql(array)}, {_to_kql(shift_count)}{"" if fill_value is None else ", " + _to_kql(fill_value)})'))

    @staticmethod
    def array_shift_right(array: _ArrayType, shift_count: _NumberType, fill_value: _ExpressionType = None) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/array_shift_rightfunction
        """
        return _ArrayExpression(KQL(f'array_shift_right({_to_kql(array)}, {_to_kql(shift_count)}{"" if fill_value is None else ", " + _to_kql(fill_value)})'))

    @staticmethod
    def array_slice(array: _ArrayType, start: _NumberType, end: _NumberType) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arrayslicefunction
        """
        return _ArrayExpression(KQL(f'array_slice({_to_kql(array)}, {_to_kql(start)}, {_to_kql(end)})'))

    @staticmethod
    def array_split(array: _ArrayType, indices: Union[_NumberType, _ArrayType]) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraysplitfunction
        """
        return _ArrayExpression(KQL(f'array_split({_to_kql(array)}, {_to_kql(indices)})'))

    @staticmethod
    def sign(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/signfunction
        """
        return _NumberExpression(KQL(f'sign({_to_kql(expr)})'))

    # def sin(self): return
    #
    #
    @staticmethod
    def split(string: _StringType, delimiter: _StringType, requested_index: _NumberType = None) -> '_ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction
        """
        return _StringExpression(_to_kql(string)).split(delimiter, requested_index)

    @staticmethod
    def sqrt(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sqrtfunction
        """
        return _NumberExpression(KQL(f'sqrt({_to_kql(expr)})'))

    @staticmethod
    def start_of_day(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction
        """
        return expr.start_of_day(offset)

    @staticmethod
    def start_of_month(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction
        """
        return expr.start_of_month(offset)

    @staticmethod
    def start_of_week(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction
        """
        return expr.start_of_week(offset)

    @staticmethod
    def start_of_year(expr: _DatetimeType, offset: _NumberType = None) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction
        """
        return expr.start_of_year(offset)

    @staticmethod
    def strcat(*strings: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcatfunction
        """
        if len(strings) < 2:
            raise ValueError("strcat requires at least two arguments")
        return _StringExpression(KQL(f"strcat({', '.join(_to_kql(s) for s in strings)})"))

    @staticmethod
    def strcat_array(expr: _ArrayType, delimiter: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-arrayfunction
        """
        return _StringExpression(KQL(f'strcat_array({_to_kql(expr)}, {_to_kql(delimiter)})'))

    @staticmethod
    def strcat_delim(delimiter: _StringType, expr1: _StringType, expr2: _StringType, *expressions: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcat-delimfunction
        """
        res = f'strcat_delim({_to_kql(delimiter)}, {_to_kql(expr1)}, {_to_kql(expr2)}'
        if len(expressions) > 0:
            res = res + ', ' + ', '.join(_to_kql(expr) for expr in expressions)
        return _StringExpression(KQL(res + ')'))

    @staticmethod
    def strcmp(expr1: _StringType, expr2: _StringType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strcmpfunction
        """
        return _NumberExpression(KQL(f'strcmp({_to_kql(expr1)}, {_to_kql(expr2)})'))

    @staticmethod
    def string_size(expr: _StringType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stringsizefunction
        """
        return _StringExpression(expr).string_size()

    @staticmethod
    def strlen(expr: _StringType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strlenfunction
        """
        return _NumberExpression(KQL(f'strlen({_to_kql(expr)})'))

    @staticmethod
    def strrep(expr: _StringType,
               multiplier: _NumberType,
               delimiter: _StringType = None) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/strrepfunction
        """
        if delimiter is None:
            res = f'strrep({_to_kql(expr)}, {_to_kql(multiplier)})'
        else:
            res = f'strrep({_to_kql(expr)}, {_to_kql(multiplier)}, {_to_kql(delimiter)})'
        return _StringExpression(KQL(res))

    @staticmethod
    def substring(expr: _StringType, start_index: _NumberType, length: _NumberType = None) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/substringfunction
        """
        return _StringExpression(KQL(
            (f'substring({_to_kql(expr)}, {_to_kql(start_index)})' if length is None else f'substring({_to_kql(expr)}, {_to_kql(start_index)}, {_to_kql(length)})')
        ))

    # def tan(self): return
    #
    #
    # def tdigest_merge(self): return

    @staticmethod
    def to_bool(expr: _ExpressionType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction
        """
        return _BooleanExpression(KQL(f'tobool({_to_kql(expr)})'))

    @staticmethod
    def to_datetime(expr: _StringType) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todatetimefunction
        """
        return _DatetimeExpression(KQL(f'todatetime({_to_kql(expr)})'))

    @staticmethod
    def to_decimal(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todecimalfunction
        """
        return _NumberExpression(KQL(f"todecimal({_to_kql(expr)})"))

    @staticmethod
    def to_double(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction
        """
        return _NumberExpression(KQL(f"todouble({_to_kql(expr)})"))

    @staticmethod
    def to_dynamic(json: _StringType) -> _DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todynamicfunction
        """
        return _DynamicExpression(KQL(f"todynamic({_to_kql(json)})"))

    @staticmethod
    def to_guid() -> AnyExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toguidfunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def to_hex(expr1: _NumberType, expr2: _NumberType = None) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tohexfunction
        """
        return _StringExpression(KQL(f'tohex({_to_kql(expr1)})' if expr2 is None else 'tohex({to_kql(expr1)}, {to_kql(expr2)})'))

    @staticmethod
    def to_int(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction
        """
        return _NumberExpression(KQL(f"toint({_to_kql(expr)})"))

    @staticmethod
    def to_long(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolongfunction
        """
        return _NumberExpression(KQL(f"tolong({_to_kql(expr)})"))

    @staticmethod
    def to_lower(expr: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolowerfunction
        """
        return expr.lower()

    @staticmethod
    def to_real(expr: _NumberType) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/todoublefunction
        """
        return _NumberExpression(KQL(f"toreal({_to_kql(expr)})"))

    @staticmethod
    def to_string(expr: _ExpressionType):
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction
        """
        return expr.to_string()

    @staticmethod
    def to_timespan(expr: _StringType) -> _TimespanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/totimespanfunction
        """
        return _TimespanExpression(KQL(f"totimespan({_to_kql(expr)})"))

    @staticmethod
    def to_upper(expr: _StringType) -> _StringExpression:
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
    def trim(regex: _StringType, text: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/trimfunction
        """
        return _StringExpression(KQL(f"trim({_to_kql(regex)}, {_to_kql(text)})"))

    # def trim_end(self): return
    #
    #
    # def trim_start(self): return

    @staticmethod
    def url_decode() -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/urldecodefunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def url_encode() -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/urlencodefunction
        """
        raise NotImplemented  # pragma: no cover

    @staticmethod
    def week_of_year() -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/weekofyearfunction
        """
        raise NotImplemented  # pragma: no cover

    # def welch_test(self): return

    @staticmethod
    def zip() -> _DynamicExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/zipfunction
        """
        raise NotImplemented  # pragma: no cover

    # ----------------------------------------------------
    # Aggregation functions
    # -----------------------------------------------------

    @staticmethod
    def any(*args: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/any-aggfunction
        """
        return _AnyAggregationExpression(KQL(f"any({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def any_if(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/anyif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f"anyif({_to_kql(expr)}, {_to_kql(predicate)})"))

    @staticmethod
    def arg_max(*args: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arg-max-aggfunction
        """
        return _AnyAggregationExpression(KQL(f"arg_max({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def arg_min(*args: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arg-min-aggfunction
        """
        return _AnyAggregationExpression(KQL(f"arg_min({', '.join(arg.kql for arg in args)})"))

    @staticmethod
    def avg(expr: _ExpressionType) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/avg-aggfunction
        """
        return _NumberAggregationExpression(KQL(f'avg({_to_kql(expr)})'))

    @staticmethod
    def avg_if(expr: _ExpressionType, predicate: _BooleanType) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/avgif-aggfunction
        """
        return _NumberAggregationExpression(KQL(f'avgif({_to_kql(expr)}, {_to_kql(predicate)})'))

    # def buildschema(self):
    #     return

    @staticmethod
    def count(col: _AnyTypeColumn = None) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/count-aggfunction
        """
        return _NumberAggregationExpression(KQL("count()" if col is None else f"count({col.kql})"))

    @staticmethod
    def count_if(predicate: _BooleanType) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countif-aggfunction
        """
        return _NumberAggregationExpression(KQL(f'countif({_to_kql(predicate)})'))

    @staticmethod
    def dcount(expr: _ExpressionType, accuracy: _NumberType = None) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dcount-aggfunction
        """
        return _NumberAggregationExpression(KQL(f'dcount({_to_kql(expr)})' if accuracy is None else f'dcount({_to_kql(expr)}, {_to_kql(accuracy)})'))

    @staticmethod
    def dcount_if(expr: _ExpressionType, predicate: _BooleanType, accuracy: _NumberType = 0) -> _NumberAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/dcountif-aggfunction
        """
        return _NumberAggregationExpression(KQL(f'dcountif({_to_kql(expr)}, {_to_kql(predicate)}, {_to_kql(accuracy)})'))

    @staticmethod
    def make_bag(expr: _ExpressionType, max_size: _NumberType = None) -> _MappingAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/make-bag-aggfunction
        """
        if max_size is not None:
            return _MappingAggregationExpression(KQL(f'make_bag({_to_kql(expr)}, {_to_kql(max_size)})'))
        return _MappingAggregationExpression(KQL(f'make_bag({_to_kql(expr)})'))

    @staticmethod
    def make_list(expr: _ExpressionType, max_size: _NumberType = None) -> _ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makelist-aggfunction
        """
        if max_size is not None:
            return _ArrayAggregationExpression(KQL(f'make_list({_to_kql(expr)}, {_to_kql(max_size)})'))
        return _ArrayAggregationExpression(KQL(f'make_list({_to_kql(expr)})'))

    @staticmethod
    def make_set(expr: _ExpressionType, max_size: _NumberType = None) -> _ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction
        """
        if max_size is not None:
            return _ArrayAggregationExpression(KQL(f'make_set({_to_kql(expr)}, {_to_kql(max_size)})'))
        return _ArrayAggregationExpression(KQL(f'make_set({_to_kql(expr)})'))

    @staticmethod
    def max(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/makeset-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'max({_to_kql(expr)})'))

    @staticmethod
    def min(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/min-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'min({_to_kql(expr)})'))

    @staticmethod
    def max_if(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/maxif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'maxif({_to_kql(expr)}, {_to_kql(predicate)})'))

    @staticmethod
    def min_if(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/minif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'minif({_to_kql(expr)}, {_to_kql(predicate)})'))

    @staticmethod
    def percentile(expr: _ExpressionType, per: _NumberType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'percentiles({_to_kql(expr)}, {_to_kql(per)})'))

    @staticmethod
    def percentiles(expr: _ExpressionType, *pers: _NumberType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return _AnyAggregationExpression(KQL(f"percentiles({_to_kql(expr)}, {', '.join(str(_to_kql(per)) for per in pers)})"))

    @staticmethod
    def percentiles_array(expr: _ExpressionType, *pers: _NumberType) -> _ArrayAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/percentiles-aggfunction
        """
        return _ArrayAggregationExpression(KQL(f"percentiles_array({_to_kql(expr)}, {', '.join(str(_to_kql(per)) for per in pers)})"))

    @staticmethod
    def stdev(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdev-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'stdev({_to_kql(expr)})'))

    @staticmethod
    def stdevif(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'stdevif({_to_kql(expr)}, {_to_kql(predicate)})'))

    @staticmethod
    def stdevp(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stdevp-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'stdevp({_to_kql(expr)})'))

    @staticmethod
    def sum(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sum-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'sum({_to_kql(expr)})'))

    @staticmethod
    def sum_if(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sumif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'sumif({_to_kql(expr)}, {_to_kql(predicate)})'))

    # def tdigest(self):
    #     return
    #
    #
    # def tdigest_merge(self):
    #     return

    @staticmethod
    def variance(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/variance-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'variance({_to_kql(expr)})'))

    @staticmethod
    def variance_if(expr: _ExpressionType, predicate: _BooleanType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/varianceif-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'varianceif({_to_kql(expr)}, {_to_kql(predicate)})'))

    @staticmethod
    def variancep(expr: _ExpressionType) -> _AnyAggregationExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/variancep-aggfunction
        """
        return _AnyAggregationExpression(KQL(f'variancep({_to_kql(expr)})'))

    # Used for mv-expand
    @staticmethod
    def to_type(column: BaseColumn, type_name: _KustoType) -> _ColumnToType:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator
        """
        return _ColumnToType(column, type_name)
