from datetime import datetime, timedelta
from typing import Any, List, Tuple, Mapping, Optional
from typing import Union

from pykusto.keywords import KUSTO_KEYWORDS
from pykusto.kql_converters import KQL
from pykusto.type_utils import plain_expression, aggregation_expression, PythonTypes, kql_converter, KustoType, typed_column, TypeRegistrar, get_base_types, NUMBER_TYPES

ExpressionType = Union[PythonTypes, 'BaseExpression']
StringType = Union[str, 'StringExpression']
BooleanType = Union[bool, 'BooleanExpression']
NumberType = Union[int, float, 'NumberExpression']
ArrayType = Union[List, Tuple, 'ArrayExpression']
MappingType = Union[Mapping, 'MappingExpression']
DatetimeType = Union[datetime, 'DatetimeExpression']
TimespanType = Union[timedelta, 'TimespanExpression']
DynamicType = Union[ArrayType, MappingType]
OrderedType = Union[DatetimeType, TimespanType, NumberType, StringType]


# All classes in the same file to prevent circular dependencies
def _subexpr_to_kql(obj: ExpressionType) -> KQL:
    """
    Convert the given expression to KQL, enclosing it in parentheses if it is a compound expression. This guarantees
    correct evaluation order. When parentheses are not needed, for example when the expressions is used as an argument
    to a function, use `to_kql` instead.

    :param obj: Expression to convert to KQL
    :return: KQL that represents the given expression
    """
    if isinstance(obj, BaseExpression):
        return obj.as_subexpression()
    return to_kql(obj)


class BaseExpression:
    kql: KQL

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'BaseExpression':
        assert cls is not BaseExpression, "BaseExpression is abstract"
        return object.__new__(cls)

    def __init__(self, kql: Union[KQL, 'BaseExpression']) -> None:
        if isinstance(kql, BaseExpression):
            self.kql = kql.kql
            return
        assert isinstance(kql, str), "Either expression or KQL required"
        self.kql = kql

    def __repr__(self) -> str:
        return str(self.kql)

    def as_subexpression(self) -> KQL:
        return KQL(f'({self.kql})')

    def get_type(self) -> 'StringExpression':
        return StringExpression(KQL(f'gettype({self.kql})'))

    def __hash__(self) -> 'StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hashfunction
        """
        return StringExpression(KQL(f'hash({self.kql})'))

    def hash_sha256(self) -> 'StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sha256hashfunction
        """
        return StringExpression(KQL(f'hash_sha256({self.kql})'))

    def is_empty(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isemptyfunction
        """
        return BooleanExpression(KQL(f'isempty({self.kql})'))

    def is_not_empty(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotemptyfunction
        """
        return BooleanExpression(KQL(f'isnotempty({self.kql})'))

    def has(self, exp: StringType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        # The pattern for the search expression must be a constant string.
        return BooleanExpression(KQL(f'{self.kql} has {to_kql(exp)}'))

    @staticmethod
    def base_binary_op(
            left: ExpressionType, operator: str, right: ExpressionType, result_type: Optional[KustoType]
    ) -> 'BaseExpression':
        registrar = plain_expression
        fallback = AnyExpression
        if isinstance(left, AggregationExpression) or isinstance(right, AggregationExpression):
            registrar = aggregation_expression
            fallback = AnyAggregationExpression
        return_type = fallback if result_type is None else registrar.registry[result_type]
        return return_type(KQL(f'{_subexpr_to_kql(left)}{operator}{_subexpr_to_kql(right)}'))

    def __eq__(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' == ', other)

    def __ne__(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' != ', other)

    def is_in(self, other: ArrayType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        """
        return BooleanExpression.binary_op(self, ' in ', other)

    def is_null(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnullfunction
        """
        return BooleanExpression(KQL(f'isnull({self.kql})'))

    def is_not_null(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotnullfunction
        """
        return BooleanExpression(KQL(f'isnotnull({self.kql})'))

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError("'in' not supported. Instead use '.is_in()'")

    def to_bool(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction
        """
        return BooleanExpression(KQL(f'tobool({self.kql})'))

    def to_string(self) -> 'StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction
        """
        return StringExpression(KQL(f'tostring({self.kql})'))

    def to_int(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction
        """
        return NumberExpression(KQL(f'toint({self.kql})'))

    def to_long(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolongfunction
        """
        return NumberExpression(KQL(f'tolong({self.kql})'))

    def assign_to_single_column(self, column: 'AnyTypeColumn') -> 'AssignmentToSingleColumn':
        return AssignmentToSingleColumn(column, self)

    def assign_to_multiple_columns(self, *columns: 'AnyTypeColumn') -> 'AssignmentBase':
        """
        This method exists for the sole purpose of providing an informative error message.
        """
        raise ValueError("Only arrays can be assigned to multiple columns")

    def assign_to(self, *columns: 'AnyTypeColumn') -> 'AssignmentBase':
        if len(columns) == 0:
            # Unspecified column name
            return AssignmentBase(None, self)
        if len(columns) == 1:
            return self.assign_to_single_column(columns[0])
        return self.assign_to_multiple_columns(*columns)


@plain_expression(KustoType.BOOL)
class BooleanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'BooleanExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, KustoType.BOOL)

    def __and__(self, other: BooleanType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return BooleanExpression.binary_op(self, ' and ', other)

    def __rand__(self, other: BooleanType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return BooleanExpression.binary_op(other, ' and ', self)

    def __or__(self, other: BooleanType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return BooleanExpression.binary_op(self, ' or ', other)

    def __ror__(self, other: BooleanType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return BooleanExpression.binary_op(other, ' or ', self)

    def __invert__(self) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/notfunction
        """
        return BooleanExpression(KQL(f'not({self.kql})'))


@plain_expression(*NUMBER_TYPES)
class NumberExpression(BaseExpression):
    @staticmethod
    def binary_op(left: NumberType, operator: str, right: NumberType) -> 'NumberExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, KustoType.INT)

    def __lt__(self, other: NumberType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' < ', other)

    def __le__(self, other: NumberType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' <= ', other)

    def __gt__(self, other: NumberType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' > ', other)

    def __ge__(self, other: NumberType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' >= ', other)

    def __add__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' + ', other)

    def __radd__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(other, ' + ', self)

    def __sub__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' - ', other)

    def __rsub__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(other, ' - ', self)

    def __mul__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' * ', other)

    def __rmul__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(other, ' * ', self)

    def __truediv__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' / ', other)

    def __rtruediv__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(other, ' / ', self)

    def __mod__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' % ', other)

    def __rmod__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(other, ' % ', self)

    def __neg__(self) -> 'NumberExpression':
        return NumberExpression(KQL(f'-{self.kql}'))

    def __abs__(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/abs-function
        """
        return NumberExpression(KQL(f'abs({self.kql})'))

    def between(self, lower: NumberType, upper: NumberType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return BooleanExpression(KQL(f'{self.kql} between ({_subexpr_to_kql(lower)} .. {_subexpr_to_kql(upper)})'))

    def acos(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/acosfunction
        """
        return NumberExpression(KQL(f'acos({self.kql})'))

    def cos(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/cosfunction
        """
        return NumberExpression(KQL(f'cos({self.kql})'))

    def floor(self, round_to: NumberType) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return NumberExpression(KQL(f'floor({self.kql}, {_subexpr_to_kql(round_to)})'))

    def bin(self, round_to: NumberType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return NumberExpression(KQL(f'bin({self.kql}, {_subexpr_to_kql(round_to)})'))

    def bin_at(self, round_to: NumberType, fixed_point: NumberType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return NumberExpression(KQL(f'bin_at({self.kql}, {_subexpr_to_kql(round_to)}, {_subexpr_to_kql(fixed_point)})'))

    def bin_auto(self) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return NumberExpression(KQL(f'bin_auto({self.kql})'))

    def ceiling(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ceilingfunction
        """
        return NumberExpression(KQL(f'ceiling({self.kql})'))

    def exp(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp-function
        """
        return NumberExpression(KQL(f'exp({self.kql})'))

    def exp10(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp10-function
        """
        return NumberExpression(KQL(f'exp10({self.kql})'))

    def exp2(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp2-function
        """
        return NumberExpression(KQL(f'exp2({self.kql})'))

    def isfinite(self) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isfinitefunction
        """
        return BooleanExpression(KQL(f'isfinite({self.kql})'))

    def is_inf(self) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isinffunction
        """
        return BooleanExpression(KQL(f'isinf({self.kql})'))

    def is_nan(self) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction
        """
        return BooleanExpression(KQL(f'isnan({self.kql})'))

    def log(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log-function
        """
        return NumberExpression(KQL(f'log({self.kql})'))

    def log10(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log10-function
        """
        return NumberExpression(KQL(f'log10({self.kql})'))

    def log2(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log2-function
        """
        return NumberExpression(KQL(f'log2({self.kql})'))

    def log_gamma(self) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/loggammafunction
        """
        return NumberExpression(KQL(f'loggamma({self.kql})'))

    def round(self, precision: NumberType = None) -> 'NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/roundfunction
        """
        return NumberExpression(KQL(f'round({self.kql})' if precision is None else f'round({self.kql}, {to_kql(precision)})'))


@plain_expression(KustoType.STRING)
class StringExpression(BaseExpression):
    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def string_size(self) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stringsizefunction
        """
        return NumberExpression(KQL(f'string_size({self.kql})'))

    def split(self, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction
        """
        if requested_index is None:
            return ArrayExpression(KQL(f'split({self.kql}, {to_kql(delimiter)})'))
        return ArrayExpression(KQL(f'split({self.kql}, {to_kql(delimiter)}, {to_kql(requested_index)})'))

    def equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' == ' if case_sensitive else ' =~ ', other)

    def not_equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' != ' if case_sensitive else ' !~ ', other)

    def matches(self, regex: StringType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return BooleanExpression.binary_op(self, ' matches regex ', regex)

    def contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return BooleanExpression.binary_op(self, ' contains_cs ' if case_sensitive else ' contains ', other)

    def not_contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return BooleanExpression.binary_op(self, ' !contains_cs ' if case_sensitive else ' !contains ', other)

    def startswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return BooleanExpression.binary_op(self, ' startswith_cs ' if case_sensitive else ' startswith ', other)

    def endswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return BooleanExpression.binary_op(self, ' endswith_cs ' if case_sensitive else ' endswith ', other)

    def lower(self) -> 'StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolowerfunction
        """
        return StringExpression(KQL(f'tolower({self.kql})'))

    def upper(self) -> 'StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toupperfunction
        """
        return StringExpression(KQL(f'toupper({self.kql})'))

    def is_utf8(self) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isutf8
        """
        return BooleanExpression(KQL(f'isutf8({self.kql})'))


@plain_expression(KustoType.DATETIME)
class DatetimeExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'DatetimeExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, KustoType.DATETIME)

    def __lt__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' < ', other)

    def __le__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' <= ', other)

    def __gt__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' > ', other)

    def __ge__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' >= ', other)

    def __add__(self, other: TimespanType) -> 'DatetimeExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return DatetimeExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: Union[DatetimeType, TimespanType]) -> Union['DatetimeExpression', 'TimespanExpression']:
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic

        # noinspection PyTypeChecker
        base_types = get_base_types(other)
        possible_types = base_types & {KustoType.DATETIME, KustoType.TIMESPAN}
        assert len(possible_types) > 0, "Invalid type subtracted from datetime"
        if len(possible_types) == 2:
            return_type = AnyExpression
        else:
            return_type = TimespanExpression if next(iter(possible_types)) == KustoType.DATETIME else DatetimeExpression
        return return_type(DatetimeExpression.binary_op(self, ' - ', other))

    def __rsub__(self, other: DatetimeType) -> 'TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return TimespanExpression.binary_op(other, ' - ', self)

    def between(self, lower: DatetimeType, upper: DatetimeType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return BooleanExpression(KQL(f'{self.kql} between ({_subexpr_to_kql(lower)} .. {_subexpr_to_kql(upper)})'))

    def floor(self, round_to: TimespanType) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return DatetimeExpression(KQL(f'floor({self.kql}, {_subexpr_to_kql(round_to)})'))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return DatetimeExpression(KQL(f'bin({self.kql}, {_subexpr_to_kql(round_to)})'))

    def bin_at(self, round_to: TimespanType, fixed_point: DatetimeType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return DatetimeExpression(KQL(f'bin_at({self.kql}, {_subexpr_to_kql(round_to)}, {to_kql(fixed_point)})'))

    def bin_auto(self) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return DatetimeExpression(KQL(f'bin_auto({self.kql})'))

    def end_of_day(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction
        """
        if offset is None:
            res = f'endofday({self.kql})'
        else:
            res = f'endofday({self.kql}, {_subexpr_to_kql(offset)})'
        return DatetimeExpression(KQL(res))

    def end_of_month(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction
        """
        if offset is None:
            res = f'endofmonth({self.kql})'
        else:
            res = f'endofmonth({self.kql}, {_subexpr_to_kql(offset)})'
        return DatetimeExpression(KQL(res))

    def end_of_week(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction
        """
        if offset is None:
            res = f'endofweek({self.kql})'
        else:
            res = f'endofweek({self.kql}, {_subexpr_to_kql(offset)})'
        return DatetimeExpression(KQL(res))

    def end_of_year(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction
        """
        if offset is None:
            res = f'endofyear({self.kql})'
        else:
            res = f'endofyear({self.kql}, {_subexpr_to_kql(offset)})'
        return DatetimeExpression(KQL(res))

    def format_datetime(self, format_string: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction
        """
        return StringExpression(KQL(f'format_datetime({self.kql}, {_subexpr_to_kql(format_string)})'))

    def get_month(self) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction
        """
        return NumberExpression(KQL(f'getmonth({self.kql})'))

    def get_year(self) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction
        """
        return NumberExpression(KQL(f'getyear({self.kql})'))

    def hour_of_day(self) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction
        """
        return NumberExpression(KQL(f'hourofday({self.kql})'))

    def start_of_day(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction
        """
        return DatetimeExpression(KQL(f'startofday({self.kql})' if offset is None else f'startofday({self.kql}, {to_kql(offset)})'))

    def start_of_month(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction
        """
        return DatetimeExpression(KQL(f'startofmonth({self.kql})' if offset is None else f'startofmonth({self.kql}, {to_kql(offset)})'))

    def start_of_week(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction
        """
        return DatetimeExpression(KQL(f'startofweek({self.kql})' if offset is None else f'startofweek({self.kql}, {to_kql(offset)})'))

    def start_of_year(self, offset: NumberType = None) -> 'DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction
        """
        return DatetimeExpression(KQL(f'startofyear({self.kql})' if offset is None else 'startofyear({self.kql}, {to_kql(offset)})'))


@plain_expression(KustoType.TIMESPAN)
class TimespanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'TimespanExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, KustoType.TIMESPAN)

    def __add__(self, other: TimespanType) -> 'TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return TimespanExpression.binary_op(self, ' + ', other)

    def __radd__(self, other: TimespanType) -> 'TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return TimespanExpression.binary_op(other, ' + ', self)

    def __sub__(self, other: TimespanType) -> 'TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return TimespanExpression.binary_op(self, ' - ', other)

    def __rsub__(self, other: TimespanType) -> 'TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return TimespanExpression.binary_op(other, ' - ', self)

    def ago(self) -> DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction
        """
        return DatetimeExpression(KQL(f'ago({to_kql(self)})'))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return TimespanExpression(KQL(f'bin({self.kql}, {_subexpr_to_kql(round_to)})'))

    def bin_at(self, round_to: TimespanType, fixed_point: TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return TimespanExpression(KQL(f'bin_at({self.kql}, {to_kql(round_to)}, {to_kql(fixed_point)})'))

    def bin_auto(self) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return TimespanExpression(KQL(f'bin_auto({self.kql})'))

    def format_timespan(self, format_string: StringType) -> StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction
        """
        return StringExpression(KQL(f'format_timespan({self.kql}, {to_kql(format_string)})'))

    def between(self, lower: TimespanType, upper: TimespanType) -> BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return BooleanExpression(KQL(f'{self.kql} between ({_subexpr_to_kql(lower)} .. {_subexpr_to_kql(upper)})'))


@plain_expression(KustoType.ARRAY)
class ArrayExpression(BaseExpression):
    def __getitem__(self, index: NumberType) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}[{to_kql(index)}]'))

    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def array_length(self) -> NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction
        """
        return NumberExpression(KQL(f'array_length({self.kql})'))

    def array_contains(self, other: ExpressionType) -> 'BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        """
        return BooleanExpression.binary_op(other, ' in ', self)

    def assign_to_multiple_columns(self, *columns: 'AnyTypeColumn') -> 'AssignmentToMultipleColumns':
        return AssignmentToMultipleColumns(columns, self)


@plain_expression(KustoType.MAPPING)
class MappingExpression(BaseExpression):
    def __getitem__(self, index: StringType) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}[{to_kql(index)}]'))

    def __getattr__(self, name: str) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}.{name}'))

    def keys(self) -> ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bagkeysfunction
        """
        return ArrayExpression(KQL(f'bag_keys({self.kql})'))


class DynamicExpression(ArrayExpression, MappingExpression):
    def __getitem__(self, index: Union[StringType, NumberType]) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}[{_subexpr_to_kql(index)}]'))


class AnyExpression(
    NumberExpression, BooleanExpression,
    StringExpression, DynamicExpression,
    DatetimeExpression, TimespanExpression
):
    pass


class AggregationExpression(BaseExpression):

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'AggregationExpression':
        assert cls is not AggregationExpression, "AggregationExpression is abstract"
        return object.__new__(cls)

    def assign_to(self, *columns: 'AnyTypeColumn') -> 'AssignmentFromAggregationToColumn':
        if len(columns) == 0:
            # Unspecified column name
            return AssignmentFromAggregationToColumn(None, self)
        if len(columns) == 1:
            return AssignmentFromAggregationToColumn(columns[0], self)
        raise ValueError("Aggregations cannot be assigned to multiple columns")

    def as_subexpression(self) -> KQL:
        return self.kql


@aggregation_expression(KustoType.BOOL)
class BooleanAggregationExpression(AggregationExpression, BooleanExpression):
    pass


@aggregation_expression(*NUMBER_TYPES)
class NumberAggregationExpression(AggregationExpression, NumberExpression):
    pass


@aggregation_expression(KustoType.STRING)
class StringAggregationExpression(AggregationExpression, StringExpression):
    pass


@aggregation_expression(KustoType.DATETIME)
class DatetimeAggregationExpression(AggregationExpression, DatetimeExpression):
    pass


@aggregation_expression(KustoType.TIMESPAN)
class TimespanAggregationExpression(AggregationExpression, TimespanExpression):
    pass


@aggregation_expression(KustoType.ARRAY)
class ArrayAggregationExpression(AggregationExpression, ArrayExpression):
    pass


@aggregation_expression(KustoType.MAPPING)
class MappingAggregationExpression(AggregationExpression, MappingExpression):
    pass


class AnyAggregationExpression(AggregationExpression, AnyExpression):
    pass


class AssignmentBase:
    _lvalue: Optional[KQL]
    _rvalue: KQL

    def __init__(self, lvalue: Optional[KQL], rvalue: ExpressionType) -> None:
        self._lvalue = lvalue
        self._rvalue = to_kql(rvalue)

    def to_kql(self) -> KQL:
        if self._lvalue is None:
            # Unspecified column name
            return self._rvalue
        return KQL(f'{self._lvalue} = {self._rvalue}')


class AssignmentToSingleColumn(AssignmentBase):
    def __init__(self, column: 'AnyTypeColumn', expression: ExpressionType) -> None:
        super().__init__(column.kql, expression)


class AssignmentFromColumnToColumn(AssignmentToSingleColumn):
    def __init__(self, target: 'AnyTypeColumn', source: 'BaseColumn') -> None:
        super().__init__(target, source)


class AssignmentToMultipleColumns(AssignmentBase):
    def __init__(self, columns: Union[List['AnyTypeColumn'], Tuple['AnyTypeColumn']], expression: ArrayType) -> None:
        super().__init__(KQL(f'({", ".join(c.kql for c in columns)})'), expression)


class AssignmentFromAggregationToColumn(AssignmentBase):
    def __init__(self, column: Optional['AnyTypeColumn'], aggregation: AggregationExpression) -> None:
        super().__init__(None if column is None else column.kql, aggregation)


class BaseColumn(BaseExpression):
    _name: str

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'BaseColumn':
        assert cls is not BaseColumn, "BaseColumn is abstract"
        return object.__new__(cls)

    def __init__(self, name: str, quote: bool = False) -> None:
        assert len(name) > 0, "Column name must not be empty"
        should_quote = quote or '.' in name or name in KUSTO_KEYWORDS or name.isdigit()
        super().__init__(KQL(f"['{name}']" if should_quote else name))
        self._name = name

    def get_name(self) -> str:
        return self._name

    def as_subexpression(self) -> KQL:
        return self.kql

    def assign_to_single_column(self, column: 'AnyTypeColumn') -> 'AssignmentFromColumnToColumn':
        return AssignmentFromColumnToColumn(column, self)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._name})'

    def __str__(self) -> str:
        return self._name


@typed_column(*NUMBER_TYPES)
class NumberColumn(BaseColumn, NumberExpression):
    pass


@typed_column(KustoType.BOOL)
class BooleanColumn(BaseColumn, BooleanExpression):
    pass


@typed_column(KustoType.ARRAY)
class ArrayColumn(BaseColumn, ArrayExpression):
    pass


@typed_column(KustoType.MAPPING)
class MappingColumn(BaseColumn, MappingExpression):
    pass


class DynamicColumn(ArrayColumn, MappingColumn):
    pass


@typed_column(KustoType.STRING)
class StringColumn(BaseColumn, StringExpression):
    pass


@typed_column(KustoType.DATETIME)
class DatetimeColumn(BaseColumn, DatetimeExpression):
    pass


@typed_column(KustoType.TIMESPAN)
class TimespanColumn(BaseColumn, TimespanExpression):
    pass


class SubtractableColumn(NumberColumn, DatetimeColumn, TimespanColumn):
    @staticmethod
    def __resolve_type(type_to_resolve: Union['NumberType', 'DatetimeType', 'TimespanType']) -> Optional[KustoType]:
        # noinspection PyTypeChecker
        base_types = get_base_types(type_to_resolve)
        possible_types = base_types & ({KustoType.DATETIME, KustoType.TIMESPAN} | NUMBER_TYPES)
        assert len(possible_types) > 0, "Invalid type subtracted"
        if possible_types == NUMBER_TYPES:
            return KustoType.INT
        if len(possible_types) > 1:
            return None
        return next(iter(possible_types))

    def __sub__(self, other: Union['NumberType', 'DatetimeType', 'TimespanType']) -> Union['NumberExpression', 'TimespanExpression', 'AnyExpression']:
        resolved_type = self.__resolve_type(other)
        if resolved_type == KustoType.DATETIME:
            # Subtracting a datetime can only result in a timespan
            resolved_type = KustoType.TIMESPAN
        elif resolved_type == KustoType.TIMESPAN:
            # Subtracting a timespan can result in either a timespan or a datetime
            resolved_type = None
        # Otherwise: subtracting a number can only result in a number

        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(self, ' - ', other, resolved_type)

    def __rsub__(self, other: Union['NumberType', 'DatetimeType', 'TimespanType']) -> Union['NumberExpression', 'TimespanExpression', 'AnyExpression']:
        resolved_type = self.__resolve_type(other)
        if resolved_type == KustoType.DATETIME:
            # Subtracting from a datetime can result in either a datetime or a timespan
            resolved_type = None
        # Otherwise: subtracting from a number or a timespan can only result in a number or a timespan respectively

        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(other, ' - ', self, resolved_type)


class AnyTypeColumn(SubtractableColumn, BooleanColumn, DynamicColumn, StringColumn):
    pass


class ColumnGenerator:
    def __getattr__(self, name: str) -> AnyTypeColumn:
        return AnyTypeColumn(name)

    def __getitem__(self, name: str) -> AnyTypeColumn:
        return AnyTypeColumn(name)

    # noinspection PyMethodMayBeStatic
    def of(self, name: str) -> AnyTypeColumn:
        """
        Workaround in case automatic column name quoting fails
        """
        return AnyTypeColumn(name, quote=True)


# Recommended usage: from pykusto.expressions import column_generator as col
# TODO: Is there a way to enforce this to be a singleton?
column_generator = ColumnGenerator()


class ColumnToType(BaseExpression):
    def __init__(self, col: BaseColumn, kusto_type: KustoType) -> None:
        super().__init__(KQL(f"{col.kql} to typeof({kusto_type.primary_name})"))


def to_kql(obj: ExpressionType) -> KQL:
    """
    Convert the given expression to KQL. If this is a subexpression of a greater expression, neighboring operators might
    take precedence over operators included in this expression, causing an incorrect evaluation order.
    If this is a concern, use `_subexpr_to_kql` instead, which will enclose this expression in parentheses if it is
    a compound expression.

    :param obj: Expression to convert to KQL
    :return: KQL that represents the given expression
    """
    if isinstance(obj, BaseExpression):
        return obj.kql
    return kql_converter.for_obj(obj)


def expression_to_type(expression: ExpressionType, type_registrar: TypeRegistrar, fallback_type: Any) -> Any:
    types = set(type_registrar.registry[base_type] for base_type in plain_expression.get_base_types(expression))
    return next(iter(types)) if len(types) == 1 else fallback_type


typed_column.assert_all_types_covered()
plain_expression.assert_all_types_covered()
aggregation_expression.assert_all_types_covered()
