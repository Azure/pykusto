from datetime import datetime, timedelta
from typing import Any, List, Tuple, Mapping, Optional, Type
from typing import Union

from pykusto.kql_converters import KQL
from pykusto.type_utils import plain_expression, aggregation_expression, PythonTypes, kql_converter, KustoType, \
    typed_column

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
        return KQL('({})'.format(self.kql))

    def gettype(self) -> 'StringExpression':
        return StringExpression(KQL('gettype({})'.format(self.kql)))

    def __hash__(self) -> 'StringExpression':
        return StringExpression(KQL('hash({})'.format(self.kql)))

    def hash_sha256(self) -> 'StringExpression':
        return StringExpression(KQL('hash_sha256({})'.format(self.kql)))

    def is_empty(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('isempty({})'.format(self.kql)))

    def is_not_empty(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('isnotempty({})'.format(self.kql)))

    def has(self, exp: str) -> 'BooleanExpression':
        # The pattern for the search expression must be a constant string.
        return BooleanExpression(KQL('{} has \"{}\"'.format(self.kql, exp)))

    @staticmethod
    def base_binary_op(
            left: ExpressionType, operator: str, right: ExpressionType, result_type: Type[KustoType]
    ) -> 'BaseExpression':
        registrar = plain_expression
        if isinstance(left, AggregationExpression) or isinstance(right, AggregationExpression):
            registrar = aggregation_expression
        return registrar.for_type(result_type)(KQL('{}{}{}'.format(
            _subexpr_to_kql(left), operator, _subexpr_to_kql(right))
        ))

    def __eq__(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' == ', other)

    def __ne__(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' != ', other)

    def is_in(self, other: ArrayType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' in ', other)

    def is_null(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('isnull({})'.format(self.kql)))

    def is_not_null(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('isnotnull({})'.format(self.kql)))

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError("'in' not supported. Instead use '.is_in()'")

    def to_bool(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('tobool({})'.format(self.kql)))

    def to_string(self) -> 'StringExpression':
        return StringExpression(KQL('tostring({})'.format(self.kql)))

    def to_int(self) -> 'NumberExpression':
        return NumberExpression(KQL('toint({})'.format(self.kql)))

    def to_long(self) -> 'NumberExpression':
        return NumberExpression(KQL('tolong({})'.format(self.kql)))

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
        return BaseExpression.base_binary_op(left, operator, right, bool)

    def __and__(self, other: BooleanType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' and ', other)

    def __or__(self, other: BooleanType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' or ', other)

    def __invert__(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('not({})'.format(self.kql)))


@plain_expression(KustoType.INT, KustoType.LONG, KustoType.REAL)
class NumberExpression(BaseExpression):
    @staticmethod
    def binary_op(left: NumberType, operator: str, right: NumberType) -> 'NumberExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, int)

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

    def __sub__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' - ', other)

    def __mul__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' * ', other)

    def __truediv__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' / ', other)

    def __mod__(self, other: NumberType) -> 'NumberExpression':
        return NumberExpression.binary_op(self, ' % ', other)

    def __neg__(self) -> 'NumberExpression':
        return NumberExpression(KQL('-{}'.format(self.kql)))

    def __abs__(self) -> 'NumberExpression':
        return NumberExpression(KQL('abs({})'.format(self.kql)))

    def between(self, lower: NumberType, upper: NumberType) -> BooleanExpression:
        return BooleanExpression(KQL('{} between ({} .. {})'.format(
            self.kql, _subexpr_to_kql(lower), _subexpr_to_kql(upper)
        )))

    def acos(self) -> 'NumberExpression':
        return NumberExpression(KQL('acos({})'.format(self.kql)))

    def cos(self) -> 'NumberExpression':
        return NumberExpression(KQL('cos({})'.format(self.kql)))

    def floor(self, round_to: NumberType) -> 'NumberExpression':
        return NumberExpression(KQL('floor({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin(self, round_to: NumberType) -> 'BaseExpression':
        return NumberExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: NumberType, fixed_point: NumberType) -> 'BaseExpression':
        return NumberExpression(KQL('bin_at({}, {}, {})'.format(self.kql,
                                                                _subexpr_to_kql(round_to),
                                                                _subexpr_to_kql(fixed_point))))

    def bin_auto(self) -> 'BaseExpression':
        return NumberExpression(KQL('bin_auto({})'.format(self.kql)))

    def ceiling(self) -> 'NumberExpression':
        return NumberExpression(KQL('ceiling({})'.format(self.kql)))

    def exp(self) -> 'NumberExpression':
        return NumberExpression(KQL('exp({})'.format(self.kql)))

    def exp10(self) -> 'NumberExpression':
        return NumberExpression(KQL('exp10({})'.format(self.kql)))

    def exp2(self) -> 'NumberExpression':
        return NumberExpression(KQL('exp2({})'.format(self.kql)))

    def isfinite(self) -> BooleanExpression:
        return BooleanExpression(KQL('isfinite({})'.format(self.kql)))

    def isinf(self) -> BooleanExpression:
        return BooleanExpression(KQL('isinf({})'.format(self.kql)))

    def isnan(self) -> BooleanExpression:
        return BooleanExpression(KQL('isnan({})'.format(self.kql)))

    def log(self) -> 'NumberExpression':
        return NumberExpression(KQL('log({})'.format(self.kql)))

    def log10(self) -> 'NumberExpression':
        return NumberExpression(KQL('log10({})'.format(self.kql)))

    def log2(self) -> 'NumberExpression':
        return NumberExpression(KQL('log2({})'.format(self.kql)))

    def loggamma(self) -> 'NumberExpression':
        return NumberExpression(KQL('loggamma({})'.format(self.kql)))

    def round(self, precision: NumberType = None) -> 'NumberExpression':
        return NumberExpression(KQL(
            ('round({}, {})' if precision is None else 'round({}, {})').format(self.kql, to_kql(precision))
        ))


@plain_expression(KustoType.STRING)
class StringExpression(BaseExpression):
    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def string_size(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))

    def split(self, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
        if requested_index is None:
            return ArrayExpression(KQL('split({}, {})'.format(self.kql, to_kql(delimiter))))
        return ArrayExpression(KQL('split({}, {}, {})'.format(
            self.kql, to_kql(delimiter), to_kql(requested_index)
        )))

    def equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' == ' if case_sensitive else ' =~ ', other)

    def not_equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' != ' if case_sensitive else ' !~ ', other)

    def matches(self, regex: StringType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' matches regex ', regex)

    def contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' contains_cs ' if case_sensitive else ' contains ', other)

    def not_contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' !contains_cs ' if case_sensitive else ' !contains ', other)

    def startswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' startswith_cs ' if case_sensitive else ' startswith ', other)

    def endswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' endswith_cs ' if case_sensitive else ' endswith ', other)

    def lower(self) -> 'StringExpression':
        return StringExpression(KQL('tolower({})'.format(self.kql)))

    def upper(self) -> 'StringExpression':
        return StringExpression(KQL('toupper({})'.format(self.kql)))

    def is_utf8(self) -> BooleanExpression:
        return BooleanExpression(KQL('isutf8({})'.format(self.kql)))


@plain_expression(KustoType.DATETIME)
class DatetimeExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'DatetimeExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, datetime)

    def __lt__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' < ', other)

    def __le__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' <= ', other)

    def __gt__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' > ', other)

    def __ge__(self, other: DatetimeType) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' >= ', other)

    def __add__(self, other: TimespanType) -> 'DatetimeExpression':
        return DatetimeExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: Union[DatetimeType, TimespanType]) -> Union['DatetimeExpression', 'TimespanExpression']:
        if isinstance(other, (datetime, DatetimeExpression)):
            return_type = TimespanExpression
        else:
            assert isinstance(other, (timedelta, TimespanExpression)), "Invalid type subtracted from datetime"
            return_type = DatetimeExpression
        return return_type(DatetimeExpression.binary_op(self, ' - ', other))

    def between(self, lower: DatetimeType, upper: DatetimeType) -> BooleanExpression:
        return BooleanExpression(KQL('{} between ({} .. {})'.format(
            self.kql, _subexpr_to_kql(lower), _subexpr_to_kql(upper)
        )))

    def floor(self, round_to: TimespanType) -> 'DatetimeExpression':
        return DatetimeExpression(KQL('floor({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        return DatetimeExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: TimespanType, fixed_point: DatetimeType) -> 'BaseExpression':
        return DatetimeExpression(KQL('bin_at({}, {}, {})'.format(
            self.kql, _subexpr_to_kql(round_to), to_kql(fixed_point)
        )))

    def bin_auto(self) -> 'DatetimeExpression':
        return DatetimeExpression(KQL('bin_auto({})'.format(self.kql)))

    def endofday(self, offset: NumberType = None) -> 'DatetimeExpression':
        if offset is None:
            res = 'endofday({})'.format(self.kql)
        else:
            res = 'endofday({}, {})'.format(self.kql, _subexpr_to_kql(offset))
        return DatetimeExpression(KQL(res))

    def endofmonth(self, offset: NumberType = None) -> 'DatetimeExpression':
        if offset is None:
            res = 'endofmonth({})'.format(self.kql)
        else:
            res = 'endofmonth({}, {})'.format(self.kql, _subexpr_to_kql(offset))
        return DatetimeExpression(KQL(res))

    def endofweek(self, offset: NumberType = None) -> 'DatetimeExpression':
        if offset is None:
            res = 'endofweek({})'.format(self.kql)
        else:
            res = 'endofweek({}, {})'.format(self.kql, _subexpr_to_kql(offset))
        return DatetimeExpression(KQL(res))

    def endofyear(self, offset: NumberType = None) -> 'DatetimeExpression':
        if offset is None:
            res = 'endofyear({})'.format(self.kql)
        else:
            res = 'endofyear({}, {})'.format(self.kql, _subexpr_to_kql(offset))
        return DatetimeExpression(KQL(res))

    def format_datetime(self, format_string: StringType) -> StringExpression:
        return StringExpression(KQL('format_datetime({}, {})'.format(self.kql, _subexpr_to_kql(format_string))))

    def getmonth(self) -> NumberExpression:
        return NumberExpression(KQL('getmonth({})'.format(self.kql)))

    def getyear(self) -> NumberExpression:
        return NumberExpression(KQL('getyear({})'.format(self.kql)))

    def hourofday(self) -> NumberExpression:
        return NumberExpression(KQL('hourofday({})'.format(self.kql)))

    def startofday(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofday({})' if offset is None else 'startofday({}, {})').format(self.kql, to_kql(offset))
        ))

    def startofmonth(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofmonth({})' if offset is None else 'startofmonth({}, {})').format(self.kql, to_kql(offset))
        ))

    def startofweek(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofweek({})' if offset is None else 'startofweek({}, {})').format(self.kql, to_kql(offset))
        ))

    def startofyear(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofyear({})' if offset is None else 'startofyear({}, {})').format(self.kql, to_kql(offset))
        ))


@plain_expression(KustoType.TIMESPAN)
class TimespanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'TimespanExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, timedelta)

    def __add__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' - ', other)

    def ago(self) -> DatetimeExpression:
        return DatetimeExpression(KQL('ago({})'.format(_subexpr_to_kql(self))))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        return TimespanExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: TimespanType, fixed_point: TimespanType) -> 'BaseExpression':
        return TimespanExpression(KQL('bin_at({}, {}, {})'.format(
            self.kql, to_kql(round_to), to_kql(fixed_point)
        )))

    def bin_auto(self) -> 'BaseExpression':
        return TimespanExpression(KQL('bin_auto({})'.format(self.kql)))

    def format_timespan(self, format_string: StringType) -> StringExpression:
        return StringExpression(KQL('format_timespan({}, {})'.format(self.kql, to_kql(format_string))))

    def between(self, lower: TimespanType, upper: TimespanType) -> BooleanExpression:
        return BooleanExpression(KQL('{} between ({} .. {})'.format(
            self.kql, _subexpr_to_kql(lower), _subexpr_to_kql(upper)
        )))


@plain_expression(KustoType.ARRAY)
class ArrayExpression(BaseExpression):
    def __getitem__(self, index: NumberType) -> 'AnyExpression':
        return AnyExpression(KQL('{}[{}]'.format(self.kql, to_kql(index))))

    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def array_length(self) -> NumberExpression:
        return NumberExpression(KQL('array_length({})'.format(self.kql)))

    def array_contains(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(other, ' in ', self)

    def assign_to_multiple_columns(self, *columns: 'AnyTypeColumn') -> 'AssignmentToMultipleColumns':
        return AssignmentToMultipleColumns(columns, self)


@plain_expression(KustoType.MAPPING)
class MappingExpression(BaseExpression):
    def __getitem__(self, index: StringType) -> 'AnyExpression':
        return AnyExpression(KQL('{}[{}]'.format(self.kql, to_kql(index))))

    def __getattr__(self, name: str) -> 'AnyExpression':
        return AnyExpression(KQL('{}.{}'.format(self.kql, name)))

    def keys(self) -> ArrayExpression:
        return ArrayExpression(KQL('bag_keys({})'.format(self.kql)))


class DynamicExpression(ArrayExpression, MappingExpression):
    def __getitem__(self, index: Union[StringType, NumberType]) -> 'AnyExpression':
        return AnyExpression(KQL('{}[{}]'.format(self.kql, _subexpr_to_kql(index))))


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


@aggregation_expression(KustoType.INT, KustoType.LONG, KustoType.REAL)
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


class AnyAggregationExpression(AggregationExpression, AnyExpression):
    pass


@aggregation_expression(KustoType.MAPPING)
class MappingAggregationExpression(AggregationExpression, MappingExpression):
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
        return KQL('{} = {}'.format(self._lvalue, self._rvalue))


class AssignmentToSingleColumn(AssignmentBase):
    def __init__(self, column: 'AnyTypeColumn', expression: ExpressionType) -> None:
        super().__init__(column.kql, expression)


class AssignmentFromColumnToColumn(AssignmentToSingleColumn):
    def __init__(self, target: 'AnyTypeColumn', source: 'BaseColumn') -> None:
        super().__init__(target, source)


class AssignmentToMultipleColumns(AssignmentBase):
    def __init__(self, columns: Union[List['AnyTypeColumn'], Tuple['AnyTypeColumn']], expression: ArrayType) -> None:
        super().__init__(KQL('({})'.format(', '.join(c.kql for c in columns))), expression)


class AssignmentFromAggregationToColumn(AssignmentBase):
    def __init__(self, column: Optional['AnyTypeColumn'], aggregation: AggregationExpression) -> None:
        super().__init__(None if column is None else column.kql, aggregation)


class BaseColumn(BaseExpression):
    _name: str

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. We can't define "get_kusto_type" as abstract because at least one
    # concrete subclass (NumberColumn) does not override it. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'BaseColumn':
        assert cls is not BaseColumn, "BaseColumn is abstract"
        return object.__new__(cls)

    def __init__(self, name: str) -> None:
        super().__init__(KQL("['{}']".format(name) if '.' in name else name))
        self._name = name

    def get_name(self) -> str:
        return self._name

    def as_subexpression(self) -> KQL:
        return self.kql

    def assign_to_single_column(self, column: 'AnyTypeColumn') -> 'AssignmentFromColumnToColumn':
        return AssignmentFromColumnToColumn(column, self)

    def get_kusto_type(self) -> KustoType:
        raise NotImplementedError("BaseColumn has no type")

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._name})'

    def __str__(self) -> str:
        return self._name


@typed_column(KustoType.INT, KustoType.LONG, KustoType.REAL)
class NumberColumn(BaseColumn, NumberExpression):
    pass


@typed_column(KustoType.BOOL)
class BooleanColumn(BaseColumn, BooleanExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.BOOL


@typed_column(KustoType.ARRAY)
class ArrayColumn(BaseColumn, ArrayExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.ARRAY


@typed_column(KustoType.MAPPING)
class MappingColumn(BaseColumn, MappingExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.MAPPING


class DynamicColumn(ArrayColumn, MappingColumn):
    def get_kusto_type(self) -> KustoType:
        raise ValueError("Column type unknown")


@typed_column(KustoType.STRING)
class StringColumn(BaseColumn, StringExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.STRING


@typed_column(KustoType.DATETIME)
class DatetimeColumn(BaseColumn, DatetimeExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.DATETIME


@typed_column(KustoType.TIMESPAN)
class TimespanColumn(BaseColumn, TimespanExpression):
    def get_kusto_type(self) -> KustoType:
        return KustoType.TIMESPAN


class AnyTypeColumn(NumberColumn, BooleanColumn, DynamicColumn, StringColumn, DatetimeColumn, TimespanColumn):
    def get_kusto_type(self) -> KustoType:
        raise ValueError("Column type unknown")


class ColumnGenerator:
    def __getattr__(self, name: str) -> AnyTypeColumn:
        return AnyTypeColumn(name)

    def __getitem__(self, name: str) -> AnyTypeColumn:
        return AnyTypeColumn(name)


# Recommended usage: from pykusto.expressions import column_generator as col
# TODO: Is there a way to enforce this to be a singleton?
column_generator = ColumnGenerator()


class ColumnToType(BaseExpression):
    def __init__(self, col: BaseColumn, kusto_type: KustoType) -> None:
        super().__init__(KQL("{} to typeof({})".format(col.kql, kusto_type.primary_name)))


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
