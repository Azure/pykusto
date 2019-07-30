from datetime import datetime, timedelta
from typing import Any, List, Tuple, Mapping, Optional
from typing import Union

from pykusto.utils import KQL
from pykusto.utils import KustoTypes, to_kql

ExpressionType = Union[KustoTypes, 'BaseExpression']
StringType = Union[str, 'StringExpression']
BooleanType = Union[bool, 'BooleanExpression']
NumberType = Union[int, float, 'NumberExpression']
ArrayType = Union[List, Tuple, 'ArrayExpression']
MappingType = Union[Mapping, 'MappingExpression']
DatetimeType = Union[datetime, 'DatetimeExpression']
TimespanType = Union[timedelta, 'TimespanExpression']
AggregationType = Union['AggregationExpression']
DynamicType = Union[ArrayType, MappingType]
OrderType = Union[DatetimeType, TimespanType, NumberType, StringType]


# All classes in the same file to prevent circular dependencies

def _subexpr_to_kql(obj: ExpressionType) -> KQL:
    if isinstance(obj, BaseExpression):
        return obj.as_subexpression()
    return to_kql(obj)


class BaseExpression:
    kql: KQL

    def __init__(self, kql: KQL) -> None:
        self.kql = kql

    def __repr__(self) -> str:
        return self.kql

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

    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> KQL:
        return KQL('{}{}{}'.format(
            _subexpr_to_kql(left), operator, _subexpr_to_kql(right))
        )

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
        raise NotImplementedError("Instead use 'is_in' or 'contains'")

    def to_bool(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('tobool({})'.format(self.kql)))

    def to_string(self) -> 'StringExpression':
        return StringExpression(KQL('tostring({})'.format(self.kql)))

    def assign_to(self, *columns: 'Column') -> 'AssignmentBase':
        if len(columns) == 0:
            # Unspecified column name
            return AssignmentBase(None, self)
        if len(columns) == 1:
            return AssignmentToSingleColumn(columns[0], self)
        raise ValueError("Only arrays can be assigned to multiple columns")


class BooleanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression(BaseExpression.binary_op(left, operator, right))

    def __and__(self, other: BooleanType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' and ', other)

    def __or__(self, other: BooleanType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' or ', other)

    def __invert__(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('not({})'.format(self.kql)))


class NumberExpression(BaseExpression):
    @staticmethod
    def binary_op(left: NumberType, operator: str, right: NumberType) -> 'NumberExpression':
        return NumberExpression(BaseExpression.binary_op(left, operator, right))

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
        return BaseExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: NumberType, fixed_point: NumberType) -> 'BaseExpression':
        return BaseExpression(KQL('bin_at({}, {}, {})'.format(self.kql,
                                                              _subexpr_to_kql(round_to),
                                                              _subexpr_to_kql(fixed_point))))

    def bin_auto(self) -> 'BaseExpression':
        return BaseExpression(KQL('bin_auto({})'.format(self.kql)))

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
        return NumberExpression(KQL('log({})'.format(self)))

    def log10(self) -> 'NumberExpression':
        return NumberExpression(KQL('log10({})'.format(self)))

    def log2(self) -> 'NumberExpression':
        return NumberExpression(KQL('log2({})'.format(self)))

    def loggamma(self) -> 'NumberExpression':
        return NumberExpression(KQL('loggamma({})'.format(self)))

    def round(self, precision: NumberType = None) -> 'NumberExpression':
        return NumberExpression(KQL(
            ('round({}, {})' if precision is None else 'round({}, {})').format(self, precision)
        ))


class StringExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.string_size()

    def string_size(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))

    def is_empty(self) -> BooleanExpression:
        return BooleanExpression(KQL('isempty({})'.format(self.kql)))

    def __add__(self, other: StringType) -> 'StringExpression':
        return StringExpression(BaseExpression.binary_op(self, ' + ', other))

    @staticmethod
    def concat(*strings: StringType) -> 'StringExpression':
        return StringExpression(KQL('strcat({})'.format(', '.join('{}'.format(
            _subexpr_to_kql(s)
        ) for s in strings))))

    def split(self, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
        if requested_index is None:
            return ArrayExpression(KQL('split({}, {}'.format(self.kql, delimiter)))
        return ArrayExpression(KQL('split({}, {}, {}'.format(self.kql, delimiter, requested_index)))

    def equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' == ' if case_sensitive else ' =~ ', other)

    def not_equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' != ' if case_sensitive else ' !~ ', other)

    def matches(self, regex: StringType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' matches regex ', regex)

    def contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' contains_cs ' if case_sensitive else ' contains ', other)

    def startswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' startswith_cs ' if case_sensitive else ' startswith ', other)

    def endswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' endswith_cs ' if case_sensitive else ' endswith ', other)

    def to_int(self) -> NumberExpression:
        return NumberExpression(KQL('toint({})'.format(self.kql)))

    def to_long(self) -> NumberExpression:
        return NumberExpression(KQL('tolong({})'.format(self.kql)))

    def lower(self) -> 'StringExpression':
        return StringExpression(KQL('tolower({})'.format(self.kql)))

    def upper(self) -> 'StringExpression':
        return StringExpression(KQL('toupper({})'.format(self.kql)))

    def is_utf8(self) -> BooleanExpression:
        return BooleanExpression(KQL('isutf8({})'.format(self.kql)))


class DatetimeExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'DatetimeExpression':
        return DatetimeExpression(BaseExpression.binary_op(left, operator, right))

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

    def __sub__(self, other: Any) -> BaseExpression:
        raise NotImplementedError("Instead use 'date_diff' or 'subtract_timespan'")

    def date_diff(self, other: DatetimeType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' - ', other)

    def subtract_timespan(self, other: TimespanType) -> 'DatetimeExpression':
        return DatetimeExpression.binary_op(self, ' - ', other)

    def between(self, lower: DatetimeType, upper: DatetimeType) -> BooleanExpression:
        return BooleanExpression(KQL('{} between ({} .. {})'.format(
            self.kql, _subexpr_to_kql(lower), _subexpr_to_kql(upper)
        )))

    def floor(self, round_to: TimespanType) -> 'DatetimeExpression':
        return DatetimeExpression(KQL('floor({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        return BaseExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: TimespanType, fixed_point: DatetimeType) -> 'BaseExpression':
        return BaseExpression(KQL('bin_at({}, {}, {})'.format(self.kql,
                                                              _subexpr_to_kql(round_to),
                                                              _subexpr_to_kql(fixed_point))))

    def bin_auto(self) -> 'BaseExpression':
        return BaseExpression(KQL('bin_auto({})'.format(self.kql)))

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
        return NumberExpression(KQL('hourofday({})'.format(self)))

    def startofday(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofday({})' if offset is None else 'startofday({}, {})').format(self.kql, offset)
        ))

    def startofmonth(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofmonth({})' if offset is None else 'startofmonth({}, {})').format(self.kql, offset)
        ))

    def startofweek(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofweek({})' if offset is None else 'startofweek({}, {})').format(self.kql, offset)
        ))

    def startofyear(self, offset: NumberType = None) -> 'DatetimeExpression':
        return DatetimeExpression(KQL(
            ('startofyear({})' if offset is None else 'startofyear({}, {})').format(self.kql, offset)
        ))


class TimespanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'TimespanExpression':
        return TimespanExpression(BaseExpression.binary_op(left, operator, right))

    def __add__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' - ', other)

    def ago(self) -> DatetimeExpression:
        return DatetimeExpression(KQL('ago({})'.format(_subexpr_to_kql(self))))

    def bin(self, round_to: TimespanType) -> 'BaseExpression':
        return BaseExpression(KQL('bin({}, {})'.format(self.kql, _subexpr_to_kql(round_to))))

    def bin_at(self, round_to: TimespanType, fixed_point: TimespanType) -> 'BaseExpression':
        return BaseExpression(KQL('bin_at({}, {}, {})'.format(self.kql,
                                                              _subexpr_to_kql(round_to),
                                                              _subexpr_to_kql(fixed_point))))

    def bin_auto(self) -> 'BaseExpression':
        return BaseExpression(KQL('bin_auto({})'.format(self.kql)))

    def format_timespan(self, format_string: StringType) -> StringExpression:
        return StringExpression(KQL('format_timespan({}, {})'.format(self.kql, _subexpr_to_kql(format_string))))

    def between(self, lower: TimespanType, upper: TimespanType) -> BooleanExpression:
        return BooleanExpression(KQL('{} between ({} .. {})'.format(
            self.kql, _subexpr_to_kql(lower), _subexpr_to_kql(upper)
        )))


class ArrayExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.array_length()

    def array_length(self) -> NumberExpression:
        return NumberExpression(KQL('array_length({})'.format(self.kql)))

    def contains(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(other, ' in ', self)

    @staticmethod
    def pack_array(*elements: ExpressionType) -> 'ArrayExpression':
        return ArrayExpression(KQL('pack_array({})'.format(
            ', '.join('{}'.format(_subexpr_to_kql(e) for e in elements))
        )))

    def __getitem__(self, index: NumberType) -> BaseExpression:
        return BaseExpression(KQL('{}[{}]'.format(self.kql, _subexpr_to_kql(index))))

    def assign_to(self, *columns: 'Column') -> 'AssignmentBase':
        if len(columns) <= 1:
            return super().assign_to(*columns)
        return AssignmentToMultipleColumns(columns, self)


class MappingExpression(BaseExpression):
    def keys(self) -> ArrayExpression:
        return ArrayExpression(KQL('bag_keys({})'.format(self.kql)))

    @staticmethod
    def pack(**kwargs: ExpressionType) -> 'MappingExpression':
        return MappingExpression(KQL('pack({})'.format(
            ', '.join('"{}", {}'.format(k, _subexpr_to_kql(v)) for k, v in kwargs)
        )))

    def __getitem__(self, index: StringType) -> BaseExpression:
        return BaseExpression(KQL('{}[{}]'.format(self.kql, _subexpr_to_kql(index))))


class AggregationExpression(BaseExpression):

    def assign_to(self, *columns: 'Column') -> 'AssignmentFromAggregationToColumn':
        if len(columns) == 0:
            # Unspecified column name
            return AssignmentFromAggregationToColumn(None, self)
        if len(columns) == 1:
            return AssignmentFromAggregationToColumn(columns[0], self)
        raise ValueError("Aggregations cannot be assigned to multiple columns")

    def as_subexpression(self) -> KQL:
        return self.kql


class BooleanAggregationExpression(AggregationExpression, BooleanExpression):
    pass


class NumberAggregationExpression(AggregationExpression, NumberExpression):
    pass


class StringAggregationExpression(AggregationExpression, StringExpression):
    pass


class DatetimeAggregationExpression(AggregationExpression, DatetimeExpression):
    pass


class TimespanAggregationExpression(AggregationExpression, TimespanExpression):
    pass


class ArrayAggregationExpression(AggregationExpression, ArrayExpression):
    pass


class MappingAggregationExpression(AggregationExpression, MappingExpression):
    pass


class AssignmentBase:
    _lvalue: Optional[KQL]
    _rvalue: KQL

    def __init__(self, lvalue: Optional[KQL], rvalue: ExpressionType) -> None:
        self._lvalue = lvalue
        self._rvalue = rvalue.as_subexpression()

    def to_kql(self) -> KQL:
        if self._lvalue is None:
            # Unspecified column name
            return self._rvalue
        return KQL('{} = {}'.format(self._lvalue, self._rvalue))

    @staticmethod
    def assign(expression: ExpressionType, *columns: 'Column') -> 'AssignmentBase':
        if len(columns) == 0:
            raise ValueError("Provide at least one column")
        if len(columns) == 1:
            return AssignmentToSingleColumn(columns[0], expression)
        return AssignmentToMultipleColumns(columns, expression)


class AssignmentToSingleColumn(AssignmentBase):
    def __init__(self, column: 'Column', expression: ExpressionType) -> None:
        super().__init__(column.kql, expression)


class AssignmentFromColumnToColumn(AssignmentToSingleColumn):
    def __init__(self, target: 'Column', source: 'Column') -> None:
        super().__init__(target, source)


class AssignmentToMultipleColumns(AssignmentBase):
    def __init__(self, columns: Union[List['Column'], Tuple['Column']], expression: ArrayType) -> None:
        super().__init__(KQL('({})'.format(', '.join(c.kql for c in columns))), expression)


class AssignmentFromAggregationToColumn(AssignmentBase):
    def __init__(self, column: Optional['Column'], aggregation: AggregationType) -> None:
        super().__init__(None if column is None else column.kql, aggregation)


class Column(
    NumberExpression, BooleanExpression, StringExpression,
    ArrayExpression, MappingExpression, DatetimeExpression,
    TimespanExpression
):
    name: str

    def __init__(self, name: str) -> None:
        super().__init__(KQL("['{}']".format(name) if '.' in name else name))
        self.name = name

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    def as_subexpression(self) -> KQL:
        return self.kql

    def __len__(self) -> NumberExpression:
        raise NotImplementedError("Column type unknown, instead use 'string_size' or 'array_length'")

    def assign_to(self, *columns: 'Column') -> 'AssignmentBase':
        if len(columns) == 0:
            return super().assign_to()
        if len(columns) == 1:
            return AssignmentFromColumnToColumn(columns[0], self)
        return ArrayExpression.assign_to(self, *columns)

    def __call__(self, *args, **kwargs):
        # Someone tried to call a non-existent method, and a column object was generated
        raise AttributeError("No such method: " + self.name.split('.')[-1])


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column(name)


# Recommended usage: from pykusto.expressions import column_generator as col
# TODO: Is there a way to enforce this to be a singleton?
column_generator = ColumnGenerator()
