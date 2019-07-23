from datetime import datetime, timedelta
from typing import Any, Sequence
from typing import Union

from utils import KQL
from utils import KustoTypes, to_kql

ExpressionType = Union[KustoTypes, 'BaseExpression']
StringType = Union[str, 'StringExpression']
BooleanType = Union[bool, 'BooleanExpression']
NumberType = Union[int, float, 'NumberExpression']
ArrayType = Union[Sequence, 'ArrayExpression']
DatetimeType = Union[datetime, 'DatetimeExpression']
TimespanType = Union[timedelta, 'TimespanExpression']


# All classes in the same file to prevent circular dependencies


class BaseExpression:
    kql: KQL

    def __init__(self, kql: KQL) -> None:
        self.kql = kql

    def __str__(self) -> str:
        return self.kql

    def as_subexpression(self) -> KQL:
        return KQL('({})'.format(self.kql))

    @staticmethod
    def _subexpression_to_kql(obj: ExpressionType) -> KQL:
        if isinstance(obj, BaseExpression):
            return obj.as_subexpression()
        return to_kql(obj)

    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> KQL:
        return KQL('{}{}{}'.format(
            BaseExpression._subexpression_to_kql(left), operator, BaseExpression._subexpression_to_kql(right))
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
        return BooleanExpression(KQL('{} between ({} .. {})'.format(self.kql, lower, upper)))


class StringExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.string_size()

    def string_size(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))

    def is_empty(self) -> BooleanExpression:
        return BooleanExpression(KQL('isempty({})'.format(self.kql)))

    @staticmethod
    def concat(*args: StringType) -> 'StringExpression':
        return StringExpression(KQL('strcat({})'.format(', '.join('{}'.format(
            BaseExpression._subexpression_to_kql(s)
        ) for s in args))))

    def split(self, delimiter: StringType, requested_index: NumberType = None) -> 'ArrayExpression':
        if requested_index is None:
            return ArrayExpression(KQL('split({}, {}'.format(self.kql, delimiter)))
        return ArrayExpression(KQL('split({}, {}, {}'.format(self.kql, delimiter, requested_index)))

    def equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' == ' if case_sensitive else ' =~ ', other)

    def not_equals(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, ' !=' if case_sensitive else ' !~ ', other)

    def matches(self, regex: StringType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(self, ' matches regex ', regex)

    def contains(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, 'contains_cs' if case_sensitive else 'contains', other)

    def startswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, 'startswith_cs' if case_sensitive else 'startswith', other)

    def endswith(self, other: StringType, case_sensitive: bool = False) -> BooleanExpression:
        return BooleanExpression.binary_op(self, 'endswith_cs' if case_sensitive else 'endswith', other)

    def to_int(self) -> NumberExpression:
        return NumberExpression(KQL('toint({})'.format(self.kql)))

    def to_long(self) -> NumberExpression:
        return NumberExpression(KQL('tolong({})'.format(self.kql)))

    def lower(self) -> 'StringExpression':
        return StringExpression(KQL('tolower({})'.format(self.kql)))

    def upper(self) -> 'StringExpression':
        return StringExpression(KQL('toupper({})'.format(self.kql)))


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
        return BooleanExpression(KQL('{} between ({} .. {})'.format(self.kql, lower, upper)))


class TimespanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: ExpressionType, operator: str, right: ExpressionType) -> 'TimespanExpression':
        return TimespanExpression(BaseExpression.binary_op(left, operator, right))

    def __add__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: TimespanType) -> 'TimespanExpression':
        return TimespanExpression.binary_op(self, ' - ', other)


class ArrayExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.array_length()

    def array_length(self) -> NumberExpression:
        return NumberExpression(KQL('array_length({})'.format(self.kql)))

    def contains(self, other: ExpressionType) -> 'BooleanExpression':
        return BooleanExpression.binary_op(other, ' in ', self)

    @staticmethod
    def pack_array(*args: ExpressionType) -> 'ArrayExpression':
        return ArrayExpression(KQL('pack_array({})'.format(
            ', '.join('{}'.format(BaseExpression._subexpression_to_kql(e) for e in args))
        )))


class MappingExpression(BaseExpression):
    def keys(self) -> ArrayExpression:
        return ArrayExpression(KQL('bag_keys({})'.format(self.kql)))

    @staticmethod
    def pack(**kwargs: ExpressionType) -> 'MappingExpression':
        return MappingExpression(KQL('pack({})'.format(
            ', '.join('"{}", {}'.format(k, BaseExpression._subexpression_to_kql(v)) for k, v in kwargs)
        )))
