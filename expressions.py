from typing import Any, Sequence
from typing import Union

from utils import KQL
from utils import KustoTypes, to_kql

ExpressionTypes = Union[KustoTypes, 'BaseExpression', 'Column']
StringTypes = Union[str, 'StringExpression', 'Column']
BooleanTypes = Union[bool, 'BooleanExpression', 'Column']
NumberTypes = Union[int, float, 'NumberExpression', 'Column']
ArrayTypes = Union[Sequence, 'ArrayExpression', 'Column']


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
    def _subexpression_to_kql(obj: ExpressionTypes) -> KQL:
        if isinstance(obj, BaseExpression):
            return obj.as_subexpression()
        return to_kql(obj)

    def __eq__(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(self, ' == ', other)

    def __ne__(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(self, ' != ', other)

    def is_in(self, other: ArrayTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(self, ' in ', other)

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError()


class BooleanExpression(BaseExpression):
    @staticmethod
    def bi_operator(left: ExpressionTypes, operator: str, right: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression(
            KQL('{}{}{}'.format(
                BaseExpression._subexpression_to_kql(left), operator, BaseExpression._subexpression_to_kql(right))
                ))

    def __and__(self, other: BooleanTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(self, ' and ', other)

    def __or__(self, other: BooleanTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(self, ' or ', other)

    def __invert__(self) -> 'BooleanExpression':
        return BooleanExpression(KQL('not({})'.format(self.kql)))


class NumberExpression(BaseExpression):
    @staticmethod
    def bi_operator(left: NumberTypes, operator: str, right: NumberTypes) -> 'NumberExpression':
        return NumberExpression(
            KQL('{}{}{}'.format(
                BaseExpression._subexpression_to_kql(left), operator, BaseExpression._subexpression_to_kql(right))
                ))

    def __lt__(self, other: NumberTypes) -> BooleanExpression:
        return BooleanExpression.bi_operator(self, ' < ', other)

    def __le__(self, other: NumberTypes) -> BooleanExpression:
        return BooleanExpression.bi_operator(self, ' <= ', other)

    def __gt__(self, other: NumberTypes) -> BooleanExpression:
        return BooleanExpression.bi_operator(self, ' > ', other)

    def __ge__(self, other: NumberTypes) -> BooleanExpression:
        return BooleanExpression.bi_operator(self, ' >= ', other)

    def __add__(self, other: NumberTypes) -> 'NumberExpression':
        return NumberExpression.bi_operator(self, ' + ', other)

    def __sub__(self, other: NumberTypes) -> 'NumberExpression':
        return NumberExpression.bi_operator(self, ' - ', other)

    def __mul__(self, other: NumberTypes) -> 'NumberExpression':
        return NumberExpression.bi_operator(self, ' * ', other)

    def __truediv__(self, other: NumberTypes) -> 'NumberExpression':
        return NumberExpression.bi_operator(self, ' / ', other)

    def __mod__(self, other: NumberTypes) -> 'NumberExpression':
        return NumberExpression.bi_operator(self, ' % ', other)

    def __neg__(self) -> 'NumberExpression':
        return NumberExpression(KQL('-{}'.format(self.kql)))

    def __abs__(self) -> 'NumberExpression':
        return NumberExpression(KQL('abs({})'.format(self.kql)))


class StringExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.string_size()

    def string_size(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))

    @staticmethod
    def concat(*args: StringTypes) -> 'StringExpression':
        return StringExpression(KQL('strcat({})'.format(', '.join('"{}"'.format(s) for s in args))))


class ArrayExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return self.array_length()

    def array_length(self) -> NumberExpression:
        return NumberExpression(KQL('array_length({})'.format(self.kql)))

    def contains(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression.bi_operator(other, ' in ', self)


class MappingExpression(BaseExpression):
    def keys(self) -> ArrayExpression:
        return ArrayExpression(KQL('bag_keys({})'.format(self.kql)))
