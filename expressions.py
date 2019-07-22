from typing import Any
from typing import Union

from utils import KQL
from utils import KustoTypes, to_kql

StringExpressionTypes = Union[str, 'StringExpression', 'Column']
ExpressionTypes = Union[KustoTypes, 'BaseExpression', 'Column']
BooleanExpressionTypes = Union[bool, 'BooleanExpression', 'Column']
NumberExpressionTypes = Union[int, float, 'NumberExpression', 'Column']


class BaseExpression:
    kql: KQL

    def __init__(self, kql: KQL) -> None:
        self.kql = kql

    def as_subexpression(self) -> KQL:
        return KQL('({})'.format(self.kql))

    @staticmethod
    def _subexpression_to_kql(obj: ExpressionTypes) -> KQL:
        if isinstance(obj, BaseExpression):
            return obj.as_subexpression()
        return to_kql(obj)

    def __eq__(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression(self, ' == ', other)

    def __ne__(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression(self, ' != ', other)

    def is_in(self, other: ExpressionTypes) -> 'BooleanExpression':
        return BooleanExpression(self, ' in ', other)

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError()


class BooleanExpression(BaseExpression):
    def __init__(self, left: 'BooleanExpressionTypes', operator: str, right: 'BooleanExpressionTypes') -> None:
        super().__init__(
            KQL('{}{}{}'.format(self._subexpression_to_kql(left), operator, self._subexpression_to_kql(right)))
        )

    def __and__(self, other: BooleanExpressionTypes) -> 'BooleanExpression':
        return BooleanExpressionTypes(self, ' and ', other)


class NumberExpression(BaseExpression):
    def __init__(self, left: 'NumberExpression', operator: str, right: 'NumberExpression') -> None:
        super().__init__(
            KQL('{}{}{}'.format(self._subexpression_to_kql(left), operator, self._subexpression_to_kql(right)))
        )

    def __lt__(self, other: 'NumberExpression') -> BooleanExpression:
        return BooleanExpression(self, ' < ', other)

    def __le__(self, other: 'NumberExpression') -> BooleanExpression:
        return BooleanExpression(self, ' <= ', other)

    def __gt__(self, other: 'NumberExpression') -> BooleanExpression:
        return BooleanExpression(self, ' > ', other)

    def __ge__(self, other: 'NumberExpression') -> BooleanExpression:
        return BooleanExpression(self, ' >= ', other)

    def __add__(self, other: 'NumberExpression') -> 'NumberExpression':
        return NumberExpression(self, ' + ', other)

    def __sub__(self, other: 'NumberExpression') -> 'NumberExpression':
        return NumberExpression(self, ' - ', other)

    def __mul__(self, other: 'NumberExpression') -> 'NumberExpression':
        return NumberExpression(self, ' * ', other)

    def __truediv__(self, other: 'NumberExpression') -> 'NumberExpression':
        return NumberExpression(self, ' / ', other)

    def __mod__(self, other: 'NumberExpression') -> 'NumberExpression':
        return NumberExpression(self, ' % ', other)


class StringExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))
