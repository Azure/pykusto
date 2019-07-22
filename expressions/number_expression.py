from typing import Union

from expressions.base_expression import BaseExpression
from expressions.boolean_expression import BooleanExpression
from utils import KQL

NumberExpressionTypes = Union[int, float, 'NumberExpression', 'Column']


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
