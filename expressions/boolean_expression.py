from typing import Union

from expressions.base_expression import BaseExpression
from utils import KQL

BooleanExpressionTypes = Union[bool, 'BooleanExpression', 'Column']


class BooleanExpression(BaseExpression):
    def __init__(self, left: 'BooleanExpressionTypes', operator: str, right: 'BooleanExpressionTypes') -> None:
        super().__init__(
            KQL('{}{}{}'.format(self._subexpression_to_kql(left), operator, self._subexpression_to_kql(right)))
        )

    def __and__(self, other: BooleanExpressionTypes) -> 'BooleanExpression':
        return BooleanExpressionTypes(self, ' and ', other)
