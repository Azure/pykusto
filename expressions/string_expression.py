from typing import Union

from expressions.base_expression import BaseExpression
from expressions.number_expression import NumberExpression
from utils import KQL

StringExpressionTypes = Union[str, 'StringExpression', 'Column']


class StringExpression(BaseExpression):
    def __len__(self) -> NumberExpression:
        return NumberExpression(KQL('string_size({})'.format(self.kql)))
