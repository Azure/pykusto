from typing import Union, Any

from expressions.boolean_expression import BooleanExpression
from utils import KQL, KustoTypes, to_kql

ExpressionTypes = Union[KustoTypes, 'BaseExpression', 'Column']


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

    def __eq__(self, other: ExpressionTypes) -> BooleanExpression:
        return BooleanExpression(self, ' == ', other)

    def __ne__(self, other: ExpressionTypes) -> BooleanExpression:
        return BooleanExpression(self, ' != ', other)

    def is_in(self, other: ExpressionTypes) -> BooleanExpression:
        return BooleanExpression(self, ' in ', other)

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError()