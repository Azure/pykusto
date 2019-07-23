from typing import Union, List, Tuple

from pykusto.column import Column
from pykusto.expressions import ArrayType, ExpressionType, AggregationType
from pykusto.utils import KQL


class AssigmentBase:
    _lvalue: KQL
    _rvalue: KQL

    def __init__(self, lvalue: KQL, rvalue: ExpressionType) -> None:
        self._lvalue = lvalue
        self._rvalue = rvalue.as_subexpression()

    def to_kql(self) -> KQL:
        return KQL('{} = {}'.format(self._lvalue, self._rvalue))

    @staticmethod
    def assign(expression: ExpressionType, *columns: 'Column') -> 'AssigmentBase':
        if len(columns) == 0:
            raise ValueError("Provide at least one column")
        if len(columns) == 1:
            return AssignmentToSingleColumn(columns[0], expression)
        return AssignmentToMultipleColumns(columns, expression)


class AssignmentToSingleColumn(AssigmentBase):
    def __init__(self, column: Column, expression: ExpressionType) -> None:
        super().__init__(column.kql, expression)


class AssignmentToMultipleColumns(AssigmentBase):
    def __init__(self, columns: Union[List[Column], Tuple[Column]], expression: ArrayType) -> None:
        super().__init__(KQL('({})'.format(', '.join(c.kql for c in columns))), expression)


class AssignmentFromAggregationToColumn(AssigmentBase):
    def __init__(self, column: Column, aggregation: AggregationType) -> None:
        super().__init__(column.kql, aggregation)
