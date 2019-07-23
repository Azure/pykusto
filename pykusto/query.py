from abc import abstractmethod
from enum import Enum
from typing import Dict

from pykusto.assignments import AssigmentBase
from pykusto.column import Column
from pykusto.expressions import BooleanType, BaseExpression
from pykusto.utils import KQL, logger


class Order(Enum):
    ASC = "asc"
    DESC = "desc"


class Nulls(Enum):
    FIRST = "first"
    LAST = "last"


class Query:
    head: 'Query'

    def __init__(self, head: 'Query' = None) -> None:
        self.head = head

    def where(self, predicate: BooleanType) -> 'WhereQuery':
        return WhereQuery(self, predicate)

    def take(self, num_rows: int) -> 'TakeQuery':
        return TakeQuery(self, num_rows)

    def sort_by(self, col: Column, order: Order = None, nulls: Nulls = None) -> 'SortQuery':
        return SortQuery(self, col, order, nulls)

    def project(self) -> 'Query':
        pass

    def extend(self, *args: BaseExpression, **kwargs: BaseExpression) -> 'Query':
        pass

    @abstractmethod
    def _compile(self) -> KQL:
        pass

    def _compile_all(self) -> KQL:
        if self.head is None:
            return KQL("")
        else:
            return KQL("{} | {}".format(self.head._compile_all(), self._compile()))

    def render(self) -> KQL:
        result = self._compile_all()
        logger.debug("Complied query: " + result)
        return result


class ExtendQuery(Query):
    extend_spec: Dict[Column, BaseExpression]

    def __init__(self, head: 'Query', *args: AssigmentBase) -> None:
        super().__init__(head)


class WhereQuery(Query):
    predicate: BooleanType

    def __init__(self, head: Query, predicate: BooleanType):
        super(WhereQuery, self).__init__(head)
        self.predicate = predicate

    def _compile(self):
        return 'where {}'.format(self.predicate.kql)


class TakeQuery(Query):
    num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(TakeQuery, self).__init__(head)
        self.num_rows = num_rows

    def _compile(self):
        return 'take {}'.format(self.num_rows)


class SortQuery(Query):
    col: Column
    order: Order
    nulls: Nulls

    def __init__(self, head: Query, col: Column, order: Order, nulls: Nulls):
        super(SortQuery, self).__init__(head)
        self.col = col
        self.order = order
        self.nulls = nulls

    def _compile(self):
        result = 'sort by {}'.format(self.col.kql, self.order.value)
        if self.order is not None:
            result += " " + str(self.order.value)
        if self.nulls is not None:
            result += " nulls " + str(self.nulls.value)
        return result
