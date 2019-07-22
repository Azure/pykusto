from abc import abstractmethod
from enum import Enum

from column import Column
from expressions import BooleanType
from utils import KQL


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

    def where(self, predicate: BooleanType) -> 'Query':
        return WhereQuery(self, predicate)

    def take(self, num_rows: int):
        return TakeQuery(self, num_rows)

    def sort_by(self, col: Column, order: Order = None, nulls: Nulls = None):
        return SortQuery(self, col, order, nulls)

    def project(self) -> 'Query':
        pass

    @abstractmethod
    def compile(self) -> KQL:
        pass

    def compile_all(self) -> KQL:
        if self.head is None:
            return KQL("")
        else:
            return KQL("{} | {}".format(self.head.compile_all(), self.compile()))


class WhereQuery(Query):
    predicate: BooleanType

    def __init__(self, head: Query, predicate: BooleanType):
        super(WhereQuery, self).__init__(head)
        self.predicate = predicate

    def compile(self):
        return 'where {}'.format(self.predicate.kql)


class TakeQuery(Query):
    num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(TakeQuery, self).__init__(head)
        self.num_rows = num_rows

    def compile(self):
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

    def compile(self):
        result = 'sort by {}'.format(self.col.kql, self.order.value)
        if self.order is not None:
            result += " " + str(self.order.value)
        if self.nulls is not None:
            result += " nulls " + str(self.nulls.value)
        return result
