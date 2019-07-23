from typing import Tuple
from abc import abstractmethod
from enum import Enum

from pykusto.column import Column
from pykusto.expressions import BooleanType
from pykusto.utils import KQL, logger


class Order(Enum):
    ASC = "asc"
    DESC = "desc"


class Nulls(Enum):
    FIRST = "first"
    LAST = "last"


class JoinKind(Enum):
    INNERUNIQUE = "innerunique"
    INNER = "inner"
    LEFTOUTER = "leftouter"
    RIGHTOUTER = "rightouter"
    FULLOUTER = "fullouter"
    LEFTANTI = "leftanti"
    ANTI = "anti"
    LEFTANTISEMI = "leftantisemi"
    RIGHTANTI = "rightanti"
    RIGHTANTISEMI = "rightantisemi"
    LEFTSEMI = "leftsemi"
    RIGHTSEMI = "rightsemi"


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

    def join(self, query: 'Query', kind: JoinKind = None):
        return JoinQuery(self, query, kind)

    def project(self) -> 'Query':
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


class JoinQuery(Query):
    query: Query
    kind: JoinKind
    on_attributes: Tuple[Tuple[Column, ...], ...]

    def __init__(self, head: Query, query: Query, kind: JoinKind,
                 on_attributes: Tuple[Tuple[Column, ...], ...] = tuple()):
        super(JoinQuery, self).__init__(head)
        self.query = query
        self.kind = kind
        self.on_attributes = on_attributes

    def on(self, col1: Column, col2: Column = None) -> 'JoinQuery':
        self.on_attributes = self.on_attributes + (((col1,),) if col2 is None else ((col1, col2),))
        return self

    @staticmethod
    def _compile_on_attribute(attribute: Tuple[Column]):
        assert len(attribute) in (1, 2)
        if len(attribute) == 1:
            return attribute[0].kql
        else:
            return "$left.{}==$right.{}".format(attribute[0].kql, attribute[1].kql)

    def _compile(self) -> KQL:
        assert self.on_attributes, "A call to join() must be followed by a call to on()"
        return KQL("join {} ({}) on {}".format(
            "" if self.kind is None else "kind={}".format(self.kind.value),
            self.query.render(),
            ", ".join([self._compile_on_attribute(attr) for attr in self.on_attributes])))
