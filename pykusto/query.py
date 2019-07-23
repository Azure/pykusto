from abc import abstractmethod
from enum import Enum
from typing import Dict
from typing import Tuple

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
    _head: 'Query'

    def __init__(self, head: 'Query' = None) -> None:
        self._head = head

    def where(self, predicate: BooleanType) -> 'WhereQuery':
        return WhereQuery(self, predicate)

    def take(self, num_rows: int) -> 'TakeQuery':
        return TakeQuery(self, num_rows)

    def sort_by(self, col: Column, order: Order = None, nulls: Nulls = None) -> 'SortQuery':
        return SortQuery(self, col, order, nulls)

    def join(self, query: 'Query', kind: JoinKind = None):
        return JoinQuery(self, query, kind)

    def project(self) -> 'Query':
        pass

    def extend(self, *args: BaseExpression, **kwargs: BaseExpression) -> 'Query':
        pass

    @abstractmethod
    def _compile(self) -> KQL:
        pass

    def _compile_all(self) -> KQL:
        if self._head is None:
            return KQL("")
        else:
            return KQL("{} | {}".format(self._head._compile_all(), self._compile()))

    def render(self) -> KQL:
        result = self._compile_all()
        logger.debug("Complied query: " + result)
        return result


class ExtendQuery(Query):
    extend_spec: Dict[Column, BaseExpression]

    def __init__(self, head: 'Query', *args: AssigmentBase) -> None:
        super().__init__(head)


class WhereQuery(Query):
    _predicate: BooleanType

    def __init__(self, head: Query, predicate: BooleanType):
        super(WhereQuery, self).__init__(head)
        self._predicate = predicate

    def _compile(self):
        return 'where {}'.format(self._predicate.kql)


class TakeQuery(Query):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(TakeQuery, self).__init__(head)
        self._num_rows = num_rows

    def _compile(self):
        return 'take {}'.format(self._num_rows)


class SortQuery(Query):
    _col: Column
    _order: Order
    _nulls: Nulls

    def __init__(self, head: Query, col: Column, order: Order, nulls: Nulls):
        super(SortQuery, self).__init__(head)
        self._col = col
        self._order = order
        self._nulls = nulls

    def _compile(self):
        result = 'sort by {}'.format(self._col.kql, self._order.value)
        if self._order is not None:
            result += " " + str(self._order.value)
        if self._nulls is not None:
            result += " nulls " + str(self._nulls.value)
        return result


class JoinQuery(Query):
    _query: Query
    _kind: JoinKind
    _on_attributes: Tuple[Tuple[Column, ...], ...]

    def __init__(self, head: Query, query: Query, kind: JoinKind,
                 on_attributes: Tuple[Tuple[Column, ...], ...] = tuple()):
        super(JoinQuery, self).__init__(head)
        self._query = query
        self._kind = kind
        self._on_attributes = on_attributes

    def on(self, col1: Column, col2: Column = None) -> 'JoinQuery':
        self._on_attributes = self._on_attributes + (((col1,),) if col2 is None else ((col1, col2),))
        return self

    @staticmethod
    def _compile_on_attribute(attribute: Tuple[Column]):
        assert len(attribute) in (1, 2)
        if len(attribute) == 1:
            return attribute[0].kql
        else:
            return "$left.{}==$right.{}".format(attribute[0].kql, attribute[1].kql)

    def _compile(self) -> KQL:
        assert self._on_attributes, "A call to join() must be followed by a call to on()"
        return KQL("join {} ({}) on {}".format(
            "" if self._kind is None else "kind={}".format(self._kind.value),
            self._query.render(),
            ", ".join([self._compile_on_attribute(attr) for attr in self._on_attributes])))
