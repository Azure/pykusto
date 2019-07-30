from abc import abstractmethod
from enum import Enum
from itertools import chain
from types import FunctionType
from typing import Tuple, List, Union, Optional

from azure.kusto.data.helpers import dataframe_from_result_table

from pykusto.client import Table
from pykusto.expressions import BooleanType, ExpressionType, AggregationExpression, OrderType, \
    StringType, AssignmentBase, AssignmentFromAggregationToColumn, AssignmentToSingleColumn, Column, BaseExpression, \
    AssignmentFromColumnToColumn
from pykusto.udf import stringify_python_func
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


class BagExpansion(Enum):
    BAG = "bag"
    ARRAY = "array"


class Query:
    _head: Optional['Query']
    _table: Optional[Table]

    def __init__(self, head=None) -> None:
        self._head = head if isinstance(head, Query) else None
        self._table = head if isinstance(head, Table) else None

    def __add__(self, other: 'Query'):
        other._head = self
        return other

    def where(self, predicate: BooleanType) -> 'WhereQuery':
        return WhereQuery(self, predicate)

    def take(self, num_rows: int) -> 'TakeQuery':
        return TakeQuery(self, num_rows)

    def limit(self, num_rows: int) -> 'LimitQuery':
        return LimitQuery(self, num_rows)

    def sample(self, num_rows: int) -> 'SampleQuery':
        return SampleQuery(self, num_rows)

    def count(self) -> 'CountQuery':
        return CountQuery(self)

    def sort_by(self, col: OrderType, order: Order = None, nulls: Nulls = None) -> 'SortQuery':
        return SortQuery(self, col, order, nulls)

    def order_by(self, col: OrderType, order: Order = None, nulls: Nulls = None) -> 'OrderQuery':
        return OrderQuery(self, col, order, nulls)

    def top(self, num_rows: int, col: Column, order: Order = None, nulls: Nulls = None) -> 'TopQuery':
        return TopQuery(self, num_rows, col, order, nulls)

    def join(self, query: 'Query', kind: JoinKind = None):
        return JoinQuery(self, query, kind)

    def project(self, *args: Union[Column, AssignmentBase, BaseExpression], **kwargs: ExpressionType) -> 'ProjectQuery':
        columns: List[Column] = []
        assignments: List[AssignmentBase] = []
        for arg in args:
            if isinstance(arg, Column):
                columns.append(arg)
            elif isinstance(arg, AssignmentBase):
                assignments.append(arg)
            elif isinstance(arg, BaseExpression):
                assignments.append(arg.assign_to())
            else:
                raise ValueError("Invalid assignment: " + arg.to_kql())
        for column_name, expression in kwargs.items():
            assignments.append(AssignmentToSingleColumn(Column(column_name), expression))
        return ProjectQuery(self, columns, assignments)

    def project_rename(self, *args: AssignmentFromColumnToColumn, **kwargs: Column) -> 'ProjectRenameQuery':
        assignments: List[AssignmentFromColumnToColumn] = list(args)
        for column_name, column in kwargs.items():
            assignments.append(AssignmentFromColumnToColumn(Column(column_name), column))
        return ProjectRenameQuery(self, assignments)

    def project_away(self, *columns: StringType):
        return ProjectAwayQuery(self, columns)

    def distinct(self, *columns: BaseExpression):
        return DistinctQuery(self, columns)

    def distinct_all(self):
        return DistinctQuery(self, (BaseExpression(KQL("*")),))

    def extend(self, *args: Union[BaseExpression, AssignmentBase], **kwargs: ExpressionType) -> 'ExtendQuery':
        assignments: List[AssignmentBase] = []
        for arg in args:
            if isinstance(arg, BaseExpression):
                assignments.append(arg.assign_to())
            else:
                assignments.append(arg)
        for column_name, expression in kwargs.items():
            assignments.append(expression.assign_to(Column(column_name)))
        return ExtendQuery(self, *assignments)

    def summarize(self, *args: Union[AggregationExpression, AssignmentFromAggregationToColumn],
                  **kwargs: AggregationExpression) -> 'SummarizeQuery':
        assignments: List[AssignmentFromAggregationToColumn] = []
        for arg in args:
            if isinstance(arg, AggregationExpression):
                assignments.append(arg.assign_to())
            elif isinstance(arg, AssignmentFromAggregationToColumn):
                assignments.append(arg)
            else:
                raise ValueError("Invalid assignment: " + str(arg))
        for column_name, agg in kwargs.items():
            assignments.append(AssignmentFromAggregationToColumn(Column(column_name), agg))
        return SummarizeQuery(self, assignments)

    def mv_expand(self, *columns: Column, bag_expansion: BagExpansion = None, with_item_index: Column = None,
                  limit: int = None):
        if len(columns) == 0:
            raise ValueError("Please specify one or more columns for mv-expand")
        return MvExpandQuery(self, columns, bag_expansion, with_item_index, limit)

    def custom(self, custom_query: str):
        return CustomQuery(self, custom_query)

    # TODO convert python types to kusto types
    def evaluate(self, udf: FunctionType, type_spec_str: str):
        return EvaluatePythonQuery(self, udf, type_spec_str)

    @abstractmethod
    def _compile(self) -> KQL:
        pass

    def _compile_all(self) -> KQL:
        if self._head is None:
            if self._table is None:
                return KQL("")
            else:
                return self._table.table
        else:
            return KQL("{} | {}".format(self._head._compile_all(), self._compile()))

    def get_table(self) -> Table:
        if self._head is None:
            return self._table
        else:
            return self._head.get_table()

    def render(self) -> KQL:
        result = self._compile_all()
        logger.debug("Complied query: " + result)
        return result

    def execute(self, table: Table = None):
        if self.get_table() is None:
            if table is None:
                raise RuntimeError("No table supplied")
            rendered_query = table.table + self.render()
        else:
            if table is not None:
                raise RuntimeError("This table is already bound to a query")
            table = self.get_table()
            rendered_query = self.render()

        logger.debug("Running query: " + rendered_query)
        return table.execute(rendered_query)

    def to_dataframe(self, table: Table = None):
        res = self.execute(table)
        return dataframe_from_result_table(res.primary_results[0])


class ProjectQuery(Query):
    _columns: List[Column]
    _assignments: List[AssignmentBase]

    def __init__(self, head: 'Query', columns: List[Column], assignments: List[AssignmentBase]) -> None:
        super().__init__(head)
        self._columns = columns
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL('project {}'.format(', '.join(chain(
            (c.kql for c in self._columns),
            (a.to_kql() for a in self._assignments)
        ))))


class ProjectRenameQuery(Query):
    _assignments: List[AssignmentBase]

    def __init__(self, head: 'Query', assignments: List[AssignmentFromColumnToColumn]) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL('project-rename {}'.format(', '.join(a.to_kql() for a in self._assignments)))


class ProjectAwayQuery(Query):
    _columns: Tuple[StringType, ...]

    def __init__(self, head: 'Query', columns: Tuple[StringType]) -> None:
        super().__init__(head)
        self._columns = columns

    def _compile(self) -> KQL:
        def col_or_wildcard_to_string(col_or_wildcard):
            return col_or_wildcard.kql if isinstance(col_or_wildcard, Column) else col_or_wildcard

        return KQL('project-away {}'.format(', '.join((col_or_wildcard_to_string(c) for c in self._columns))))


class DistinctQuery(Query):
    _columns: Tuple[BaseExpression, ...]

    def __init__(self, head: 'Query', columns: Tuple[BaseExpression]) -> None:
        super().__init__(head)
        self._columns = columns

    def _compile(self) -> KQL:
        return KQL('distinct {}'.format(', '.join((c.kql for c in self._columns))))


class ExtendQuery(Query):
    _assignments: Tuple[AssignmentBase, ...]

    def __init__(self, head: 'Query', *assignments: AssignmentBase) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL('extend {}'.format(', '.join(a.to_kql() for a in self._assignments)))


class WhereQuery(Query):
    _predicate: BooleanType

    def __init__(self, head: Query, predicate: BooleanType):
        super(WhereQuery, self).__init__(head)
        self._predicate = predicate

    def _compile(self) -> KQL:
        return KQL('where {}'.format(self._predicate.kql))


class _SingleNumberQuery(Query):
    _num_rows: int
    _query_name: str

    def __init__(self, head: Query, query_name: str, num_rows: int):
        super(_SingleNumberQuery, self).__init__(head)
        self._query_name = query_name
        self._num_rows = num_rows

    def _compile(self) -> KQL:
        return KQL('{} {}'.format(self._query_name, self._num_rows))


class TakeQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(TakeQuery, self).__init__(head, 'take', num_rows)


class LimitQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(LimitQuery, self).__init__(head, 'limit', num_rows)


class SampleQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(SampleQuery, self).__init__(head, 'sample', num_rows)


class CountQuery(Query):
    _num_rows: int

    def __init__(self, head: Query):
        super(CountQuery, self).__init__(head)

    def _compile(self) -> KQL:
        return KQL('count')


class _OrderQueryBase(Query):
    class OrderSpec:
        col: OrderType
        order: Order
        nulls: Nulls

        def __init__(self, col: OrderType, order: Order, nulls: Nulls):
            self.col = col
            self.order = order
            self.nulls = nulls

    _query_name: str
    _order_specs: List[OrderSpec]

    def __init__(self, head: Query, query_name: str, col: OrderType, order: Order, nulls: Nulls):
        super(_OrderQueryBase, self).__init__(head)
        self._query_name = query_name
        self._order_specs = []
        self.then_by(col, order, nulls)

    def then_by(self, col: OrderType, order: Order, nulls: Nulls):
        self._order_specs.append(_OrderQueryBase.OrderSpec(col, order, nulls))
        return self

    @staticmethod
    def _compile_order_spec(order_spec):
        res = str(order_spec.col.kql)
        if order_spec.order is not None:
            res += " " + str(order_spec.order.value)
        if order_spec.nulls is not None:
            res += " nulls " + str(order_spec.nulls.value)
        return res

    def _compile(self) -> KQL:
        return KQL(
            '{} by {}'.format(self._query_name,
                              ", ".join([self._compile_order_spec(order_spec) for order_spec in self._order_specs])))


class SortQuery(_OrderQueryBase):
    def __init__(self, head: Query, col: OrderType, order: Order, nulls: Nulls):
        super(SortQuery, self).__init__(head, "sort", col, order, nulls)


class OrderQuery(_OrderQueryBase):
    def __init__(self, head: Query, col: OrderType, order: Order, nulls: Nulls):
        super(OrderQuery, self).__init__(head, "order", col, order, nulls)


class TopQuery(Query):
    _num_rows: int
    _order_spec: OrderQuery.OrderSpec

    def __init__(self, head: Query, num_rows: int, col: Column, order: Order, nulls: Nulls):
        super(TopQuery, self).__init__(head)
        self._num_rows = num_rows
        self._order_spec = OrderQuery.OrderSpec(col, order, nulls)

    def _compile(self) -> KQL:
        # noinspection PyProtectedMember
        return KQL('top {} by {}'.format(self._num_rows, SortQuery._compile_order_spec(self._order_spec)))


class JoinException(Exception):
    pass


class JoinQuery(Query):
    _joined_query: Query
    _kind: JoinKind
    _on_attributes: Tuple[Tuple[Column, ...], ...]

    def __init__(self, head: Query, joined_query: Query, kind: JoinKind,
                 on_attributes: Tuple[Tuple[Column, ...], ...] = tuple()):
        super(JoinQuery, self).__init__(head)
        self._joined_query = joined_query
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
        if len(self._on_attributes) == 0:
            raise JoinException("A call to join() must be followed by a call to on()")
        if self._joined_query.get_table() is None:
            raise JoinException("The joined query must have a table")

        return KQL("join {} ({}) on {}".format(
            "" if self._kind is None else "kind={}".format(self._kind.value),
            self._joined_query.render(),
            ", ".join([self._compile_on_attribute(attr) for attr in self._on_attributes])))


class SummarizeQuery(Query):
    _assignments: List[AssignmentFromAggregationToColumn]
    _by_columns: List[Union[Column, BaseExpression]]
    _by_assignments: List[AssignmentToSingleColumn]

    def __init__(self, head: Query,
                 assignments: List[AssignmentFromAggregationToColumn]):
        super(SummarizeQuery, self).__init__(head)
        self._assignments = assignments
        self._by_columns = []
        self._by_assignments = []

    def by(self, *args: Union[AssignmentToSingleColumn, Column, BaseExpression],
           **kwargs: BaseExpression):
        for arg in args:
            if isinstance(arg, Column) or isinstance(arg, BaseExpression):
                self._by_columns.append(arg)
            elif isinstance(arg, AssignmentToSingleColumn):
                self._by_assignments.append(arg)
            else:
                raise ValueError("Invalid assignment: " + arg.to_kql())
        for column_name, group_exp in kwargs.items():
            self._by_assignments.append(AssignmentToSingleColumn(Column(column_name), group_exp))
        return self

    def _compile(self) -> KQL:
        result = 'summarize {}'.format(', '.join(a.to_kql() for a in self._assignments))
        if len(self._by_assignments) != 0 or len(self._by_columns) != 0:
            result += ' by {}'.format(', '.join(chain(
                (c.kql for c in self._by_columns),
                (a.to_kql() for a in self._by_assignments)
            )))
        return KQL(result)


class MvExpandQuery(Query):
    _columns: Tuple[Column]
    _bag_expansion: BagExpansion
    _with_item_index: Column
    _limit: int

    def __init__(self, head: Query, columns: Tuple[Column], bag_expansion: BagExpansion, with_item_index: Column,
                 limit: int):
        super(MvExpandQuery, self).__init__(head)
        self._columns = columns
        self._bag_expansion = bag_expansion
        self._with_item_index = with_item_index
        self._limit = limit

    def _compile(self) -> KQL:
        res = "mv-expand "
        if self._bag_expansion is not None:
            res += "bagexpansion={} ".format(self._bag_expansion.value)
        if self._with_item_index is not None:
            res += "with_itemindex={} ".format(self._with_item_index.kql)
        res += ", ".join([c.kql for c in self._columns])
        if self._limit:
            res += " limit {}".format(self._limit)
        return KQL(res)


class CustomQuery(Query):
    _custom_query: str

    def __init__(self, head: Query, custom_query: str):
        super(CustomQuery, self).__init__(head)
        self._custom_query = custom_query

    def _compile(self) -> KQL:
        return KQL(self._custom_query)


class EvaluatePythonQuery(Query):
    _udf: FunctionType
    _type_specs: str

    def __init__(self, head: Query, udf: FunctionType, type_specs: str):
        super(EvaluatePythonQuery, self).__init__(head)
        self._udf = udf
        self._type_specs = type_specs

    def _compile(self) -> KQL:
        return KQL('evaluate python({},"{}")'.format(
            self._type_specs,
            stringify_python_func(self._udf)
        ))
