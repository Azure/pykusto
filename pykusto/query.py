from abc import abstractmethod
from copy import copy, deepcopy
from itertools import chain
from types import FunctionType
from typing import Tuple, List, Union, Optional

from pykusto.client import Table, KustoResponse
from pykusto.enums import Order, Nulls, JoinKind, Distribution, BagExpansion
from pykusto.expressions import BooleanType, ExpressionType, AggregationExpression, OrderedType, \
    StringType, AssignmentBase, AssignmentFromAggregationToColumn, AssignmentToSingleColumn, AnyTypeColumn, \
    BaseExpression, \
    AssignmentFromColumnToColumn, AnyExpression, to_kql, expression_to_type, BaseColumn, NumberType
from pykusto.kql_converters import KQL
from pykusto.logger import logger
from pykusto.type_utils import KustoType, typed_column, plain_expression
from pykusto.udf import stringify_python_func


class Query:
    _head: Optional['Query']
    _table: Optional[Table]

    def __init__(self, head=None) -> None:
        self._head = head if isinstance(head, Query) else None
        self._table = head if isinstance(head, Table) else None

    def __add__(self, other: 'Query') -> 'Query':
        self_copy = deepcopy(self)
        other_copy = deepcopy(other)

        other_base = other_copy
        while other_base._head is not None:
            if other_base._head._head is None:
                break
            other_base = other_base._head
        other_base._head = self_copy
        return other_copy

    def __deepcopy__(self, memo) -> 'Query':
        new_object = copy(self)
        if self._head is not None:
            new_object._head = self._head.__deepcopy__(memo)
        return new_object

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

    def sort_by(self, col: OrderedType, order: Order = None, nulls: Nulls = None) -> 'SortQuery':
        return SortQuery(self, col, order, nulls)

    def order_by(self, col: OrderedType, order: Order = None, nulls: Nulls = None) -> 'OrderQuery':
        return OrderQuery(self, col, order, nulls)

    def top(self, num_rows: int, col: AnyTypeColumn, order: Order = None, nulls: Nulls = None) -> 'TopQuery':
        return TopQuery(self, num_rows, col, order, nulls)

    def join(self, query: 'Query', kind: JoinKind = None) -> 'JoinQuery':
        return JoinQuery(self, query, kind)

    def project(self, *args: Union[AssignmentBase, BaseExpression], **kwargs: ExpressionType) -> 'ProjectQuery':
        return ProjectQuery(self, self.extract_assignments(*args, **kwargs))

    def project_rename(self, *args: AssignmentFromColumnToColumn, **kwargs: AnyTypeColumn) -> 'ProjectRenameQuery':
        assignments: List[AssignmentFromColumnToColumn] = list(args)
        for column_name, column in kwargs.items():
            assignments.append(AssignmentFromColumnToColumn(AnyTypeColumn(column_name), column))
        return ProjectRenameQuery(self, assignments)

    def project_away(self, *columns: StringType) -> 'ProjectAwayQuery':
        return ProjectAwayQuery(self, columns)

    def distinct(self, *columns: BaseColumn) -> 'DistinctQuery':
        return DistinctQuery(self, columns)

    def distinct_all(self) -> 'DistinctQuery':
        return DistinctQuery(self, (AnyTypeColumn(KQL("*")),))

    def extend(self, *args: Union[BaseExpression, AssignmentBase], **kwargs: ExpressionType) -> 'ExtendQuery':
        return ExtendQuery(self, *self.extract_assignments(*args, **kwargs))

    def summarize(self, *args: Union[AggregationExpression, AssignmentFromAggregationToColumn],
                  **kwargs: AggregationExpression) -> 'SummarizeQuery':
        assignments: List[AssignmentFromAggregationToColumn] = []
        for arg in args:
            if isinstance(arg, AggregationExpression):
                assignments.append(arg.assign_to())
            else:
                assert isinstance(arg, AssignmentFromAggregationToColumn), "Invalid assignment"
                assignments.append(arg)
        for column_name, agg in kwargs.items():
            assignments.append(AssignmentFromAggregationToColumn(AnyTypeColumn(column_name), agg))
        return SummarizeQuery(self, assignments)

    def mv_expand(
            self, *args: Union[BaseExpression, AssignmentBase], bag_expansion: BagExpansion = None,
            with_item_index: BaseColumn = None, limit: int = None, **kwargs: ExpressionType
    ) -> 'MvExpandQuery':
        assignments = self.extract_assignments(*args, **kwargs)
        if len(assignments) == 0:
            raise ValueError("Please specify one or more columns for mv-expand")
        return MvExpandQuery(self, bag_expansion, with_item_index, limit, *assignments)

    def custom(self, custom_query: str) -> 'CustomQuery':
        return CustomQuery(self, custom_query)

    def evaluate(self, plugin_name, *args: ExpressionType, distribution: Distribution = None) -> 'EvaluateQuery':
        return EvaluateQuery(self, plugin_name, *args, distribution=distribution)

    def evaluate_udf(
            self, udf: FunctionType, extend: bool = True, distribution: Distribution = None, **type_specs: KustoType
    ) -> 'EvaluateQuery':
        return EvaluateQuery(
            self, 'python',
            AnyExpression(KQL(f'typeof({("*, " if extend else "") + ", ".join(field_name + ":" + kusto_type.primary_name for field_name, kusto_type in type_specs.items())})')),
            stringify_python_func(udf),
            distribution=distribution
        )

    def bag_unpack(self, col: AnyTypeColumn, prefix: str = None) -> 'EvaluateQuery':
        if prefix is None:
            return EvaluateQuery(self, 'bag_unpack', col)
        return EvaluateQuery(self, 'bag_unpack', col, prefix)

    @abstractmethod
    def _compile(self) -> KQL:
        raise NotImplementedError()  # pragma: no cover

    def _compile_all(self, use_full_table_name) -> KQL:
        if self._head is None:
            if self._table is None:
                return KQL("")
            else:
                table = self._table
                if use_full_table_name:
                    return table.to_query_format(fully_qualified=True)
                else:
                    return table.to_query_format()
        else:
            return KQL(f"{self._head._compile_all(use_full_table_name)} | {self._compile()}")

    def get_table(self) -> Table:
        if self._head is None:
            return self._table
        else:
            return self._head.get_table()

    def render(self, use_full_table_name: bool = False) -> KQL:
        result = self._compile_all(use_full_table_name)
        logger.debug("Complied query: " + result)
        return result

    def execute(self, table: Table = None) -> KustoResponse:
        if self.get_table() is None:
            if table is None:
                raise RuntimeError("No table supplied")
            rendered_query = table.to_query_format() + self.render()
        else:
            if table is not None:
                raise RuntimeError("This table is already bound to a query")
            table = self.get_table()
            rendered_query = self.render()

        logger.debug("Running query: " + rendered_query)
        return table.execute(rendered_query)

    def to_dataframe(self, table: Table = None):
        return self.execute(table).to_dataframe()

    @staticmethod
    def extract_assignments(*args: Union[AssignmentBase, BaseExpression], **kwargs: ExpressionType) -> List[AssignmentBase]:
        assignments: List[AssignmentBase] = []
        for arg in args:
            if isinstance(arg, BaseExpression):
                assignments.append(arg.assign_to())
            else:
                assignments.append(arg)
        for column_name, expression in kwargs.items():
            column_type = expression_to_type(expression, typed_column, AnyTypeColumn)
            if isinstance(expression, BaseExpression):
                assignments.append(expression.assign_to(column_type(column_name)))
            else:
                expression_type = expression_to_type(expression, plain_expression, AnyExpression)
                assignments.append(expression_type(to_kql(expression)).assign_to(column_type(column_name)))
        return assignments


class ProjectQuery(Query):
    _columns: List[AnyTypeColumn]
    _assignments: List[AssignmentBase]

    def __init__(self, head: 'Query', assignments: List[AssignmentBase]) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"project {', '.join(a.to_kql() for a in self._assignments)}")


class ProjectRenameQuery(Query):
    _assignments: List[AssignmentBase]

    def __init__(self, head: 'Query', assignments: List[AssignmentFromColumnToColumn]) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"project-rename {', '.join(a.to_kql() for a in self._assignments)}")


class ProjectAwayQuery(Query):
    _columns: Tuple[StringType, ...]

    def __init__(self, head: 'Query', columns: Tuple[StringType]) -> None:
        super().__init__(head)
        self._columns = columns

    def _compile(self) -> KQL:
        return KQL(f"project-away {', '.join(str(c) for c in self._columns)}")


class DistinctQuery(Query):
    _columns: Tuple[BaseColumn, ...]

    def __init__(self, head: 'Query', columns: Tuple[BaseColumn]) -> None:
        super().__init__(head)
        self._columns = columns

    def sample(self, number_of_values: NumberType) -> 'SampleDistinctQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sampledistinctoperator
        """
        assert len(self._columns) == 1, "sample-distinct supports only one column"
        return SampleDistinctQuery(self._head, self._columns[0], number_of_values)

    def top_hitters(self, number_of_values: NumberType) -> 'TopHittersQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tophittersoperator
        """
        assert len(self._columns) == 1, "top-hitters supports only one column"
        return TopHittersQuery(self._head, self._columns[0], number_of_values)

    def _compile(self) -> KQL:
        return KQL(f"distinct {', '.join(c.kql for c in self._columns)}")


class SampleDistinctQuery(Query):
    _number_of_values: NumberType
    _column: BaseColumn

    def __init__(self, head: 'Query', column: BaseColumn, number_of_values: NumberType) -> None:
        super().__init__(head)
        self._column = column
        self._number_of_values = number_of_values

    def _compile(self) -> KQL:
        return KQL(f"sample-distinct {to_kql(self._number_of_values)} of {self._column.kql}")


class TopHittersQuery(Query):
    _number_of_values: NumberType
    _column: BaseColumn
    _by_expression: Optional[NumberType]

    def __init__(self, head: 'Query', column: BaseColumn, number_of_values: NumberType, by_expression: Optional[NumberType] = None) -> None:
        super().__init__(head)
        self._column = column
        self._number_of_values = number_of_values
        self._by_expression = by_expression

    def by(self, by_expression: NumberType) -> 'TopHittersQuery':
        assert self._by_expression is None, "duplicate 'by' clause"
        return TopHittersQuery(self._head, self._column, self._number_of_values, by_expression)

    def _compile(self) -> KQL:
        return KQL(f"top-hitters {to_kql(self._number_of_values)} of {self._column.kql}{'' if self._by_expression is None else f' by {to_kql(self._by_expression)}'}")


class ExtendQuery(Query):
    _assignments: Tuple[AssignmentBase, ...]

    def __init__(self, head: 'Query', *assignments: AssignmentBase) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"extend {', '.join(a.to_kql() for a in self._assignments)}")


class WhereQuery(Query):
    _predicate: BooleanType

    def __init__(self, head: Query, predicate: BooleanType):
        super(WhereQuery, self).__init__(head)
        self._predicate = predicate

    def _compile(self) -> KQL:
        return KQL(f'where {self._predicate.kql}')


class _SingleNumberQuery(Query):
    _num_rows: int
    _query_name: str

    def __init__(self, head: Query, query_name: str, num_rows: int):
        super(_SingleNumberQuery, self).__init__(head)
        self._query_name = query_name
        self._num_rows = num_rows

    def _compile(self) -> KQL:
        return KQL(f'{self._query_name} {self._num_rows}')


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
        col: OrderedType
        order: Order
        nulls: Nulls

        def __init__(self, col: OrderedType, order: Order, nulls: Nulls):
            self.col = col
            self.order = order
            self.nulls = nulls

    _query_name: str
    _order_specs: List[OrderSpec]

    def __init__(self, head: Query, query_name: str, col: OrderedType, order: Order, nulls: Nulls):
        super(_OrderQueryBase, self).__init__(head)
        self._query_name = query_name
        self._order_specs = []
        self.then_by(col, order, nulls)

    def then_by(self, col: OrderedType, order: Order = None, nulls: Nulls = None):
        self._order_specs.append(_OrderQueryBase.OrderSpec(col, order, nulls))
        return self

    @staticmethod
    def _compile_order_spec(order_spec: OrderSpec) -> str:
        res = str(order_spec.col.kql)
        if order_spec.order is not None:
            res += " " + str(order_spec.order.value)
        if order_spec.nulls is not None:
            res += " nulls " + str(order_spec.nulls.value)
        return res

    def _compile(self) -> KQL:
        return KQL(f'{self._query_name} by {", ".join([self._compile_order_spec(order_spec) for order_spec in self._order_specs])}')


class SortQuery(_OrderQueryBase):
    def __init__(self, head: Query, col: OrderedType, order: Order, nulls: Nulls):
        super(SortQuery, self).__init__(head, "sort", col, order, nulls)


class OrderQuery(_OrderQueryBase):
    def __init__(self, head: Query, col: OrderedType, order: Order, nulls: Nulls):
        super(OrderQuery, self).__init__(head, "order", col, order, nulls)


class TopQuery(Query):
    _num_rows: int
    _order_spec: OrderQuery.OrderSpec

    def __init__(self, head: Query, num_rows: int, col: AnyTypeColumn, order: Order, nulls: Nulls):
        super(TopQuery, self).__init__(head)
        self._num_rows = num_rows
        self._order_spec = OrderQuery.OrderSpec(col, order, nulls)

    def _compile(self) -> KQL:
        # noinspection PyProtectedMember
        return KQL(f'top {self._num_rows} by {SortQuery._compile_order_spec(self._order_spec)}')


class JoinException(Exception):
    pass


class JoinQuery(Query):
    _joined_query: Query
    _kind: JoinKind
    _on_attributes: Tuple[Tuple[AnyTypeColumn, ...], ...]

    def __init__(self, head: Query, joined_query: Query, kind: JoinKind,
                 on_attributes: Tuple[Tuple[AnyTypeColumn, ...], ...] = tuple()):
        super(JoinQuery, self).__init__(head)
        self._joined_query = joined_query
        self._kind = kind
        self._on_attributes = on_attributes

    def on(self, col1: AnyTypeColumn, col2: AnyTypeColumn = None) -> 'JoinQuery':
        self._on_attributes = self._on_attributes + (((col1,),) if col2 is None else ((col1, col2),))
        return self

    @staticmethod
    def _compile_on_attribute(attribute: Tuple[AnyTypeColumn]):
        assert len(attribute) in (1, 2)
        if len(attribute) == 1:
            return attribute[0].kql
        else:
            return f"$left.{attribute[0].kql}==$right.{attribute[1].kql}"

    def _compile(self) -> KQL:
        if len(self._on_attributes) == 0:
            raise JoinException("A call to join() must be followed by a call to on()")
        if self._joined_query.get_table() is None:
            raise JoinException("The joined query must have a table")

        return KQL(f'join {"" if self._kind is None else f"kind={self._kind.value}"} '
                   f'({self._joined_query.render(use_full_table_name=True)}) on '
                   f'{", ".join([self._compile_on_attribute(attr) for attr in self._on_attributes])}')


class SummarizeQuery(Query):
    _assignments: List[AssignmentFromAggregationToColumn]
    _by_columns: List[Union[AnyTypeColumn, BaseExpression]]
    _by_assignments: List[AssignmentToSingleColumn]

    def __init__(self, head: Query,
                 assignments: List[AssignmentFromAggregationToColumn]):
        super(SummarizeQuery, self).__init__(head)
        self._assignments = assignments
        self._by_columns = []
        self._by_assignments = []

    def by(self, *args: Union[AssignmentToSingleColumn, AnyTypeColumn, BaseExpression],
           **kwargs: BaseExpression):
        for arg in args:
            if isinstance(arg, AnyTypeColumn) or isinstance(arg, BaseExpression):
                self._by_columns.append(arg)
            else:
                assert isinstance(arg, AssignmentToSingleColumn), "Invalid assignment"
                self._by_assignments.append(arg)
        for column_name, group_exp in kwargs.items():
            self._by_assignments.append(AssignmentToSingleColumn(AnyTypeColumn(column_name), group_exp))
        return self

    def _compile(self) -> KQL:
        result = f"summarize {', '.join(a.to_kql() for a in self._assignments)}"
        if len(self._by_assignments) != 0 or len(self._by_columns) != 0:
            result += f' by {", ".join(chain((c.kql for c in self._by_columns), (a.to_kql() for a in self._by_assignments)))}'
        return KQL(result)


class MvExpandQuery(Query):
    _assignments: Tuple[AssignmentBase]
    _bag_expansion: BagExpansion
    _with_item_index: BaseColumn
    _limit: int

    def __init__(self, head: Query, bag_expansion: BagExpansion, with_item_index: BaseColumn, limit: int, *assignments: AssignmentBase):
        super(MvExpandQuery, self).__init__(head)
        self._assignments = assignments
        self._bag_expansion = bag_expansion
        self._with_item_index = with_item_index
        self._limit = limit

    def _compile(self) -> KQL:
        res = "mv-expand "
        if self._bag_expansion is not None:
            res += f"bagexpansion={self._bag_expansion.value} "
        if self._with_item_index is not None:
            res += f"with_itemindex={self._with_item_index.kql} "
        res += ", ".join(a.to_kql() for a in self._assignments)
        if self._limit:
            res += f" limit {self._limit}"
        return KQL(res)


class CustomQuery(Query):
    _custom_query: str

    def __init__(self, head: Query, custom_query: str):
        super(CustomQuery, self).__init__(head)
        self._custom_query = custom_query

    def _compile(self) -> KQL:
        return KQL(self._custom_query)


class EvaluateQuery(Query):
    _plugin_name: str
    _args: Tuple[ExpressionType]
    _distribution: Distribution

    def __init__(self, head: Query, plugin_name: str, *args: ExpressionType, distribution: Distribution = None):
        super().__init__(head)
        self._plugin_name = plugin_name
        self._args = args
        self._distribution = distribution

    def _compile(self) -> KQL:
        return KQL(f'evaluate {"" if self._distribution is None else f"hint.distribution={self._distribution.value} "}'
                   f'{self._plugin_name}({", ".join(to_kql(arg) for arg in self._args)})')
