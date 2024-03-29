from abc import abstractmethod
from copy import copy, deepcopy
from itertools import chain
from os import linesep
from types import FunctionType
from typing import Tuple, List, Union, Optional

from .client_base import Table, KustoResponseBase, RetryConfig, ClientRequestProperties
from .enums import Order, Nulls, JoinKind, Distribution, BagExpansion
from .expressions import BooleanType, ExpressionType, AggregationExpression, _AssignmentBase, _AssignmentFromAggregationToColumn, _AssignmentToSingleColumn, \
    _AnyTypeColumn, BaseExpression, _AssignmentFromColumnToColumn, AnyExpression, _to_kql, _expression_to_type, BaseColumn, NumberType, OrderedType
# These seem like redundant imports, but without them typeguard is confused
# noinspection PyUnresolvedReferences
from .expressions import _DatetimeExpression, _TimespanExpression, _NumberExpression, _StringExpression, _BooleanExpression  # noqa: F401
from .functions import Functions as f
from .kql_converters import KQL
from .logger import _logger
from .type_utils import _KustoType, _typed_column, _plain_expression
from .udf import _stringify_python_func


class Query:
    _head: Optional['Query']
    _table: Optional[Table]
    _table_name: Optional[str]

    def __init__(self, head=None) -> None:
        """
        When building the query, try and use the query best practices:
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices
        """
        self._head = head if isinstance(head, Query) else None
        self._table = head if isinstance(head, Table) else None
        self._table_name = head if isinstance(head, str) else None

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

    def where(self, *predicates: BooleanType) -> 'Query':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/whereoperator

        Implicitly apply conjunction if multiple predicates are provided. You can use predicates which are calculated at runtime and result in boolean values, which are
        pre-processed: 'True' values ignored, and 'False' values cause all other predicates to be ignored. If the result of pre-processing is a single 'True' predicate, no 'where'
        clause will be generated.

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Whenever possible, use time filters first.
        * Prefer filtering on table columns, and not on calculated columns.
        * Simplest terms first: If you have multiple clauses conjoined with and, put first the clauses that involve just one column.

        Warning: to apply a logical 'not', do not use the Python 'not' operator, it will simply produce a 'False' boolean value. Use either the `~` operator or `f.not_of()`.
        """
        filtered_predicates = []
        for predicate in predicates:
            if predicate is True:
                # This predicate has no effect on the outcome
                continue
            if predicate is False:
                # All other predicates have no effect on the outcome
                filtered_predicates = [False]
                break
            filtered_predicates.append(predicate)
        if len(filtered_predicates) == 0:
            # Do no generate 'where' clause
            return self
        return _WhereQuery(self, *filtered_predicates)

    def take(self, num_rows: int) -> '_TakeQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/takeoperator
        """
        return _TakeQuery(self, num_rows)

    def limit(self, num_rows: int) -> '_LimitQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/limitoperator
        """
        return _LimitQuery(self, num_rows)

    def sample(self, num_rows: int) -> '_SampleQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sampleoperator
        """
        return _SampleQuery(self, num_rows)

    def count(self) -> '_CountQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/countoperator
        """
        return _CountQuery(self)

    def sort_by(self, col: OrderedType, order: Order = None, nulls: Nulls = None) -> '_SortQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sortoperator
        """
        return _SortQuery(self, col, order, nulls)

    def order_by(self, col: OrderedType, order: Order = None, nulls: Nulls = None) -> '_SortQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/orderoperator
        """
        return self.sort_by(col, order, nulls)

    def top(self, num_rows: int, col: OrderedType, order: Order = None, nulls: Nulls = None) -> '_TopQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/topoperator
        """
        return _TopQuery(self, num_rows, col, order, nulls)

    def join(self, query: 'Query', kind: JoinKind = None) -> '_JoinQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/joinoperator

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Select the table with the fewer rows to be the first one ("left" side).
        * Across clusters, run the query on the "right" side of the join, where most of the data is located.

        Best practices once `hint.strategy` is supported (see `here <https://github.com/Azure/pykusto/issues/104>`_):

        * When left side is small (up to ~100,000 records) and right side is large use `hint.strategy=broadcast`.
        * When both sides are too large	use `hint.strategy=shuffle`.
        """
        return _JoinQuery(self, query, kind)

    def project(self, *args: Union[_AssignmentBase, BaseExpression], **kwargs: ExpressionType) -> '_ProjectQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/projectoperator
        """
        return _ProjectQuery(self, self._extract_assignments(*args, **kwargs))

    def project_rename(self, *args: _AssignmentFromColumnToColumn, **kwargs: BaseColumn) -> '_ProjectRenameQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/projectrenameoperator
        """
        assignments: List[_AssignmentFromColumnToColumn] = list(args)
        for column_name, column in kwargs.items():
            assignments.append(_AssignmentFromColumnToColumn(_AnyTypeColumn(column_name), column))
        return _ProjectRenameQuery(self, assignments)

    def project_away(self, *columns: Union[BaseColumn, str]) -> '_ProjectAwayQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/projectawayoperator
        """
        return _ProjectAwayQuery(self, columns)

    def distinct(self, *columns: BaseColumn) -> '_DistinctQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/distinctoperator
        """
        return _DistinctQuery(self, columns)

    def distinct_all(self) -> '_DistinctQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/distinctoperator
        """
        return _DistinctQuery(self, (_AnyTypeColumn(KQL("*")),))

    def extend(self, *args: Union[BaseExpression, _AssignmentBase], **kwargs: ExpressionType) -> '_ExtendQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/extendoperator
        """
        return _ExtendQuery(self, *self._extract_assignments(*args, **kwargs))

    def summarize(self, *args: Union[AggregationExpression, _AssignmentFromAggregationToColumn],
                  **kwargs: AggregationExpression) -> '_SummarizeQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/summarizeoperator
        """
        assignments: List[_AssignmentFromAggregationToColumn] = []
        for arg in args:
            if isinstance(arg, AggregationExpression):
                assignments.append(arg.assign_to())
            else:
                assert isinstance(arg, _AssignmentFromAggregationToColumn), "Invalid assignment"
                assignments.append(arg)
        for column_name, agg in kwargs.items():
            assignments.append(_AssignmentFromAggregationToColumn(_AnyTypeColumn(column_name), agg))
        return _SummarizeQuery(self, assignments)

    def mv_expand(
            self, *args: Union[BaseExpression, _AssignmentBase], bag_expansion: BagExpansion = None,
            with_item_index: BaseColumn = None, limit: int = None, **kwargs: ExpressionType
    ) -> '_MvExpandQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/mvexpandoperator
        """
        assignments = self._extract_assignments(*args, **kwargs)
        if len(assignments) == 0:
            raise ValueError("Please specify one or more columns for mv-expand")
        return _MvExpandQuery(self, bag_expansion, with_item_index, limit, *assignments)

    def custom(self, custom_query: str) -> '_CustomQuery':
        return _CustomQuery(self, custom_query)

    def evaluate(self, plugin_name, *args: ExpressionType, distribution: Distribution = None) -> '_EvaluateQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/evaluateoperator
        """
        return _EvaluateQuery(self, plugin_name, *args, distribution=distribution)

    def evaluate_udf(
            self, udf: FunctionType, extend: bool = True, distribution: Distribution = None, **type_specs: _KustoType
    ) -> '_EvaluateQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/pythonplugin
        """
        return _EvaluateQuery(
            self, 'python',
            AnyExpression(KQL(f'typeof({("*, " if extend else "") + ", ".join(field_name + ":" + kusto_type.primary_name for field_name, kusto_type in type_specs.items())})')),
            _stringify_python_func(udf),
            distribution=distribution
        )

    def bag_unpack(self, col: BaseColumn, prefix: str = None) -> '_EvaluateQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bag-unpackplugin
        """
        if prefix is None:
            return _EvaluateQuery(self, 'bag_unpack', col)
        return _EvaluateQuery(self, 'bag_unpack', col, prefix)

    @abstractmethod
    def _compile(self) -> KQL:
        raise NotImplementedError()

    def _compile_all(self, use_full_table_name) -> KQL:
        if self._head is None:
            if self._table is None:
                if self._table_name is None:
                    return KQL("")
                else:
                    return KQL(self.get_table_name())
            else:
                table = self._table
                if use_full_table_name:
                    return table.to_query_format(fully_qualified=True)
                else:
                    return table.to_query_format()
        else:
            return KQL(f"{self._head._compile_all(use_full_table_name)} | {self._compile()}")

    def get_table(self) -> Optional[Table]:
        if self._head is None:
            return self._table
        else:
            return self._head.get_table()

    def get_table_name(self) -> Optional[str]:
        if self._head is None:
            return self._table_name
        else:
            return self._head.get_table_name()

    def render(self, use_full_table_name: bool = False) -> KQL:
        result = self._compile_all(use_full_table_name)
        _logger.debug("Complied query: " + result)
        return result

    def pretty_render(self, use_full_table_name: bool = False) -> KQL:
        kql = self.render(use_full_table_name)
        if kql is not None:
            kql = KQL(kql.replace(" |", linesep + "|"))
        return kql

    def execute(self, table: Table = None, properties: ClientRequestProperties = None, retry_config: RetryConfig = None) -> KustoResponseBase:
        if self.get_table() is None:
            if table is None:
                raise RuntimeError("No table supplied")
            rendered_query = table.to_query_format() + self.render()
        else:
            if table is not None:
                raise RuntimeError("This table is already bound to a query")
            table = self.get_table()
            rendered_query = self.render()

        _logger.debug("Running query: " + rendered_query)
        return table.execute(rendered_query, properties, retry_config)

    def to_dataframe(self, table: Table = None, properties: ClientRequestProperties = None, retry_config: RetryConfig = None):
        return self.execute(table, properties, retry_config).to_dataframe()

    @staticmethod
    def _extract_assignments(*args: Union[_AssignmentBase, BaseExpression], **kwargs: ExpressionType) -> List[_AssignmentBase]:
        assignments: List[_AssignmentBase] = []
        for arg in args:
            if isinstance(arg, BaseExpression):
                assignments.append(arg.assign_to())
            else:
                assignments.append(arg)
        for column_name, expression in kwargs.items():
            column_type = _expression_to_type(expression, _typed_column, _AnyTypeColumn)
            if isinstance(expression, BaseExpression):
                assignments.append(expression.assign_to(column_type(column_name)))
            else:
                expression_type = _expression_to_type(expression, _plain_expression, AnyExpression)
                assignments.append(expression_type(_to_kql(expression)).assign_to(column_type(column_name)))
        return assignments


class _ProjectQuery(Query):
    _assignments: List[_AssignmentBase]

    def __init__(self, head: 'Query', assignments: List[_AssignmentBase]) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"project {', '.join(a.to_kql() for a in self._assignments)}")


class _ProjectRenameQuery(Query):
    _assignments: List[_AssignmentBase]

    def __init__(self, head: 'Query', assignments: List[_AssignmentFromColumnToColumn]) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"project-rename {', '.join(a.to_kql() for a in self._assignments)}")


class _ProjectAwayQuery(Query):
    _columns: Tuple[BaseColumn, ...]

    def __init__(self, head: 'Query', columns: Tuple[Union[BaseColumn, str], ...]) -> None:
        super().__init__(head)
        self._columns = tuple(c if isinstance(c, BaseColumn) else _AnyTypeColumn(c) for c in columns)

    def _compile(self) -> KQL:
        return KQL(f"project-away {', '.join(_to_kql(c) for c in self._columns)}")


class _DistinctQuery(Query):
    _columns: Tuple[BaseColumn, ...]

    def __init__(self, head: 'Query', columns: Tuple[BaseColumn, ...]) -> None:
        super().__init__(head)
        self._columns = columns

    def sample(self, number_of_values: NumberType) -> '_SampleDistinctQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sampledistinctoperator
        """
        assert len(self._columns) == 1, "sample-distinct supports only one column"
        return _SampleDistinctQuery(self._head, self._columns[0], number_of_values)

    def top_hitters(self, number_of_values: NumberType) -> '_TopHittersQuery':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tophittersoperator
        """
        assert len(self._columns) == 1, "top-hitters supports only one column"
        return _TopHittersQuery(self._head, self._columns[0], number_of_values)

    def _compile(self) -> KQL:
        return KQL(f"distinct {', '.join(c.kql for c in self._columns)}")


class _SampleDistinctQuery(Query):
    _number_of_values: NumberType
    _column: BaseColumn

    def __init__(self, head: 'Query', column: BaseColumn, number_of_values: NumberType) -> None:
        super().__init__(head)
        self._column = column
        self._number_of_values = number_of_values

    def _compile(self) -> KQL:
        return KQL(f"sample-distinct {_to_kql(self._number_of_values)} of {self._column.kql}")


class _TopHittersQuery(Query):
    _number_of_values: NumberType
    _column: BaseColumn
    _by_expression: Optional[NumberType]

    def __init__(self, head: 'Query', column: BaseColumn, number_of_values: NumberType, by_expression: Optional[NumberType] = None) -> None:
        super().__init__(head)
        self._column = column
        self._number_of_values = number_of_values
        self._by_expression = by_expression

    def by(self, by_expression: NumberType) -> '_TopHittersQuery':
        assert self._by_expression is None, "duplicate 'by' clause"
        return _TopHittersQuery(self._head, self._column, self._number_of_values, by_expression)

    def _compile(self) -> KQL:
        return KQL(f"top-hitters {_to_kql(self._number_of_values)} of {self._column.kql}{'' if self._by_expression is None else f' by {_to_kql(self._by_expression)}'}")


class _ExtendQuery(Query):
    _assignments: Tuple[_AssignmentBase, ...]

    def __init__(self, head: 'Query', *assignments: _AssignmentBase) -> None:
        super().__init__(head)
        self._assignments = assignments

    def _compile(self) -> KQL:
        return KQL(f"extend {', '.join(a.to_kql() for a in self._assignments)}")


class _WhereQuery(Query):
    _predicates: Tuple[BooleanType, ...]

    def __init__(self, head: Query, *predicates: BooleanType):
        super(_WhereQuery, self).__init__(head)
        self._predicates = predicates

    def _compile(self) -> KQL:
        if len(self._predicates) == 1:
            return KQL(f'where {_to_kql(self._predicates[0])}')
        return KQL(f'where {f.all_of(*self._predicates)}')


class _SingleNumberQuery(Query):
    _num_rows: int
    _query_name: str

    def __init__(self, head: Query, query_name: str, num_rows: int):
        super(_SingleNumberQuery, self).__init__(head)
        self._query_name = query_name
        self._num_rows = num_rows

    def _compile(self) -> KQL:
        return KQL(f'{self._query_name} {self._num_rows}')


class _TakeQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(_TakeQuery, self).__init__(head, 'take', num_rows)


class _LimitQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(_LimitQuery, self).__init__(head, 'limit', num_rows)


class _SampleQuery(_SingleNumberQuery):
    _num_rows: int

    def __init__(self, head: Query, num_rows: int):
        super(_SampleQuery, self).__init__(head, 'sample', num_rows)


class _CountQuery(Query):
    _num_rows: int

    def __init__(self, head: Query):
        super(_CountQuery, self).__init__(head)

    def _compile(self) -> KQL:
        return KQL('count')


class _OrderQueryBase(Query):
    class OrderSpec:
        col: OrderedType
        order: Optional[Order]
        nulls: Optional[Nulls]

        def __init__(self, col: OrderedType, order: Optional[Order], nulls: Optional[Nulls]):
            self.col = col
            self.order = order
            self.nulls = nulls

    _query_name: str
    _order_specs: List[OrderSpec]

    def __init__(self, head: Query, query_name: str, col: OrderedType, order: Optional[Order], nulls: Optional[Nulls]):
        super().__init__(head)
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


class _SortQuery(_OrderQueryBase):
    def __init__(self, head: Query, col: OrderedType, order: Optional[Order], nulls: Optional[Nulls]):
        super(_SortQuery, self).__init__(head, "sort", col, order, nulls)


class _TopQuery(Query):
    _num_rows: int
    _order_spec: _OrderQueryBase.OrderSpec

    def __init__(self, head: Query, num_rows: int, col: OrderedType, order: Order, nulls: Nulls):
        super(_TopQuery, self).__init__(head)
        self._num_rows = num_rows
        self._order_spec = _OrderQueryBase.OrderSpec(col, order, nulls)

    def _compile(self) -> KQL:
        # noinspection PyProtectedMember
        return KQL(f'top {self._num_rows} by {_SortQuery._compile_order_spec(self._order_spec)}')


class JoinException(Exception):
    pass


class _JoinQuery(Query):
    _joined_query: Query
    _kind: JoinKind
    _on_attributes: Tuple[Union[Tuple[BaseColumn], Tuple[BaseColumn, BaseColumn]], ...]

    def __init__(self, head: Query, joined_query: Query, kind: Optional[JoinKind],
                 on_attributes: Tuple[Tuple[BaseColumn, ...], ...] = tuple()):
        super(_JoinQuery, self).__init__(head)
        self._joined_query = joined_query
        self._kind = kind
        self._on_attributes = on_attributes

    def on(self, *join_cols: Union[BaseColumn, Tuple[BaseColumn, BaseColumn]]) -> '_JoinQuery':
        for col in join_cols:
            self._inner_on_with_or_without_table(col)
        return self

    def _inner_on_with_or_without_table(self, col: Union[BaseColumn, Tuple[BaseColumn, BaseColumn]]) -> '_JoinQuery':
        if isinstance(col, BaseColumn):
            return self._inner_on(col)
        elif isinstance(col, tuple) and len(col) == 2 and isinstance(col[0], BaseColumn) and isinstance(col[1], BaseColumn):
            return self._inner_on_with_table(*col)
        else:
            # Re-apply coverage to this line once the corresponding test is re-enabled, after this is resolved: https://github.com/agronholm/typeguard/issues/159
            raise JoinException(  # pragma: no cover
                "A join argument could be a column, or a tuple of two columns corresponding to the input and join "
                f"tables column names. However, the join argument provided is {col} of type {type(col)}"
            )

    def _inner_on(self, col: BaseColumn) -> '_JoinQuery':
        self._on_attributes = self._on_attributes + ((col,),)
        return self

    def _inner_on_with_table(self, col1: BaseColumn, col2: BaseColumn) -> '_JoinQuery':
        self._on_attributes = self._on_attributes + ((col1, col2),)
        return self

    @staticmethod
    def _compile_on_attribute(attribute: Union[Tuple[BaseColumn], Tuple[BaseColumn, BaseColumn]]):
        assert len(attribute) in (1, 2)
        if len(attribute) == 1:
            return attribute[0].kql
        else:
            return f"$left.{attribute[0].kql}==$right.{attribute[1].kql}"

    def _assert_joined_query(self) -> None:
        if self._joined_query.get_table() is None:
            raise JoinException("The joined query must have a table")

    def _assert_on_attributes(self) -> None:
        if len(self._on_attributes) == 0:
            raise JoinException("A call to join() must be followed by a call to on()")

    def _compile(self) -> KQL:
        self._assert_joined_query()
        self._assert_on_attributes()

        return KQL(f'join {"" if self._kind is None else f"kind={self._kind.value}"} '
                   f'({self._joined_query.render(use_full_table_name=True)}) on '
                   f'{", ".join([self._compile_on_attribute(attr) for attr in self._on_attributes])}')


class _SummarizeQuery(Query):
    _assignments: List[_AssignmentFromAggregationToColumn]
    _by_columns: List[Union[BaseColumn, BaseExpression]]
    _by_assignments: List[_AssignmentToSingleColumn]

    def __init__(
            self,
            head: Query,
            assignments: List[_AssignmentFromAggregationToColumn],
    ):
        super(_SummarizeQuery, self).__init__(head)
        self._assignments = assignments
        self._by_columns = []
        self._by_assignments = []
        self._shuffle_by_keys = False
        self._shuffle_keys = []

    def by(self, *args: Union[_AssignmentToSingleColumn, BaseColumn, BaseExpression],
           **kwargs: BaseExpression):
        for arg in args:
            if isinstance(arg, BaseColumn) or isinstance(arg, BaseExpression):
                self._by_columns.append(arg)
            else:
                assert isinstance(arg, _AssignmentToSingleColumn), "Invalid assignment"
                self._by_assignments.append(arg)
        for column_name, group_exp in kwargs.items():
            self._by_assignments.append(_AssignmentToSingleColumn(_AnyTypeColumn(column_name), group_exp))
        return self

    def shuffle_key(self, *keys: Union[_AssignmentToSingleColumn, BaseColumn, BaseExpression]):
        if keys is not None:
            for key in keys:
                self._shuffle_keys.append(key)

        self._shuffle_by_keys = True
        return self

    def _compile(self) -> KQL:
        result = f'summarize {self._compile_shuffle_hint()}{", ".join(a.to_kql() for a in self._assignments)}'
        if len(self._by_assignments) != 0 or len(self._by_columns) != 0:
            result += f' by {", ".join(chain((c.kql for c in self._by_columns), (a.to_kql() for a in self._by_assignments)))}'
        return KQL(result)

    def _compile_shuffle_hint(self) -> str:
        if not self._shuffle_by_keys:
            return ""
        if len(self._shuffle_keys) == 0:
            return "hint.strategy = shuffle "
        else:
            return f'{" ".join([f"hint.shufflekey = {k.kql}" for k in self._shuffle_keys])} '


class _MvExpandQuery(Query):
    _assignments: Tuple[_AssignmentBase]
    _bag_expansion: Optional[BagExpansion]
    _with_item_index: Optional[BaseColumn]
    _limit: Optional[int]

    def __init__(self, head: Query, bag_expansion: Optional[BagExpansion], with_item_index: Optional[BaseColumn], limit: Optional[int], *assignments: _AssignmentBase):
        super(_MvExpandQuery, self).__init__(head)
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
        if self._limit is not None:
            res += f" limit {self._limit}"
        return KQL(res)


class _CustomQuery(Query):
    _custom_query: str

    def __init__(self, head: Query, custom_query: str):
        super(_CustomQuery, self).__init__(head)
        self._custom_query = custom_query

    def _compile(self) -> KQL:
        return KQL(self._custom_query)


class _EvaluateQuery(Query):
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
                   f'{self._plugin_name}({", ".join(_to_kql(arg) for arg in self._args)})')
