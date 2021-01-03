from datetime import datetime, timedelta
from typing import Any, List, Tuple, Mapping, Optional
from typing import Union

from .keywords import _KUSTO_KEYWORDS
from .kql_converters import KQL
from .type_utils import _plain_expression, _aggregation_expression, PythonTypes, _kql_converter, _KustoType, \
    _typed_column, _TypeRegistrar, _get_base_types, _NUMBER_TYPES

_ExpressionType = Union[PythonTypes, 'BaseExpression']
_StringType = Union[str, '_StringExpression']
_BooleanType = Union[bool, '_BooleanExpression']
_NumberType = Union[int, float, '_NumberExpression']
_ArrayType = Union[List, Tuple, '_ArrayExpression']
_MappingType = Union[Mapping, '_MappingExpression']
_DatetimeType = Union[datetime, '_DatetimeExpression']
_TimespanType = Union[timedelta, '_TimespanExpression']
_DynamicType = Union[_ArrayType, _MappingType]
_OrderedType = Union[_DatetimeType, _TimespanType, _NumberType, _StringType]


# All classes in the same file to prevent circular dependencies

class BaseExpression:
    kql: KQL

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'BaseExpression':
        assert cls is not BaseExpression, "BaseExpression is abstract"
        return object.__new__(cls)

    def __init__(self, kql: Union[KQL, 'BaseExpression']) -> None:
        if isinstance(kql, BaseExpression):
            self.kql = kql.kql
            return
        assert isinstance(kql, str), "Either expression or KQL required"
        self.kql = kql

    def __repr__(self) -> str:
        return str(self.kql)

    def __bool__(self) -> bool:
        raise TypeError(
            "Conversion of expression to boolean is not allowed, to prevent accidental use of the logical operators: 'and', 'or', and 'not'. "
            "Instead either use the bitwise operators '&', '|' and '~' (but note the difference in operator precedence!), "
            "or the functions 'all_of', 'any_of' and 'not_of'"
        )

    def as_subexpression(self) -> KQL:
        return KQL(f'({self.kql})')

    def get_type(self) -> '_StringExpression':
        return _StringExpression(KQL(f'gettype({self.kql})'))

    def __hash__(self, mod: Union['_NumberExpression', int] = None) -> '_StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hashfunction
        """
        if mod is not None:
            return _StringExpression(KQL(f'hash({self.kql}, {_to_kql(mod)})'))
        return _StringExpression(KQL(f'hash({self.kql})'))

    def hash_sha256(self) -> '_StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sha256hashfunction
        """
        return _StringExpression(KQL(f'hash_sha256({self.kql})'))

    def is_empty(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isemptyfunction
        """
        return _BooleanExpression(KQL(f'isempty({self.kql})'))

    def is_not_empty(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotemptyfunction
        """
        return _BooleanExpression(KQL(f'isnotempty({self.kql})'))

    @staticmethod
    def base_binary_op(
            left: _ExpressionType, operator: str, right: _ExpressionType, result_type: Optional[_KustoType]
    ) -> 'BaseExpression':
        registrar = _plain_expression
        fallback = AnyExpression
        if isinstance(left, AggregationExpression) or isinstance(right, AggregationExpression):
            registrar = _aggregation_expression
            fallback = _AnyAggregationExpression
        return_type = fallback if result_type is None else registrar.registry[result_type]
        return return_type(KQL(f'{_to_kql(left, True)}{operator}{_to_kql(right, True)}'))

    def __eq__(self, other: _ExpressionType) -> '_BooleanExpression':
        return _BooleanExpression.binary_op(self, ' == ', other)

    def __ne__(self, other: _ExpressionType) -> '_BooleanExpression':
        return _BooleanExpression.binary_op(self, ' != ', other)

    def is_in(self, other: _DynamicType, case_sensitive: bool = False) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/sethaselementfunction

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Use `case_sensitive=True` when possible
        """
        if isinstance(other, (List, Tuple)):
            # For a literal array, we can use 'in'
            # The following RHS is the only place where a literal list does not require being surrounded by 'dynamic()'
            return _BooleanExpression(KQL(f'{self.kql} {"in" if case_sensitive else "in~"} ({", ".join(map(_to_kql, other))})'))
        # Otherwise, Kusto does not accept 'in', and we need to use 'set_has_element', which is always case-sensitive
        assert case_sensitive, "Member test for non-literal array cannot be case insensitive"
        return _BooleanExpression(KQL(f'set_has_element({_to_kql(other)}, {self.kql})'))

    def not_in(self, other: _DynamicType, case_sensitive: bool = False) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Use `case_sensitive=True` when possible
        """
        if isinstance(other, (List, Tuple)):
            # For a literal array, we can use 'in'
            # The following RHS is the only place where a literal list does not require being surrounded by 'dynamic()'
            return _BooleanExpression(KQL(f'{self.kql} {"!in" if case_sensitive else "!in~"} ({", ".join(map(_to_kql, other))})'))
        # Otherwise, for some reason Kusto does not accept 'in', and we need to use 'contains' as if 'other' was a string
        return _BooleanExpression.binary_op(other, ' !contains_cs ' if case_sensitive else ' !contains ', self)

    def is_null(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnullfunction
        """
        return _BooleanExpression(KQL(f'isnull({self.kql})'))

    def is_not_null(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnotnullfunction
        """
        return _BooleanExpression(KQL(f'isnotnull({self.kql})'))

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError("'in' not supported. Instead use '.is_in()'")

    def to_bool(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toboolfunction
        """
        return _BooleanExpression(KQL(f'tobool({self.kql})'))

    def to_string(self) -> '_StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tostringfunction
        """
        return _StringExpression(KQL(f'tostring({self.kql})'))

    def to_int(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tointfunction
        """
        return _NumberExpression(KQL(f'toint({self.kql})'))

    def to_long(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolongfunction
        """
        return _NumberExpression(KQL(f'tolong({self.kql})'))

    def assign_to_single_column(self, column: '_AnyTypeColumn') -> '_AssignmentToSingleColumn':
        return _AssignmentToSingleColumn(column, self)

    def assign_to_multiple_columns(self, *columns: '_AnyTypeColumn') -> '_AssignmentBase':
        """
        This method exists for the sole purpose of providing an informative error message.
        """
        raise ValueError("Only arrays can be assigned to multiple columns")

    def assign_to(self, *columns: '_AnyTypeColumn') -> '_AssignmentBase':
        if len(columns) == 0:
            # Unspecified column name
            return _AssignmentBase(None, self)
        if len(columns) == 1:
            return self.assign_to_single_column(columns[0])
        return self.assign_to_multiple_columns(*columns)


@_plain_expression(_KustoType.BOOL)
class _BooleanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: _ExpressionType, operator: str, right: _ExpressionType) -> '_BooleanExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, _KustoType.BOOL)

    def __and__(self, other: _BooleanType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression.binary_op(self, ' and ', other)

    def __rand__(self, other: _BooleanType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression.binary_op(other, ' and ', self)

    def __or__(self, other: _BooleanType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression.binary_op(self, ' or ', other)

    def __ror__(self, other: _BooleanType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/logicaloperators
        """
        return _BooleanExpression.binary_op(other, ' or ', self)

    def __invert__(self) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/notfunction
        Note that using the Python 'not' does not have the desired effect, because unfortunately its behavior cannot be overridden.
        """
        return _BooleanExpression(KQL(f'not({self.kql})'))


@_plain_expression(*_NUMBER_TYPES)
class _NumberExpression(BaseExpression):
    @staticmethod
    def binary_op(left: _NumberType, operator: str, right: _NumberType) -> '_NumberExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, _KustoType.INT)

    def __lt__(self, other: _NumberType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' < ', other)

    def __le__(self, other: _NumberType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' <= ', other)

    def __gt__(self, other: _NumberType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' > ', other)

    def __ge__(self, other: _NumberType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' >= ', other)

    def __add__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(self, ' + ', other)

    def __radd__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(other, ' + ', self)

    def __sub__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(self, ' - ', other)

    def __rsub__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(other, ' - ', self)

    def __mul__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(self, ' * ', other)

    def __rmul__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(other, ' * ', self)

    def __truediv__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(self, ' / ', other)

    def __rtruediv__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(other, ' / ', self)

    def __mod__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(self, ' % ', other)

    def __rmod__(self, other: _NumberType) -> '_NumberExpression':
        return _NumberExpression.binary_op(other, ' % ', self)

    def __neg__(self) -> '_NumberExpression':
        return _NumberExpression(KQL(f'-{self.kql}'))

    def __abs__(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/abs-function
        """
        return _NumberExpression(KQL(f'abs({self.kql})'))

    def between(self, lower: _NumberType, upper: _NumberType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return _BooleanExpression(KQL(f'{self.kql} between ({_to_kql(lower, True)} .. {_to_kql(upper, True)})'))

    def acos(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/acosfunction
        """
        return _NumberExpression(KQL(f'acos({self.kql})'))

    def cos(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/cosfunction
        """
        return _NumberExpression(KQL(f'cos({self.kql})'))

    def floor(self, round_to: _NumberType) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return _NumberExpression(KQL(f'floor({self.kql}, {_to_kql(round_to)})'))

    def bin(self, round_to: _NumberType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return _NumberExpression(KQL(f'bin({self.kql}, {_to_kql(round_to)})'))

    def bin_at(self, round_to: _NumberType, fixed_point: _NumberType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return _NumberExpression(KQL(f'bin_at({self.kql}, {_to_kql(round_to)}, {_to_kql(fixed_point)})'))

    def bin_auto(self) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return _NumberExpression(KQL(f'bin_auto({self.kql})'))

    def ceiling(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/ceilingfunction
        """
        return _NumberExpression(KQL(f'ceiling({self.kql})'))

    def exp(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp-function
        """
        return _NumberExpression(KQL(f'exp({self.kql})'))

    def exp10(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp10-function
        """
        return _NumberExpression(KQL(f'exp10({self.kql})'))

    def exp2(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/exp2-function
        """
        return _NumberExpression(KQL(f'exp2({self.kql})'))

    def isfinite(self) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isfinitefunction
        """
        return _BooleanExpression(KQL(f'isfinite({self.kql})'))

    def is_inf(self) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isinffunction
        """
        return _BooleanExpression(KQL(f'isinf({self.kql})'))

    def is_nan(self) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isnanfunction
        """
        return _BooleanExpression(KQL(f'isnan({self.kql})'))

    def log(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log-function
        """
        return _NumberExpression(KQL(f'log({self.kql})'))

    def log10(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log10-function
        """
        return _NumberExpression(KQL(f'log10({self.kql})'))

    def log2(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/log2-function
        """
        return _NumberExpression(KQL(f'log2({self.kql})'))

    def log_gamma(self) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/loggammafunction
        """
        return _NumberExpression(KQL(f'loggamma({self.kql})'))

    def round(self, precision: _NumberType = None) -> '_NumberExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/roundfunction
        """
        return _NumberExpression(KQL(f'round({self.kql})' if precision is None else f'round({self.kql}, {_to_kql(precision)})'))


@_plain_expression(_KustoType.STRING)
class _StringExpression(BaseExpression):
    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def string_size(self) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/stringsizefunction
        """
        return _NumberExpression(KQL(f'string_size({self.kql})'))

    def split(self, delimiter: _StringType, requested_index: _NumberType = None) -> '_ArrayExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/splitfunction
        """
        if requested_index is None:
            return _ArrayExpression(KQL(f'split({self.kql}, {_to_kql(delimiter)})'))
        return _ArrayExpression(KQL(f'split({self.kql}, {_to_kql(delimiter)}, {_to_kql(requested_index)})'))

    def equals(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Use `case_sensitive=True` when possible
        """
        return _BooleanExpression.binary_op(self, ' == ' if case_sensitive else ' =~ ', other)

    def not_equals(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * Use `case_sensitive=True` when possible
        """
        return _BooleanExpression.binary_op(self, ' != ' if case_sensitive else ' !~ ', other)

    def matches(self, regex: _StringType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return _BooleanExpression.binary_op(self, ' matches regex ', regex)

    def contains(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * When looking for full tokens, `has` works better, since it doesn't look for substrings.
        * Use `case_sensitive=True` when possible
        """
        return _BooleanExpression.binary_op(self, ' contains_cs ' if case_sensitive else ' contains ', other)

    def not_contains(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators

        Best practices (`full list <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_):

        * When looking for full tokens, `not_has` works better, since it doesn't look for substrings.
        * Use `case_sensitive=True` when possible
        """
        return _BooleanExpression.binary_op(self, ' !contains_cs ' if case_sensitive else ' !contains ', other)

    def startswith(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return _BooleanExpression.binary_op(self, ' startswith_cs ' if case_sensitive else ' startswith ', other)

    def endswith(self, other: _StringType, case_sensitive: bool = False) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return _BooleanExpression.binary_op(self, ' endswith_cs ' if case_sensitive else ' endswith ', other)

    def lower(self) -> '_StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/tolowerfunction

        Don't use this method for case-insensitive comparisons. Instead use the `equals` method with `case_sensitive=False`
        (see `best practices <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_).
        """
        return _StringExpression(KQL(f'tolower({self.kql})'))

    def upper(self) -> '_StringExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/toupperfunction

        Don't use this method for case-insensitive comparisons. Instead use the `equals` method with `case_sensitive=False`
        (see `best practices <https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/best-practices>`_).
        """
        return _StringExpression(KQL(f'toupper({self.kql})'))

    def is_utf8(self) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/isutf8
        """
        return _BooleanExpression(KQL(f'isutf8({self.kql})'))

    def has(self, exp: _StringType, case_sensitive: bool = False) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return _BooleanExpression(KQL(
            f'{self.as_subexpression()} {"has_cs" if case_sensitive else "has"} {_to_kql(exp, True)}'
        ))

    def has_not(self, exp: _StringType, case_sensitive: bool = False) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return _BooleanExpression(KQL(
            f'{self.as_subexpression()} {"!has_cs" if case_sensitive else "!has"} {_to_kql(exp, True)}'
        ))


@_plain_expression(_KustoType.DATETIME)
class _DatetimeExpression(BaseExpression):
    @staticmethod
    def binary_op(left: _ExpressionType, operator: str, right: _ExpressionType) -> '_DatetimeExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, _KustoType.DATETIME)

    def __lt__(self, other: _DatetimeType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' < ', other)

    def __le__(self, other: _DatetimeType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' <= ', other)

    def __gt__(self, other: _DatetimeType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' > ', other)

    def __ge__(self, other: _DatetimeType) -> _BooleanExpression:
        return _BooleanExpression.binary_op(self, ' >= ', other)

    def __add__(self, other: _TimespanType) -> '_DatetimeExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _DatetimeExpression.binary_op(self, ' + ', other)

    def __sub__(self, other: Union[_DatetimeType, _TimespanType]) -> Union['_DatetimeExpression', '_TimespanExpression']:
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic

        # noinspection PyTypeChecker
        base_types = _get_base_types(other)
        possible_types = base_types & {_KustoType.DATETIME, _KustoType.TIMESPAN}
        assert len(possible_types) > 0, "Invalid type subtracted from datetime"
        if len(possible_types) == 2:
            return_type = AnyExpression
        else:
            return_type = _TimespanExpression if next(iter(possible_types)) == _KustoType.DATETIME else _DatetimeExpression
        return return_type(_DatetimeExpression.binary_op(self, ' - ', other))

    def __rsub__(self, other: _DatetimeType) -> '_TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _TimespanExpression.binary_op(other, ' - ', self)

    def between(self, lower: _DatetimeType, upper: _DatetimeType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return _BooleanExpression(KQL(f'{self.kql} between ({_to_kql(lower, True)} .. {_to_kql(upper, True)})'))

    def floor(self, round_to: _TimespanType) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/floorfunction
        """
        return _DatetimeExpression(KQL(f'floor({self.kql}, {_to_kql(round_to)})'))

    def bin(self, round_to: _TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return _DatetimeExpression(KQL(f'bin({self.kql}, {_to_kql(round_to)})'))

    def bin_at(self, round_to: _TimespanType, fixed_point: _DatetimeType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return _DatetimeExpression(KQL(f'bin_at({self.kql}, {_to_kql(round_to)}, {_to_kql(fixed_point)})'))

    def bin_auto(self) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return _DatetimeExpression(KQL(f'bin_auto({self.kql})'))

    def end_of_day(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofdayfunction
        """
        if offset is None:
            res = f'endofday({self.kql})'
        else:
            res = f'endofday({self.kql}, {_to_kql(offset)})'
        return _DatetimeExpression(KQL(res))

    def end_of_month(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofmonthfunction
        """
        if offset is None:
            res = f'endofmonth({self.kql})'
        else:
            res = f'endofmonth({self.kql}, {_to_kql(offset)})'
        return _DatetimeExpression(KQL(res))

    def end_of_week(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofweekfunction
        """
        if offset is None:
            res = f'endofweek({self.kql})'
        else:
            res = f'endofweek({self.kql}, {_to_kql(offset)})'
        return _DatetimeExpression(KQL(res))

    def end_of_year(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/endofyearfunction
        """
        if offset is None:
            res = f'endofyear({self.kql})'
        else:
            res = f'endofyear({self.kql}, {_to_kql(offset)})'
        return _DatetimeExpression(KQL(res))

    def format_datetime(self, format_string: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-datetimefunction
        """
        return _StringExpression(KQL(f'format_datetime({self.kql}, {_to_kql(format_string)})'))

    def get_month(self) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getmonthfunction
        """
        return _NumberExpression(KQL(f'getmonth({self.kql})'))

    def get_year(self) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/getyearfunction
        """
        return _NumberExpression(KQL(f'getyear({self.kql})'))

    def hour_of_day(self) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/hourofdayfunction
        """
        return _NumberExpression(KQL(f'hourofday({self.kql})'))

    def start_of_day(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofdayfunction
        """
        return _DatetimeExpression(KQL(f'startofday({self.kql})' if offset is None else f'startofday({self.kql}, {_to_kql(offset)})'))

    def start_of_month(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofmonthfunction
        """
        return _DatetimeExpression(KQL(f'startofmonth({self.kql})' if offset is None else f'startofmonth({self.kql}, {_to_kql(offset)})'))

    def start_of_week(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofweekfunction
        """
        return _DatetimeExpression(KQL(f'startofweek({self.kql})' if offset is None else f'startofweek({self.kql}, {_to_kql(offset)})'))

    def start_of_year(self, offset: _NumberType = None) -> '_DatetimeExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/startofyearfunction
        """
        return _DatetimeExpression(KQL(f'startofyear({self.kql})' if offset is None else 'startofyear({self.kql}, {to_kql(offset)})'))


@_plain_expression(_KustoType.TIMESPAN)
class _TimespanExpression(BaseExpression):
    @staticmethod
    def binary_op(left: _ExpressionType, operator: str, right: _ExpressionType) -> '_TimespanExpression':
        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(left, operator, right, _KustoType.TIMESPAN)

    def __add__(self, other: _TimespanType) -> '_TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _TimespanExpression.binary_op(self, ' + ', other)

    def __radd__(self, other: _TimespanType) -> '_TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _TimespanExpression.binary_op(other, ' + ', self)

    def __sub__(self, other: _TimespanType) -> '_TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _TimespanExpression.binary_op(self, ' - ', other)

    def __rsub__(self, other: _TimespanType) -> '_TimespanExpression':
        # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datetime-timespan-arithmetic
        return _TimespanExpression.binary_op(other, ' - ', self)

    def ago(self) -> _DatetimeExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/agofunction
        """
        return _DatetimeExpression(KQL(f'ago({_to_kql(self)})'))

    def bin(self, round_to: _TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binfunction
        """
        return _TimespanExpression(KQL(f'bin({self.kql}, {_to_kql(round_to)})'))

    def bin_at(self, round_to: _TimespanType, fixed_point: _TimespanType) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/binatfunction
        """
        return _TimespanExpression(KQL(f'bin_at({self.kql}, {_to_kql(round_to)}, {_to_kql(fixed_point)})'))

    def bin_auto(self) -> 'BaseExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bin-autofunction
        """
        return _TimespanExpression(KQL(f'bin_auto({self.kql})'))

    def format_timespan(self, format_string: _StringType) -> _StringExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/format-timespanfunction
        """
        return _StringExpression(KQL(f'format_timespan({self.kql}, {_to_kql(format_string)})'))

    def between(self, lower: _TimespanType, upper: _TimespanType) -> _BooleanExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/betweenoperator
        """
        return _BooleanExpression(KQL(f'{self.kql} between ({_to_kql(lower, True)} .. {_to_kql(upper, True)})'))


class _BaseDynamicExpression(BaseExpression):
    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> '_BaseDynamicExpression':
        assert cls is not _BaseDynamicExpression, "BaseDynamicExpression is abstract"
        return object.__new__(cls)

    def __getitem__(self, index: Union[_StringType, _NumberType]) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}[{_to_kql(index)}]'))

    def contains(self, other: _ExpressionType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        # For some reason Kusto does not accept 'in', and we need to use 'contains' as if this were a string
        if not isinstance(other, (BaseExpression, str)):
            # When 'other' is a literal, it has to be a string, because syntactically 'contains' works only on strings.
            other = str(_to_kql(other))
        return _BooleanExpression.binary_op(self, ' contains ', other)


@_plain_expression(_KustoType.ARRAY)
class _ArrayExpression(_BaseDynamicExpression):
    def __getitem__(self, index: _NumberType) -> 'AnyExpression':
        return super().__getitem__(index)

    # We would like to allow using len(), but Python requires it to return an int, so we can't
    def array_length(self) -> _NumberExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/arraylengthfunction
        """
        return _NumberExpression(KQL(f'array_length({self.kql})'))

    def array_contains(self, other: _ExpressionType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return self.contains(other)

    def assign_to_multiple_columns(self, *columns: '_AnyTypeColumn') -> '_AssignmentToMultipleColumns':
        return _AssignmentToMultipleColumns(columns, self)


@_plain_expression(_KustoType.MAPPING)
class _MappingExpression(_BaseDynamicExpression):
    def __getitem__(self, index: _StringType) -> 'AnyExpression':
        return super().__getitem__(index)

    def __getattr__(self, name: str) -> 'AnyExpression':
        return AnyExpression(KQL(f'{self.kql}.{name}'))

    def keys(self) -> _ArrayExpression:
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/bagkeysfunction
        """
        return _ArrayExpression(KQL(f'bag_keys({self.kql})'))

    def bag_contains(self, other: _ExpressionType) -> '_BooleanExpression':
        """
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/dynamic#operators-and-functions-over-dynamic-types
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/inoperator
        https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/datatypes-string-operators
        """
        return self.contains(other)


class _DynamicExpression(_ArrayExpression, _MappingExpression):
    def __getitem__(self, index: Union[_StringType, _NumberType]) -> 'AnyExpression':
        return super().__getitem__(index)


class AnyExpression(
    _NumberExpression, _BooleanExpression,
    _StringExpression, _DynamicExpression,
    _DatetimeExpression, _TimespanExpression
):
    pass


class AggregationExpression(BaseExpression):

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'AggregationExpression':
        assert cls is not AggregationExpression, "AggregationExpression is abstract"
        return object.__new__(cls)

    def assign_to(self, *columns: '_AnyTypeColumn') -> '_AssignmentFromAggregationToColumn':
        if len(columns) == 0:
            # Unspecified column name
            return _AssignmentFromAggregationToColumn(None, self)
        if len(columns) == 1:
            return _AssignmentFromAggregationToColumn(columns[0], self)
        raise ValueError("Aggregations cannot be assigned to multiple columns")

    def as_subexpression(self) -> KQL:
        return self.kql


@_aggregation_expression(_KustoType.BOOL)
class _BooleanAggregationExpression(AggregationExpression, _BooleanExpression):
    pass


@_aggregation_expression(*_NUMBER_TYPES)
class _NumberAggregationExpression(AggregationExpression, _NumberExpression):
    pass


@_aggregation_expression(_KustoType.STRING)
class _StringAggregationExpression(AggregationExpression, _StringExpression):
    pass


@_aggregation_expression(_KustoType.DATETIME)
class _DatetimeAggregationExpression(AggregationExpression, _DatetimeExpression):
    pass


@_aggregation_expression(_KustoType.TIMESPAN)
class _TimespanAggregationExpression(AggregationExpression, _TimespanExpression):
    pass


@_aggregation_expression(_KustoType.ARRAY)
class _ArrayAggregationExpression(AggregationExpression, _ArrayExpression):
    pass


@_aggregation_expression(_KustoType.MAPPING)
class _MappingAggregationExpression(AggregationExpression, _MappingExpression):
    pass


class _AnyAggregationExpression(AggregationExpression, AnyExpression):
    pass


class _AssignmentBase:
    _lvalue: Optional[KQL]
    _rvalue: KQL

    def __init__(self, lvalue: Optional[KQL], rvalue: _ExpressionType) -> None:
        self._lvalue = lvalue
        self._rvalue = _to_kql(rvalue)

    def to_kql(self) -> KQL:
        if self._lvalue is None:
            # Unspecified column name
            return self._rvalue
        return KQL(f'{self._lvalue} = {self._rvalue}')


class _AssignmentToSingleColumn(_AssignmentBase):
    def __init__(self, column: '_AnyTypeColumn', expression: _ExpressionType) -> None:
        super().__init__(column.kql, expression)


class _AssignmentFromColumnToColumn(_AssignmentToSingleColumn):
    def __init__(self, target: '_AnyTypeColumn', source: 'BaseColumn') -> None:
        super().__init__(target, source)


class _AssignmentToMultipleColumns(_AssignmentBase):
    def __init__(self, columns: Union[List['_AnyTypeColumn'], Tuple['_AnyTypeColumn']], expression: _ArrayType) -> None:
        super().__init__(KQL(f'({", ".join(c.kql for c in columns)})'), expression)


class _AssignmentFromAggregationToColumn(_AssignmentBase):
    def __init__(self, column: Optional['_AnyTypeColumn'], aggregation: AggregationExpression) -> None:
        super().__init__(None if column is None else column.kql, aggregation)


class BaseColumn(BaseExpression):
    _name: str

    # We would prefer to use 'abc' to make the class abstract, but this can be done only if there is at least one
    # abstract method, which we don't have here. Overriding __new___ is the next best solution.
    def __new__(cls, *args, **kwargs) -> 'BaseColumn':
        assert cls is not BaseColumn, "BaseColumn is abstract"
        return object.__new__(cls)

    def __init__(self, name: str, quote: bool = False) -> None:
        assert len(name) > 0, "Column name must not be empty"
        should_quote = quote or '.' in name or name in _KUSTO_KEYWORDS or name.isdigit()
        super().__init__(KQL(f"['{name}']" if should_quote else name))
        self._name = name

    def get_name(self) -> str:
        return self._name

    def as_subexpression(self) -> KQL:
        return self.kql

    def assign_to_single_column(self, column: '_AnyTypeColumn') -> '_AssignmentFromColumnToColumn':
        return _AssignmentFromColumnToColumn(column, self)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._name})'

    def __str__(self) -> str:
        return self._name


@_typed_column(*_NUMBER_TYPES)
class _NumberColumn(BaseColumn, _NumberExpression):
    pass


@_typed_column(_KustoType.BOOL)
class _BooleanColumn(BaseColumn, _BooleanExpression):
    pass


@_typed_column(_KustoType.ARRAY)
class _ArrayColumn(BaseColumn, _ArrayExpression):
    pass


@_typed_column(_KustoType.MAPPING)
class _MappingColumn(BaseColumn, _MappingExpression):
    pass


class _DynamicColumn(_ArrayColumn, _MappingColumn):
    pass


@_typed_column(_KustoType.STRING)
class _StringColumn(BaseColumn, _StringExpression):
    pass


@_typed_column(_KustoType.DATETIME)
class _DatetimeColumn(BaseColumn, _DatetimeExpression):
    pass


@_typed_column(_KustoType.TIMESPAN)
class _TimespanColumn(BaseColumn, _TimespanExpression):
    pass


class _SubtractableColumn(_NumberColumn, _DatetimeColumn, _TimespanColumn):
    @staticmethod
    def __resolve_type(type_to_resolve: Union['_NumberType', '_DatetimeType', '_TimespanType']) -> Optional[_KustoType]:
        # noinspection PyTypeChecker
        base_types = _get_base_types(type_to_resolve)
        possible_types = base_types & ({_KustoType.DATETIME, _KustoType.TIMESPAN} | _NUMBER_TYPES)
        assert len(possible_types) > 0, "Invalid type subtracted"
        if possible_types == _NUMBER_TYPES:
            return _KustoType.INT
        if len(possible_types) > 1:
            return None
        return next(iter(possible_types))

    def __sub__(self, other: Union['_NumberType', '_DatetimeType', '_TimespanType']) -> Union['_NumberExpression', '_TimespanExpression', 'AnyExpression']:
        resolved_type = self.__resolve_type(other)
        if resolved_type == _KustoType.DATETIME:
            # Subtracting a datetime can only result in a timespan
            resolved_type = _KustoType.TIMESPAN
        elif resolved_type == _KustoType.TIMESPAN:
            # Subtracting a timespan can result in either a timespan or a datetime
            resolved_type = None
        # Otherwise: subtracting a number can only result in a number

        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(self, ' - ', other, resolved_type)

    def __rsub__(self, other: Union['_NumberType', '_DatetimeType', '_TimespanType']) -> Union['_NumberExpression', '_TimespanExpression', 'AnyExpression']:
        resolved_type = self.__resolve_type(other)
        if resolved_type == _KustoType.DATETIME:
            # Subtracting from a datetime can result in either a datetime or a timespan
            resolved_type = None
        # Otherwise: subtracting from a number or a timespan can only result in a number or a timespan respectively

        # noinspection PyTypeChecker
        return BaseExpression.base_binary_op(other, ' - ', self, resolved_type)


class _AnyTypeColumn(_SubtractableColumn, _BooleanColumn, _DynamicColumn, _StringColumn):
    pass


class ColumnGenerator:
    def __getattr__(self, name: str) -> _AnyTypeColumn:
        return _AnyTypeColumn(name)

    def __getitem__(self, name: str) -> _AnyTypeColumn:
        return _AnyTypeColumn(name)

    # noinspection PyMethodMayBeStatic
    def of(self, name: str) -> _AnyTypeColumn:
        """
        Workaround in case automatic column name quoting fails
        """
        return _AnyTypeColumn(name, quote=True)


# Recommended usage: from pykusto.expressions import column_generator as col
# TODO: Is there a way to enforce this to be a singleton?
column_generator = ColumnGenerator()


class _ColumnToType(BaseExpression):
    def __init__(self, col: BaseColumn, kusto_type: _KustoType) -> None:
        super().__init__(KQL(f"{col.kql} to typeof({kusto_type.primary_name})"))


def _to_kql(obj: _ExpressionType, parentheses: bool = False) -> KQL:
    """
    Convert the given expression to KQL. If this is a subexpression of a greater expression, neighboring operators might
    take precedence over operators included in this expression, causing an incorrect evaluation order. This might not be a concern, if
    for example the expression is used as an argument to a function.
    If evaluation order is a concern, use `parentheses=True`, which will enclose this expression in parentheses (but only if it is
    a compound expression), thus guaranteeing correct evaluation order.

    :param obj: Expression to convert to KQL
    :return: KQL that represents the given expression
    """
    if isinstance(obj, BaseExpression):
        return obj.as_subexpression() if parentheses else obj.kql
    return _kql_converter.for_obj(obj)


def _expression_to_type(expression: _ExpressionType, type_registrar: _TypeRegistrar, fallback_type: Any) -> Any:
    types = set(type_registrar.registry[base_type] for base_type in _plain_expression.get_base_types(expression))
    return next(iter(types)) if len(types) == 1 else fallback_type


_typed_column.assert_all_types_covered()
_plain_expression.assert_all_types_covered()
_aggregation_expression.assert_all_types_covered()
