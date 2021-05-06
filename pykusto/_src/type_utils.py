from datetime import datetime, timedelta
from enum import Enum
from typing import Union, Mapping, Type, Dict, Callable, Tuple, List, Set, FrozenSet

PythonTypes = Union[str, int, float, bool, datetime, Mapping, List, Tuple, timedelta]


class _KustoType(Enum):
    """
    https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/
    Order of entries determines preference when converting Python to Kusto types
    """
    BOOL = ('bool', 'I8', 'System.SByte', bool)
    DATETIME = ('datetime', 'DateTime', 'System.DateTime', datetime)
    ARRAY = ('dynamic', 'Dynamic', 'System.Object', List, Tuple)
    MAPPING = ('dynamic', 'Dynamic', 'System.Object', Mapping)
    LONG = ('long', 'I64', 'System.Int64', int)
    INT = ('int', 'I32', 'System.Int32', int)
    REAL = ('real', 'R64', 'System.Double', float)
    STRING = ('string', 'StringBuffer', 'System.String', str)
    TIMESPAN = ('timespan', 'TimeSpan', 'System.TimeSpan', timedelta)
    DECIMAL = ('decimal', 'Decimal', 'System.Data.SqlTypes.SqlDecimal', int)
    GUID = ('guid', 'UniqueId', 'System.Guid')  # Not supported by Kusto yet

    # Deprecated types, kept here for back compatibility
    # https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/scalar-data-types/unsupported-data-types
    FLOAT = ('float', 'R32', 'System.Single', float)
    INT16 = ('int16', 'I16', 'System.Int16', int)
    UINT16 = ('uint16', 'UI16', 'System.UInt16', int)
    UINT32 = ('uint32', 'UI32', 'System.UInt32', int)
    UINT64 = ('uint64', 'UI64', 'System.UInt64', int)
    UINT8 = ('uint8', 'UI8', 'System.Byte', int)

    primary_name: str
    internal_name: str
    dot_net_name: str
    python_types: Tuple[PythonTypes, ...]

    def __init__(self, primary_name: str, internal_name: str, dot_net_name: str, *python_types: PythonTypes) -> None:
        self.primary_name = primary_name
        self.internal_name = internal_name
        self.dot_net_name = dot_net_name
        self.python_types = python_types

    def is_type_of(self, obj) -> bool:
        for python_type in self.python_types:
            if python_type is not None and isinstance(obj, python_type):
                return True
        return False

    def is_superclass_of(self, t: Type) -> bool:
        for python_type in self.python_types:
            if python_type is not None and issubclass(t, python_type):
                return True
        return False


_INTERNAL_NAME_TO_TYPE: Dict[str, _KustoType] = {t.internal_name: t for t in _KustoType}
_DOT_NAME_TO_TYPE: Dict[str, _KustoType] = {t.dot_net_name: t for t in _KustoType}
_NUMBER_TYPES: FrozenSet[_KustoType] = frozenset([
    _KustoType.INT, _KustoType.LONG, _KustoType.REAL, _KustoType.DECIMAL, _KustoType.FLOAT,
    _KustoType.INT16, _KustoType.UINT16, _KustoType.UINT32, _KustoType.UINT64, _KustoType.UINT8
])
_PYTHON_TYPE_TO_KUSTO_TYPE: Dict[Type, _KustoType] = {
    python_type: kusto_type
    for kusto_type in reversed(_KustoType)
    for python_type in kusto_type.python_types
}


class _TypeRegistrar:
    """
    A factory for annotations that are used to create a mapping between Kusto types and python types / functions.
    Each annotation must be called with a Kusto type as a parameter. The `for_obj` and `for_type` methods
    can then be used to retrieve the python type or function corresponding to a given Kusto type.
    """
    name: str
    registry: Dict[_KustoType, Union[Type, Callable]]

    def __init__(self, name: str) -> None:
        """
        :param name: Name is used for better logging and clearer errors
        """
        self.name = name
        self.registry = {}

    def __repr__(self) -> str:
        return self.name

    def __call__(self, *types: _KustoType) -> Callable[[Union[Type, Callable]], Union[Type, Callable]]:
        def inner(wrapped: Union[Type, Callable]) -> Union[Type, Callable]:
            for t in types:
                previous = self.registry.setdefault(t, wrapped)
                if previous is not wrapped:
                    raise TypeError(f"{self}: type already registered: {t.primary_name}")
            return wrapped

        return inner

    def for_obj(self, obj: PythonTypes) -> Union[Type, Callable]:
        """
        Given an object of Kusto type, retrieve the python type or function associated with the object's type, and call
        it with the given object as a parameter

        :param obj: An object of Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if registered_type.is_type_of(obj):
                return registered_callable(obj)
        raise ValueError(f"{self}: no registered callable for object {obj} of type {type(obj).__name__}")

    def for_type(self, t: Type[PythonTypes]) -> Union[Type, Callable]:
        """
        Given a Kusto type, retrieve the associated python type or function

        :param t: A Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if registered_type.is_superclass_of(t):
                return registered_callable
        raise ValueError(f"{self}: no registered callable for type {t.__name__}")

    def inverse(self, target_callable: Union[Type, Callable]) -> Set[_KustoType]:
        result: Set[_KustoType] = set()
        for kusto_type, associated_callable in self.registry.items():
            if isinstance(target_callable, associated_callable):
                result.add(kusto_type)
        return result

    def get_base_types(self, obj: Union[Type, Callable]) -> Set[_KustoType]:
        """
        For a given object, return the associated basic type, which is a member of :class:`KustoType`

        :param obj: The given object for which the type is resolved
        :return: A type which is a member of `KustoType`
        """
        for kusto_type in _KustoType:
            if kusto_type.is_type_of(obj):
                # The object is already a member of Kusto types
                return {kusto_type}
        # The object is one of the expression types decorated with a TypeRegistrar, therefore the original types are
        base_types: Set[_KustoType] = self.inverse(obj)
        assert len(base_types) > 0, f"get_base_types called for unsupported type: {type(obj).__name__}"
        return base_types

    def assert_all_types_covered(self) -> None:
        missing = set(t for t in _KustoType if len(t.python_types) > 0) - set(self.registry.keys())
        assert len(missing) == 0, [t.name for t in missing]


_kql_converter = _TypeRegistrar("KQL Converter")
_typed_column = _TypeRegistrar("Column")
_plain_expression = _TypeRegistrar("Plain expression")
_aggregation_expression = _TypeRegistrar("Aggregation expression")


def _get_base_types(obj: Union[Type, Callable]) -> Set[_KustoType]:
    """
    A registrar-agnostic version of TypeRegistrar.get_base_types
    """
    for kusto_type in _KustoType:
        if kusto_type.is_type_of(obj):
            # The object is already a member of Kusto types
            return {kusto_type}
    for type_registrar in (_plain_expression, _aggregation_expression, _typed_column):
        base_types = type_registrar.inverse(obj)
        if len(base_types) > 0:
            break
    assert len(base_types) > 0, f"get_base_types called for unsupported type: {type(obj).__name__}"
    return base_types
