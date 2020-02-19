from datetime import datetime, timedelta
from typing import Union, Mapping, Type, Dict, Callable, Tuple, List, Any, Set

# TODO: Unhandled data types: guid, decimal
PythonTypes = Union[str, int, float, bool, datetime, Mapping, List, Tuple, timedelta]


class KustoType:
    name: str
    internal_name: str
    dot_net_name: str
    python_types: Tuple[Type[PythonTypes]]

    def __init__(self, name: str, internal_name: str, dot_net_name: str, *python_types: Type[PythonTypes]) -> None:
        self.name = name
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


class KustoTypes:
    BOOL = KustoType('bool', 'I8', 'System.SByte', bool)
    DATETIME = KustoType('datetime', 'DateTime', 'System.DateTime', datetime)
    # noinspection PyTypeChecker
    DECIMAL = KustoType('decimal', 'Decimal', 'System.Data.SqlTypes.SqlDecimal', None)  # TODO
    ARRAY = KustoType('dynamic', 'Dynamic', 'System.Object', List, Tuple)
    MAPPING = KustoType('dynamic', 'Dynamic', 'System.Object', Mapping)
    # noinspection PyTypeChecker
    GUID = KustoType('guid', 'UniqueId', 'System.Guid', None)  # TODO
    INT = KustoType('int', 'I32', 'System.Int32', int)
    LONG = KustoType('long', 'I64', 'System.Int64', int)
    REAL = KustoType('real', 'R64', 'System.Double', float)
    STRING = KustoType('string', 'StringBuffer', 'System.String', str)
    TIMESPAN = KustoType('timespan', 'TimeSpan', 'System.TimeSpan', timedelta)
    NULL = KustoType('null', 'null', 'null', type(None))


# noinspection PyTypeChecker
ALL_TYPES: Tuple[KustoType] = tuple(getattr(KustoTypes, f) for f in dir(KustoTypes) if not f.startswith('__'))
INTERNAL_NAME_TO_TYPE: Dict[str, KustoType] = {t.internal_name: t for t in ALL_TYPES}
DOT_NAME_TO_TYPE: Dict[str, KustoType] = {t.dot_net_name: t for t in ALL_TYPES}


def get_base_types(obj: Any) -> Set[KustoType]:
    """
    For a given object, return the associated basic type, which is a member of `KustoTypes`

    :param obj: The given object for which the type is resolved
    :return: A type which is a member of `KustoTypes`
    """
    for kusto_type in ALL_TYPES:
        if kusto_type.is_type_of(obj):
            # The object is already a member of Kusto types
            return {kusto_type}
    # The object is one of the expression types decorated with a TypeRegistrar, therefore the original types are
    # recorded the field _base_types
    obj_type = type(obj)
    base_types = getattr(obj_type, '_base_types', None)
    if base_types is None:
        raise TypeError("get_base_types called for unsupported type: {}".format(obj_type.__name__))
    return base_types


class TypeRegistrar:
    """
    A factory for annotations that are used to create a mapping between Kusto types and python types and functions.
    Each annotation must be called with a Kusto type as a parameter. The `for_obj` and `for_type` methods
    can then be used to retrieve the python type or function corresponding to a given Kusto type.
    """
    name: str
    registry: Dict[KustoType, Callable]

    def __init__(self, name: str) -> None:
        """
        :param name: Name is used for better logging and clearer errors
        """
        self.name = name
        self.registry = {}

    def __repr__(self) -> str:
        return self.name

    def __call__(self, *types: KustoType) -> Callable:
        def inner(wrapped):
            for t in types:
                previous = self.registry.setdefault(t, wrapped)
                if previous is not wrapped:
                    raise TypeError("{}: type already registered: {}".format(self, t.name))
            wrapped._base_types = set(types)
            return wrapped

        return inner

    def for_obj(self, obj: PythonTypes) -> Callable:
        """
        Given an object of Kusto type, retrieve the python type or function associated with the object's type, and call
        it with the given object as a parameter

        :param obj: An object of Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if registered_type.is_type_of(obj):
                return registered_callable(obj)
        raise ValueError("{}: no registered callable for object {} of type {}".format(self, obj, type(obj).__name__))

    def for_type(self, t: Type[PythonTypes]) -> Callable:
        """
        Given a Kusto type, retrieve the associated python type or function

        :param t: A Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if registered_type.is_superclass_of(t):
                return registered_callable
        raise ValueError("{}: no registered callable for type {}".format(self, t.__name__))


kql_converter = TypeRegistrar("KQL Converter")
column = TypeRegistrar("Column")
plain_expression = TypeRegistrar("Plain expression")
aggregation_expression = TypeRegistrar("Aggregation expression")
