from datetime import datetime, timedelta
from enum import Enum
from typing import Union, Mapping, Type, Dict, Callable, Tuple, List, Any, Set

# TODO: Unhandled data types: guid, decimal
PythonTypes = Union[str, int, float, bool, datetime, Mapping, List, Tuple, timedelta]


class KustoType(Enum):
    BOOL = ('bool', 'I8', 'System.SByte', bool)
    DATETIME = ('datetime', 'DateTime', 'System.DateTime', datetime)
    # noinspection PyTypeChecker
    DECIMAL = ('decimal', 'Decimal', 'System.Data.SqlTypes.SqlDecimal', None)  # TODO
    ARRAY = ('dynamic', 'Dynamic', 'System.Object', List, Tuple)
    MAPPING = ('dynamic', 'Dynamic', 'System.Object', Mapping)
    # noinspection PyTypeChecker
    GUID = ('guid', 'UniqueId', 'System.Guid', None)  # TODO
    INT = ('int', 'I32', 'System.Int32', int)
    LONG = ('long', 'I64', 'System.Int64', int)
    REAL = ('real', 'R64', 'System.Double', float)
    STRING = ('string', 'StringBuffer', 'System.String', str)
    TIMESPAN = ('timespan', 'TimeSpan', 'System.TimeSpan', timedelta)
    NULL = ('null', 'null', 'null', type(None))

    primary_name: str
    internal_name: str
    dot_net_name: str
    python_types: Tuple[Type[PythonTypes]]

    def __init__(self, primary_name: str, internal_name: str, dot_net_name: str, *python_types: Type[PythonTypes]) -> None:
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


INTERNAL_NAME_TO_TYPE: Dict[str, KustoType] = {t.internal_name: t for t in KustoType}
DOT_NAME_TO_TYPE: Dict[str, KustoType] = {t.dot_net_name: t for t in KustoType}


def get_base_types(obj: Any) -> Set[KustoType]:
    """
    For a given object, return the associated basic type, which is a member of :class:`KustoType`

    :param obj: The given object for which the type is resolved
    :return: A type which is a member of `KustoType`
    """
    for kusto_type in KustoType:
        if kusto_type.is_type_of(obj):
            # The object is already a member of Kusto types
            return {kusto_type}
    # The object is one of the expression types decorated with a TypeRegistrar, therefore the original types are
    # recorded the field _base_types
    obj_type = type(obj)
    base_types = getattr(obj_type, '_base_types', None)
    assert base_types is not None, "get_base_types called for unsupported type: {}".format(obj_type.__name__)
    return base_types


class TypeRegistrar:
    """
    A factory for annotations that are used to create a mapping between Kusto types and python types / functions.
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
        def inner(wrapped) -> Callable:
            for t in types:
                previous = self.registry.setdefault(t, wrapped)
                if previous is not wrapped:
                    raise TypeError("{}: type already registered: {}".format(self, t.primary_name))
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
typed_column = TypeRegistrar("Column")
plain_expression = TypeRegistrar("Plain expression")
aggregation_expression = TypeRegistrar("Aggregation expression")
