from datetime import datetime, timedelta
from enum import Enum
from numbers import Number
from typing import Union, Mapping, Type, Dict, Callable, Tuple, List, Any, Set

KustoTypes = Union[str, Number, bool, datetime, Mapping, List, Tuple, timedelta]
# TODO: Unhandled data types: guid, decimal


def get_base_types(obj: Any) -> Set[Type[KustoTypes]]:
    """
    For a given object, return the associated basic type, which is a member of `KustoTypes`

    :param obj: The given object for which the type is resolved
    :return: A type which is a member of `KustoTypes`
    """
    obj_type = type(obj)
    for kusto_type in KustoTypes.__args__:
        if isinstance(obj, kusto_type):
            # The object is already a member of Kusto types
            return {kusto_type}
    # The object is one of the expression types decorated with a TypeRegistrar, therefore the original types are
    # recorded the field _base_types
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
    registry: Dict[Type[KustoTypes], Callable]

    def __init__(self, name: str) -> None:
        """
        :param name: Name is used for better logging and clearer errors
        """
        self.name = name
        self.registry = {}

    def __repr__(self) -> str:
        return self.name

    def __call__(self, *types: Type[KustoTypes]) -> Callable:
        def inner(wrapped):
            for t in types:
                previous = self.registry.setdefault(t, wrapped)
                if previous is not wrapped:
                    raise TypeError("{}: type already registered: {}".format(self, t.__name__))
            wrapped._base_types = set(types)
            return wrapped

        return inner

    def for_obj(self, obj: KustoTypes) -> Callable:
        """
        Given an object of Kusto type, retrieve the python type or function associated with the object's type, and call
        it with the given object as a parameter

        :param obj: An object of Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if isinstance(obj, registered_type):
                return registered_callable(obj)
        raise ValueError("{}: no registered callable for object {} of type {}".format(self, obj, type(obj).__name__))

    def for_type(self, t: Type[KustoTypes]) -> Callable:
        """
        Given a Kusto type, retrieve the associated python type or function

        :param t: A Kusto type
        :return: Associated python object
        """
        for registered_type, registered_callable in self.registry.items():
            if issubclass(t, registered_type):
                return registered_callable
        raise ValueError("{}: no registered callable for type {}".format(self, t.__name__))


kql_converter = TypeRegistrar("KQL Converter")
plain_expression = TypeRegistrar("Plain expression")
aggregation_expression = TypeRegistrar("Aggregation expression")


class TypeName(Enum):
    BOOL = 'bool'
    DATETIME = 'datetime'
    DECIMAL = 'decimal'
    DYNAMIC = 'dynamic'
    GUID = 'guid'
    INT = 'int'
    LONG = 'long'
    REAL = 'real'
    STRING = 'string'
    TIMESPAN = 'timespan'
