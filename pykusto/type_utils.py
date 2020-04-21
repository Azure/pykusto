import json
from datetime import datetime, timedelta
from enum import Enum
from itertools import chain
from numbers import Number
from typing import Union, Mapping, Type, Dict, Callable, Tuple, List, Any, Set, NewType

# TODO: Unhandled data types: guid, decimal
PythonTypes = Union[str, int, float, bool, datetime, Mapping, List, Tuple, timedelta]
KQL = NewType('KQL', str)


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
    registry: Dict[KustoType, Callable[[Any], KQL]]

    def __init__(self, name: str) -> None:
        """
        :param name: Name is used for better logging and clearer errors
        """
        self.name = name
        self.registry = {}

    def __repr__(self) -> str:
        return self.name

    def __call__(self, *types: KustoType) -> Callable[[Callable[[Any], KQL]], Callable[[Any], KQL]]:
        def inner(wrapped: Callable[[Any], KQL]) -> Callable[[Any], KQL]:
            for t in types:
                previous = self.registry.setdefault(t, wrapped)
                if previous is not wrapped:
                    raise TypeError("{}: type already registered: {}".format(self, t.primary_name))
            wrapped._base_types = set(types)
            return wrapped

        return inner

    def for_obj(self, obj: PythonTypes) -> KQL:
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

    def for_type(self, t: Type[PythonTypes]) -> Callable[[Any], KQL]:
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


@kql_converter(KustoType.DATETIME)
def datetime_to_kql(dt: datetime) -> KQL:
    return KQL(dt.strftime('datetime(%Y-%m-%d %H:%M:%S.%f)'))


@kql_converter(KustoType.TIMESPAN)
def timedelta_to_kql(td: timedelta) -> KQL:
    hours, remainder = divmod(td.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return KQL('time({days}.{hours}:{minutes}:{seconds}.{microseconds})'.format(
        days=td.days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
        microseconds=td.microseconds,
    ))


@kql_converter(KustoType.ARRAY, KustoType.MAPPING)
def dynamic_to_kql(d: Union[Mapping, List, Tuple]) -> KQL:
    try:
        query = list(json.dumps(d))
    except TypeError:
        # Using exceptions as part of normal flow is not best practice, however in this case we have a good reason.
        # The given object might contain a non-primitive object somewhere inside it, and the only way to find it is to go through the entire hierarchy, which is exactly
        # what's being done in the process of json conversion.
        # Also keep in mind that exception handling in Python has no performance overhead (unlike e.g. Java).
        return build_dynamic(d)

    # Convert square brackets to round brackets (Issue #11)
    counter = 0
    prev = ""
    for i, c in enumerate(query):
        if counter == 0:
            if c == "[":
                query[i] = "("
            elif c == "]":
                query[i] = ")"
            elif c in ['"', '\''] and prev != "\\":
                counter += 1
        elif counter > 0:
            if c in ['"', '\''] and prev != "\\":
                counter -= 1
        prev = query[i]
    assert counter == 0
    return KQL("".join(query))


def build_dynamic(d: Union[Mapping, List, Tuple]) -> KQL:
    from pykusto.expressions import BaseExpression
    if isinstance(d, BaseExpression):
        return d.kql
    if isinstance(d, Mapping):
        return KQL('pack({})'.format(', '.join(map(build_dynamic, chain(*d.items())))))
    if isinstance(d, (List, Tuple)):
        return KQL('pack_array({})'.format(', '.join(map(build_dynamic, d))))
    from pykusto.expressions import to_kql
    return to_kql(d)


@kql_converter(KustoType.BOOL)
def bool_to_kql(b: bool) -> KQL:
    return KQL('true') if b else KQL('false')


@kql_converter(KustoType.STRING)
def str_to_kql(s: str) -> KQL:
    return KQL('"{}"'.format(s))


@kql_converter(KustoType.INT, KustoType.LONG, KustoType.REAL)
def number_to_kql(n: Number) -> KQL:
    return KQL(str(n))


# noinspection PyUnusedLocal
@kql_converter(KustoType.NULL)
def none_to_kql(n: None) -> KQL:
    return KQL("")
