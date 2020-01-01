import logging
from datetime import datetime, timedelta
from numbers import Number
from typing import Union, Mapping, Type, Dict, Callable, Any, Tuple, List

logger = logging.getLogger("pykusto")

KustoTypes = Union[str, Number, bool, datetime, Mapping, List, Tuple, timedelta]
# TODO: Unhandled data types: guid, decimal


class TypeRegistrar:
    registry: Dict[Type[KustoTypes], Callable]

    def __init__(self) -> None:
        self.registry = {}

    def __call__(self, *types: Type[KustoTypes]) -> Callable:
        def inner(wrapped):
            for t in types:
                self.registry[t] = wrapped
            return wrapped

        return inner

    def for_obj(self, obj: Any) -> Any:
        for registered_type, registered_callable in self.registry.items():
            if isinstance(obj, registered_type):
                return registered_callable(obj)
        raise ValueError("No registered callable for object {} of type {}".format(obj, type(obj).__name__))

    def for_type(self, t: Type[KustoTypes]) -> Callable:
        for registered_type, registered_callable in self.registry.items():
            if issubclass(t, registered_type):
                return registered_callable
        raise ValueError("No registered callable for type {}".format(t.__name__))


kql_converter = TypeRegistrar()
plain_expression = TypeRegistrar()
aggregation_expression = TypeRegistrar()
