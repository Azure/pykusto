import json
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from numbers import Number
from typing import Tuple, Union, Mapping, List

from pykusto.keyword import Keyword
from pykusto.type_utils import kql_converter, KustoType, NUMBER_TYPES


class KQL(metaclass=ABCMeta):
    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()


class Word(KQL):
    def __init__(self, keyword: Keyword) -> None:
        self.keyword = keyword

    def __str__(self) -> str:
        return self.keyword.value()

    @staticmethod
    @kql_converter(KustoType.BOOL)
    def from_bool(b: bool) -> KQL:
        return Word(Keyword.TRUE) if b else Word(Keyword.FALSE)


@kql_converter(KustoType.STRING)
class LiteralString(KQL):
    def __init__(self, s) -> None:
        self.s = s

    def __str__(self) -> str:
        return f'"{self.s}"'


@kql_converter(*NUMBER_TYPES)
class LiteralNumber(KQL):
    def __init__(self, n: Number) -> None:
        self.n = n

    def __str__(self) -> str:
        return str(self.n)


class Function(KQL):
    def __init__(self, name: Keyword, *args: KQL) -> None:
        self.name = name
        self.args = args

    def __str__(self) -> str:
        return f'{self.name.value()}({", ".join(map(str, self.args))})'


class Operator(KQL):
    def __init__(self, name: Keyword) -> None:
        self.name = name

    def __str__(self) -> str:
        return f'| {self.name.value()}'


class Compound(KQL):
    def __init__(self, *components: KQL) -> None:
        self.components = components

    def __str__(self) -> str:
        return ' '.join(map(str, self.components))


class OperatorWithArgs(Compound):
    def __init__(self, name: Keyword, *args: KQL) -> None:
        # noinspection PyTypeChecker
        super().__init__(*((Operator(name), ) + args))


@kql_converter(KustoType.DATETIME)
class Datetime(KQL):
    def __init__(self, dt: datetime) -> None:
        self.dt = dt

    def __str__(self) -> str:
        return f'{Keyword.DATETIME.value()}({self.dt.strftime("%Y-%m-%d %H:%M:%S.%f")})'


@kql_converter(KustoType.TIMESPAN)
class Timedelta(KQL):
    def __init__(self, td: timedelta) -> None:
        self.td = td

    def __str__(self) -> str:
        hours, remainder = divmod(self.td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f'{Keyword.TIME}({self.td.days}.{hours}:{minutes}:{seconds}.{self.td.microseconds})'


@kql_converter(KustoType.ARRAY, KustoType.MAPPING)
class LiteralDynamic(KQL):
    def __init__(self, d: Union[Mapping, List, Tuple]) -> None:
        try:
            self.elements = list(json.dumps(d))
        except TypeError:
            # Using exceptions as part of normal flow is not best practice, however in this case we have a good reason.
            # The given object might contain a non-primitive object somewhere inside it, and the only way to find it is to go through the entire hierarchy, which is exactly
            # what's being done in the process of json conversion.
            # Also keep in mind that exception handling in Python has no performance overhead (unlike e.g. Java).
            raise ValueError("Literal dynamic cannot contain non-primitive objects. Instead use 'pack_dynamic'.")

    def __str__(self) -> str:
        # Convert square brackets to round brackets (Issue #11)
        counter = 0
        prev = ""
        for i, c in enumerate(self.elements):
            if counter == 0:
                if c == "[":
                    self.elements[i] = "("
                elif c == "]":
                    self.elements[i] = ")"
                elif c in ['"', '\''] and prev != "\\":
                    counter += 1
            elif counter > 0:
                if c in ['"', '\''] and prev != "\\":
                    counter -= 1
            prev = self.elements[i]
        assert counter == 0
        return "".join(self.elements)


kql_converter.assert_all_types_covered()
