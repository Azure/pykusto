from typing import Union

from predicate import Predicate
from utils import KustoTypes


class Column:
    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    # Using a string as the return type spec works around the circular reference problem
    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    def __eq__(self, other: Union['Column', KustoTypes]) -> Predicate:
        if isinstance(other, Column):
            return Predicate('{}=={}'.format(self.name, other.name))
        # TODO: Handle the rest of the types


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
