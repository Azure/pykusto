from typing import Union

from predicate import Predicate
from utils import KustoTypes, to_kql


class Column:
    kql_name: str
    contains_dot: bool
    name: str

    def __init__(self, name: str) -> None:
        self.name = name
        self.contains_dot = '.' in name
        if self.contains_dot:
            self.kql_name = "['{}']".format(self.name)
        else:
            self.kql_name = self.name

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    def __eq__(self, other: Union['Column', KustoTypes]) -> Predicate:
        if isinstance(other, Column):
            other_kql = other.kql_name
        else:
            other_kql = to_kql(other)
        return Predicate('{}=={}'.format(self.name, other_kql))


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
