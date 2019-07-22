from typing import Union

from predicate import Predicate
from utils import KustoTypes, to_kql

ColumnOrKustoType = Union['Column', KustoTypes]


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

    @staticmethod
    def _to_kql(obj):
        if isinstance(obj, Column):
            return obj.kql_name
        return to_kql(obj)

    def _generate_predicate(self, operator: str, other: ColumnOrKustoType) -> Predicate:
        return Predicate('{}{}{}'.format(self.kql_name, operator, self._to_kql(other)))

    def __lt__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate('<', other)

    def __eq__(self, other: Union['Column', KustoTypes]) -> Predicate:
        return self._generate_predicate('==', other)


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
