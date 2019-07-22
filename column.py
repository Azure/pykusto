from typing import Union, Sequence, Any

from expression import Expression
from predicate import Predicate
from utils import KustoTypes, to_kql

ColumnOrKustoType = Union['Column', KustoTypes]
ExpressionTypes = Union['Column', KustoTypes, Expression]


class Column:
    kql_name: str
    name: str

    def __init__(self, name: str) -> None:
        self.name = name
        if '.' in name:
            self.kql_name = "['{}']".format(self.name)
        else:
            self.kql_name = self.name

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    @staticmethod
    def _subexpression_to_kql(obj: ExpressionTypes) -> str:
        if isinstance(obj, Column):
            return obj.kql_name
        if isinstance(obj, Expression):
            return '({})'.format(obj.kql)
        return to_kql(obj)

    def _generate_predicate(self, operator: str, other: ColumnOrKustoType) -> Predicate:
        return Predicate('{}{}{}'.format(self.kql_name, operator, self._subexpression_to_kql(other)))

    def _generate_expression(self, operator: str, other: ColumnOrKustoType) -> Expression:
        return Expression('{}{}{}'.format(self.kql_name, operator, self._subexpression_to_kql(other)))

    def __lt__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' < ', other)

    def __le__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' <= ', other)

    def __eq__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' == ', other)

    def __ne__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' != ', other)

    def __gt__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' > ', other)

    def __ge__(self, other: ColumnOrKustoType) -> Predicate:
        return self._generate_predicate(' >= ', other)

    def is_in(self, other: Sequence) -> Predicate:
        return self._generate_predicate(' in ', other)

    def contained(self, other: ColumnOrKustoType) -> Predicate:
        return Column._generate_predicate(other, ' in ', self)

    def __len__(self) -> Expression:
        """
        Works only on columns of type string
        """
        return Expression('string_size({})'.format(self.kql_name))

    def __contains__(self, other: Any) -> bool:
        """
        Deliberately not implemented, because "not in" inverses the result of this method, and there is no way to
        override it
        """
        raise NotImplementedError()

    def __add__(self, other: ColumnOrKustoType) -> Expression:
        return self._generate_expression(' + ', other)

    def __sub__(self, other: ColumnOrKustoType) -> Expression:
        return self._generate_expression(' - ', other)

    def __mul__(self, other: ColumnOrKustoType) -> Expression:
        return self._generate_expression(' * ', other)

    def __truediv__(self, other: ColumnOrKustoType) -> Expression:
        return self._generate_expression(' / ', other)

    def __mod__(self, other: ColumnOrKustoType) -> Expression:
        return self._generate_expression(' % ', other)


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
