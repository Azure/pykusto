from typing import Union, Sequence, Any

from expression import Expression
from predicate import Predicate
from utils import KustoTypes, to_kql, KQL

ColumnOrKustoType = Union['Column', KustoTypes]
ExpressionTypes = Union['Column', KustoTypes, Expression]


class Column:
    kql_name: KQL
    name: str

    def __init__(self, name: str) -> None:
        self.name = name
        self.kql_name = KQL("['{}']".format(self.name) if '.' in name else self.name)

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    @staticmethod
    def _subexpression_to_kql(obj: ExpressionTypes) -> KQL:
        if isinstance(obj, Column):
            return obj.kql_name
        if isinstance(obj, Expression):
            return KQL('({})'.format(obj.kql))
        return to_kql(obj)

    def _generate_predicate(self, operator: str, other: ExpressionTypes) -> Predicate:
        return Predicate(KQL('{}{}{}'.format(self.kql_name, operator, self._subexpression_to_kql(other))))

    def _generate_expression(self, operator: str, other: ColumnOrKustoType) -> Expression:
        return Expression(KQL('{}{}{}'.format(self.kql_name, operator, self._subexpression_to_kql(other))))

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
        return Expression(KQL('string_size({})'.format(self.kql_name)))

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
