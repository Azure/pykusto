from expressions import NumberExpression, BooleanExpression, StringExpression, MappingExpression, ArrayExpression
from utils import KQL


class Column(NumberExpression, BooleanExpression, StringExpression, ArrayExpression, MappingExpression):
    name: str

    def __init__(self, name: str) -> None:
        super().__init__(KQL("['{}']".format(name) if '.' in name else name))
        self.name = name

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)

    def as_subexpression(self) -> KQL:
        return self.kql


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
