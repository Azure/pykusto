from expressions.boolean_expression import BooleanExpression
from expressions.number_expression import NumberExpression
from expressions.string_expression import StringExpression
from utils import KQL


class Column(NumberExpression, BooleanExpression, StringExpression):
    name: str

    def __init__(self, name: str, kql: KQL) -> None:
        super().__init__(KQL("['{}']".format(name) if '.' in name else name))
        self.name = name

    def __getattr__(self, name: str) -> 'Column':
        return Column(self.name + '.' + name)


class ColumnGenerator:
    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, name: str) -> Column:
        return Column[name]


# Recommended usage: from pykusto.column import columnGenerator as c
# TODO: Is there a way to enforce this to be a singleton?
columnGenerator = ColumnGenerator()
