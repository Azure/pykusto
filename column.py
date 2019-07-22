
class Column:
    def __init__(self, name: str) -> None:
        self.name = name

    # Using a string as the return type spec works around the circular reference problem
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
