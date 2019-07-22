from typing import Any


class Column:
    def __init__(self, name: str) -> None:
        self.name = name


class ColumnGenerator:
    def __getattribute__(self, name: str) -> Any:
        return Column(name)


# TODO: Is there a way to enforce this to be a singleton?
generator = ColumnGenerator()
