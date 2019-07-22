from abc import abstractmethod

from column import Column
from predicate import Predicate


class Query:
    def __init__(self, head : 'Query' = None) -> None:
        self.head = head

    def where(self, predicate: Predicate) -> 'Query':
        return WhereQuery(self, predicate)

    def take(self, num_rows : int):
        return TakeQuery(self, num_rows)

    def sort_by(self, col : Column):
        return SortQuery(self, col)

    def project(self) -> 'Query':
        pass

    @abstractmethod
    def compile(self) -> str:
        pass

    def compile_all(self) -> str:
        if self.head is None:
            return ""
        else:
            return "{} | {}".format(self.head.compile_all(), self.compile())


class WhereQuery(Query):
    def __init__(self, head : 'Query', predicate : 'Predicate'):
        super(WhereQuery, self).__init__(head)
        self.predicate = predicate

    def compile(self):
        return 'where {}'.format(self.predicate)


class TakeQuery(Query):
    def __init__(self, head : 'Query', num_rows : int):
        super(TakeQuery, self).__init__(head)
        self.num_rows = num_rows

    def compile(self):
        return 'take {}'.format(self.num_rows)


# TODO asc/desc, nulls
class SortQuery(Query):
    def __init__(self, head: 'Query', col: 'Column'):
        super(SortQuery, self).__init__(head)
        self.col = col

    def compile(self):
        return 'sort by {}'.format(self.col)









# example usage: print(Query().where(Predicate('bla')).take(5).sort_by(Column('bla')).compile_all())