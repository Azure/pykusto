from inspect import getmembers, isfunction

from pykusto import functions as f
from pykusto.assignments import AssigmentBase
from pykusto.column import column_generator as col
from pykusto.query import Query, Order, Nulls, JoinKind
from test.test_base import TestBase


class TestQuery(TestBase):
    def test_sanity(self):
        self.assertEqual(
            Query().where(col.foo > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render(),
            " | where foo > 4 | take 5 | sort by bar asc nulls last"
        )

    def test_join(self):
        self.assertEqual(
            Query().where(col.foo > 4).take(5).join(
                Query().take(2), kind=JoinKind.INNER).on(col.col0).on(col.col1, col.col2).render(),
            " | where foo > 4 | take 5 | join kind=inner ( | take 2) on col0, $left.col1==$right.col2"
        )

    def test_function(self):
        for o in getmembers(f, isfunction):
            if isfunction(o[1]) and o[1].__module__ == f.__name__:
                try:
                    if " | where ({}(foo)) > 4 | take 5 | sort by bar asc nulls last".format(o[0]) != \
                            Query().where(o[1](col.foo) > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render():
                        print(o[1])
                except TypeError:
                    print(o[1])

    def test_extend(self):
        self.assertEqual(
            Query().extend(AssigmentBase.assign(col.v1 + col.v2, col.sum), foo=col.bar * 4).take(5).render(),
            " | extend sum = (v1 + v2), foo = (bar * 4) | take 5",
        )
