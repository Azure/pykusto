from inspect import getmembers, isfunction

from pykusto import functions as f
from pykusto.column import columnGenerator as Col
from pykusto.query import Query, Order, Nulls
from test.test_base import TestBase


class TestQuery(TestBase):
    def test_sanity(self):
        self.assertEqual(
            " | where (abs(foo)) > 4 | take 5 | sort by bar asc nulls last",
            Query().where(f.abs(Col.foo) > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).render()
        )

    def test_function(self):
        for o in getmembers(f, isfunction):
            if isfunction(o[1]) and o[1].__module__ == f.__name__:
                try:
                    if " | where ({}(foo)) > 4 | take 5 | sort by bar asc nulls last".format(o[0]) != \
                            Query().where(o[1](Col.foo) > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).render():
                        print(o[1])
                except TypeError:
                    print(o[1])
