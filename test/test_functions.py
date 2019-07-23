from inspect import getmembers, isfunction

from pykusto import functions as f
from pykusto.column import column_generator as col
from pykusto.query import Query, Order, Nulls
from test.test_base import TestBase


class TestFunction(TestBase):
    def test_rendering(self):
        for o in getmembers(f, isfunction):
            if isfunction(o[1]) and o[1].__module__ == f.__name__:
                try:
                    if " | where ({}(foo)) > 4 | take 5 | sort by bar asc nulls last".format(o[0]) != \
                            Query().where(o[1](col.foo) > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render():
                        print(o[0])
                except TypeError:
                    print(o[0])
