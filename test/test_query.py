from unittest import TestCase

from column import columnGenerator as Col
from query import Query, Order, Nulls


class TestQuery(TestCase):
    def test_where(self):
        print(Query().where(Col.foo > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).compile_all())
