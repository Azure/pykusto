from unittest import TestCase

from column import columnGenerator as Col
from query import Query, Order, Nulls


class TestQuery(TestCase):
    def test_sanity(self):
        self.assertEqual(
            Query().where(Col.foo > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).compile_all(),
            " | where foo > 4 | take 5 | sort by bar asc nulls last"
        )
