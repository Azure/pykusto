from pykusto import functions as f
from pykusto.column import columnGenerator as Col
from pykusto.query import Query, Order, Nulls
from test.test_base import TestBase


class TestQuery(TestBase):
    def test_sanity(self):
        self.assertEqual(
            Query().where(f.abs(Col.foo) > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).render(),
            " | where foo > 4 | take 5 | sort by bar asc nulls last"
        )
