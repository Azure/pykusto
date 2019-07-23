from pykusto import functions as f
from pykusto.column import columnGenerator as Col
from pykusto.query import Query, Order, Nulls, JoinKind
from test.test_base import TestBase


class TestQuery(TestBase):
    def test_sanity(self):
        self.assertEqual(
            " | where (abs(foo)) > 4 | take 5 | sort by bar asc nulls last",
            Query().where(f.abs(Col.foo) > 4).take(5).sort_by(Col.bar, Order.ASC, Nulls.LAST).render()
        )

    def test_join(self):
        self.assertEqual(
            Query().where(Col.foo > 4).take(5).join(
                Query().take(2), kind=JoinKind.INNER).on(Col.col0).on(Col.col1, Col.col2).render(),
            " | where foo > 4 | take 5 | join kind=inner ( | take 2) on col0, $left.col1==$right.col2"
        )
