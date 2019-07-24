from pykusto import functions as f
from pykusto.assignments import AssignmentBase
from pykusto.column import column_generator as col
from pykusto.query import Query, Order, Nulls, JoinKind, JoinException, BagExpansion
from pykusto.tables import Table
from test.test_base import TestBase
from test.test_table import MockKustoClient


class TestQuery(TestBase):
    def test_sanity(self):
        # test concatenation #
        self.assertEqual(
            Query().where(col.foo > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render(),
            " | where foo > 4 | take 5 | sort by bar asc nulls last"
        )

    def test_where(self):
        self.assertEqual(
            Query().where(col.foo > 4).render(),
            " | where foo > 4"
        )

    def test_take(self):
        self.assertEqual(
            Query().take(3).render(),
            " | take 3"
        )

    def test_sort(self):
        self.assertEqual(
            Query().sort_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
            " | sort by foo desc nulls first"
        )

    def test_order(self):
        self.assertEqual(
            Query().order_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
            " | order by foo desc nulls first"
        )

    def test_order_expression_in_arg(self):
        self.assertEqual(
            Query().order_by(f.strlen(col.foo), order=Order.DESC, nulls=Nulls.FIRST).render(),
            " | order by strlen(foo) desc nulls first"
        )

    def test_sort_multiple_cols(self):
        self.assertEqual(
            Query().sort_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).then_by(col.bar, Order.ASC,
                                                                                  Nulls.LAST).render(),
            " | sort by foo desc nulls first, bar asc nulls last"
        )

    def test_top(self):
        self.assertEqual(
            Query().top(3, col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
            " | top 3 by foo desc nulls first"
        )

    def test_join_with_table(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', 'test_table')

        self.assertEqual(
            Query().where(col.foo > 4).take(5).join(
                Query(table), kind=JoinKind.INNER).on(col.col0).on(col.col1, col.col2).render(),
            " | where foo > 4 | take 5 | join kind=inner (test_table) on col0, $left.col1==$right.col2"
        )

    def test_join_with_table_and_query(self):
        mock_kusto_client = MockKustoClient()
        table = Table(mock_kusto_client, 'test_db', 'test_table')

        self.assertEqual(
            Query().where(col.foo > 4).take(5).join(
                Query(table).where(col.bla == 2).take(6), kind=JoinKind.INNER).on(col.col0).on(col.col1,
                                                                                               col.col2).render(),
            " | where foo > 4 | take 5 | join kind=inner (test_table | where bla == 2 | take 6) on col0, "
            "$left.col1==$right.col2"
        )

    def test_join_no_joined_table(self):
        self.assertRaises(
            JoinException,
            Query().where(col.foo > 4).take(5).join(
                Query().take(2), kind=JoinKind.INNER).on(col.col0).on(col.col1, col.col2).render
        )

    def test_join_no_on(self):
        self.assertRaises(
            JoinException,
            Query().where(col.foo > 4).take(5).join(
                Query().take(2), kind=JoinKind.INNER).render
        )

    def test_extend(self):
        self.assertEqual(
            Query().extend(AssignmentBase.assign(col.v1 + col.v2, col.sum), foo=col.bar * 4).take(5).render(),
            " | extend sum = (v1 + v2), foo = (bar * 4) | take 5",
        )

    def test_summarize(self):
        self.assertEqual(
            Query().summarize(f.count(col.foo), my_count=f.count(col.bar)).render(),
            " | summarize count(foo), my_count = (count(bar))",
        )

    def test_summarize_by(self):
        self.assertEqual(
            Query().summarize(f.count(col.foo), my_count=f.count(col.bar)).by(col.bla, f.bin(col.date, 1),
                                                                              time_range=f.bin(col.time, 10)).render(),
            " | summarize count(foo), my_count = (count(bar)) by bla, bin(date, 1), time_range = (bin(time, 10))",
        )

    def test_mv_expand(self):
        self.assertEqual(
            Query().mv_expand(col.a, col.b, col.c).render(),
            " | mv-expand a, b, c",
        )

    def test_mv_expand_args(self):
        self.assertEqual(
            Query().mv_expand(col.a, col.b, col.c, bag_expansion=BagExpansion.BAG, with_item_index=col.foo,
                              limit=4).render(),
            " | mv-expand bagexpansion=bag with_itemindex=foo a, b, c limit 4",
        )

    def test_mv_expand_no_args(self):
        self.assertRaises(
            ValueError,
            Query().mv_expand
        )

    def test_limit(self):
        self.assertEqual(
            Query().limit(3).render(),
            " | limit 3"
        )

    def test_sample(self):
        self.assertEqual(
            Query().sample(3).render(),
            " | sample 3"
        )

    def test_count(self):
        self.assertEqual(
            Query().count().render(),
            " | count"
        )
