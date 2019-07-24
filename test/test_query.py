from pykusto import functions as f
from pykusto.client import PyKustoClient
from pykusto.expressions import AssignmentBase
from pykusto.expressions import column_generator as col
from pykusto.query import Query, Order, Nulls, JoinKind, JoinException, BagExpansion
from test.test_base import TestBase
from test.test_table import MockKustoClient


class TestQuery(TestBase):
    def test_sanity(self):
        # test concatenation #
        self.assertEqual(
            " | where foo > 4 | take 5 | sort by bar asc nulls last",
            Query().where(col.foo > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render(),
        )

    def test_add_queries(self):
        query = Query().where(col.foo > 4) + Query().take(5) + Query().sort_by(col.bar, Order.ASC, Nulls.LAST)
        self.assertEqual(
            " | where foo > 4 | take 5 | sort by bar asc nulls last",
            query.render(),
        )

    def test_where(self):
        self.assertEqual(
            " | where foo > 4",
            Query().where(col.foo > 4).render(),
        )

    def test_take(self):
        self.assertEqual(
            " | take 3",
            Query().take(3).render(),
        )

    def test_sort(self):
        self.assertEqual(
            " | sort by foo desc nulls first",
            Query().sort_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_order(self):
        self.assertEqual(
            " | order by foo desc nulls first",
            Query().order_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_order_expression_in_arg(self):
        self.assertEqual(
            " | order by strlen(foo) desc nulls first",
            Query().order_by(f.strlen(col.foo), order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_sort_multiple_cols(self):
        self.assertEqual(
            " | sort by foo desc nulls first, bar asc nulls last",
            Query().sort_by(col.foo, order=Order.DESC, nulls=Nulls.FIRST).then_by(col.bar, Order.ASC,
                                                                                  Nulls.LAST).render(),
        )

    def test_top(self):
        self.assertEqual(
            " | top 3 by foo desc nulls first",
            Query().top(3, col.foo, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_join_with_table(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']

        self.assertEqual(
            " | where foo > 4 | take 5 | join kind=inner (test_table) on col0, $left.col1==$right.col2",
            Query().where(col.foo > 4).take(5).join(
                Query(table), kind=JoinKind.INNER).on(col.col0).on(col.col1, col.col2).render(),
        )

    def test_join_with_table_and_query(self):
        mock_kusto_client = MockKustoClient()
        table = PyKustoClient(mock_kusto_client)['test_db']['test_table']

        self.assertEqual(
            " | where foo > 4 | take 5 | join kind=inner (test_table | where bla == 2 | take 6) on col0, "
            "$left.col1==$right.col2",
            Query().where(col.foo > 4).take(5).join(
                Query(table).where(col.bla == 2).take(6), kind=JoinKind.INNER).on(col.col0).on(col.col1,
                                                                                               col.col2).render(),
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
            " | extend sum = (v1 + v2), foo = (bar * 4) | take 5",
            Query().extend(AssignmentBase.assign(col.v1 + col.v2, col.sum), foo=col.bar * 4).take(5).render(),
        )

    def test_summarize(self):
        self.assertEqual(
            " | summarize count(foo), my_count = (count(bar))",
            Query().summarize(f.count(col.foo), my_count=f.count(col.bar)).render(),
        )

    def test_summarize_by(self):
        self.assertEqual(
            " | summarize count(foo), my_count = (count(bar)) by bla, bin(date, 1), time_range = (bin(time, 10))",
            Query().summarize(f.count(col.foo), my_count=f.count(col.bar)).by(col.bla, f.bin(col.date, 1),
                                                                              time_range=f.bin(col.time, 10)).render(),
        )

    def test_mv_expand(self):
        self.assertEqual(
            " | mv-expand a, b, c",
            Query().mv_expand(col.a, col.b, col.c).render(),
        )

    def test_mv_expand_args(self):
        self.assertEqual(
            " | mv-expand bagexpansion=bag with_itemindex=foo a, b, c limit 4",
            Query().mv_expand(col.a, col.b, col.c, bag_expansion=BagExpansion.BAG, with_item_index=col.foo,
                              limit=4).render(),
        )

    def test_mv_expand_no_args(self):
        self.assertRaises(
            ValueError,
            Query().mv_expand
        )

    def test_limit(self):
        self.assertEqual(
            " | limit 3",
            Query().limit(3).render(),
        )

    def test_sample(self):
        self.assertEqual(
            " | sample 3",
            Query().sample(3).render(),
        )

    def test_count(self):
        self.assertEqual(
            " | count",
            Query().count().render(),
        )

    def test_project_away(self):
        self.assertEqual(
            " | project-away a, bc",
            Query().project_away(col.a, col.bc).render(),
        )

    def test_project_away_wildcard(self):
        self.assertEqual(
            " | project-away a, b*",
            Query().project_away(col.a, "b*").render(),
        )

    def test_project_rename(self):
        self.assertEqual(
            " | project-rename a = b, c = d",
            Query().project_rename(a=col.b, c=col.d).render(),
        )
