from pykusto.assignments import AssigmentBase
from pykusto.column import column_generator as col
from pykusto.query import Query, Order, Nulls, JoinKind, JoinException
from pykusto.tables import Table
from pykusto import functions as f
from test.test_base import TestBase
from test.test_table import MockKustoClient


class TestQuery(TestBase):
    def test_sanity(self):
        self.assertEqual(
            Query().where(col.foo > 4).take(5).sort_by(col.bar, Order.ASC, Nulls.LAST).render(),
            " | where foo > 4 | take 5 | sort by bar asc nulls last"
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
            Query().extend(AssigmentBase.assign(col.v1 + col.v2, col.sum), foo=col.bar * 4).take(5).render(),
            " | extend sum = (v1 + v2), foo = (bar * 4) | take 5",
        )

    def test_summarize(self):
        self.assertEqual(
            Query().summarize(f.count(col.foo), my_count=f.count(col.bar)).render(),
            " | summarize count(foo), my_count = (count(bar))",
        )
