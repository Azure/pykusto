from pykusto.column import column_generator as col
from pykusto.query import Query
from test.test_base import TestBase


class TestExpressions(TestBase):
    def test_contains(self):
        self.assertEqual(
            ' | where foo contains "bar"',
            Query().where(col.foo.contains('bar')).render(),
        )
