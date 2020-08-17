from os import linesep

import pandas as pd

from pykusto import PyKustoClient, Order, Nulls, JoinKind, Distribution, BagExpansion, column_generator as col, Functions as f, Query, JoinException
# noinspection PyProtectedMember
from pykusto._src.type_utils import _KustoType
from test.test_base import TestBase, mock_databases_response, MockKustoClient, mock_response
from test.test_base import mock_table as t, mock_columns_response
from test.udf import func, STRINGIFIED


class TestQuery(TestBase):
    def test_sanity(self):
        # test concatenation #
        self.assertEqual(
            "mock_table | where numField > 4 | take 5 | sort by stringField asc nulls last",
            Query(t).where(t.numField > 4).take(5).sort_by(t.stringField, Order.ASC, Nulls.LAST).render(),
        )

    def test_add_queries(self):
        query_a = Query(t).where(t.numField > 4)
        query_b = Query(t).take(5)
        query_c = Query(t).where(t.numField2 > 1).sort_by(t.stringField, Order.ASC, Nulls.LAST)
        query = query_a + query_b + query_c

        self.assertEqual(
            "mock_table | where numField > 4 | take 5 | where numField2 > 1 | sort by stringField asc nulls last",
            query.render(),
        )

        # make sure the originals didn't change
        self.assertEqual(
            "mock_table | where numField > 4",
            query_a.render(),
        )
        self.assertEqual(
            "mock_table | take 5",
            query_b.render(),
        )
        self.assertEqual(
            "mock_table | where numField2 > 1 | sort by stringField asc nulls last",
            query_c.render(),
        )

    def test_add_queries_with_table(self):
        table = PyKustoClient(MockKustoClient(columns_response=mock_columns_response([('numField', _KustoType.INT)])))['test_db']['mock_table']
        query_a = Query(table).where(table.numField > 4)
        query_b = Query(t).take(5).take(2).sort_by(t.stringField, Order.ASC, Nulls.LAST)
        query = query_a + query_b
        self.assertEqual(
            "mock_table | where numField > 4 | take 5 | take 2 | sort by stringField asc nulls last",
            query.render(),
        )

        # make sure the originals didn't change
        self.assertEqual(
            "mock_table | where numField > 4",
            query_a.render(),
        )
        self.assertEqual(
            "mock_table | take 5 | take 2 | sort by stringField asc nulls last",
            query_b.render(),
        )

    def test_add_queries_with_table_name(self):
        query_a = Query('mock_table').where(col.numField > 4)
        query_b = Query().take(5)
        query = query_a + query_b
        self.assertEqual(
            "mock_table | where numField > 4 | take 5",
            query.render(),
        )
        self.assertEqual(
            "mock_table",
            query.get_table_name(),
        )

        # make sure the originals didn't change
        self.assertEqual(
            "mock_table | where numField > 4",
            query_a.render(),
        )
        self.assertEqual(
            "mock_table",
            query_a.get_table_name(),
        )

        self.assertEqual(
            " | take 5",
            query_b.render(),
        )
        self.assertEqual(
            None,
            query_b.get_table_name(),
        )

    def test_pretty_render(self):
        query = Query('mock_table').where(col.numField > 4).take(5)
        self.assertEqual(
            "mock_table" + linesep +
            "| where numField > 4" + linesep +
            "| take 5",
            query.pretty_render(),
        )

    def test_where(self):
        self.assertEqual(
            "mock_table | where numField > 4",
            Query(t).where(t.numField > 4).render(),
        )

    def test_where_multiple_predicates(self):
        self.assertEqual(
            'mock_table | where boolField and (numField > numField2) and (stringField contains "hello")',
            Query(t).where(t.boolField, t.numField > t.numField2, t.stringField.contains('hello')).render(),
        )

    def test_where_no_predicates(self):
        self.assertEqual(
            'mock_table | project numField',
            Query(t).where().project(t.numField).render(),
        )

    def test_where_true_predicate(self):
        self.assertEqual(
            'mock_table | where boolField | project numField',
            Query(t).where(t.boolField, True).project(t.numField).render(),
        )

    def test_where_only_true_predicate(self):
        self.assertEqual(
            'mock_table | project numField',
            Query(t).where(True).project(t.numField).render(),
        )

    def test_where_false_predicate(self):
        self.assertEqual(
            'mock_table | where false | project numField',
            Query(t).where(t.boolField, False).project(t.numField).render(),
        )

    def test_where_not(self):
        self.assertEqual(
            "mock_table | where not(boolField)",
            Query(t).where(f.not_of(t.boolField)).render(),
        )

    def test_take(self):
        self.assertEqual(
            "mock_table | take 3",
            Query(t).take(3).render(),
        )

    def test_sort(self):
        self.assertEqual(
            "mock_table | sort by numField desc nulls first",
            Query(t).sort_by(t.numField, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_order(self):
        self.assertEqual(
            "mock_table | sort by numField desc nulls first",
            Query(t).order_by(t.numField, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_order_expression_in_arg(self):
        self.assertEqual(
            "mock_table | sort by strlen(stringField) desc nulls first",
            Query(t).order_by(f.strlen(t.stringField), order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_sort_multiple_cols(self):
        self.assertEqual(
            "mock_table | sort by stringField desc nulls first, numField asc nulls last",
            Query(t).sort_by(t.stringField, order=Order.DESC, nulls=Nulls.FIRST).then_by(t.numField, Order.ASC, Nulls.LAST).render(),
        )

    def test_no_params_for_sort(self):
        self.assertEqual(
            "mock_table | sort by numField, stringField",
            Query(t).sort_by(t.numField).then_by(t.stringField).render(),
        )
        self.assertEqual(
            "mock_table | sort by numField desc nulls first, stringField",
            Query(t).sort_by(t.numField, order=Order.DESC, nulls=Nulls.FIRST).then_by(t.stringField).render(),
        )

    def test_top(self):
        self.assertEqual(
            "mock_table | top 3 by numField desc nulls first",
            Query(t).top(3, t.numField, order=Order.DESC, nulls=Nulls.FIRST).render(),
        )

    def test_join_with_table(self):
        table = PyKustoClient(MockKustoClient(columns_response=mock_columns_response([('tableStringField', _KustoType.STRING), ('numField', _KustoType.INT)])))['test_db'][
            'mock_table']

        self.assertEqual(
            'mock_table | where numField > 4 | take 5 | join kind=inner (cluster("test_cluster.kusto.windows.net").database("test_db").table("mock_table")) '
            'on numField, $left.stringField==$right.tableStringField',
            Query(t).where(t.numField > 4).take(5).join(
                Query(table), kind=JoinKind.INNER
            ).on(t.numField).on(t.stringField, table.tableStringField).render(),
        )

    def test_join_with_table_and_query(self):
        table = PyKustoClient(MockKustoClient(columns_response=mock_columns_response([
            ('tableStringField', _KustoType.STRING), ('numField', _KustoType.INT)
        ])))['test_db']['mock_table']

        self.assertEqual(
            'mock_table | where numField > 4 | take 5 | join kind=inner (cluster("test_cluster.kusto.windows.net").database("test_db").table("mock_table") | where numField == 2 '
            '| take 6) on numField, $left.stringField==$right.tableStringField',
            Query(t).where(t.numField > 4).take(5).join(
                Query(table).where(table.numField == 2).take(6), kind=JoinKind.INNER
            ).on(t.numField).on(t.stringField, table.tableStringField).render(),
        )

    def test_join_no_joined_table(self):
        self.assertRaises(
            JoinException("The joined query must have a table"),
            lambda: Query(t).where(t.numField > 4).take(5).join(Query().take(2), kind=JoinKind.INNER).on(t.numField).on(t.stringField, t.stringField2).render()
        )

    def test_join_no_on(self):
        self.assertRaises(
            JoinException("A call to join() must be followed by a call to on()"),
            Query(t).where(t.numField > 4).take(5).join(
                Query(t).take(2), kind=JoinKind.INNER).render
        )

    def test_extend(self):
        self.assertEqual(
            "mock_table | extend sumField = numField + numField2, foo = numField3 * 4 | take 5",
            Query(t).extend((t.numField + t.numField2).assign_to(col.sumField), foo=t.numField3 * 4).take(5).render(),
        )

    def test_extend_assign_to_multiple_columns(self):
        self.assertEqual(
            "mock_table | extend (newField1, newField2) = arrayField, shoo = numField * 4",
            Query(t).extend(t.arrayField.assign_to(col.newField1, col.newField2), shoo=t.numField * 4).render(),
        )

    def test_extend_assign_non_array_to_multiple_columns(self):
        self.assertRaises(
            ValueError("Only arrays can be assigned to multiple columns"),
            lambda: t.stringField.assign_to(col.newField1, col.newField2),
        )

    def test_extend_generate_column_name(self):
        self.assertEqual(
            "mock_table | extend numField + numField2, foo = numField3 * 4",
            Query(t).extend(t.numField + t.numField2, foo=t.numField3 * 4).render(),
        )

    def test_extend_build_dynamic(self):
        self.assertEqual(
            'mock_table | extend foo = pack("Name", stringField, "Roles", pack_array(stringField2, stringField3))',
            Query(t).extend(foo={'Name': t.stringField, 'Roles': [t.stringField2, t.stringField3]}).render(),
        )

    def test_summarize(self):
        self.assertEqual(
            "mock_table | summarize count(stringField), my_count = count(stringField2)",
            Query(t).summarize(f.count(t.stringField), my_count=f.count(t.stringField2)).render(),
        )

    def test_summarize_by(self):
        self.assertEqual(
            "mock_table | summarize count(stringField), my_count = count(stringField2) by boolField, bin(numField, 1), time_range = bin(dateField, 10)",
            Query(t).summarize(f.count(t.stringField), my_count=f.count(t.stringField2)).by(t.boolField, f.bin(t.numField, 1), time_range=f.bin(t.dateField, 10)).render(),
        )

    def test_summarize_by_expression(self):
        self.assertEqual(
            "mock_table | summarize count(stringField) by tostring(mapField)",
            Query(t).summarize(f.count(t.stringField)).by(f.to_string(t.mapField)).render(),
        )

    def test_mv_expand(self):
        self.assertEqual(
            "mock_table | mv-expand arrayField, arrayField2, arrayField3",
            Query(t).mv_expand(t.arrayField, t.arrayField2, t.arrayField3).render(),
        )

    def test_mv_expand_assign(self):
        self.assertEqual(
            "mock_table | mv-expand expanded_field = arrayField",
            Query(t).mv_expand(expanded_field=t.arrayField).render(),
        )

    def test_mv_expand_assign_to(self):
        self.assertEqual(
            "mock_table | mv-expand expanded_field = arrayField",
            Query(t).mv_expand(t.arrayField.assign_to(col.expanded_field)).render(),
        )

    def test_mv_expand_assign_to_with_assign_other_params(self):
        self.assertEqual(
            "mock_table | mv-expand bagexpansion=bag with_itemindex=foo expanded_field = arrayField, expanded_field2 = arrayField2 limit 4",
            Query(t).mv_expand(
                t.arrayField.assign_to(col.expanded_field), expanded_field2=t.arrayField2, bag_expansion=BagExpansion.BAG, with_item_index=col.foo, limit=4
            ).render(),
        )

    def test_mv_expand_assign_multiple(self):
        self.assertEqual(
            "mock_table | mv-expand expanded_field = arrayField, expanded_field2 = arrayField2",
            Query(t).mv_expand(expanded_field=t.arrayField, expanded_field2=t.arrayField2).render(),
        )

    def test_mv_expand_to_type(self):
        self.assertEqual(
            "mock_table | mv-expand arrayField to typeof(string), arrayField2 to typeof(int), arrayField3",
            Query(t).mv_expand(f.to_type(t.arrayField, _KustoType.STRING), f.to_type(t.arrayField2, _KustoType.INT), t.arrayField3).render(),
        )

    def test_mv_expand_args(self):
        self.assertEqual(
            "mock_table | mv-expand bagexpansion=bag with_itemindex=foo arrayField, arrayField2, arrayField3 limit 4",
            Query(t).mv_expand(t.arrayField, t.arrayField2, t.arrayField3, bag_expansion=BagExpansion.BAG, with_item_index=col.foo, limit=4).render(),
        )

    def test_mv_expand_no_args(self):
        self.assertRaises(
            ValueError("Please specify one or more columns for mv-expand"),
            Query(t).mv_expand
        )

    def test_limit(self):
        self.assertEqual(
            "mock_table | limit 3",
            Query(t).limit(3).render(),
        )

    def test_sample(self):
        self.assertEqual(
            "mock_table | sample 3",
            Query(t).sample(3).render(),
        )

    def test_count(self):
        self.assertEqual(
            "mock_table | count",
            Query(t).count().render(),
        )

    def test_project(self):
        self.assertEqual(
            "mock_table | project stringField, numField",
            Query(t).project(t.stringField, t.numField).render(),
        )

    def test_project_with_expression(self):
        self.assertEqual(
            "mock_table | project foo = numField * 4",
            Query(t).project(foo=t.numField * 4).render(),
        )

    def test_project_assign_to_multiple_columns(self):
        self.assertEqual(
            "mock_table | project (foo, bar) = arrayField",
            Query(t).project(t.arrayField.assign_to(col.foo, col.bar)).render(),
        )

    def test_project_unspecified_column(self):
        self.assertEqual(
            "mock_table | project numField + numField2",
            Query(t).project(t.numField + t.numField2).render(),
        )

    def test_project_away(self):
        self.assertEqual(
            "mock_table | project-away stringField, numField",
            Query(t).project_away(t.stringField, t.numField).render(),
        )

    def test_project_away_wildcard(self):
        self.assertEqual(
            "mock_table | project-away stringField, b*",
            Query(t).project_away(t.stringField, "b*").render(),
        )

    def test_project_rename(self):
        self.assertEqual(
            "mock_table | project-rename a = stringField, c = numField",
            Query(t).project_rename(a=t.stringField, c=t.numField).render(),
        )

    def test_custom(self):
        self.assertEqual(
            "mock_table | some custom query",
            Query(t).custom("some custom query").render(),
        )

    def test_distinct(self):
        self.assertEqual(
            "mock_table | distinct stringField, numField * 2",
            Query(t).distinct(t.stringField, t.numField * 2).render(),
        )

    def test_distinct_sample(self):
        self.assertEqual(
            "mock_table | sample-distinct 5 of stringField",
            Query(t).distinct(t.stringField).sample(5).render(),
        )

    def test_top_hitters(self):
        self.assertEqual(
            "mock_table | top-hitters 5 of stringField",
            Query(t).distinct(t.stringField).top_hitters(5).render(),
        )

    def test_top_hitters_by(self):
        self.assertEqual(
            "mock_table | top-hitters 5 of stringField by numField",
            Query(t).distinct(t.stringField).top_hitters(5).by(t.numField).render(),
        )

    def test_distinct_all(self):
        self.assertEqual(
            "mock_table | distinct *",
            Query(t).distinct_all().render(),
        )

    def test_evaluate(self):
        self.assertEqual(
            "mock_table | evaluate some_plugin(numField, 3)",
            Query(t).evaluate('some_plugin', t.numField, 3).render(),
        )

    def test_evaluate_with_distribution(self):
        self.assertEqual(
            "mock_table | evaluate hint.distribution=per_shard some_plugin(numField, 3)",
            Query(t).evaluate('some_plugin', t.numField, 3, distribution=Distribution.PER_SHARD).render(),
        )

    def test_udf(self):
        self.assertEqual(
            f"mock_table | evaluate python(typeof(*, StateZone:string), {STRINGIFIED})",
            Query(t).evaluate_udf(func, StateZone=_KustoType.STRING).render(),
        )

    def test_udf_no_extend(self):
        self.assertEqual(
            f"mock_table | evaluate python(typeof(StateZone:string), {STRINGIFIED})",
            Query(t).evaluate_udf(func, extend=False, StateZone=_KustoType.STRING).render(),
        )

    def test_bag_unpack(self):
        self.assertEqual(
            "mock_table | evaluate bag_unpack(mapField)",
            Query(t).bag_unpack(t.mapField).render(),
        )

    def test_bag_unpack_with_prefix(self):
        self.assertEqual(
            'mock_table | evaluate bag_unpack(mapField, "bar_")',
            Query(t).bag_unpack(t.mapField, 'bar_').render(),
        )

    def test_to_dataframe(self):
        rows = (['foo', 10], ['bar', 20], ['baz', 30])
        columns = ('stringField', 'numField')
        client = PyKustoClient(MockKustoClient(
            databases_response=mock_databases_response([('test_db', [('mock_table', [('stringField', _KustoType.STRING), ('numField', _KustoType.INT)])])]),
            main_response=mock_response(rows, columns),
        ))
        client.wait_for_items()
        table = client.test_db.mock_table
        self.assertTrue(
            pd.DataFrame(rows, columns=columns).equals(Query(table).take(10).to_dataframe())
        )
