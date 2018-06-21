# -*- coding: utf-8 -*-
"""Composed operator query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.hash_join import HashJoin
from s3filter.op.join_expression import JoinExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sort import Sort, SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.top import Top
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_sort_topk():
    """Executes a sorted top k query, which must use the top operator as record sorting can't be pushed into s3. The
    results are collated.

    :return: None
    """

    limit = 5

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))
    s = query_plan.add_operator(Sort([
        SortExpression('_5', float, 'ASC')
    ], 's', False))
    t = query_plan.add_operator(Top(limit, 't', False))
    c = query_plan.add_operator(Collate('c', False))

    ts.connect(s)
    s.connect(t)
    t.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert len(c.tuples()) == limit + 1

    assert c.tuples()[0] == field_names

    prev = None
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        if num_rows > 1:
            if prev is None:
                prev = t
            else:
                lt = IndexedTuple.build(t, field_names)
                prev_lt = IndexedTuple.build(prev, field_names)
                assert float(lt['_5']) > float(prev_lt['_5'])

    # Write the metrics
    query_plan.print_metrics()


def test_join_topk():
    """Tests a top k with a join

    :return: None
    """

    limit = 5

    query_plan = QueryPlan()

    # Query plan
    ts1 = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts1', False))
    ts1_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'ts1_project', False))
    ts2 = query_plan.add_operator(SQLTableScan('nation.csv', 'select * from S3Object;', 'ts2', False))
    ts2_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'ts2_project', False))
    j = query_plan.add_operator(HashJoin(JoinExpression('s_nationkey', 'n_nationkey'), 'j', False))
    t = query_plan.add_operator(Top(limit, 't', False))
    c = query_plan.add_operator(Collate('c', False))

    ts1.connect(ts1_project)
    ts2.connect(ts2_project)
    j.connect_left_producer(ts1_project)
    j.connect_right_producer(ts2_project)
    j.connect(t)
    t.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    c.print_tuples()

    field_names = ['s_nationkey', 'n_nationkey']

    assert len(c.tuples()) == limit + 1

    assert c.tuples()[0] == field_names

    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = IndexedTuple.build(t, field_names)
            assert lt['s_nationkey'] == lt['n_nationkey']

    # Write the metrics
    query_plan.print_metrics()
