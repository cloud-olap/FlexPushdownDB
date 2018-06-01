# -*- coding: utf-8 -*-
"""Join query tests

"""

from op.collate import Collate
from op.join import Join, JoinExpression
from op.project import Project, ProjectExpr
from op.sql_table_scan import SQLTableScan
from op.tuple import LabelledTuple
from plan.query_plan import QueryPlan
from util.test_util import gen_test_id


def test_join_baseline():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan("Baseline Join Test")

    # Query plan
    ts1 = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts1', False))
    ts1_project = query_plan.add_operator(
        Project([ProjectExpr(lambda t_: t_['_3'], 's_nationkey')], 'ts1_project', False))
    ts2 = query_plan.add_operator(SQLTableScan('nation.csv', 'select * from S3Object;', 'ts2', False))
    ts2_project = query_plan.add_operator(
        Project([ProjectExpr(lambda t_: t_['_0'], 'n_nationkey')], 'ts2_project', False))
    j = query_plan.add_operator(Join(JoinExpression('s_nationkey', 'n_nationkey'), 'j', False))
    c = query_plan.add_operator(Collate('c', False))

    ts1.connect(ts1_project)
    ts2.connect(ts2_project)
    j.connect_left_producer(ts1_project)
    j.connect_right_producer(ts2_project)
    j.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['s_nationkey', 'n_nationkey']

    assert len(c.tuples()) == 10000 + 1

    assert c.tuples()[0] == field_names

    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = LabelledTuple(t, field_names)
            assert lt['s_nationkey'] == lt['n_nationkey']

    # Write the metrics
    query_plan.print_metrics()
