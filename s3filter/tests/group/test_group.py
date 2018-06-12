# -*- coding: utf-8 -*-
"""Group by query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.group import Group
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.tuple import LabelledTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_group_count():
    """Tests a group by query with a count aggregate

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Count Group Test")

    # Query plan
    # select s_nationkey, count(s_suppkey) from supplier.csv group by s_nationkey
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))

    g = query_plan.add_operator(Group(['_3'],
                                      [
                                          AggregateExpression(AggregateExpression.COUNT, lambda t_: t_['_0'])
                                          # count(s_suppkey)
                                      ],
                                      'g',
                                      False))

    c = query_plan.add_operator(Collate('c', False))

    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1']

    assert c.tuples()[0] == field_names

    assert len(c.tuples()) == 25 + 1

    nation_24 = filter(lambda t: LabelledTuple(t, field_names)['_0'] == '24', c.tuples())[0]
    assert nation_24[1] == 393
    assert num_rows == 25 + 1

    # Write the metrics
    query_plan.print_metrics()


def test_group_sum():
    """Tests a group by query with a sum aggregate

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Sum Group Test")

    # Query plan
    # select s_nationkey, sum(float(s_acctbal)) from supplier.csv group by s_nationkey
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))

    g = query_plan.add_operator(Group(['_3'],
                                      [
                                          AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5']))
                                      ],
                                      'g',
                                      False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t_))

    field_names = ['_0', '_1']

    assert c.tuples()[0] == field_names

    assert len(c.tuples()) == 25 + 1

    nation_24 = filter(lambda t_: LabelledTuple(t_, field_names)['_0'] == '24', c.tuples())[0]
    assert round(nation_24[1], 2) == 1833872.56

    # Write the metrics
    query_plan.print_metrics()


def test_group_empty():
    """Executes a group where no records are returned. We tst this as it's somewhat peculiar with s3 select, in so much
    as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
    should be present in the results.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Empty Group Test")

    # Query plan
    # select s_nationkey, sum(float(s_acctbal)) from supplier.csv group by s_nationkey
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object limit 0;', 'ts', False))

    g = query_plan.add_operator(Group(['_3'],
                                      [
                                          AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5']))
                                      ],
                                      'g',
                                      False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1']

    assert c.tuples()[0] == field_names

    assert len(c.tuples()) == 0 + 1

    # Write the metrics
    query_plan.print_metrics()
