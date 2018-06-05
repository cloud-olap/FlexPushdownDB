# -*- coding: utf-8 -*-
"""Project operator query tests

"""

from op.collate import Collate
from op.project import Project, ProjectExpression
from op.sql_table_scan import SQLTableScan
from op.tuple import LabelledTuple
from plan.query_plan import QueryPlan
from util.test_util import gen_test_id


def test_project_simple():
    """Tests a projection

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Simple Projection Test")

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              'select * from S3Object '
                                              'limit 3;',
                                              'ts',
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'p',
        False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['n_regionkey', 'n_nationkey', 'n_comment']

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == field_names

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['0', '0', ' haggle. carefully final deposits detect slyly agai']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['1', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['1', '2',
            'y alongside of the pending deposits. carefully special packages '
            'are about the ironic forges. slyly special ']

    # Write the metrics
    query_plan.print_metrics()


def test_project_empty():
    """Executes an projection query with no results returned. We test this as it's somewhat peculiar with s3 select,
     in so much as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS,
     no field names tuple should be present in the results.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Empty Projection Test")

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              "select * from S3Object "
                                              "limit 0;",
                                              'ts',
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'p',
        False))

    c = query_plan.add_operator(Collate('c', False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 0

    # Write the metrics
    query_plan.print_metrics()
