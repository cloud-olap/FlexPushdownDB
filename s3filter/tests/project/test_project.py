# -*- coding: utf-8 -*-
"""Project operator query tests

"""

import os
import pstats
from datetime import datetime

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.random_table_scan import RandomIntColumnDef, RandomDateColumnDef, RandomStringColumnDef, \
    RandomTableScan
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_project_simple():
    """Tests a projection

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              'select * from S3Object '
                                              'limit 3;',
                                              'ts', query_plan,
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'p', query_plan,
        False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    field_names = ['n_regionkey', 'n_nationkey', 'n_comment']

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == field_names

    assert c.tuples()[1] == ['0', '0', ' haggle. carefully final deposits detect slyly agai']
    assert c.tuples()[2] == ['1', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert c.tuples()[3] == ['1', '2',
                             'y alongside of the pending deposits. carefully special packages '
                             'are about the ironic forges. slyly special ']

    # Write the metrics
    query_plan.print_metrics()


def test_pandas_project_simple():
    """Tests a projection

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              'select * from S3Object '
                                              'limit 3;', True,
                                              'ts', query_plan,
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'p', query_plan,
        False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    field_names = ['n_regionkey', 'n_nationkey', 'n_comment']

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == field_names

    assert c.tuples()[1] == ['0', '0', ' haggle. carefully final deposits detect slyly agai']
    assert c.tuples()[2] == ['1', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert c.tuples()[3] == ['1', '2',
                             'y alongside of the pending deposits. carefully special packages '
                             'are about the ironic forges. slyly special ']

    # Write the metrics
    query_plan.print_metrics()


def test_project_empty():
    """Executes an projection query with no results returned. We tst this as it's somewhat peculiar with s3 select,
     in so much as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS,
     no field names tuple should be present in the results.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              "select * from S3Object "
                                              "limit 0;",
                                              'ts', query_plan,
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'p', query_plan,
        False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 0

    # Write the metrics
    query_plan.print_metrics()


def test_project_perf():
    """Executes a projection over many source rows to examine performance.

    :return: None
    """

    num_rows = 10000
    profile_file_name = os.path.join(ROOT_DIR, "../tests-output/" + gen_test_id() + ".prof")

    query_plan = QueryPlan(is_async=False, buffer_size=0)

    # Query plan
    random_col_defs = [
        RandomIntColumnDef(0, 9),
        RandomStringColumnDef(10, 20),
        RandomDateColumnDef(datetime.strptime('2017-01-01', '%Y-%m-%d'),
                            datetime.strptime('2018-01-01', '%Y-%m-%d'))
    ]

    random_table_scan = query_plan.add_operator(
        RandomTableScan(num_rows,
                        random_col_defs,
                        'random_table_scan', query_plan,
                        False))

    project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'r_0'),
            ProjectExpression(lambda t_: t_['_1'], 'r_1'),
            ProjectExpression(lambda t_: t_['_2'], 'r_2')
        ],
        'project', query_plan,
        False))

    project.set_profiled(True, profile_file_name)

    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    random_table_scan.connect(project)
    project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # collate.print_tuples()

    # Write the metrics
    s = pstats.Stats(profile_file_name)
    s.strip_dirs().sort_stats("time").print_stats()
    query_plan.print_metrics()

    # Assert the results
    assert len(collate.tuples()) == num_rows + 1
