# -*- coding: utf-8 -*-
"""Sort query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sort import Sort, SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_sort_asc():
    """Executes a sorted query. The results are collated.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv',
                                              'select * from S3Object '
                                              'limit 3;',
                                              False, 'ts', query_plan,
                                              False))

    s = query_plan.add_operator(Sort([SortExpression('_5', float, 'ASC')], 's', query_plan, False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    ts.connect(s)
    s.connect(c)

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert c.tuples()[1] == ['2', 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', '5', '15-679-861-2259', '4032.68',
                             ' slyly bold instructions. idle dependen']
    assert c.tuples()[2] == ['3', 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', '1', '11-383-516-1199',
                             '4192.40',
                             'blithely silent requests after the express dependencies are sl']
    assert c.tuples()[3] == ['1', 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', '17', '27-918-335-1736',
                             '5755.94',
                             'each slyly above the careful']

    # Write the metrics
    query_plan.print_metrics()


def test_sort_desc():
    """Executes a sorted query. The results are collated.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv',
                                              'select * from S3Object '
                                              'limit 3;',
                                              False, 'ts', query_plan,
                                              False))

    s = query_plan.add_operator(Sort([SortExpression('_5', float, 'DESC')], 's', query_plan, False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    ts.connect(s)
    s.connect(c)

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert c.tuples()[1] == ['1', 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', '17', '27-918-335-1736',
                             '5755.94', 'each slyly above the careful']
    assert c.tuples()[2] == ['3', 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', '1', '11-383-516-1199',
                             '4192.40', 'blithely silent requests after the express dependencies are sl']
    assert c.tuples()[3] == ['2', 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', '5', '15-679-861-2259', '4032.68',
                             ' slyly bold instructions. idle dependen']

    # Write the metrics
    query_plan.print_metrics()


def test_sort_empty():
    """Executes a sorted query with no results returned. We tst this as it's somewhat peculiar with s3 select, in so much
    as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
    should be present in the results.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv',
                                              'select * from S3Object '
                                              'limit 0;',
                                              False, 'ts', query_plan,
                                              False))

    s = query_plan.add_operator(Sort([SortExpression('_5', float, 'ASC')], 's', query_plan, False))

    c = query_plan.add_operator(Collate('c', query_plan, False))

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    ts.connect(s)
    s.connect(c)

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
