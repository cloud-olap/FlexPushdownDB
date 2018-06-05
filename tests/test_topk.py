# -*- coding: utf-8 -*-
"""Top k query tests

"""
from op.collate import Collate
from op.sql_table_scan import SQLTableScan
from op.top import Top
from plan.query_plan import QueryPlan
from util.test_util import gen_test_id


def test_limit_topk():
    """Executes a top k query by using S3 select's limit clause. The results are then collated.

    :return: None
    """

    limit = 500
    num_rows = 0

    query_plan = QueryPlan("Limit TopK Test")

    # Query plan
    ts = query_plan.add_operator(
        SQLTableScan('supplier.csv', 'select * from S3Object limit {};'.format(limit), 'ts', False))
    c = query_plan.add_operator(Collate('c', False))

    ts.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == limit + 1

    # Write the metrics
    query_plan.print_metrics()


def test_abort_topk():
    """Executes a top k query using the top operator (which stops the scan once it has reached the target
    number of tuples). The results are then collated.

    :return: None
    """

    limit = 500
    num_rows = 0

    query_plan = QueryPlan("Abort TopK Test")

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object;', 'ts', False))
    t = query_plan.add_operator(Top(limit, 't', False))
    c = query_plan.add_operator(Collate('c', False))

    ts.connect(t)
    t.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == limit + 1

    # Write the metrics
    query_plan.print_metrics()


def test_topk_empty():
    """Executes a topk query with no results returned. We test this as it's somewhat peculiar with s3 select, in so much
    as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
    should be present in the results.

    :return: None
    """

    limit = 500
    num_rows = 0

    query_plan = QueryPlan("Abort TopK Test")

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('supplier.csv', 'select * from S3Object limit 0', 'ts', False))
    t = query_plan.add_operator(Top(limit, 't', False))
    c = query_plan.add_operator(Collate('c', False))

    ts.connect(t)
    t.connect(c)

    # Write the plan graph
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == 0

    # Write the metrics
    query_plan.print_metrics()
