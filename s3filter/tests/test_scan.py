# -*- coding: utf-8 -*-
"""Table scan tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.table_scan import TableScan
from s3filter.op.tuple import LabelledTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_scan_simple():
    """Executes a scan. The results are then collated.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Simple Scan Test")

    # Query plan
    ts = query_plan.add_operator(
        TableScan('nation.csv',
                  'ts',
                  False))
    c = query_plan.add_operator(
        Collate('c', False))

    ts.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 25 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['0', 'ALGERIA', '0', ' haggle. carefully final deposits detect slyly agai']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['1', 'ARGENTINA', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['2', 'BRAZIL', '1',
            'y alongside of the pending deposits. carefully special packages are about '
            'the ironic forges. slyly special ']

    # Write the metrics
    query_plan.print_metrics()


def test_scan_empty():
    """Executes a scan where no records are returned. We tst this as it's somewhat peculiar with s3 select, in so much
    as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
    should be present in the results.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan("Empty Scan Test")

    # Query plan
    ts = query_plan.add_operator(
        SQLTableScan('nation.csv',
                     "select * from s3object limit 0",
                     'ts',
                     False))
    c = query_plan.add_operator(
        Collate('c', False))

    ts.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 0

    # Write the metrics
    query_plan.print_metrics()
