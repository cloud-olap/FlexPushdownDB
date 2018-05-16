# -*- coding: utf-8 -*-
"""Top k query tests

"""
from op.collate import Collate
from op.table_scan import TableScan
from op.top import Top


def test_limit_topk():
    """Executes a top k query by using S3 select's limit clause. The results are then collated.

    :return: None
    """

    limit = 500
    num_rows = 0

    # Query plan
    ts = TableScan('customer.csv', 'select * from S3Object limit {};'.format(limit))
    c = Collate()

    ts.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == limit


def test_abort_topk():
    """Executes a top k query using the top operator (which stops the scan once it has reached the target
    number of tuples). The results are then collated.

    :return: None
    """

    limit = 500
    num_rows = 0

    # Query plan
    ts = TableScan('customer.csv', 'select * from S3Object;')
    t = Top(limit)
    c = Collate()

    ts.connect(t)
    t.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == limit
