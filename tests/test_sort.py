# -*- coding: utf-8 -*-
"""Sort query tests

"""
from op.collate import Collate
from op.sort import Sort
from op.table_scan import TableScan


def test_sort():
    """Executes a sorted query. The results are collated.

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('supplier.csv', 'select * from S3Object;')
    s = Sort(5, float, 'ASC')
    c = Collate()

    ts.connect(s)
    s.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    prev = None
    for t in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        if prev is None:
            prev = t
        else:
            assert float(t[5]) > float(prev[5])

    assert num_rows == 10000

