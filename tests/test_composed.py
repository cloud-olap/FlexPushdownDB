# -*- coding: utf-8 -*-
"""Composed operator query tests

"""

from op.collate import Collate
from op.join import Join
from op.sort import Sort
from op.table_scan import TableScan
from op.top import Top


def test_sort_topk():
    """Executes a sorted top k query, which must use the top operator as record sorting can't be pushed into s3. The
    results are collated.

    :return: None
    """

    limit = 5
    num_rows = 0

    # Query plan
    ts = TableScan('supplier.csv', 'select * from S3Object;')
    s = Sort(5, float, 'ASC')
    t = Top(limit)
    c = Collate()

    ts.connect(s)
    s.connect(t)
    t.connect(c)

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

    assert num_rows == limit


def test_join_topk():
    """Tests a top k with a join

    :return: None
    """

    limit = 5
    num_rows = 0

    # Query plan
    ts1 = TableScan('supplier.csv', 'select * from S3Object;')
    ts2 = TableScan('nation.csv', 'select * from S3Object;')
    j = Join('supplier.csv', 3, 'nation.csv', 0)
    t = Top(limit)
    c = Collate()

    ts1.set_consumer(j)
    ts2.set_consumer(j)
    j.set_producer1(ts1)
    j.set_producer2(ts2)
    j.set_consumer(t)
    t.set_producer(j)
    t.set_consumer(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    for t in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        assert t[3] == t[7]

    assert len(c.tuples) == limit
