# -*- coding: utf-8 -*-
"""Top k query tests

"""
from op.collate import Collate
from op.join import Join
from op.sort import Sort
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

    TODO: This is just testing joins at the moment

    :return: None
    """

    limit = 5
    num_rows = 0

    # Query plan
    ts1 = TableScan('supplier.csv', 'select * from S3Object;')
    ts2 = TableScan('nation.csv', 'select * from S3Object;')
    j = Join('supplier.csv', 3, 'nation.csv', 0)
    c = Collate()

    ts1.connect(j)
    ts2.connect(j)
    j.connect(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    for t in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        assert t[3] == t[7]

    assert len(c.tuples) == 10000
