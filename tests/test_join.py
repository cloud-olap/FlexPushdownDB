# -*- coding: utf-8 -*-
"""Composed operator query tests

"""
from op.collate import Collate
from op.join import Join
from op.table_scan import TableScan


def test_join():
    """Tests a join

    :return: None
    """

    num_rows = 0

    # Query plan
    ts1 = TableScan('supplier.csv', 'select * from S3Object;')
    ts2 = TableScan('nation.csv', 'select * from S3Object;')
    j = Join('supplier.csv', 3, 'nation.csv', 0)
    c = Collate()

    ts1.add_consumer(j)
    ts2.add_consumer(j)
    j.add_producer(ts1)
    j.add_producer(ts2)
    j.add_consumer(c)
    c.add_producer(j)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        assert t[3] == t[7]

    assert len(c.tuples()) == 10000
