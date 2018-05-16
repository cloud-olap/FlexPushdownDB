# -*- coding: utf-8 -*-
"""Group by query tests

"""

from op.collate import Collate
from op.group import Group
from op.table_scan import TableScan


def test_group_count():
    """Tests a group by query with a count aggregate

    :return: None
    """

    num_rows = 0

    # Query plan
    # select s_nationkey, count(s_suppkey) from supplier.csv group by s_nationkey
    ts = TableScan('supplier.csv', 'select * from S3Object;')
    g = Group(3, 0, str, 'COUNT')
    c = Collate()

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    nation_24 = filter(lambda t: t[0] == '24', c.tuples)[0]
    assert nation_24[1] == 393
    assert num_rows == 25


def test_group_sum():
    """Tests a group by query with a sum aggregate

    :return: None
    """

    num_rows = 0

    # Query plan
    # select s_nationkey, sum(float(s_acctbal)) from supplier.csv group by s_nationkey
    ts = TableScan('supplier.csv', 'select * from S3Object;')
    g = Group(3, 5, float, 'SUM')
    c = Collate()

    ts.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples:
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    nation_24 = filter(lambda t: t[0] == '24', c.tuples)[0]
    assert round(nation_24[1], 2) == 1833872.56
    assert num_rows == 25
