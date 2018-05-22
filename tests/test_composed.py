# -*- coding: utf-8 -*-
"""Composed operator query tests

"""

from op.collate import Collate
from op.join import Join, JoinExpression
from op.sort import Sort, SortExpression
from op.table_scan import TableScan
from op.top import Top
from op.tuple import LabelledTuple


def test_sort_topk():
    """Executes a sorted top k query, which must use the top operator as record sorting can't be pushed into s3. The
    results are collated.

    :return: None
    """

    limit = 5

    # Query plan
    ts = TableScan('supplier.csv', 'select * from S3Object;')
    s = Sort([
        SortExpression('_5', float, 'ASC')
    ])
    t = Top(limit)
    c = Collate()

    ts.connect(s)
    s.connect(t)
    t.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert len(c.tuples()) == limit + 1

    assert c.tuples()[0] == field_names

    prev = None
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))
        if num_rows > 1:
            if prev is None:
                prev = t
            else:
                lt = LabelledTuple(t, field_names)
                prev_lt = LabelledTuple(prev, field_names)
                assert float(lt['_5']) > float(prev_lt['_5'])


def test_join_topk():
    """Tests a top k with a join

    :return: None
    """

    limit = 5

    # Query plan
    ts1 = TableScan('supplier.csv', 'select * from S3Object;')
    ts2 = TableScan('nation.csv', 'select * from S3Object;')
    j = Join(JoinExpression('supplier.csv', '_3', 'nation.csv', '_0'))
    t = Top(limit)
    c = Collate()

    ts1.add_consumer(j)
    ts2.add_consumer(j)
    j.add_producer(ts1)
    j.add_producer(ts2)
    j.add_consumer(t)
    t.add_producer(j)
    t.add_consumer(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['supplier.csv._0', 'supplier.csv._1', 'supplier.csv._2', 'supplier.csv._3', 'supplier.csv._4',
                   'supplier.csv._5', 'supplier.csv._6', 'nation.csv._0', 'nation.csv._1', 'nation.csv._2',
                   'nation.csv._3']

    assert len(c.tuples()) == limit + 1

    assert c.tuples()[0] == field_names

    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = LabelledTuple(t, field_names)
            assert lt['supplier.csv._3'] == lt['nation.csv._0']
