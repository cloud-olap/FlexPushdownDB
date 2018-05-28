# -*- coding: utf-8 -*-
"""Sort query tests

"""

from op.collate import Collate
from op.sort import Sort, SortExpression
from op.table_scan import TableScan
from op.tuple import LabelledTuple


def test_sort_asc():
    """Executes a sorted query. The results are collated.

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('supplier.csv',
                   'select * from S3Object '
                   'limit 3;', 'ts', False)
    s = Sort([SortExpression('_5', float, 'ASC')], 's', False)
    c = Collate('c', False)

    ts.connect(s)
    s.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['2', 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', '5', '15-679-861-2259', '4032.68',
            ' slyly bold instructions. idle dependen']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['3', 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', '1', '11-383-516-1199', '4192.40',
            'blithely silent requests after the express dependencies are sl']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['1', 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', '17', '27-918-335-1736', '5755.94',
            'each slyly above the careful']


def test_sort_desc():
    """Executes a sorted query. The results are collated.

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('supplier.csv',
                   'select * from S3Object '
                   'limit 3;', 'ts', False)
    s = Sort([SortExpression('_5', float, 'DESC')], 's', False)
    c = Collate('c', False)

    ts.connect(s)
    s.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3', '_4', '_5', '_6']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['1', 'Supplier#000000001', ' N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ', '17', '27-918-335-1736', '5755.94',
            'each slyly above the careful']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['3', 'Supplier#000000003', 'q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3', '1', '11-383-516-1199', '4192.40',
            'blithely silent requests after the express dependencies are sl']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['2', 'Supplier#000000002', '89eJ5ksX3ImxJQBvxObC,', '5', '15-679-861-2259', '4032.68',
            ' slyly bold instructions. idle dependen']
