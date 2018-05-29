# -*- coding: utf-8 -*-
"""Join query tests

"""

from op.collate import Collate
from op.join import Join, JoinExpression
from op.table_scan import TableScan
from op.tuple import LabelledTuple


def test_join():
    """Tests a join

    :return: None
    """

    # Query plan
    ts1 = TableScan('supplier.csv', 'select * from S3Object;', 'ts1', False)
    ts2 = TableScan('nation.csv', 'select * from S3Object;', 'ts2', False)
    j = Join(JoinExpression('ts1', '_3', 'ts2', '_0'), 'j', False)
    c = Collate('c', False)

    ts1.connect(j)
    ts2.connect(j)
    j.connect(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['ts1._0', 'ts1._1', 'ts1._2', 'ts1._3', 'ts1._4',
                   'ts1._5', 'ts1._6',
                   'ts2._0', 'ts2._1', 'ts2._2', 'ts2._3']

    assert len(c.tuples()) == 10000 + 1

    assert c.tuples()[0] == field_names

    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = LabelledTuple(t, field_names)
            assert lt['ts1._3'] == lt['ts2._0']
