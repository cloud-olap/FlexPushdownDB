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
    j = Join(JoinExpression('supplier.csv', '_3', 'nation.csv', '_0'), 'j', False)
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

    field_names = ['supplier.csv._0', 'supplier.csv._1', 'supplier.csv._2', 'supplier.csv._3', 'supplier.csv._4',
                   'supplier.csv._5', 'supplier.csv._6',
                   'nation.csv._0', 'nation.csv._1', 'nation.csv._2', 'nation.csv._3']

    assert len(c.tuples()) == 10000 + 1

    assert c.tuples()[0] == field_names

    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = LabelledTuple(t, field_names)
            assert lt['supplier.csv._3'] == lt['nation.csv._0']
