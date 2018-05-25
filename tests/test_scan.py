# -*- coding: utf-8 -*-
"""Table scan tests

"""

from op.collate import Collate
from op.table_scan import TableScan
from op.tuple import LabelledTuple


def test_scan():
    """Executes a scan. The results are then collated.

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('nation.csv',
                   'select * from S3Object '
                   'limit 3;', 'ts', False)
    c = Collate()

    ts.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['0', 'ALGERIA', '0', ' haggle. carefully final deposits detect slyly agai']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['1', 'ARGENTINA', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['2', 'BRAZIL', '1', 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ']
