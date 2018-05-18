# -*- coding: utf-8 -*-
"""Table scan tests

"""

from op.collate import Collate
from op.table_scan import TableScan


def test_scan():
    """Executes a scan. The results are then collated.

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('nation.csv', 'select * from S3Object')
    c = Collate()

    ts.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for _ in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert num_rows == 25
