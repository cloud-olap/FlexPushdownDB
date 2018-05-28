# -*- coding: utf-8 -*-
"""Simple top k query

Performs a full "table" load and simply stops iterating through the response once the desired
record count limit has been reached. This is intended to demonstrate the worst case/most naive
method of loading of records.

"""

import timeit
from op.collate import Collate
from op.table_scan import TableScan
from op.top import Top


def main():

    limit = 500

    # Query plan
    ts = TableScan('supplier.csv', 'select * from S3Object;', 'ts', False)
    t = Top(limit)
    c = Collate()

    ts.connect(t)
    t.connect(c)

    start_time = timeit.default_timer()

    # Start the query
    ts.start()

    # Metrics
    num_rows = len(c.tuples())
    elapsed = timeit.default_timer() - start_time

    print ({'simple': {'row_count': num_rows, 'elapsed_seconds': elapsed}})


if __name__ == "__main__":
    main()
