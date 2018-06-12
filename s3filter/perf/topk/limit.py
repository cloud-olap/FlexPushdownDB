# -*- coding: utf-8 -*-
"""Limit top k query

Performs a partial "table" load by using the LIMIT keyword in S3 select. Note that the documentation at
https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html#s3-glacier-select-sql-reference-limit
does not specify precisely what limit does. Experimentation indicates it returns a number of rows less than the number
specified in the limit clause.

"""

import timeit
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan


def main():

    limit = 500

    # Query plan
    ts = SQLTableScan('supplier.csv', 'select * from S3Object limit {};'.format(limit), 'ts', False)
    c = Collate('c', False)

    ts.connect(c)

    start_time = timeit.default_timer()

    # Start the query
    ts.start()

    # Metrics
    num_rows = len(c.tuples())
    elapsed = timeit.default_timer() - start_time

    print ({'limit': {'row_count': num_rows, 'elapsed_seconds': elapsed}})


if __name__ == "__main__":
    main()
