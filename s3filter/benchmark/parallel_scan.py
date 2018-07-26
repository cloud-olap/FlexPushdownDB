import os
import sys

from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_parallel_scan import SQLTableParallelScan
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan


def main():
    parts = 32
    processes = 32
    query_plan = QueryPlan()

    # Query plan
    ts = query_plan.add_operator(
        SQLTableParallelScan('lineitem.csv',
                             #"SELECT * FROM S3Object WHERE l_orderkey = '18436'",
                             "SELECT * FROM S3Object",
                             'parallel_scan',
                             parts,
                             processes,
                             query_plan,
                             False))

    query_plan.execute()

if __name__ == "__main__":
    main()
