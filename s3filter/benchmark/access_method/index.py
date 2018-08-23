# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.table_range_access import TableRangeAccess

from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np
import os

def main():
    
    run(True, True, 0, 2, 0.1, 'access_method_benchmark/shards-10GB', 16) 

def run(parallel, use_pandas, buffer_size, table_parts, perc, path, nthreads=16):
    
    secure = False
    use_native = False
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)
    
    # Scan Index Files
    upper = perc * 100
    index_scan = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan('{}/index/index_f0_{}.csv'.format(path, p),
                        "select first_byte, last_byte "
                        " from S3Object "
                        " where cast(name as float) < {};".format(upper),
                        use_pandas, secure, use_native,
                        'index_scan_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
 
    # Range accesses 
    range_access = map(lambda p: 
               query_plan.add_operator(
                    TableRangeAccess('{}/data_{}.csv'.format(path, p),
                        use_pandas, secure, use_native,
                        'range_access_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))

    map( lambda o: o.set_nthreads(nthreads), range_access )

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(range_access[p]), enumerate(index_scan))
    map(lambda (p, o): o.connect(collate), enumerate(range_access))

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    tuples = collate.tuples()

    #collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

if __name__ == "__main__":
    main()
