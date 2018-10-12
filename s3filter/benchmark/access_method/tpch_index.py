# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.table_range_access import TableRangeAccess
from s3filter.op.aggregate import Aggregate

from s3filter.query.tpch import get_file_key
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np
import os

def main():
    run(True, True, 0, 1, 0, 901.04, 1) 

def run(parallel, use_pandas, buffer_size, table_parts, lower, upper, sf, nthreads = 16):
    
    secure = False
    use_native = False
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(None, is_async=parallel, buffer_size=buffer_size)
    assert sf == 1 or sf == 10
    
    # Scan Index Files
    index_scan = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan('tpch-sf{}/lineitem_sharded/index/index_l_extendedprice.csv.{}'.format(sf, p if sf==1 else p+1),
                        "select first_byte, last_byte "
                        " from S3Object "
                        " where cast(col_values as float) >= {} and cast(col_values as float) <= {};".format(lower, upper),
                        use_pandas, secure, use_native,
                        'index_scan_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
 
    # Range accesses 
    range_access = map(lambda p: 
               query_plan.add_operator(
                    TableRangeAccess(get_file_key('lineitem', True, p, sf=sf),
                        use_pandas, secure, use_native,
                        'range_access_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))

    map( lambda o: o.set_nthreads(nthreads), range_access )
    
    # Aggregation
    def agg_fun(df):
        return pd.DataFrame( { 'count' : [len(df)] } ) 

    aggregate = query_plan.add_operator(
                    Aggregate([], True, 'agg', query_plan, False, agg_fun))
    
    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(range_access[p]), enumerate(index_scan))
    map(lambda (p, o): o.connect(aggregate), enumerate(range_access))
    aggregate.connect(collate)

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

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

if __name__ == "__main__":
    main()
