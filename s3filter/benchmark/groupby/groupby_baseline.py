# -*- coding: utf-8 -*-
"""Groupby Benchmark 

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.group import Group
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np

def main():
    file_format = 'groupby_benchmark/shards-zipf-10GB/groupby_powerlaw_data_{}.csv'
    run(['G2'], ['F0', 'F1'], parallel=True, use_pandas=True, buffer_size=0, table_parts=2, files = file_format)

def run(group_fields, agg_fields, parallel, use_pandas, buffer_size, table_parts, files):
    """
    
    :return: None
    """

    secure = False
    use_native = False
    print('')
    print("Groupby Benchmark, Baseline. Group Fields: {} Aggregate Fields: {}".format(group_fields, agg_fields))
    print("----------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)
    
    # Scan
    scan = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan(files.format(p),
                        "select * from S3Object;", use_pandas, secure, use_native,
                        'scan_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
  
    # Project
    def project_fn(df):
        df.columns = ['G0', 'G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9',
                      'F0', 'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8', 'F9',]
        df = df.filter(items=group_fields + agg_fields, axis=1)
        return df
   
    project_exprs = [ProjectExpression(lambda t_: t_['_{}'.format(x)], 'G{}'.format(x)) for x in range(10)] \
                  + [ProjectExpression(lambda t_: t_['_1{}'.format(x)], 'F{}'.format(x)) for x in range(10)]
    
    project = map(lambda p: 
                  query_plan.add_operator( 
                      Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
                  range(0, table_parts))

    # Groupby
    def groupby_fn(df):
        
        df[agg_fields] = df[agg_fields].astype(np.float)
        grouped = df.groupby(group_fields)
        agg_df = pd.DataFrame({f: grouped[f].sum() for n, f in enumerate(agg_fields)})
        return agg_df.reset_index()

    groupby = map(lambda p:
                  query_plan.add_operator(
                    Group( group_fields, [], 'groupby_{}'.format(p), query_plan, False, groupby_fn)),
                  range(0, table_parts))

    groupby_reduce = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce', query_plan, False, groupby_fn))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    #profile_path = '../benchmark-output/groupby/'
    #scan[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_scan_0" + ".prof"))
    #project[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_project_0" + ".prof"))
    #groupby[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_0" + ".prof"))
    #groupby_reduce.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_reduce" + ".prof"))
    #collate.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_collate" + ".prof"))

    map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    map(lambda (p, o): o.connect(groupby[p]), enumerate(project))
    map(lambda (p, o): o.connect(groupby_reduce), enumerate(groupby))
    groupby_reduce.connect(collate)
    

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
