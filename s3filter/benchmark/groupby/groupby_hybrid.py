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
from s3filter.op.groupby_filter_build import GroupbyFilterBuild
from s3filter.op.groupby_decoder import GroupbyDecoder
import s3filter.util.constants
import pandas as pd
import numpy as np
import sys

def main():
    file_format = 'groupby_benchmark/shards-zipf-10GB/groupby_powerlaw_data_{}.csv'
    run(['G2'], ['F0', 'F1'], parallel=True, use_pandas=True, buffer_size=0, table_parts=2, files=file_format)


def run(group_fields, agg_fields, parallel, use_pandas, buffer_size, table_parts, files, nlargest=6):
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

    ##########################
    ## Phase 1. Find out group names 
    ##########################
    # Scan
    scan_phase1 = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan(files.format(p),
                        "select {} from S3Object;".format(','.join(group_fields)), use_pandas, secure, use_native,
                        'scan_phase1_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
  
    # Project
    def project_fn(df):
        df.columns = group_fields
        return df
   
    project_exprs = [ProjectExpression(lambda t_: t_['_{}'.format(n)], v) for n, v in enumerate(group_fields)] 
    
    project = map(lambda p: 
                  query_plan.add_operator( 
                      Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
                  range(0, table_parts))

    # Groupby
    def groupby_fn(df):
        grouped = df.groupby(group_fields)
        agg_df = grouped.agg({ group_fields[0]: 'count'})
        agg_df.columns = ['_count']
        return agg_df.reset_index()
    
    groupby = map(lambda p:
                  query_plan.add_operator(
                    Group( group_fields, [], 'groupby_{}'.format(p), query_plan, False, groupby_fn)),
                  range(0, table_parts))

    def groupby_reduce_fn(df):
        return df.groupby(group_fields).sum().reset_index()

    groupby_reduce = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce', query_plan, False, groupby_reduce_fn))
   
    # GroupbyFilterBuild
    agg_exprs = [ ('SUM', 'CAST({} AS float)'.format(agg_field)) for agg_field in agg_fields ]
    
    groupby_filter_build = query_plan.add_operator(
                     GroupbyFilterBuild( group_fields, agg_fields, agg_exprs, 'groupby_filter_build', query_plan, False)) 
    
    groupby_filter_build.set_nlargest(nlargest)

    ##########################
    ## Phase 2. Perform aggregation at S3.
    ##########################
    
    # ========================== 
    # remote aggregation branch  
    # ========================== 
    # Scan (remote aggregation)
    scan_phase2_remote = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan(files.format(p),
                        "", use_pandas, secure, use_native,
                        'scan_phase2_remote_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
   
    groupby_decoder = map(lambda p:
                         query_plan.add_operator(
                            GroupbyDecoder(agg_fields, 'groupby_decoder_{}'.format(p), query_plan, False)),
                         range(0, table_parts))
   
    def groupby_fn_phase2_remote(df):
        df[agg_fields] = df[agg_fields].astype(np.float)
        grouped = df.groupby(group_fields)
        agg_df = pd.DataFrame({f: grouped[f].sum() for n, f in enumerate(agg_fields)})
        return agg_df.reset_index()

    groupby_reduce_phase2_remote = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce_phase2_remote', query_plan, False, groupby_fn_phase2_remote))
    
    #scan_phase1[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_phase1_0" + ".prof"))
    #scan_phase2[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_phase2_0" + ".prof"))
    
    # ========================== 
    # local aggregation branch  
    # ========================== 
    # Scan (local aggregation)
    scan_phase2_local = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan(files.format(p),
                        "", use_pandas, secure, use_native,
                        'scan_phase2_local_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
    
    # Project (phase 2 local aggregation)
    def project_fn_phase2_local(df):
        df.columns = group_fields + agg_fields
        return df

    project_exprs = [ProjectExpression(lambda t_: t_['_{}'.format(n)], v) for n, v in enumerate(group_fields)] 
    
    project_phase2_local = map(lambda p: 
                  query_plan.add_operator( 
                      Project(project_exprs, 'project_phase2_local_{}'.format(p), query_plan, False, project_fn_phase2_local)),
                  range(0, table_parts))

    # Groupby (phase 2 local aggregation)
    def groupby_fn_phase2_local(df):
        df[agg_fields] = df[agg_fields].astype(np.float)
        grouped = df.groupby(group_fields)
        agg_df = pd.DataFrame({f: grouped[f].sum() for n, f in enumerate(agg_fields)})
        return agg_df.reset_index()
    
    groupby_phase2_local = map(lambda p:
                  query_plan.add_operator(
                        Group( group_fields, [], 'groupby_phase2_local_{}'.format(p), query_plan, False, groupby_fn_phase2_local)),
                  range(0, table_parts))
    
    # Groupby reduce (phase 2 local aggregation)
    groupby_reduce_phase2_local = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce_phase2_local', query_plan, False, groupby_fn_phase2_local))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))
   
    # phase 1
    map(lambda (p, o): o.connect(project[p]), enumerate(scan_phase1))
    map(lambda (p, o): o.connect(groupby[p]), enumerate(project))
    map(lambda (p, o): o.connect(groupby_reduce), enumerate(groupby))
    groupby_reduce.connect(groupby_filter_build)
    
    # phase 2 remote aggregation
    map(lambda o: groupby_filter_build.connect(o, 0), scan_phase2_remote)
    map(lambda o: groupby_filter_build.connect(o, 1), groupby_decoder)
    map(lambda (p, o): o.connect(groupby_decoder[p]), enumerate(scan_phase2_remote))
    map(lambda (p, o): o.connect(groupby_reduce_phase2_remote), enumerate(groupby_decoder))
    groupby_reduce_phase2_remote.connect(collate)
    
    # phase 2 local aggregation
    map(lambda o: groupby_filter_build.connect(o, 2), scan_phase2_local)
    map(lambda (p, o): o.connect(project_phase2_local[p]), enumerate(scan_phase2_local))
    map(lambda (p, o): o.connect(groupby_phase2_local[p]), enumerate(project_phase2_local))
    map(lambda (p, o): o.connect(groupby_reduce_phase2_local), enumerate(groupby_phase2_local))
    groupby_reduce_phase2_local.connect(collate)

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
