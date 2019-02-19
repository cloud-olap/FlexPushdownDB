# -*- coding: utf-8 -*-
"""Groupby Benchmark 

"""

import os

import numpy as np
import pandas as pd

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.group import Group
from s3filter.op.groupby_decoder import GroupbyDecoder
from s3filter.op.groupby_filter_build import GroupbyFilterBuild
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id


def main():
    file_format = 'groupby_benchmark/shards-10GB/groupby_data_{}.csv'
    #file_format = 'groupby_benchmark/shards-10GB/groupby_data_{}.csv'
    run(['G2'], ['F0', 'F1'], parallel=True, use_pandas=True, buffer_size=0, table_parts=2, files=file_format, format_=Format.CSV)


def run(group_fields, agg_fields, parallel, use_pandas, buffer_size, table_parts, files, format_):
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
                        "select {} from S3Object;".format(','.join(group_fields)), format_, use_pandas, secure, use_native,
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
        return df.drop_duplicates()

    groupby = map(lambda p:
                  query_plan.add_operator(
                    Group( group_fields, [], 'groupby_{}'.format(p), query_plan, False, groupby_fn)),
                  range(0, table_parts))

    groupby_reduce = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce', query_plan, False, groupby_fn))
   
    # GroupbyFilterBuild
    agg_exprs = [ ('SUM', 'CAST({} AS float)'.format(agg_field)) for agg_field in agg_fields ]
    
    groupby_filter_build = query_plan.add_operator(
                     GroupbyFilterBuild( group_fields, agg_fields, agg_exprs, 'groupby_filter_build', query_plan, False)) 

    ##########################
    ## Phase 2. Perform aggregation at S3.
    ##########################
    # Scan
    scan_phase2 = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan('groupby_benchmark/shards-10GB/groupby_data_{}.csv'.format(p),
                        "", format_, use_pandas, secure, use_native,
                        'scan_phase2_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
    
    groupby_decoder = map(lambda p:
                         query_plan.add_operator(
                            GroupbyDecoder(agg_fields, 'groupby_decoder_{}'.format(p), query_plan, False)),
                         range(0, table_parts))
   
    def groupby_fn_phase2(df):
        #print df
        df[agg_fields] = df[agg_fields].astype(np.float)
        grouped = df.groupby(group_fields)
        agg_df = pd.DataFrame({f: grouped[f].sum() for n, f in enumerate(agg_fields)})
        return agg_df.reset_index()

    groupby_reduce_phase2 = query_plan.add_operator(
                     Group( group_fields, [], 'groupby_reduce_phase2', query_plan, False, groupby_fn_phase2))
    
    #scan_phase1[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_phase1_0" + ".prof"))
    #scan_phase2[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_phase2_0" + ".prof"))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))
   
    # phase 1
    map(lambda (p, o): o.connect(project[p]), enumerate(scan_phase1))
    map(lambda (p, o): o.connect(groupby[p]), enumerate(project))
    map(lambda (p, o): o.connect(groupby_reduce), enumerate(groupby))
    groupby_reduce.connect(groupby_filter_build)
    
    # phase 2
    map(lambda (p, o): groupby_filter_build.connect(o, 0), enumerate(scan_phase2))
    map(lambda (p, o): groupby_filter_build.connect(o, 1), enumerate(groupby_decoder))
    map(lambda (p, o): o.connect(groupby_decoder[p]), enumerate(scan_phase2))
    map(lambda (p, o): o.connect(groupby_reduce_phase2), enumerate(groupby_decoder))
    # map(lambda (p, o): groupby_reduce.connect(o), enumerate(groupby_decoder))
   
    groupby_reduce_phase2.connect(collate) 
    

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
