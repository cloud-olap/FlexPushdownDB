"""Top K baseline

"""
import os

import numpy as np

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.top import Top
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id


def main():
    path = 'topk_benchmark/10GB-100shards' 
    run('F0', 100, True, True, 'ASC', buffer_size=0, table_parts=1, path=path, format_=Format.CSV)

def run(sort_field, k, parallel, use_pandas, sort_order, buffer_size, table_parts, path, format_):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    secure = False
    use_native = False
    print('')
    print("Top K Benchmark, Filtered. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)
    
    # Scan
    scan = map(lambda p: 
               query_plan.add_operator(
                    SQLTableScan("{}/topk_data_{}.csv".format(path, p),
                        "select * from S3Object;", format_, use_pandas, secure, use_native,
                        'scan_{}'.format(p), query_plan,
                        False)),
               range(0, table_parts))
  
    # Project
    def project_fn(df):
        df.columns = ['F0', 'F1', 'F2']
        df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
        return df
   
    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)] 
    
    project = map(lambda p: 
                  query_plan.add_operator( 
                      Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
                  range(0, table_parts))

    # TopK
    sort_expr = SortExpression(sort_field, 'float', sort_order)
    topk = map(lambda p: 
               query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False)),
               range(0, table_parts))

    # TopK reduce
    topk_reduce = query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'topk_reduce', query_plan, False)) 

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    #profile_path = '../benchmark-output/topk/'
    #scan[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_scan_0" + ".prof"))
    #project[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_project_0" + ".prof"))
    #groupby[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_0" + ".prof"))
    #groupby_reduce.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_reduce" + ".prof"))
    #collate.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_collate" + ".prof"))

    map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    map(lambda (p, o): o.connect(topk[p]), enumerate(project))
    map(lambda (p, o): o.connect(topk_reduce), enumerate(topk))
    topk_reduce.connect(collate)

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
