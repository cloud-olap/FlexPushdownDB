# -*- coding: utf-8 -*-
"""Parquet Filter+Project Benchmark 

"""

import numpy as np
import pandas as pd

from s3filter.op.aggregate import Aggregate
from s3filter.op.collate import Collate
from s3filter.op.project import Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format


def main():
    path = 'parquet/csv_vs_parquet_10MB/10_column_10MB.csv'

    run(True, True, 0, selectivity1not0=True, path=path, format_=Format.PARQUET)


def run(parallel, use_pandas, buffer_size, selectivity1not0, path, format_):
    secure = False
    use_native = False
    print('')
    print("CSV/Parquet Filter Return 1 Column Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    if (selectivity1not0):
        # SQL scan the file
        scan = map(lambda p:
                   query_plan.add_operator(
                       SQLTableScan(path,
                                    "select F0 from S3Object "
                                    "where cast(F0 as int) >= 0;", format_,
                                    use_pandas, secure, use_native,
                                    'scan_{}'.format(p), query_plan,
                                    False)),
                   range(0, 1))
    else: # selectivity = 0
        scan = map(lambda p:
                   query_plan.add_operator(
                       SQLTableScan(path,
                                    "select F0 from S3Object "
                                    "where cast(F0 as int) < 0;", format_,
                                    use_pandas, secure, use_native,
                                    'scan_{}'.format(p), query_plan,
                                    False)),
                   range(0, 1))

    # project
    def fn(df):
        df.columns = ["F0"]
        return df

    project = map(lambda p:
                  query_plan.add_operator(
                      Project([], 'project_{}'.format(p), query_plan, False, fn)),
                  range(0, 1))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], True, 'agg', query_plan, False, agg_fun))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    map(lambda (p, o): o.connect(aggregate), enumerate(project))
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
    # query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

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