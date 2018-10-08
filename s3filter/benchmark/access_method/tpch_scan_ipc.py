# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

from s3filter import ROOT_DIR
from s3filter.multiprocessing.worker_system import WorkerSystem
from s3filter.op.collate import Collate
from s3filter.op.operator_connector import connect_many_to_many, connect_many_to_one, connect_one_to_one
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.table_scan import TableScan
from s3filter.op.table_range_access import TableRangeAccess
from s3filter.query.tpch import get_file_key
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.filter import Filter, PredicateExpression
from s3filter.op.aggregate import Aggregate

from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np
import os


def main():
    # Pickling
    run(True, True, 0, 1, 0, 901.04, 1, False, False, False)

    # Shared Mem
    run(True, True, 0, 1, 0, 901.04, 1, True, False, False)

    # Pickling + Merged
    run(True, True, 0, 1, 0, 901.04, 1, False, True, False)

    # Shared Mem + Merged
    run(True, True, 0, 1, 0, 901.04, 1, True, True, False)

    # # Pickling + Inline
    # run(True, True, 0, 1, 0, 901.04, 1, False, False, True)
    #
    # # Shared Mem + Inline
    # run(True, True, 0, 1, 0, 901.04, 1, True, False, True)


def run(parallel, use_pandas, buffer_size, table_parts, lower, upper, sf, use_shared_mem, merge_ops, inline_ops):
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    system = WorkerSystem()

    if merge_ops:
        query_plan = merged_query_plan(system, parallel, use_pandas, buffer_size, table_parts, lower, upper, sf,
                                       use_shared_mem)
    else:
        query_plan = separate_query_plan(system, parallel, use_pandas, buffer_size, table_parts, lower, upper, sf,
                                         use_shared_mem, inline_ops)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print("use_shared_mem: {}".format(use_shared_mem))
    print("merge_ops: {}".format(merge_ops))
    print("inline_ops: {}".format(inline_ops))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"),
                           gen_test_id() + "-" + str(table_parts) + '-' + str(use_shared_mem))

    # Start the query
    query_plan.execute()
    print('Done')

    collate = query_plan.get_operator('collate')

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    assert(len(tuples) == 1 + 1)
    np.testing.assert_approx_equal(float(tuples[1][0]), 187537)


def merged_query_plan(system, parallel, use_pandas, buffer_size, table_parts, lower, upper, sf, use_shared_mem):
    secure = False
    use_native = False
    query_plan = QueryPlan(system, is_async=parallel, buffer_size=buffer_size, use_shared_mem=use_shared_mem)

    def filter_fn(df):
        df[['_5']] = df[['_5']].astype(np.float)
        criterion = (df['_5'] >= lower) & (df['_5'] <= upper)
        return df[criterion]

    scan_filter = map(lambda p:
                      query_plan.add_operator(
                          TableScan(get_file_key('lineitem', True, p, sf=sf),
                                    use_pandas, secure, use_native,
                                    'scan_filter_{}'.format(p), query_plan,
                                    False, fn=filter_fn)),
                      range(0, table_parts))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], True, 'agg', query_plan, False, agg_fun))
    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))
    connect_many_to_one(scan_filter, aggregate)
    connect_one_to_one(aggregate, collate)
    scan_filter[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                                   gen_test_id() + "_scan_filter_0_" + str(use_shared_mem) + ".prof"))
    aggregate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_aggregate_" + str(use_shared_mem) + ".prof"))
    collate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_collate_" + str(use_shared_mem) + ".prof"))
    return query_plan


def separate_query_plan(system, parallel, use_pandas, buffer_size, table_parts, lower, upper, sf, use_shared_mem,
                        inline_ops):
    secure = False
    use_native = False
    query_plan = QueryPlan(system, is_async=parallel, buffer_size=buffer_size, use_shared_mem=use_shared_mem)

    # scan the file
    scan = map(lambda p:
               query_plan.add_operator(
                   TableScan(get_file_key('lineitem', True, p, sf=sf),
                             use_pandas, secure, use_native,
                             'scan_{}'.format(p), query_plan,
                             False)),
               range(0, table_parts))

    def filter_fn(df):
        df[['_5']] = df[['_5']].astype(np.float)
        criterion = (df['_5'] >= lower) & (df['_5'] <= upper)
        return df[criterion]

    filter = map(lambda p:
                 query_plan.add_operator(
                     Filter(PredicateExpression(None, filter_fn),
                            'filter_{}'.format(p), query_plan,
                            False)),
                 range(0, table_parts))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], True, 'agg', query_plan, False, agg_fun))
    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda p: p.set_async(not inline_ops), filter)

    connect_many_to_many(scan, filter)
    connect_many_to_one(filter, aggregate)
    connect_one_to_one(aggregate, collate)

    scan[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_scan_0_" + str(use_shared_mem) + ".prof"))
    filter[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_filter_0_" + str(use_shared_mem) + ".prof"))
    aggregate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_aggregate_" + str(use_shared_mem) + ".prof"))
    collate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_collate_" + str(use_shared_mem) + ".prof"))
    return query_plan


if __name__ == "__main__":
    main()
