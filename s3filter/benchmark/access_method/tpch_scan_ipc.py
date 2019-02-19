# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os

import numpy as np
import pandas as pd

from s3filter import ROOT_DIR
from s3filter.multiprocessing.worker_system import WorkerSystem
from s3filter.op.aggregate import Aggregate
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter, PredicateExpression
from s3filter.op.operator_connector import connect_many_to_many, connect_many_to_one, connect_one_to_one
from s3filter.op.table_scan import TableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.tpch import get_file_key
from s3filter.util.test_util import gen_test_id


def main():
    shared_mem_buffer_size = 1 * 1024 * 1024

    # sf = 1
    # table_parts = 32
    # result = 1

    sf = 10
    table_parts = 96
    result = 7

    # sf=100
    # table_parts = 96
    # result = 70

    # # Pickling
    # run(parallel=True, use_pandas=True, shared_mem_buffer_size=-1, buffer_size=0,
    #     table_parts=table_parts, lower=0, upper=901.04, sharded=True, sf=sf,
    #     use_shared_mem=False, merge_ops=False, inline_ops=False,
    #     result=result)
    #
    # Shared Mem
    run(parallel=True, use_pandas=True, shared_mem_buffer_size=shared_mem_buffer_size, buffer_size=0,
        table_parts=table_parts, lower=0, upper=901.04, sharded=True, sf=sf,
        use_shared_mem=True, merge_ops=False, inline_ops=False,
        result=result)
    #
    # # Pickling + Merged
    # run(parallel=True, use_pandas=True, shared_mem_buffer_size=-1, buffer_size=0,
    #     table_parts=table_parts, lower=0, upper=901.04, sharded=True, sf=sf,
    #     use_shared_mem=False, merge_ops=True, inline_ops=False,
    #     result=result)
    #
    # # Shared Mem + Merged
    # run(parallel=True, use_pandas=True, shared_mem_buffer_size=shared_mem_buffer_size, buffer_size=0,
    #     table_parts=table_parts, lower=0, upper=901.04, sharded=True, sf=sf,
    #     use_shared_mem=True, merge_ops=True, inline_ops=False,
    #     result=result)

    # # Pickling + Inline
    # run(parallel=True, use_pandas=True, shared_mem_buffer_size=-1, buffer_size=0,
    #     table_parts=table_parts, lower=0, upper=901.04, sharded=True, sf=sf,
    #     use_shared_mem=False, merge_ops=False, inline_ops=True,
    #     result=result)

    # Doesn't work

    # # Shared Mem + Inline
    # run(parallel=True, use_pandas=True, buffer_size=0, table_parts=8, lower=0, upper=901.04,
    #         sharded=False, sf=1, use_shared_mem=True, merge_ops=False, inline_ops=True)


def run(parallel, use_pandas, shared_mem_buffer_size, buffer_size, table_parts, lower, upper,
        sharded, sf, use_shared_mem, merge_ops, inline_ops, result):
    print('')
    print("IPC Benchmark")
    print("-------------")

    if merge_ops:
        query_plan = merged_query_plan(
            shared_mem_buffer_size, parallel, use_pandas, buffer_size, table_parts, lower, upper,
            sharded, sf, use_shared_mem)
    else:
        query_plan = separate_query_plan(
            shared_mem_buffer_size, parallel, use_pandas, buffer_size, table_parts, lower, upper,
            sharded, sf, use_shared_mem, inline_ops, merge_ops)

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
    print("sharded: {}".format(sharded))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"),
                           gen_test_id() + "-" + str(table_parts) + '-' + str(merge_ops))

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

    assert (len(tuples) == 1 + 1)
    np.testing.assert_approx_equal(float(tuples[1][0]), result)


def merged_query_plan(shared_mem_buffer_size, parallel, use_pandas, buffer_size, table_parts, lower, upper,
                      sharded, sf, use_shared_mem):
    secure = False
    use_native = False

    if use_shared_mem:
        system = WorkerSystem(shared_mem_buffer_size)
    else:
        system = None

    query_plan = QueryPlan(system, is_async=parallel, buffer_size=buffer_size, use_shared_mem=use_shared_mem)

    def filter_fn(df):
        return (df['_5'].astype(np.float) >= lower) & (df['_5'].astype(np.float) <= upper)

    scan_filter = map(lambda p:
                      query_plan.add_operator(
                          TableScan(get_file_key('lineitem', sharded, p, sf=sf),
                                    use_pandas, secure, use_native,
                                    'scan_filter_{}'.format(p), query_plan,
                                    False, fn=filter_fn)),
                      range(0, table_parts))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], use_pandas, 'agg', query_plan, False, agg_fun))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    connect_many_to_one(scan_filter, aggregate)
    connect_one_to_one(aggregate, collate)

    profile_file_suffix = get_profile_file_suffix(inline_ops=False, merge_ops=True, use_shared_mem=use_shared_mem)

    scan_filter[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                                   gen_test_id() + "_scan_filter_0" + profile_file_suffix + ".prof"))
    aggregate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_aggregate" + profile_file_suffix + ".prof"))
    collate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_collate" + profile_file_suffix + ".prof"))
    return query_plan


def separate_query_plan(shared_mem_buffer_size, parallel, use_pandas, buffer_size, table_parts, lower, upper,
                        sharded, sf, use_shared_mem, inline_ops, merge_ops):
    secure = False
    use_native = False

    if use_shared_mem:
        system = WorkerSystem(shared_mem_buffer_size)
    else:
        system = None

    query_plan = QueryPlan(system, is_async=parallel, buffer_size=buffer_size, use_shared_mem=use_shared_mem)

    # scan the file
    scan = map(lambda p:
               query_plan.add_operator(
                   TableScan(get_file_key('lineitem', sharded, p, sf=sf),
                             use_pandas, secure, use_native,
                             'scan_{}'.format(p), query_plan,
                             False)),
               range(0, table_parts))

    def filter_fn(df):
        return (df['_5'].astype(np.float) >= lower) & (df['_5'].astype(np.float) <= upper)

    filter_ = map(lambda p:
                 query_plan.add_operator(
                     Filter(PredicateExpression(None, filter_fn),
                            'filter_{}'.format(p), query_plan,
                            False)),
                 range(0, table_parts))

    # aggregation
    def agg_fun(df):
        return pd.DataFrame({'count': [len(df)]})

    aggregate = query_plan.add_operator(
        Aggregate([], use_pandas, 'agg', query_plan, False, agg_fun))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda p: p.set_async(not inline_ops), filter_)

    connect_many_to_many(scan, filter_)
    connect_many_to_one(filter_, aggregate)
    connect_one_to_one(aggregate, collate)

    profile_file_suffix = get_profile_file_suffix(inline_ops, merge_ops, use_shared_mem)

    scan[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_scan_0" + profile_file_suffix + ".prof"))
    filter_[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_filter_0" + profile_file_suffix + ".prof"))
    aggregate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                              gen_test_id() + "_aggregate" + profile_file_suffix + ".prof"))
    collate.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
                                            gen_test_id() + "_collate" + profile_file_suffix + ".prof"))
    return query_plan


def get_profile_file_suffix(inline_ops, merge_ops, use_shared_mem):
    profile_file_suffix = ""

    if use_shared_mem:
        profile_file_suffix += "_ipc=shared-mem"
    else:
        profile_file_suffix += "_ipc=pickling"

    if merge_ops:
        profile_file_suffix += "_merge_ops=True"
    else:
        profile_file_suffix += "_merge_ops=False"

    if inline_ops:
        profile_file_suffix += "_inline_ops=True"
    else:
        profile_file_suffix += "_inline_ops=False"

    return profile_file_suffix


if __name__ == "__main__":
    main()
