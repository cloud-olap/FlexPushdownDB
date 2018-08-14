# -*- coding: utf-8 -*-
"""Top k query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan, SQLShardedTableScan
from s3filter.op.limit import Limit
from s3filter.op.top import TopKTableScan, Top
from s3filter.plan.query_plan import QueryPlan
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.sort import SortExpression
import multiprocessing
from s3filter.util.test_util import gen_test_id


def __test_topk_baseline(sort_index='_5', sort_order='DESC'):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    limit = 50
    num_rows = 0
    shards = 32
    processes = multiprocessing.cpu_count()

    print("Baseline TopK with order {} on field {}\n".format(sort_order, sort_index))

    query_plan = QueryPlan(is_async=True)

    # Query plan
    # ts = query_plan.add_operator(
    #     SQLTableScan('lineitem.csv', 'select * from S3Object limit {};'.format(limit), 'table_scan', query_plan, False))
    sort_exp = SortExpression(sort_index, float, sort_order)
    top_op = query_plan.add_operator(Top(limit, sort_exp, 'topk', query_plan, False))
    for process in range(processes):
        proc_parts = [x for x in range(shards) if x % processes == process]
        pc = query_plan.add_operator(SQLShardedTableScan("lineitem.csv", 'select * from S3Object;',
                                                         "topk_table_scan_parts_{}".format(proc_parts), proc_parts,
                                                         "sf1000-lineitem", query_plan, False))
        pc.set_profiled(True, "topk_table_scan_parts_{}.txt".format(proc_parts))
        pc_top = query_plan.add_operator(Top(limit, sort_exp, 'topk_parts_{}'.format(proc_parts), query_plan, False))
        pc.connect(pc_top)
        pc_top.connect(top_op)

    c = query_plan.add_operator(Collate('collate', query_plan, False))

    top_op.connect(c)

    # Write the plan graph
    # query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    assert num_rows == limit + 1

    # Write the metrics
    query_plan.print_metrics()
    query_plan.stop()


def __test_topk_with_sampling(k_scale=1, sort_index='_5', sort_field='l_extendedprice', sort_order='DESC'):
    """
    Executes the optimized topk query by firstly retrieving the first k tuples.
    Based on the retrieved tuples, table scan operator gets only the tuples larger/less than the most significant
    tuple in the sample
    :return:
    """

    limit = 50
    num_rows = 0
    shards = 32
    processes = multiprocessing.cpu_count()

    print("Sampling params:")
    print("Scale: {}, Sort Field: {}, Sort Order: {}\n".format(k_scale, sort_field, sort_order))

    query_plan = QueryPlan(is_async=True)

    # Query plan
    ts = query_plan.add_operator(
        TopKTableScan('lineitem.csv', 'select * from S3Object', limit, k_scale,
                      SortExpression(sort_index, float, sort_order, sort_field),
                      shards, processes, 'topk_table_scan', query_plan, False))
    c = query_plan.add_operator(Collate('collate', query_plan, False))

    ts.connect(c)

    # Write the plan graph
    # query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    assert num_rows == limit

    # Write the metrics
    query_plan.print_metrics()
    query_plan.stop()
    topk_op = query_plan.operators["topk_table_scan"]
    max_table_scan, _ = max([(op, op.op_metrics.elapsed_time()) for op in query_plan.operators.values()],
                         key=lambda tup: tup[1])
    OpMetrics.print_overall_metrics([max_table_scan, topk_op, topk_op.sample_op], "TopKTableScan total time")


# def test_limit_topk():
#     """Executes a top k query by using S3 select's limit clause. The results are then collated.
#
#     :return: None
#     """
#
#     limit = 500
#     num_rows = 0
#
#     query_plan = QueryPlan()
#
#     # Query plan
#     ts = query_plan.add_operator(
#         SQLTableScan('lineitem.csv', 'select * from S3Object limit {};'.format(limit), 'ts', query_plan, False))
#     c = query_plan.add_operator(Collate('c', query_plan, False))
#
#     ts.connect(c)
#
#     # Write the plan graph
#     # query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())
#
#     # Start the query
#     query_plan.execute()
#
#     # Assert the results
#     for _ in c.tuples():
#         num_rows += 1
#         # print("{}:{}".format(num_rows, t))
#
#     assert num_rows == limit + 1
#
#     # Write the metrics
#     query_plan.print_metrics()
#
#
# def test_abort_topk():
#     """Executes a top k query using the top operator (which stops the scan once it has reached the target
#     number of tuples). The results are then collated.
#
#     :return: None
#     """
#
#     limit = 5
#     num_rows = 0
#
#     query_plan = QueryPlan()
#
#     # Query plan
#     ts = query_plan.add_operator(SQLTableScan('lineitem.csv', 'select * from S3Object;', 'ts', query_plan, False))
#     t = query_plan.add_operator(Limit(limit, 't', query_plan, False))
#     c = query_plan.add_operator(Collate('c', query_plan, False))
#
#     ts.connect(t)
#     t.connect(c)
#
#     # Write the plan graph
#     # query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())
#
#     # Start the query
#     query_plan.execute()
#
#     # Assert the results
#     for _ in c.tuples():
#         num_rows += 1
#         # print("{}:{}".format(num_rows, t))
#
#     assert num_rows == limit + 1
#
#     # Write the metrics
#     query_plan.print_metrics()
#
#
# def test_topk_empty():
#     """Executes a topk query with no results returned. We tst this as it's somewhat peculiar with s3 select, in so much
#     as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
#     should be present in the results.
#
#     :return: None
#     """
#
#     limit = 500
#     num_rows = 0
#
#     query_plan = QueryPlan()
#
#     # Query plan
#     ts = query_plan.add_operator(
#         SQLTableScan('lineitem.csv', 'select * from S3Object limit 0', 'ts', query_plan, False))
#     t = query_plan.add_operator(Limit(limit, 't', query_plan, False))
#     c = query_plan.add_operator(Collate('c', query_plan, False))
#
#     ts.connect(t)
#     t.connect(c)
#
#     # Write the plan graph
#     # query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())
#
#     # Start the query
#     query_plan.execute()
#
#     # Assert the results
#     for _ in c.tuples():
#         num_rows += 1
#         # print("{}:{}".format(num_rows, t))
#
#     assert num_rows == 0
#
#     # Write the metrics
#     query_plan.print_metrics()

def test_all():
    __test_topk_baseline('_4', 'ASC')
    __test_topk_baseline('_4', 'DESC')
    __test_topk_baseline('_5', 'ASC')
    __test_topk_baseline('_5', 'DESC')
    for i in range(4):
        scale = pow(2, i)
        __test_topk_with_sampling(scale, '_4', 'l_quantity', 'ASC')
        __test_topk_with_sampling(scale, '_4', 'l_quantity', 'DESC')
        __test_topk_with_sampling(scale, '_5', 'l_extendedprice', 'ASC')
        __test_topk_with_sampling(scale, '_5', 'l_extendedprice', 'DESC')


if __name__ == "__main__":
    test_all()

    # test_limit_topk()
    # test_abort_topk()
    # test_topk_empty()
