"""Top k query tests

"""

from s3filter.op.collate import Collate
from s3filter.op.sql_sharded_table_scan import SQLShardedTableScan
from s3filter.op.top import TopKTableScan, Top
from s3filter.plan.query_plan import QueryPlan
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.sort import SortExpression
import multiprocessing


def topk_baseline(stats, k, sort_index='_5', col_type=float, col_name='l_quantity', sort_order='DESC', use_pandas=True,
                         filtered=False):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    limit = k
    num_rows = 0
    shards = 96
    parallel_shards = True
    shards_prefix = "tpch-sf10/lineitem_sharded"
    processes = multiprocessing.cpu_count()

    query_stats = ['baseline' if filtered is False else 'filtered', shards_prefix, col_name, sort_order, limit, '']

    sql = 'select * from S3Object;'

    if filtered:
        sql = '''select l_extendedprice
                  from S3Object;'''
        sort_index = '_0'

    print("\n\nBaseline TopK with order {} on field {}\n".format(sort_order, col_name))

    query_plan = QueryPlan(is_async=True)

    # Query plan
    # ts = query_plan.add_operator(
    #     SQLTableScan('lineitem.csv', 'select * from S3Object limit {};'.format(limit), 'table_scan', query_plan, False))
    sort_exp = SortExpression(sort_index, col_type, sort_order)
    top_op = query_plan.add_operator(Top(limit, sort_exp, use_pandas, 'topk', query_plan, False))
    for process in range(processes):
        proc_parts = [x for x in range(1, shards + 1) if x % processes == process]
        pc = query_plan.add_operator(SQLShardedTableScan("tpch-sf10/lineitem.tbl", sql, use_pandas, False, True,
                                                         "topk_table_scan_parts_{}".format(proc_parts), proc_parts,
                                                         shards_prefix, parallel_shards, query_plan, False))
        # pc.set_profiled(True, "topk_table_scan_parts_{}.txt".format(proc_parts))
        pc_top = query_plan.add_operator(Top(limit, sort_exp, use_pandas, 'topk_parts_{}'.format(proc_parts),
                                             query_plan, False))
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

    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    query_stats += [0,
                    0,
                    query_plan.total_elapsed_time,
                    query_plan.total_elapsed_time,
                    rows,
                    bytes_scanned,
                    bytes_returned,
                    data_cost,
                    computation_cost,
                    cost
                    ]
    stats.append(query_stats)

    # Write the metrics
    query_plan.print_metrics()
    query_plan.stop()


def topk_with_sampling(stats, k, k_scale=1, sort_index='_5', col_type=float, sort_field='l_extendedprice',
                              sort_order='DESC', use_pandas=True, filtered=False):
    """
    Executes the optimized topk query by firstly retrieving the first k tuples.
    Based on the retrieved tuples, table scan operator gets only the tuples larger/less than the most significant
    tuple in the sample
    :return:
    """

    limit = k
    num_rows = 0
    shards = 96
    parallel_shards = True
    shards_prefix = "tpch-sf10/lineitem_sharded"
    processes = multiprocessing.cpu_count()

    query_stats = ['sampling', shards_prefix, sort_field, sort_order, limit, k_scale]

    sql = 'select * from S3Object;'

    if filtered:
        sql = '''select l_extendedprice
              from S3Object;'''
        sort_index = '_0'

    print("\n\nSampling params:")
    print("Scale: {}, Sort Field: {}, Sort Order: {}\n".format(k_scale, sort_field, sort_order))

    query_plan = QueryPlan(is_async=True)

    # Query plan
    ts = query_plan.add_operator(
        TopKTableScan('tpch-sf10/lineitem.tbl', sql, use_pandas, False, limit, k_scale,
                      SortExpression(sort_index, col_type, sort_order, sort_field),
                      shards, parallel_shards, shards_prefix, processes, 'topk_table_scan', query_plan, False))
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

    assert num_rows == limit + 1

    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    query_stats += [ts.msv,
                    ts.op_metrics.sampling_time,
                    query_plan.total_elapsed_time,
                    ts.op_metrics.sampling_time + query_plan.total_elapsed_time,
                    rows,
                    bytes_scanned,
                    bytes_returned,
                    data_cost,
                    computation_cost,
                    cost
                    ]
    stats.append(query_stats)

    # Write the metrics
    query_plan.print_metrics()
    query_plan.stop()
    topk_op = query_plan.operators["topk_table_scan"]
    max_table_scan, _ = max([(op, op.op_metrics.elapsed_time()) for op in query_plan.operators.values()],
                         key=lambda tup: tup[1])
    OpMetrics.print_overall_metrics([max_table_scan, topk_op, topk_op.sample_op], "TopKTableScan total time")


def run_all():
    use_pandas = True
    stats = []
    stats.append([
        'Method',
        'Table',
        'Sort Field',
        'Sort Order',
        'K',
        'K scale',
        'Sampling Threshold',
        'Sampling time (Sec)',
        'Total query time (Sec)'
        'Returned Rows',
        'Bytes Scanned MB',
        'Bytes Returned MB',
        'Data Cost $',
        'Computation Cost $',
        'Total Cost $'
    ])

    # varying K
    for k in range(100, 400, 100):
        # topk_baseline(stats, k, '_4', float, 'l_extendedprice', 'ASC', use_pandas=use_pandas, filtered=False)
        topk_baseline(stats, k, '_4', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas, filtered=False)
        # topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'ASC', use_pandas=use_pandas, filtered=True)
        topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas, filtered=True)
        for i in range(4):
            scale = pow(2, i)
            # topk_with_sampling(stats, k, scale, '_5', float, 'l_extendedprice', 'ASC', use_pandas)
            topk_with_sampling(stats, k, scale, '_5', float, 'l_extendedprice', 'DESC', use_pandas)

    for stat in stats:
        print(",".join([str(x) if type(x) is not str else x for x in stat]))


if __name__ == "__main__":
    run_all()
