"""Top k query tests

"""

from s3filter.op.collate import Collate
from s3filter.op.sql_sharded_table_scan import SQLShardedTableScan
from s3filter.op.top import TopKTableScan, Top
from s3filter.plan.query_plan import QueryPlan
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.sort import SortExpression
import multiprocessing


def topk_baseline(stats, k, sort_index='_5', col_type=float, col_name='l_extendedprice', sort_order='DESC', use_pandas=True,
                         filtered=False, table_name='tpch-sf1/lineitem.csv', shards_prefix='tpch-sf1/lineitem_sharded'):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    limit = k
    num_rows = 0
    shards = 32
    parallel_shards = True
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
        proc_parts = [x for x in range(0, shards) if x % processes == process]
        pc = query_plan.add_operator(SQLShardedTableScan(table_name, sql, use_pandas, True, False,
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
        # print("{}:{}".format(num_rows, t))

    # assert num_rows == limit + 1

    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    query_stats += [0,
                    0,
                    query_plan.total_elapsed_time,
                    query_plan.total_elapsed_time,
                    0,
                    rows,
                    bytes_scanned,
                    bytes_returned,
                    data_cost,
                    computation_cost,
                    cost,
                    num_rows == limit + 1
                    ]
    stats.append(query_stats)

    # Write the metrics
    query_plan.print_metrics()
    query_plan.stop()


def topk_with_sampling(stats, k, k_scale=1, sort_index='_5', col_type=float, sort_field='l_extendedprice',
                       sort_order='DESC', use_pandas=True, filtered=False, conservative=False,
                       table_name='tpch-sf1/lineitem.csv', shards_prefix='tpch-sf1/lineitem_sharded'):
    """
    Executes the optimized topk query by firstly retrieving the first k tuples.
    Based on the retrieved tuples, table scan operator gets only the tuples larger/less than the most significant
    tuple in the sample
    :return:
    """

    limit = k
    num_rows = 0
    shards = 31
    parallel_shards = True
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
        TopKTableScan(table_name, sql, use_pandas, True, False, limit, k_scale,
                      SortExpression(sort_index, col_type, sort_order, sort_field), conservative,
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
        # print("{}:{}".format(num_rows, t))

    # assert num_rows == limit + 1

    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    query_stats += [ts.msv,
                    ts.op_metrics.sampling_time,
                    query_plan.total_elapsed_time,
                    ts.op_metrics.sampling_time + query_plan.total_elapsed_time,
                    0 if num_rows >= limit else 1,
                    rows,
                    bytes_scanned,
                    bytes_returned,
                    data_cost,
                    computation_cost,
                    cost,
                    num_rows == limit + 1
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
        'Query time (Sec)',
        'Total query time (Sec)',
        'Second trial time (Sec)',
        'Returned Rows',
        'Bytes Scanned MB',
        'Bytes Returned MB',
        'Data Cost $',
        'Computation Cost $',
        'Total Cost $',
        'Succeeded'
    ])

    # varying K
    for k in [100, 1000, 10000, 100000]:
        # topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'ASC', use_pandas=use_pandas, filtered=False)
        topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas, filtered=False)
        # topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'ASC', use_pandas=use_pandas, filtered=True)
        # topk_baseline(stats, k, '_5', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas, filtered=True)

    for k in [100, 1000, 10000, 100000]:
        for kscale in [1, 2, 4, 8, 16, 100, 1000]:
            # scale = pow(2, i)
            # topk_with_sampling(stats, k, scale, '_5', float, 'l_extendedprice', 'ASC', use_pandas)
            topk_with_sampling(stats, k, kscale, '_5', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas,
                               filtered=False,
                               conservative=True)
            topk_with_sampling(stats, k, kscale, '_5', float, 'l_extendedprice', 'DESC', use_pandas=use_pandas,
                               filtered=False,
                               conservative=False)

    for stat in stats:
        print(",".join([str(x) if type(x) is not str else x for x in stat]))


if __name__ == "__main__":
    # run_all()
    import sys,os

    stats_header = [
        'Method',
        'Table',
        'Sort Field',
        'Sort Order',
        'K',
        'K scale',
        'Sampling Threshold',
        'Sampling time (Sec)',
        'Query time (Sec)',
        'Total query time (Sec)',
        'Second trial time (Sec)',
        'Returned Rows',
        'Bytes Scanned MB',
        'Bytes Returned MB',
        'Data Cost $',
        'Computation Cost $',
        'Total Cost $',
        'Succeeded'
    ]

    if len(sys.argv) >= 4:
        topk_type = sys.argv[1]
        k = int(sys.argv[2])
        k_scale = int(sys.argv[3])
        is_conservative = True if int(sys.argv[4]) != 0 else False
        table_name = sys.argv[5]
        shards_prefix = sys.argv[6]
        if len(sys.argv) >= 8:
            stats_file_name = sys.argv[7]
        else:
            stats_file_name = 'topk_stats.txt'

        run_stats = []

        if topk_type == 'baseline':
            topk_baseline(stats=run_stats,
                          k=k,
                          sort_index='_5',
                          col_type=float,
                          col_name='l_extendedprice',
                          sort_order='DESC',
                          use_pandas=True,
                          filtered=False,
                          table_name=table_name,
                          shards_prefix=shards_prefix)
        elif topk_type == 'sampled':
            topk_with_sampling(stats=run_stats,
                               k=k,
                               k_scale=k_scale,
                               sort_index='_5',
                               col_type=float,
                               sort_field='l_extendedprice',
                               sort_order='DESC',
                               use_pandas=True,
                               filtered=False,
                               conservative=is_conservative,
                               table_name=table_name,
                               shards_prefix=shards_prefix)

        proj_dir = os.environ['PYTHONPATH'].split(":")[0]
        stats_dir = os.path.join(proj_dir, '..')
        stats_dir = os.path.join(stats_dir, stats_file_name)

        if os.path.exists(stats_dir):
            mode = 'a'
        else:
            mode = 'w'

        with open(stats_dir, mode) as stats_file:
            if mode == 'w':
                stats_file.write(",".join([str(x) if type(x) is not str else x for x in stats_header]) + "\n")
            stats_file.write(",".join([str(x) if type(x) is not str else x for x in run_stats[0]]) + "\n")
