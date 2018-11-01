"""Top K baseline

"""
from s3filter import ROOT_DIR

from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.top import Top
from s3filter.op.top_filter_build import TopKFilterBuild
from s3filter.op.table_sampler import TableRandomSampleGenerator
from s3filter.op.random_sample_byte_range_builder import ByteRangeBuilder
from s3filter.op.table_range_access import TableRangeAccess

from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sort import SortExpression

import numpy as np
import pandas as pd
import multiprocessing


def main():
    tbl_s3key = 'tpch-sf1/lineitem.csv'
    shards_path = 'tpch-sf1/lineitem_sharded/lineitem.csv'
    run_memory_indexed_sampling([], '_5', 'l_extendedprice', 100, sample_size=10000, batch_size=100, parallel=True,
                               use_pandas=True,
                               sort_order='DESC', buffer_size=0, table_parts_start=0, table_parts_end=31,
                               tbl_s3key=tbl_s3key,
                               shards_path=shards_path)
    return
    run_local_indexed_sampling([], '_5', 'l_extendedprice', 100, sample_size=10000, batch_size=100, parallel=True, use_pandas=True,
                               sort_order='DESC', buffer_size=0, table_parts_start=0, table_parts_end=31, tbl_s3key=tbl_s3key,
                               shards_path=shards_path)
    run_head_table_sampling([], '_5', 'l_extendedprice', 100, sample_size=10000, parallel=True, use_pandas=True,
                         sort_order='DESC', buffer_size=0, table_parts_start=0, table_parts_end=31,
                         shards_path=shards_path)


def run_memory_indexed_sampling(stats, sort_field_index, sort_field, k, sample_size, batch_size, parallel, use_pandas, sort_order, buffer_size,
                               table_parts_start, table_parts_end, tbl_s3key, shards_path):
    """
    Executes the randomly sampled topk query by firstly building a random sample, then extracting the filtering threshold
    Finally scanning the table to retrieve only the records beyond the threshold
    :return:
    """

    secure = False
    use_native = False
    n_threads = multiprocessing.cpu_count()

    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    stats += ['sampling_{}_{}'.format('indexed', 'non-filtered'), shards_path, sort_field, sort_order, sample_size,
              batch_size]

    # Query plan
    query_plan = QueryPlan(system=None, is_async=parallel, buffer_size=buffer_size)

    # Sampling
    tbl_smpler = query_plan.add_operator(
                                 TableRandomSampleGenerator(tbl_s3key, sample_size, batch_size, "table_sampler", query_plan,
                                                            False))

    sample_scanners = map(lambda p: query_plan.add_operator(
                                 TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
                                                  query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    map(lambda op: op.set_nthreads(n_threads), sample_scanners)

    # sample_scan = query_plan.add_operator(
    #     TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
    #                      query_plan, False))
    # sample_scan.set_nthreads(n_threads)

    # Sampling project
    def project_fn(df):
        df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
        df = df[[sort_field]].astype(np.float, errors='ignore')
        return df

    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]

    sample_project = map(lambda p: query_plan.add_operator(
                                Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn)),
                         range(table_parts_start, table_parts_end + 1))
    # sample_project = query_plan.add_operator(
    #                             Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn))

    # TopK samples
    sort_expr = [SortExpression(sort_field, float, sort_order)]
    sample_topk = map(lambda p: query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    # sample_topk = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False))

    sample_topk_reduce = query_plan.add_operator(
                            Top(k, sort_expr, use_pandas, 'sample_topk_reduce', query_plan, False))

    # Generate SQL command for second scan
    sql_gen = query_plan.add_operator(
                   TopKFilterBuild(sort_order, 'float', 'select * from S3object ',
                                   ' CAST({} as float) '.format(sort_field), 'sql_gen', query_plan, False ))

    # # Scan
    # scan = map(lambda p:
    #            query_plan.add_operator(
    #                 SQLTableScan("{}.{}".format(shards_path, p),
    #                     "", use_pandas, secure, use_native,
    #                     'scan_{}'.format(p), query_plan,
    #                     False)),
    #            range(table_parts_start, table_parts_end + 1))

    # # Project
    # def project_fn(df):
    #     df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
    #     df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
    #     return df
    #
    # project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]
    #
    # project = map(lambda p:
    #               query_plan.add_operator(
    #                   Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
    #               range(table_parts_start, table_parts_end + 1))
    #
    # # TopK
    # topk = map(lambda p:
    #            query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False)),
    #            range(table_parts_start, table_parts_end + 1))
    #
    # # TopK reduce
    # topk_reduce = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_reduce', query_plan, False))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda op: tbl_smpler.connect(op), sample_scanners)
    map(lambda (p, o): o.connect(sample_project[p]), enumerate(sample_scanners))
    map(lambda (p, o): o.connect(sample_topk[p]), enumerate(sample_project))
    map(lambda o: o.connect(sample_topk_reduce), sample_topk)
    sample_topk_reduce.connect(sql_gen)
    sql_gen.connect(collate)

    # map(lambda (p, o): sql_gen.connect(o), enumerate(scan))
    # map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    # map(lambda (p, o): o.connect(topk[p]), enumerate(project))
    # map(lambda (p, o): o.connect(topk_reduce), enumerate(topk))
    # topk_reduce.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts_end - table_parts_start))
    print('')

    # Start the query
    query_plan.execute()
    print('Done')
    # tuples = collate.tuples()

    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    sampling_time = query_plan.total_elapsed_time
    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    stats += [sql_gen.threshold,
              sampling_time,
              0,
              sampling_time,
              rows,
              bytes_scanned,
              bytes_returned,
              data_cost,
              computation_cost,
              cost
              ]


def run_s3_indexed_sampling(stats, sort_field_index, sort_field, k, sample_size, batch_size, parallel, use_pandas, sort_order, buffer_size,
                               table_parts_start, table_parts_end, tbl_s3key, shards_path):
    """
    Executes the randomly sampled topk query by firstly building a random sample, then extracting the filtering threshold
    Finally scanning the table to retrieve only the records beyond the threshold
    :return:
    """

    secure = False
    use_native = False
    n_threads = multiprocessing.cpu_count()

    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    stats = ['sampling_{}_{}'.format('indexed', 'non-filtered'), shards_path, sort_field, sort_order, sample_size,
             batch_size]

    # Query plan
    query_plan = QueryPlan(system=None, is_async=parallel, buffer_size=buffer_size)

    # Sampling
    tbl_smpler = query_plan.add_operator(
                                 TableRandomSampleGenerator(tbl_s3key, sample_size, batch_size, "table_sampler", query_plan,
                                                            False))

    tbl_sample_range_scanners = map(lambda p: query_plan.add_operator(
                                    SQLTableScan(tbl_smpler.index_mng.get_s3_index_path(), "", use_pandas, secure, use_native,
                                    'scan_sample_index_{}'.format(p), query_plan, False)), range(5))

    sample_byte_ranges_builder = query_plan.add_operator(
        ByteRangeBuilder(sample_size, batch_size, 'byte_range_builder', query_plan, False)
    )

    sample_scanners = map(lambda p: query_plan.add_operator(
                                 TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
                                                  query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    map(lambda op: op.set_nthreads(n_threads), sample_scanners)

    # sample_scan = query_plan.add_operator(
    #     TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
    #                      query_plan, False))
    # sample_scan.set_nthreads(n_threads)

    # Sampling project
    def project_fn(df):
        df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
        df = df[[sort_field]].astype(np.float, errors='ignore')
        return df

    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]

    sample_project = map(lambda p: query_plan.add_operator(
                                Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn)),
                         range(table_parts_start, table_parts_end + 1))
    # sample_project = query_plan.add_operator(
    #                             Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn))

    # TopK samples
    sort_expr = SortExpression(sort_field, float, sort_order)
    sample_topk = map(lambda p: query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    # sample_topk = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False))

    sample_topk_reduce = query_plan.add_operator(
                            Top(k, sort_expr, use_pandas, 'sample_topk_reduce', query_plan, False))

    # Generate SQL command for second scan
    sql_gen = query_plan.add_operator(
                   TopKFilterBuild(sort_order, 'float', 'select * from S3object ',
                                   ' CAST({} as float) '.format(sort_field), 'sql_gen', query_plan, False ))

    # # Scan
    # scan = map(lambda p:
    #            query_plan.add_operator(
    #                 SQLTableScan("{}.{}".format(shards_path, p),
    #                     "", use_pandas, secure, use_native,
    #                     'scan_{}'.format(p), query_plan,
    #                     False)),
    #            range(table_parts_start, table_parts_end + 1))

    # # Project
    # def project_fn(df):
    #     df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
    #     df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
    #     return df
    #
    # project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]
    #
    # project = map(lambda p:
    #               query_plan.add_operator(
    #                   Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
    #               range(table_parts_start, table_parts_end + 1))
    #
    # # TopK
    # topk = map(lambda p:
    #            query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False)),
    #            range(table_parts_start, table_parts_end + 1))
    #
    # # TopK reduce
    # topk_reduce = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_reduce', query_plan, False))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda op: tbl_smpler.connect(op), tbl_sample_range_scanners)
    map(lambda op: op.connect(sample_byte_ranges_builder), tbl_sample_range_scanners)
    map(lambda op: sample_byte_ranges_builder.connect(op), sample_scanners)
    map(lambda (p, o): o.connect(sample_project[p]), enumerate(sample_scanners))
    map(lambda (p, o): o.connect(sample_topk[p]), enumerate(sample_project))
    map(lambda o: o.connect(sample_topk_reduce), sample_topk)
    sample_topk_reduce.connect(sql_gen)
    sql_gen.connect(collate)

    # map(lambda (p, o): sql_gen.connect(o), enumerate(scan))
    # map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    # map(lambda (p, o): o.connect(topk[p]), enumerate(project))
    # map(lambda (p, o): o.connect(topk_reduce), enumerate(topk))
    # topk_reduce.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts_end - table_parts_start))
    print('')

    # Start the query
    query_plan.execute()
    print('Done')
    # tuples = collate.tuples()

    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    sampling_time = query_plan.total_elapsed_time
    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    stats += [sql_gen.threshold,
              sampling_time,
              0,
              sampling_time,
              rows,
              bytes_scanned,
              bytes_returned,
              data_cost,
              computation_cost,
              cost
              ]


def run_local_indexed_sampling(stats, sort_field_index, sort_field, k, sample_size, batch_size, parallel, use_pandas, sort_order, buffer_size,
                               table_parts_start, table_parts_end, tbl_s3key, shards_path):
    """
    Executes the randomly sampled topk query by firstly building a random sample, then extracting the filtering threshold
    Finally scanning the table to retrieve only the records beyond the threshold
    :return:
    """

    secure = False
    use_native = False
    n_threads = multiprocessing.cpu_count()

    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    stats += ['sampling_{}_{}'.format('indexed', 'non-filtered'), shards_path, sort_field, sort_order, sample_size,
             batch_size]

    # Query plan
    query_plan = QueryPlan(system=None, is_async=parallel, buffer_size=buffer_size)

    # Sampling
    tbl_smpler = query_plan.add_operator(
                                 TableRandomSampleGenerator(tbl_s3key, sample_size, batch_size, "table_sampler", query_plan,
                                                            False))
    sample_scan = map(lambda p: query_plan.add_operator(
                                 TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
                                                  query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    map(lambda (i, op): sample_scan[i].set_nthreads(n_threads), enumerate(sample_scan))

    # sample_scan = query_plan.add_operator(
    #     TableRangeAccess(tbl_s3key, use_pandas, secure, use_native, "sample_scan_{}".format(p),
    #                      query_plan, False))
    # sample_scan.set_nthreads(n_threads)

    # Sampling project
    def project_fn(df):
        df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
        df = df[[sort_field]].astype(np.float, errors='ignore')
        return df

    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]

    sample_project = map(lambda p: query_plan.add_operator(
                                Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn)),
                         range(table_parts_start, table_parts_end + 1))
    # sample_project = query_plan.add_operator(
    #                             Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn))

    # TopK samples
    sort_expr = [SortExpression(sort_field, float, sort_order)]
    sample_topk = map(lambda p: query_plan.add_operator(
                    Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))
    # sample_topk = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'sample_topk_{}'.format(p), query_plan, False))

    sample_topk_reduce = query_plan.add_operator(
                            Top(k, sort_expr, use_pandas, 'sample_topk_reduce', query_plan, False))

    # Generate SQL command for second scan
    sql_gen = query_plan.add_operator(
                   TopKFilterBuild(sort_order, 'float', 'select * from S3object ',
                                   ' CAST({} as float) '.format(sort_field), 'sql_gen', query_plan, False ))

    # # Scan
    # scan = map(lambda p:
    #            query_plan.add_operator(
    #                 SQLTableScan("{}.{}".format(shards_path, p),
    #                     "", use_pandas, secure, use_native,
    #                     'scan_{}'.format(p), query_plan,
    #                     False)),
    #            range(table_parts_start, table_parts_end + 1))
    #
    # # Project
    # def project_fn(df):
    #     df.columns = [sort_field if x == sort_field_index else x for x in df.columns]
    #     df[ [sort_field] ] = df[ [sort_field] ].astype(np.float)
    #     return df
    #
    # project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]
    #
    # project = map(lambda p:
    #               query_plan.add_operator(
    #                   Project(project_exprs, 'project_{}'.format(p), query_plan, False, project_fn)),
    #               range(table_parts_start, table_parts_end + 1))
    #
    # # TopK
    # topk = map(lambda p:
    #            query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_{}'.format(p), query_plan, False)),
    #            range(table_parts_start, table_parts_end + 1))
    #
    # # TopK reduce
    # topk_reduce = query_plan.add_operator(
    #                 Top(k, sort_expr, use_pandas, 'topk_reduce', query_plan, False))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    #profile_path = '../benchmark-output/groupby/'
    #scan[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_scan_0" + ".prof"))
    #project[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_project_0" + ".prof"))
    #groupby[0].set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_0" + ".prof"))
    #groupby_reduce.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_groupby_reduce" + ".prof"))
    #collate.set_profiled(True, os.path.join(ROOT_DIR, profile_path, gen_test_id() + "_collate" + ".prof"))

    map(lambda o: tbl_smpler.connect(o), sample_scan)
    map(lambda (p, o): o.connect(sample_project[p]), enumerate(sample_scan))
    map(lambda (p, o): o.connect(sample_topk[p]), enumerate(sample_project))
    map(lambda o: o.connect(sample_topk_reduce), sample_topk)
    sample_topk_reduce.connect(sql_gen)
    sql_gen.connect(collate)

    # map(lambda (p, o): sql_gen.connect(o), enumerate(scan))
    # map(lambda (p, o): o.connect(project[p]), enumerate(scan))
    # map(lambda (p, o): o.connect(topk[p]), enumerate(project))
    # map(lambda (p, o): o.connect(topk_reduce), enumerate(topk))
    # topk_reduce.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts_end - table_parts_start))
    print('')

    # Write the plan graph
    # query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    # tuples = collate.tuples()

    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    sampling_time = query_plan.total_elapsed_time
    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    stats += [sql_gen.threshold,
              sampling_time,
              0,
              sampling_time,
              rows,
              bytes_scanned,
              bytes_returned,
              data_cost,
              computation_cost,
              cost
              ]


def run_head_table_sampling(stats, sort_field_index, sort_field, k, sample_size, parallel, use_pandas, sort_order, buffer_size,
        table_parts_start, table_parts_end, tbl_s3key, shards_path):

    secure = False
    use_native = False
    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    stats += ['sampling_{}_{}'.format('head', 'non-filtered'), shards_path, sort_field, sort_order, sample_size, 1]

    # Query plan
    query_plan = QueryPlan(system=None, is_async=parallel, buffer_size=buffer_size)

    # Sampling
    table_parts = table_parts_end - table_parts_start + 1
    per_part_samples = int(sample_size / table_parts)
    table_name = os.path.basename(tbl_s3key)
    sample_scan = map(lambda p:
                      query_plan.add_operator(
                          SQLTableScan("{}/{}.{}".format(shards_path, table_name, p),
                                       'select {} from S3Object limit {};'.format(sort_field, per_part_samples),
                                       use_pandas, secure, use_native,
                                       'sample_scan_{}'.format(p), query_plan, False)),
                      range(table_parts_start, table_parts_end + 1))

    # Sampling project
    def project_fn(df):
        df.columns = [sort_field]
        df[[sort_field]] = df[[sort_field]].astype(np.float)
        return df

    project_exprs = [ProjectExpression(lambda t_: t_['_0'], sort_field)]

    sample_project = map(lambda p:
                         query_plan.add_operator(
                             Project(project_exprs, 'sample_project_{}'.format(p), query_plan, False, project_fn)),
                         range(table_parts_start, table_parts_end + 1))

    # TopK samples
    sort_expr = [SortExpression(sort_field, float, sort_order)]
    sample_topk = query_plan.add_operator(
        Top(k, sort_expr, use_pandas, 'sample_topk', query_plan, False))

    # Generate SQL command for second scan
    sql_gen = query_plan.add_operator(
        TopKFilterBuild(sort_order, 'float', 'select * from S3object ',
                        ' CAST({} as float) '.format(sort_field), 'sql_gen', query_plan, False))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda (p, o): o.connect(sample_project[p]), enumerate(sample_scan))
    map(lambda (p, o): o.connect(sample_topk), enumerate(sample_project))
    sample_topk.connect(sql_gen)
    sql_gen.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print('')

    # Start the query
    query_plan.execute()
    print('Done')

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    sampling_time = query_plan.total_elapsed_time
    cost, bytes_scanned, bytes_returned, rows = query_plan.cost()
    computation_cost = query_plan.computation_cost()
    data_cost = query_plan.data_cost()[0]

    stats += [sql_gen.threshold,
              sampling_time,
              0,
              sampling_time,
              rows,
              bytes_scanned,
              bytes_returned,
              data_cost,
              computation_cost,
              cost
              ]


if __name__ == "__main__":
    # main()
    #
    import sys,os
    # sys.exit(0)
    stats_header = [
        'Method',
        'Table',
        'Sort Field',
        'Sort Order',
        'Sample Size',
        'batch_size',
        'Sampling Threshold',
        'Sampling time (Sec)',
        'Query time (Sec)',
        'Total query time (Sec)',
        'Returned Rows',
        'Bytes Scanned MB',
        'Bytes Returned MB',
        'Data Cost $',
        'Computation Cost $',
        'Total Cost $',
    ]

    if len(sys.argv) >= 4:
        sampling_type = sys.argv[1]
        k = int(sys.argv[2])
        k_scale = int(sys.argv[3])
        batch_size = int(sys.argv[4])
        is_conservative = True
        table_name = sys.argv[5]
        shards_prefix = sys.argv[6]
        shards_start = int(sys.argv[7])
        shards_end = int(sys.argv[8])
        filtered = False if int(sys.argv[9]) == 0 else True
        if len(sys.argv) >= 11:
            stats_file_name = sys.argv[10]
        else:
            stats_file_name = 'indexed_sampling_topk_stats.txt'

        run_stats = []

        if sampling_type == 'indexed':
            run_memory_indexed_sampling(stats=run_stats,
                                        sort_field_index='_5',
                                        sort_field='l_extendedprice',
                                        k=k,
                                        sample_size=k * k_scale,
                                        batch_size=100,
                                        parallel=True,
                                        use_pandas=True,
                                        sort_order='DESC',
                                        buffer_size=0,
                                        table_parts_start=shards_start,
                                        table_parts_end=shards_end,
                                        tbl_s3key=table_name,
                                        shards_path=shards_prefix)
        elif sampling_type == 'beginning':
            run_head_table_sampling(
                stats=run_stats,
                sort_field_index='_5',
                sort_field='l_extendedprice',
                k=k,
                sample_size=k * k_scale,
                parallel=True,
                use_pandas=True,
                sort_order='DESC',
                buffer_size=0,
                table_parts_start=shards_start,
                table_parts_end=shards_end,
                tbl_s3key=table_name,
                shards_path=shards_prefix)

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
