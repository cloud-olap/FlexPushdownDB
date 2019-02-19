# -*- coding: utf-8 -*-
"""TPCH Q3 Bloom Benchmark

"""
import os
from datetime import date

import numpy as np

from s3filter import ROOT_DIR
from s3filter.benchmark.tpch import tpch_results
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.group import Group
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one, connect_one_to_many
from s3filter.op.project import Project
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.op.top import Top
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix
from s3filter.sql.format import Format
from s3filter.util import test_util
from s3filter.util.test_util import gen_test_id


def main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts, lineitem_sharded, fp_rate,
         other_parts, format_, expected_result, customer_filter_sql=None,
         order_filter_sql=None, lineitem_filter_sql=None):
    run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, format_=format_,
        customer_parts=customer_parts,
        order_parts=order_parts, lineitem_parts=lineitem_parts, customer_sharded=customer_sharded,
        order_sharded=order_sharded, lineitem_sharded=lineitem_sharded, other_parts=other_parts, fp_rate=fp_rate, sf=sf,
        expected_result=expected_result, customer_filter_sql=customer_filter_sql,
        order_filter_sql=order_filter_sql, lineitem_filter_sql=lineitem_filter_sql)


def run(parallel, use_pandas, secure, use_native, buffer_size, format_, customer_parts, order_parts, lineitem_parts,
        customer_sharded,
        order_sharded, lineitem_sharded, other_parts, fp_rate, sf, expected_result, customer_filter_sql=None,
        order_filter_sql=None, lineitem_filter_sql=None):
    """

    :return: None
    """

    print('')
    print("TPCH Q3 Bloom Join")
    print("------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    customer_scan = map(lambda p:
                        query_plan.add_operator(
                            SQLTableScan(get_file_key('customer', customer_sharded, p, sf),
                                         "select "
                                         "  c_custkey "
                                         "from "
                                         "  S3Object "
                                         "where "
                                         "  c_mktsegment = 'BUILDING' "
                                         "  {} "
                                         "  {} "
                                         .format(
                                             ' and ' + customer_filter_sql if customer_filter_sql is not None else '',
                                             get_sql_suffix('customer', customer_parts, p, customer_sharded,
                                                            add_where=False)),
                                         format_,
                                         use_pandas, secure, use_native,
                                         'customer_scan' + '_{}'.format(p),
                                         query_plan,
                                         False)),
                        range(0, customer_parts))

    def customer_project_fn(df):
        df = df.filter(items=['_0'], axis=1)

        df.rename(columns={'_0': 'c_custkey'},
                  inplace=True)

        return df

    customer_project = map(lambda p:
                           query_plan.add_operator(
                               Project([],
                                       'customer_project' + '_{}'.format(p),
                                       query_plan,
                                       False, customer_project_fn)),
                           range(0, customer_parts))

    customer_bloom_create = query_plan.add_operator(
        BloomCreate('c_custkey', 'customer_bloom_create', query_plan, False,
                    fp_rate))

    customer_map = map(lambda p:
                       query_plan.add_operator(Map('c_custkey', 'customer_map' + '_' + str(p), query_plan, False)),
                       range(0, customer_parts))

    order_scan = map(lambda p:
                     query_plan.add_operator(
                         SQLTableScanBloomUse(get_file_key('orders', order_sharded, p, sf),
                                              "select "
                                              "  o_custkey, o_orderkey, o_orderdate, o_shippriority "
                                              "from "
                                              "  S3Object "
                                              "where "
                                              "  cast(o_orderdate as timestamp) < cast('1995-03-01' as timestamp) "
                                              "  {} "
                                              "  {} "
                                              .format(
                                                  ' and ' + order_filter_sql if order_filter_sql is not None else '',
                                                  get_sql_suffix('orders', order_parts, p, order_sharded,
                                                                 add_where=False)),
                                              'o_custkey',format_,
                                              use_pandas, secure, use_native,
                                              'order_scan' + '_{}'.format(p),
                                              query_plan,
                                              False)),
                     range(0, order_parts))

    def order_project_fn(df):
        df = df.filter(items=['_0', '_1', '_2', '_3'], axis=1)

        df.rename(columns={'_0': 'o_custkey', '_1': 'o_orderkey', '_2': 'o_orderdate', '_3': 'o_shippriority'},
                  inplace=True)

        return df

    order_project = map(lambda p:
                        query_plan.add_operator(
                            Project([],
                                    'order_project' + '_{}'.format(p),
                                    query_plan,
                                    False, order_project_fn)),
                        range(0, customer_parts))

    order_map_1 = map(lambda p:
                      query_plan.add_operator(Map('o_custkey', 'order_map_1' + '_' + str(p), query_plan, False)),
                      range(0, order_parts))

    customer_order_join_build = map(lambda p:
                                    query_plan.add_operator(
                                        HashJoinBuild('c_custkey',
                                                      'customer_order_join_build' + '_' + str(p), query_plan,
                                                      False)),
                                    range(0, other_parts))

    customer_order_join_probe = map(lambda p:
                                    query_plan.add_operator(
                                        HashJoinProbe(JoinExpression('c_custkey', 'o_custkey'),
                                                      'customer_order_join_probe' + '_' + str(p),
                                                      query_plan, False)),
                                    range(0, other_parts))

    order_bloom_create = query_plan.add_operator(
        BloomCreate('o_orderkey', 'order_bloom_create', query_plan, False,
                    fp_rate))

    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            SQLTableScanBloomUse(get_file_key('lineitem', lineitem_sharded, p, sf),
                                                 "select "
                                                 "  l_orderkey, l_extendedprice, l_discount "
                                                 "from "
                                                 "  S3Object "
                                                 "where "
                                                 "  cast(l_shipdate as timestamp) > cast('1995-03-01' as timestamp) "
                                                 "  {} "
                                                 "  {} "
                                                 .format(
                                                     ' and ' + lineitem_filter_sql if lineitem_filter_sql is not None else '',
                                                     get_sql_suffix('lineitem', lineitem_parts, p, lineitem_sharded,
                                                                    add_where=False)),
                                                 'l_orderkey', format_,
                                                 use_pandas, secure, use_native,
                                                 'lineitem_scan' + '_{}'.format(p),
                                                 query_plan,
                                                 False)),
                        range(0, lineitem_parts))

    def lineitem_project_fn(df):
        df = df.filter(items=['_0', '_1', '_2'], axis=1)

        df.rename(columns={'_0': 'l_orderkey', '_1': 'l_extendedprice', '_2': 'l_discount'},
                  inplace=True)

        return df

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               Project([],
                                       'lineitem_project' + '_{}'.format(p),
                                       query_plan,
                                       False, lineitem_project_fn)),
                           range(0, lineitem_parts))

    lineitem_map = map(lambda p:
                       query_plan.add_operator(Map('l_orderkey', 'lineitem_map' + '_' + str(p), query_plan, False)),
                       range(0, lineitem_parts))

    order_map_2 = map(lambda p:
                      query_plan.add_operator(Map('o_orderkey', 'order_map_2' + '_' + str(p), query_plan, False)),
                      range(0, other_parts))

    customer_order_lineitem_join_build = map(lambda p:
                                             query_plan.add_operator(
                                                 HashJoinBuild('o_orderkey',
                                                               'customer_order_lineitem_join_build' + '_' + str(p),
                                                               query_plan,
                                                               False)),
                                             range(0, other_parts))

    customer_order_lineitem_join_probe = map(lambda p:
                                             query_plan.add_operator(
                                                 HashJoinProbe(JoinExpression('o_orderkey', 'l_orderkey'),
                                                               'customer_order_lineitem_join_probe' + '_' + str(p),
                                                               query_plan, False)),
                                             range(0, other_parts))

    def groupby_fn(df):
        df['l_extendedprice'] = df['l_extendedprice'].astype(np.float)
        df['l_discount'] = df['l_discount'].astype(np.float)
        df['revenue'] = df['l_extendedprice'] * (1 - df['l_discount'])
        grouped = df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'])
        agg_df = grouped['revenue'].sum()
        return agg_df.reset_index()

    group = map(lambda p:
                query_plan.add_operator(
                    Group(
                        ['l_orderkey', 'o_orderdate', 'o_shippriority'],  # l_partkey
                        [
                            AggregateExpression(AggregateExpression.SUM,
                                                lambda t_: float(t_['l_extendedprice'] * (1 - t_['l_discount'])))
                        ],
                        'group' + '_{}'.format(p), query_plan,
                        False, groupby_fn)),
                range(0, other_parts))

    def group_reduce_fn(df):
        grouped = df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'])
        agg_df = grouped['revenue'].sum()
        return agg_df.reset_index()

    group_reduce = query_plan.add_operator(
        Group(
            ['l_orderkey', 'o_orderdate', 'o_shippriority'],  # l_partkey
            [
                AggregateExpression(AggregateExpression.SUM,
                                    lambda t_: float(t_['l_extendedprice'] * (1 - t_['l_discount'])))
            ],
            'group_reduce', query_plan,
            False, group_reduce_fn))

    top = query_plan.add_operator(
        Top(10, [SortExpression('revenue', float, 'DESC'), SortExpression('o_orderdate', date, 'ASC')], use_pandas,
            'top', query_plan,
            False))

    collate = query_plan.add_operator(tpch_q19.collate_op('collate', query_plan))

    # Inline what we can
    map(lambda o: o.set_async(False), lineitem_project)
    map(lambda o: o.set_async(False), customer_project)
    map(lambda o: o.set_async(False), order_project)
    map(lambda o: o.set_async(False), lineitem_map)
    map(lambda o: o.set_async(False), customer_map)
    map(lambda o: o.set_async(False), order_map_1)
    map(lambda o: o.set_async(False), order_map_2)

    # Connect the operators
    connect_many_to_many(customer_scan, customer_project)
    connect_many_to_many(customer_project, customer_map)

    connect_many_to_one(customer_project, customer_bloom_create)
    connect_one_to_many(customer_bloom_create, order_scan)

    connect_many_to_many(order_scan, order_project)
    connect_many_to_many(order_project, order_map_1)

    connect_all_to_all(customer_map, customer_order_join_build)
    connect_many_to_many(customer_order_join_build, customer_order_join_probe)
    connect_all_to_all(order_map_1, customer_order_join_probe)

    # connect_many_to_one(customer_order_join_probe, collate)

    connect_many_to_one(order_project, order_bloom_create)
    connect_one_to_many(order_bloom_create, lineitem_scan)

    connect_many_to_many(lineitem_scan, lineitem_project)
    connect_many_to_many(lineitem_project, lineitem_map)

    connect_many_to_many(customer_order_join_probe, order_map_2)
    connect_all_to_all(order_map_2, customer_order_lineitem_join_build)
    connect_many_to_many(customer_order_lineitem_join_build, customer_order_lineitem_join_probe)
    connect_all_to_all(lineitem_map, customer_order_lineitem_join_probe)

    # connect_many_to_one(customer_order_lineitem_join_probe, collate)

    connect_many_to_many(customer_order_lineitem_join_probe, group)
    # connect_many_to_one(group, collate)

    connect_many_to_one(group, group_reduce)
    connect_one_to_one(group_reduce, top)
    connect_one_to_one(top, collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print('secure: {}'.format(secure))
    print('use_native: {}'.format(use_native))
    print("customer_parts: {}".format(customer_parts))
    print("order_parts: {}".format(order_parts))
    print("lineitem_parts: {}".format(lineitem_parts))
    print("customer_sharded: {}".format(customer_sharded))
    print("order_sharded: {}".format(order_sharded))
    print("lineitem_sharded: {}".format(lineitem_sharded))
    print("other_parts: {}".format(other_parts))
    print("fp_rate: {}".format(fp_rate))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['l_orderkey', 'o_orderdate', 'o_shippriority', 'revenue']

    assert len(tuples) == 10 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    test_util.assert_tuples(expected_result, tuples)


if __name__ == "__main__":
    main(sf=1,
         customer_parts=4, customer_sharded=False,
         order_parts=4, order_sharded=False,
         lineitem_parts=4, lineitem_sharded=False,
         fp_rate=0.1,
         other_parts=2,
         expected_result=tpch_results.q3_sf1_testing_expected_result,
         format_=Format.CSV,
         customer_filter_sql=tpch_results.q3_sf1_testing_params['customer_filter'],
         order_filter_sql=tpch_results.q3_sf1_testing_params['order_filter'],
         lineitem_filter_sql=tpch_results.q3_sf1_testing_params['lineitem_filter'])
