# -*- coding: utf-8 -*-
"""TPCH Q19 Filtered Benchmark

"""

import os

import numpy

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np


def main():
    if s3filter.util.constants.TPCH_SF == 10:
        run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=96,
            part_parts=4, lineitem_sharded=True, part_sharded=True, sf=10)
    elif s3filter.util.constants.TPCH_SF == 1:
        # run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=32,
        #     part_parts=4, lineitem_sharded=True, part_sharded=False, sf=1)
        run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=2,
            part_parts=2, lineitem_sharded=False, part_sharded=False, sf=1)


def run(parallel, use_pandas, secure, use_native, buffer_size, lineitem_parts, part_parts, lineitem_sharded,
        part_sharded, sf):
    """

    :return: None
    """

    print('')
    print("TPCH Q19 Filtered Join")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Define the operators
    lineitem_scan = \
        map(lambda p:
            query_plan.add_operator(
                tpch_q19.sql_scan_lineitem_select_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_filtered_op(
                    lineitem_sharded,
                    p,
                    lineitem_parts,
                    use_pandas,
                    secure,
                    use_native,
                    'lineitem_scan' + '_' + str(p),
                    query_plan, sf)),
            range(0, lineitem_parts))

    part_scan = map(lambda p:
                    query_plan.add_operator(
                        tpch_q19.sql_scan_part_partkey_brand_size_container_where_filtered_op(
                            part_sharded,
                            p,
                            part_parts,
                            use_pandas,
                            secure,
                            use_native,
                            'part_scan' + '_' + str(p), query_plan, sf)),
                    range(0, part_parts))

    lineitem_project = \
        map(lambda p:
            query_plan.add_operator(
                tpch_q19.project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_filtered_op(
                    'lineitem_project' + '_' + str(p), query_plan)),
            range(0, lineitem_parts))

    lineitem_map = map(lambda p:
                       query_plan.add_operator(Map('l_partkey', 'lineitem_map' + '_' + str(p), query_plan, False)),
                       range(0, part_parts))

    part_project = map(lambda p:
                       query_plan.add_operator(
                           tpch_q19.project_partkey_brand_size_container_filtered_op('part_project' + '_' + str(p),
                                                                                     query_plan)),
                       range(0, part_parts))

    part_map = map(lambda p:
                   query_plan.add_operator(Map('p_partkey', 'part_map' + '_' + str(p), query_plan, False)),
                   range(0, part_parts))

    # lineitem_part_join = map(lambda p:
    #                          query_plan.add_operator(tpch_q19.join_op(query_plan)),
    #                          range(0, part_parts))

    lineitem_part_join_build = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinBuild('p_partkey',
                                                     'lineitem_partjoin_build' + '_' + str(p), query_plan,
                                                     False)),
                                   range(0, part_parts))

    lineitem_part_join_probe = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                     'lineitem_part_join_probe' + '_' + str(p),
                                                     query_plan, False)),
                                   range(0, part_parts))

    filter_op = map(lambda p:
                    query_plan.add_operator(tpch_q19.filter_def('filter_op' + '_' + str(p), query_plan)),
                    range(0, part_parts))
    aggregate = map(lambda p:
                    query_plan.add_operator(tpch_q19.aggregate_def('aggregate' + '_' + str(p), query_plan, use_pandas)),
                    range(0, part_parts))

    def aggregate_reduce_fn(df):
        sum1_ = df['_0'].astype(np.float).sum()
        return pd.DataFrame({'_0': [sum1_]})

    aggregate_reduce = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0']))
            ],
            use_pandas,
            'aggregate_reduce',
            query_plan,
            False, aggregate_reduce_fn))

    aggregate_project = query_plan.add_operator(
        tpch_q19.aggregate_project_def('aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q19.collate_op('collate', query_plan))

    # Connect the operators
    # lineitem_scan.connect(lineitem_project)
    connect_many_to_many(lineitem_scan, lineitem_project)
    connect_all_to_all(lineitem_project, lineitem_map)

    # part_scan.connect(part_project)
    connect_many_to_many(part_scan, part_project)
    connect_many_to_many(part_project, part_map)

    # lineitem_part_join.connect_left_producer(lineitem_project)
    connect_all_to_all(part_map, lineitem_part_join_build)

    # lineitem_part_join.connect_right_producer(part_project)
    connect_many_to_many(lineitem_part_join_build, lineitem_part_join_probe)
    connect_many_to_many(lineitem_map, lineitem_part_join_probe)

    # lineitem_part_join.connect(filter_op)
    connect_many_to_many(lineitem_part_join_probe, filter_op)

    # filter_op.connect(aggregate)
    connect_many_to_many(filter_op, aggregate)

    # aggregate.connect(aggregate_project)
    connect_many_to_one(aggregate, aggregate_reduce)
    connect_one_to_one(aggregate_reduce, aggregate_project)

    # aggregate_project.connect(collate)
    connect_one_to_one(aggregate_project, collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print('secure: {}'.format(secure))
    print('use_native: {}'.format(use_native))
    print("lineitem parts: {}".format(lineitem_parts))
    print("part_parts: {}".format(part_parts))
    print("lineitem_sharded: {}".format(lineitem_sharded))
    print("part_sharded: {}".format(part_sharded))
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

    field_names = ['revenue']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    numpy.testing.assert_almost_equal(tuples[1], 3468861.097000001)


if __name__ == "__main__":
    main()
