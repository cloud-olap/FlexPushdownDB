# -*- coding: utf-8 -*-
"""TPCH Q17 Bloom Join Benchmark

"""

import os

import numpy

from s3filter import ROOT_DIR
from s3filter.benchmark.tpch import tpch_results
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one, connect_one_to_many
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q17
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd
import numpy as np


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result):
    run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=lineitem_parts,
        part_parts=part_parts, lineitem_sharded=lineitem_sharded, part_sharded=part_sharded, other_parts=other_parts, sf=sf, fp_rate=fp_rate,
        expected_result=expected_result)


def run(parallel, use_pandas, secure, use_native, buffer_size, lineitem_parts, part_parts, lineitem_sharded,
        part_sharded, other_parts, sf, fp_rate, expected_result):
    """
    :return: None
    """

    print('')
    print("TPCH Q17 Bloom Join")
    print("-------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    part_scan = map(lambda p:
                    query_plan.add_operator(
                        tpch_q17.sql_scan_select_partkey_where_brand_container_op(
                            part_sharded,
                            p,
                            part_parts,
                            use_pandas,
                            secure,
                            use_native,
                            'part_scan' + '_' + str(p),
                            query_plan,
                            sf)),
                    range(0, part_parts))

    part_project = map(lambda p:
                       query_plan.add_operator(
                           tpch_q17.project_partkey_op(
                               'part_project' + '_' + str(p),
                               query_plan)),
                       range(0, part_parts))

    part_lineitem_join_build_map = map(lambda p:
                                       query_plan.add_operator(
                                           Map('p_partkey', 'part_lineitem_join_build_map' + '_' + str(p), query_plan,
                                               False)),
                                       range(0, part_parts))

    part_bloom_create = query_plan.add_operator(
        tpch_q17.bloom_create_partkey_op(fp_rate, 'part_bloom_create',
                                         query_plan))

    lineitem_bloom_use = \
        map(lambda p:
            query_plan.add_operator(
                tpch_q17.bloom_scan_lineitem_select_orderkey_partkey_quantity_extendedprice_bloom_partkey_op(
                    lineitem_sharded,
                    p,
                    part_parts,
                    use_pandas,
                    secure,
                    use_native,
                    'lineitem_bloom_use' + '_' + str(p),
                    query_plan,
                    sf)),
            range(0, lineitem_parts))

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               tpch_q17.project_lineitem_filtered_orderkey_partkey_quantity_extendedprice_op(
                                   'lineitem_project' + '_' + str(p),
                                   query_plan)),
                           range(0, lineitem_parts))

    part_lineitem_join_probe_map = map(lambda p:
                                       query_plan.add_operator(
                                           Map('l_partkey', 'part_lineitem_join_probe_map' + '_' + str(p), query_plan,
                                               False)),
                                       range(0, lineitem_parts))

    part_lineitem_join_build = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinBuild('p_partkey',
                                                     'part_lineitem_join_build' + '_' + str(p), query_plan,
                                                     False)),
                                   range(0, other_parts))

    part_lineitem_join_probe = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                     'part_lineitem_join_probe' + '_' + str(p),
                                                     query_plan, False)),
                                   range(0, other_parts))

    lineitem_part_avg_group = map(lambda p:
                                  query_plan.add_operator(
                                      tpch_q17.group_partkey_avg_quantity_op('lineitem_part_avg_group' + '_' + str(p),
                                                                             query_plan)),
                                  range(0, other_parts))

    lineitem_part_avg_group_project = map(lambda p:
                                          query_plan.add_operator(
                                              tpch_q17.project_partkey_avg_quantity_op(
                                                  'lineitem_part_avg_group_project' + '_' + str(p), query_plan)),
                                          range(0, other_parts))

    part_lineitem_join_avg_group_join_build = \
        map(lambda p:
            query_plan.add_operator(
                HashJoinBuild('l_partkey',
                              'part_lineitem_join_avg_group_join_build' + '_' + str(p),
                              query_plan,
                              False)),
            range(0, other_parts))

    part_lineitem_join_avg_group_join_probe = \
        map(lambda p:
            query_plan.add_operator(
                HashJoinProbe(JoinExpression('l_partkey', 'p_partkey'),
                              'part_lineitem_join_avg_group_join_probe' + '_' + str(p),
                              query_plan,
                              False)),
            range(0, other_parts))

    lineitem_filter = map(lambda p:
                          query_plan.add_operator(
                              tpch_q17.filter_lineitem_quantity_op('lineitem_filter' + '_' + str(p), query_plan)),
                          range(0, other_parts))

    extendedprice_sum_aggregate = map(lambda p:
                                      query_plan.add_operator(
                                          tpch_q17.aggregate_sum_extendedprice_op(
                                              use_pandas,
                                              'extendedprice_sum_aggregate' + '_' + str(p),
                                              query_plan)),
                                      range(0, other_parts))

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

    extendedprice_sum_aggregate_project = query_plan.add_operator(
        tpch_q17.project_avg_yearly_op('extendedprice_sum_aggregate_project', query_plan))

    collate = query_plan.add_operator(tpch_q17.collate_op('collate', query_plan))

    # Inline what we can
    map(lambda o: o.set_async(False), lineitem_project)
    map(lambda o: o.set_async(False), part_project)
    map(lambda o: o.set_async(False), lineitem_filter)
    map(lambda o: o.set_async(False), part_lineitem_join_probe_map)
    map(lambda o: o.set_async(False), part_lineitem_join_build_map)
    map(lambda o: o.set_async(False), lineitem_part_avg_group)
    map(lambda o: o.set_async(False), lineitem_part_avg_group_project)
    map(lambda o: o.set_async(False), extendedprice_sum_aggregate)
    extendedprice_sum_aggregate_project.set_async(False)

    # Connect the operators
    # part_scan.connect(part_project)
    map(lambda (p, o): o.connect(part_project[p]), enumerate(part_scan))
    # map(lambda (p, o): o.connect(part_bloom_create_map[p]), enumerate(part_project))
    map(lambda (p, o): o.connect(part_lineitem_join_build_map[p]), enumerate(part_project))

    connect_many_to_one(part_project, part_bloom_create)
    connect_one_to_many(part_bloom_create, lineitem_bloom_use)

    # part_project.connect(part_bloom_create)
    # map(lambda (p1, o1): map(lambda (p2, o2): o1.connect(o2), enumerate(part_bloom_create)),
    #     enumerate(part_bloom_create_map))

    # part_bloom_create.connect(lineitem_bloom_use)
    # map(lambda (p1, o1): map(lambda (p2, o2): o1.connect(o2), enumerate(lineitem_bloom_use)),
    #     enumerate(part_bloom_create))

    # lineitem_bloom_use.connect(lineitem_project)
    map(lambda (p, o): o.connect(lineitem_project[p]), enumerate(lineitem_bloom_use))

    # part_lineitem_join.connect_left_producer(part_project)
    map(lambda (p1, o1): map(lambda (p2, o2): o1.connect(o2), enumerate(part_lineitem_join_build)),
        enumerate(part_lineitem_join_build_map))
    map(lambda (p, o): part_lineitem_join_probe[p].connect_build_producer(o), enumerate(part_lineitem_join_build))

    # part_lineitem_join.connect_right_producer(lineitem_project)
    # map(lambda (p, o): o.connect(part_lineitem_join_probe_map[p]), enumerate(lineitem_project))
    connect_many_to_many(lineitem_project, part_lineitem_join_probe_map)

    map(lambda (p1, o1): map(lambda (p2, o2): o2.connect_tuple_producer(o1), enumerate(part_lineitem_join_probe)),
        enumerate(part_lineitem_join_probe_map))

    # part_lineitem_join.connect(lineitem_part_avg_group)
    map(lambda (p, o): o.connect(lineitem_part_avg_group[p]), enumerate(part_lineitem_join_probe))

    # lineitem_part_avg_group.connect(lineitem_part_avg_group_project)
    map(lambda (p, o): o.connect(lineitem_part_avg_group_project[p]), enumerate(lineitem_part_avg_group))

    # part_lineitem_join_avg_group_join.connect_left_producer(lineitem_part_avg_group_project)
    map(lambda (p, o): o.connect(part_lineitem_join_avg_group_join_build[p]),
        enumerate(lineitem_part_avg_group_project))

    # part_lineitem_join_avg_group_join.connect_right_producer(part_lineitem_join)
    map(lambda (p, o): part_lineitem_join_avg_group_join_probe[p].connect_build_producer(o),
        enumerate(part_lineitem_join_avg_group_join_build))
    map(lambda (p, o): part_lineitem_join_avg_group_join_probe[p].connect_tuple_producer(o),
        enumerate(part_lineitem_join_probe))

    # part_lineitem_join_avg_group_join.connect(lineitem_filter)
    map(lambda (p, o): o.connect(lineitem_filter[p]), enumerate(part_lineitem_join_avg_group_join_probe))

    # lineitem_filter.connect(extendedprice_sum_aggregate)
    map(lambda (p, o): o.connect(extendedprice_sum_aggregate[p]), enumerate(lineitem_filter))

    # extendedprice_sum_aggregate.connect(extendedprice_sum_aggregate_project)
    map(lambda (p, o): o.connect(aggregate_reduce), enumerate(extendedprice_sum_aggregate))
    aggregate_reduce.connect(extendedprice_sum_aggregate_project)

    # extendedprice_sum_aggregate_project.connect(collate)
    extendedprice_sum_aggregate_project.connect(collate)

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

    field_names = ['avg_yearly']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    if s3filter.util.constants.TPCH_SF == 10:
        assert round(float(tuples[1][0]),
                     10) == 372414.2899999995  # TODO: This isn't correct but haven't checked tpch17 on 10 sf yet
    elif s3filter.util.constants.TPCH_SF == 1:
        numpy.testing.assert_approx_equal(float(tuples[1][0]), expected_result)


if __name__ == "__main__":
    main(1, 4, False, 4, False, 2, 0.1, tpch_results.q17_sf1_expected_result)
