# -*- coding: utf-8 -*-
"""TPCH Q17 Filtered Join Benchmark

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
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q17
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants


def main():
    if s3filter.util.constants.TPCH_SF == 10:
        run(parallel=True, use_pandas=False, buffer_size=8192, lineitem_parts=96, part_parts=4)
    elif s3filter.util.constants.TPCH_SF == 1:
        # run(parallel=True, use_pandas=False, buffer_size=8192, lineitem_parts=1, part_parts=1)
        # run(parallel=True, use_pandas=False, buffer_size=8192, lineitem_parts=32, part_parts=4)
        run(parallel=True, use_pandas=True, buffer_size=0, lineitem_parts=32, part_parts=4)


def run(parallel, use_pandas, buffer_size, lineitem_parts, part_parts):
    """
    :return: None
    """

    print('')
    print("TPCH Q17 Filtered Join")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    part_scan = map(lambda p:
                    query_plan.add_operator(
                        tpch_q17.sql_scan_select_partkey_where_brand_container_op(
                            part_parts != 1,
                            p,
                            part_parts,
                            use_pandas,
                            'part_scan' + '_' + str(p),
                            query_plan)),
                    range(0, part_parts))

    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            tpch_q17.sql_scan_lineitem_select_orderkey_partkey_quantity_extendedprice(
                                lineitem_parts != 1,
                                p,
                                lineitem_parts,
                                use_pandas,
                                'lineitem_scan' + '_' + str(p),
                                query_plan)),
                        range(0, lineitem_parts))

    part_project = map(lambda p:
                       query_plan.add_operator(
                           tpch_q17.project_partkey_op(
                               'part_project' + '_' + str(p),
                               query_plan)),
                       range(0, part_parts))

    part_map = map(lambda p:
                   query_plan.add_operator(Map('p_partkey', 'part_map' + '_' + str(p), query_plan, False)),
                   range(0, part_parts))

    lineitem_map = map(lambda p:
                       query_plan.add_operator(Map('l_partkey', 'lineitem_map' + '_' + str(p), query_plan, False)),
                       range(0, lineitem_parts))

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               tpch_q17.project_lineitem_filtered_orderkey_partkey_quantity_extendedprice_op(
                                   'lineitem_project' + '_' + str(p), query_plan)),
                           range(0, lineitem_parts))

    # part_lineitem_join = map(lambda p:
    #                          query_plan.add_operator(
    #                              tpch_q17.join_p_partkey_l_partkey_op('part_lineitem_join', query_plan)),
    #                          range(0, lineitem_parts))

    part_lineitem_join_build = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinBuild('p_partkey',
                                                     'part_lineitem_join_build' + '_' + str(p), query_plan,
                                                     False)),
                                   range(0, part_parts))

    part_lineitem_join_probe = map(lambda p:
                                   query_plan.add_operator(
                                       HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                     'part_lineitem_join_probe' + '_' + str(p),
                                                     query_plan, False)),
                                   range(0, part_parts))

    lineitem_part_avg_group = map(lambda p:
                                  query_plan.add_operator(
                                      tpch_q17.group_partkey_avg_quantity_op('lineitem_part_avg_group' + '_' + str(p),
                                                                             query_plan)),
                                  range(0, part_parts))

    lineitem_part_avg_group_project = map(lambda p:
                                          query_plan.add_operator(
                                              tpch_q17.project_partkey_avg_quantity_op(
                                                  'lineitem_part_avg_group_project' + '_' + str(p), query_plan)),
                                          range(0, part_parts))

    # part_lineitem_join_avg_group_join = map(lambda p:
    #                                         query_plan.add_operator(
    #                                             tpch_q17.join_l_partkey_p_partkey_op(
    #                                                 'part_lineitem_join_avg_group_join', query_plan)),
    #                                         range(0, lineitem_parts))

    part_lineitem_join_avg_group_join_build = map(lambda p:
                                                  query_plan.add_operator(
                                                      HashJoinBuild('l_partkey',
                                                                    'part_lineitem_join_avg_group_join_build' + '_' + str(
                                                                        p), query_plan, False)),
                                                  range(0, part_parts))

    part_lineitem_join_avg_group_join_probe = map(lambda p:
                                                  query_plan.add_operator(
                                                      HashJoinProbe(JoinExpression('l_partkey', 'p_partkey'),
                                                                    'part_lineitem_join_avg_group_join_probe' + '_' + str(
                                                                        p),
                                                                    query_plan, False)),
                                                  range(0, part_parts))

    lineitem_filter = map(lambda p:
                          query_plan.add_operator(
                              tpch_q17.filter_lineitem_quantity_op('lineitem_filter' + '_' + str(p), query_plan)),
                          range(0, part_parts))

    extendedprice_sum_aggregate = map(lambda p:
                                      query_plan.add_operator(
                                          tpch_q17.aggregate_sum_extendedprice_op(
                                              'extendedprice_sum_aggregate' + '_' + str(p),
                                              query_plan)),
                                      range(0, part_parts))

    aggregate_reduce = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0']))
            ],
            'aggregate_reduce',
            query_plan,
            False))

    extendedprice_sum_aggregate_project = query_plan.add_operator(
        tpch_q17.project_avg_yearly_op('extendedprice_sum_aggregate_project', query_plan))

    collate = query_plan.add_operator(tpch_q17.collate_op('collate', query_plan))

    # Connect the operators
    # part_scan.connect(part_project)
    map(lambda (p, o): o.connect(part_project[p]), enumerate(part_scan))
    map(lambda (p, o): o.connect(part_map[p]), enumerate(part_project))

    # lineitem_scan.connect(lineitem_project)
    map(lambda (p, o): o.connect(lineitem_project[p]), enumerate(lineitem_scan))
    map(lambda (p, o): o.connect(lineitem_map[p]), enumerate(lineitem_project))

    # part_lineitem_join.connect_left_producer(part_project)
    # part_lineitem_join.connect_right_producer(lineitem_project)
    # part_lineitem_join.connect(lineitem_part_avg_group)
    map(lambda (p1, o1): map(lambda (p2, o2): o1.connect(o2), enumerate(part_lineitem_join_build)), enumerate(part_map))
    map(lambda (p, o): part_lineitem_join_probe[p].connect_build_producer(o), enumerate(part_lineitem_join_build))
    map(lambda (p1, o1): map(lambda (p2, o2): o2.connect_tuple_producer(o1), enumerate(part_lineitem_join_probe)),
        enumerate(lineitem_map))
    map(lambda (p, o): o.connect(lineitem_part_avg_group[p]), enumerate(part_lineitem_join_probe))

    # map(lambda (p, o): o.map(Mapper('_1', 1, part_lineitem_join_probe)), enumerate(lineitem_scan))

    # lineitem_part_avg_group.connect(lineitem_part_avg_group_project)
    map(lambda (p, o): o.connect(lineitem_part_avg_group_project[p]), enumerate(lineitem_part_avg_group))

    # part_lineitem_join_avg_group_join.connect_left_producer(lineitem_part_avg_group_project)
    # part_lineitem_join_avg_group_join.connect_right_producer(part_lineitem_join)
    # part_lineitem_join_avg_group_join.connect(lineitem_filter)

    map(lambda (p, o): o.connect(part_lineitem_join_avg_group_join_build[p]),
        enumerate(lineitem_part_avg_group_project))

    # map(lambda (p, o): map(lambda (bp, bo): o.connect_build_producer(bo),
    #                        enumerate(part_lineitem_join_avg_group_join_build)),
    #     enumerate(part_lineitem_join_avg_group_join_probe))
    map(lambda (p, o): part_lineitem_join_avg_group_join_probe[p].connect_build_producer(o),
        enumerate(part_lineitem_join_avg_group_join_build))
    map(lambda (p, o): part_lineitem_join_avg_group_join_probe[p % part_parts].connect_tuple_producer(o),
        enumerate(part_lineitem_join_probe))
    map(lambda (p, o): o.connect(lineitem_filter[p]), enumerate(part_lineitem_join_avg_group_join_probe))

    # lineitem_filter.connect(extendedprice_sum_aggregate)
    map(lambda (p, o): o.connect(extendedprice_sum_aggregate[p]), enumerate(lineitem_filter))
    # extendedprice_sum_aggregate.connect(extendedprice_sum_aggregate_project)
    # extendedprice_sum_aggregate_project.connect(collate)
    map(lambda (p, o): o.connect(aggregate_reduce), enumerate(extendedprice_sum_aggregate))
    aggregate_reduce.connect(extendedprice_sum_aggregate_project)
    extendedprice_sum_aggregate_project.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("lineitem parts: {}".format(lineitem_parts))
    print("part_parts: {}".format(part_parts))
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
        numpy.testing.assert_almost_equal(float(tuples[1][0]), 372414.29)


if __name__ == "__main__":
    main()
