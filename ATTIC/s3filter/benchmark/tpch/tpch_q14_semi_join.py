# -*- coding: utf-8 -*-
"""TPCH Q14 Semi Join Benchmark

"""

import os
from datetime import datetime, timedelta

import numpy

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants


def main():
    if s3filter.util.constants.TPCH_SF == 10:
        run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=96,
            part_parts=4, lineitem_sharded=True, part_sharded=True)
    elif s3filter.util.constants.TPCH_SF == 1:
        run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=32,
            part_parts=4, lineitem_sharded=True, part_sharded=False)


def run(parallel, use_pandas, secure, use_native, buffer_size, lineitem_parts, part_parts, lineitem_sharded,
        part_sharded):
    """

    :return: None
    """

    print('')
    print("TPCH Q14 Semi Join")
    print("------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    # DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_scan_1 = map(lambda p:
                      query_plan.add_operator(
                          tpch_q14.sql_scan_part_partkey_where_brand12_operator_def(
                              part_sharded,
                              p,
                              part_parts,
                              use_pandas,
                              secure,
                              use_native,
                              'part_table_scan_1' + '_' + str(p),
                              query_plan)),
                      range(0, part_parts))
    part_1_project = map(lambda p:
                         query_plan.add_operator(
                             tpch_q14.project_p_partkey_operator_def('part_1_project' + '_' + str(p),
                                                                     query_plan)),
                         range(0, part_parts))
    part_bloom_create = map(lambda p:
                            query_plan.add_operator(
                                tpch_q14.bloom_create_p_partkey_operator_def('part_bloom_create' + '_' + str(p),
                                                                             query_plan)),
                            range(0, part_parts))
    lineitem_scan_1 = map(lambda p:
                          query_plan.add_operator(
                              tpch_q14.bloom_scan_lineitem_partkey_where_shipdate_operator_def(
                                  min_shipped_date,
                                  max_shipped_date,
                                  lineitem_sharded,
                                  p,
                                  use_pandas,
                                  secure,
                                  use_native,
                                  'lineitem_scan_1' + '_' + str(p),
                                  query_plan)),
                          range(0, lineitem_parts))
    lineitem_1_project = map(lambda p:
                             query_plan.add_operator(
                                 tpch_q14.project_l_partkey_operator_def('lineitem_1_project' + '_' + str(p),
                                                                         query_plan)),
                             range(0, lineitem_parts))
    part_lineitem_join_1_build = map(lambda p:
                                     query_plan.add_operator(
                                         HashJoinBuild('p_partkey', 'part_lineitem_join_1_build' + '_' + str(p),
                                                       query_plan, False)),
                                     range(0, part_parts))
    part_lineitem_join_1_probe = map(lambda p:
                                     query_plan.add_operator(
                                         HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                       'part_lineitem_join_1_probe' + '_' + str(p),
                                                       query_plan, False)),
                                     range(0, part_parts))
    join_bloom_create = map(lambda p:
                            query_plan.add_operator(
                                tpch_q14.bloom_create_l_partkey_operator_def('join_bloom_create' + '_' + str(p),
                                                                             query_plan)),
                            range(0, part_parts))
    part_scan_2 = map(lambda p:
                      query_plan.add_operator(
                          tpch_q14.bloom_scan_part_partkey_type_brand12_operator_def(
                              part_sharded,
                              p,
                              part_parts,
                              use_pandas,
                              secure,
                              use_native,
                              'part_table_scan_2' + '_' + str(p),
                              query_plan)),
                      range(0, part_parts))
    part_2_project = map(lambda p:
                         query_plan.add_operator(
                             tpch_q14.project_partkey_type_operator_def('part_2_project' + '_' + str(p),
                                                                        query_plan)),
                         range(0, part_parts))
    lineitem_scan_2 = map(lambda p:
                          query_plan.add_operator(
                              tpch_q14.bloom_scan_lineitem_where_shipdate_operator_def(
                                  min_shipped_date,
                                  max_shipped_date,
                                  lineitem_sharded,
                                  p,
                                  use_pandas,
                                  secure,
                                  use_native,
                                  'lineitem_scan_2' + '_' + str(p),
                                  query_plan)),
                          range(0, lineitem_parts))
    lineitem_2_project = map(lambda p:
                             query_plan.add_operator(
                                 tpch_q14.project_partkey_extendedprice_discount_operator_def(
                                     'lineitem_2_project' + '_' + str(p), query_plan)),
                             range(0, lineitem_parts))

    part_lineitem_join_2_build = map(lambda p:
                                     query_plan.add_operator(
                                         HashJoinBuild('p_partkey', 'part_lineitem_join_2_build' + '_' + str(p),
                                                       query_plan, False)),
                                     range(0, part_parts))
    part_lineitem_join_2_probe = map(lambda p:
                                     query_plan.add_operator(
                                         HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'),
                                                       'part_lineitem_join_2_probe' + '_' + str(p),
                                                       query_plan, False)),
                                     range(0, part_parts))

    part_aggregate = map(lambda p:
                         query_plan.add_operator(
                             tpch_q14.aggregate_promo_revenue_operator_def(
                                 'part_aggregate' + '_' + str(p),
                                 query_plan)),
                         range(0, part_parts))

    aggregate_reduce = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0'])),
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_1']))
            ],
            'aggregate_reduce',
            query_plan,
            False))

    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def('aggregate_project', query_plan))

    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))

    # Connect the operators
    map(lambda o, p: o.connect(part_1_project[p]), enumerate(part_scan_1))
    map(lambda (p, o): o.connect(part_bloom_create[p]), enumerate(part_1_project)),
    map(lambda (p, o): map(lambda (bp, bo): bo.connect(o), enumerate(part_bloom_create)),
        enumerate(lineitem_scan_1))
    map(lambda (p, o): o.connect(part_lineitem_join_1_build[p]), enumerate(part_1_project))
    map(lambda (p, o): map(lambda (bp, bo): o.connect_build_producer(bo), enumerate(part_lineitem_join_1_build)),
        enumerate(part_lineitem_join_1_probe))
    map(lambda (p, o): o.connect(lineitem_1_project[p]), enumerate(lineitem_scan_1))
    map(lambda (p, o): part_lineitem_join_1_probe[p % part_parts].connect_tuple_producer(o),
        enumerate(lineitem_1_project))
    map(lambda (p, o): o.connect(join_bloom_create[p]), enumerate(part_lineitem_join_1_probe))
    map(lambda (p, o): map(lambda (bp, bo): o.connect(bo), enumerate(part_scan_2)),
        enumerate(join_bloom_create))
    map(lambda (p, o): map(lambda (bp, bo): bo.connect(o), enumerate(join_bloom_create)),
        enumerate(lineitem_scan_2))
    map(lambda (p, o): o.connect(part_2_project[p]), enumerate(part_scan_2))
    map(lambda (p, o): o.connect(part_lineitem_join_2_build[p]), enumerate(part_2_project))
    map(lambda (p, o): map(lambda (bp, bo): o.connect_build_producer(bo), enumerate(part_lineitem_join_2_build)),
        enumerate(part_lineitem_join_2_probe))
    map(lambda (p, o): o.connect(lineitem_2_project[p]), enumerate(lineitem_scan_2))
    map(lambda (p, o): part_lineitem_join_2_probe[p % part_parts].connect_tuple_producer(o),
        enumerate(lineitem_2_project))
    map(lambda (p, o): o.connect(part_aggregate[p]), enumerate(part_lineitem_join_2_probe))
    map(lambda (p, o): o.connect(aggregate_reduce), enumerate(part_aggregate))
    aggregate_reduce.connect(aggregate_project)
    aggregate_project.connect(collate)

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

    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['promo_revenue']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    if s3filter.util.constants.TPCH_SF == 10:
        assert round(float(tuples[1][0]), 10) == 15.4488836202
    elif s3filter.util.constants.TPCH_SF == 1:
        numpy.testing.assert_almost_equal(float(tuples[1][0]), 15.0901165263)


if __name__ == "__main__":
    main()
