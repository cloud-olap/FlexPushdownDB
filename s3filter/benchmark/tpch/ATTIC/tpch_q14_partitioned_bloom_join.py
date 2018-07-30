# -*- coding: utf-8 -*-
"""TPCH Q14 Bloom Join Benchmark

"""
import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def main():
    run(False, 0, 1)
    run(False, 0, 2)

    run(False, 8192, 1)
    run(False, 8192, 2)

    run(True, 8192, 1)
    run(True, 8192, 2)
    run(True, 8192, 4)
    run(True, 8192, 8)


def run(parallel, buffer_size, parts):
    """

    :return: None
    """

    print('')
    print("TPCH Q14 Partitioned Bloom Join")
    print("-------------------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    # DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))
    sum_aggregate = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0'])),
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_1']))
            ],
            'sum_aggregate',
            query_plan,
            False))
    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def('aggregate_project', query_plan))

    hash_join_build_ops = []
    hash_join_probe_ops = []
    part_bloom_create_ops = []
    line_item_scan_ops = []
    for p in range(0, parts):
        part_scan = query_plan.add_operator(
            tpch_q14.sql_scan_part_partkey_type_part_where_brand12_sharded_operator_def(p, parts,
                                                                                        'part_scan' + '_' + str(p),
                                                                                        query_plan))
        part_scan_project = query_plan.add_operator(
            tpch_q14.project_partkey_type_operator_def('part_scan_project' + '_' + str(p), query_plan))

        part_bloom_create = query_plan.add_operator(
            tpch_q14.bloom_create_p_partkey_operator_def('part_bloom_create' + '_' + str(p), query_plan))
        part_bloom_create_ops.append(part_bloom_create)

        lineitem_scan = query_plan.add_operator(
            tpch_q14.bloom_scan_lineitem_where_shipdate_sharded_operator_def(min_shipped_date, max_shipped_date, p,
                                                                             parts, 'lineitem_scan' + '_' + str(p),
                                                                             query_plan))
        line_item_scan_ops.append(lineitem_scan)

        lineitem_scan_project = query_plan.add_operator(
            tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_scan_project' + '_' + str(p),
                                                                         query_plan))

        join_build = query_plan.add_operator(
            HashJoinBuild('p_partkey', 'join_build' + '_' + str(p), query_plan, False))
        hash_join_build_ops.append(join_build)

        join_probe = query_plan.add_operator(
            HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'), 'join_probe' + '_' + str(p),
                          query_plan, False))
        hash_join_probe_ops.append(join_probe)

        aggregate = query_plan.add_operator(
            tpch_q14.aggregate_promo_revenue_operator_def('aggregate' + '_' + str(p), query_plan))

        part_scan.connect(part_scan_project)
        lineitem_scan.connect(lineitem_scan_project)

        part_scan_project.connect(part_bloom_create)
        # part_bloom_create.connect(lineitem_scan)

        part_scan_project.connect(join_build)

        join_probe.connect(aggregate)
        # aggregate.connect(aggregate_project)
        aggregate.connect(sum_aggregate)

        join_probe.connect_tuple_producer(lineitem_scan_project)
        # join_probe.connect(merge)

    for bloom_create_op in part_bloom_create_ops:
        for scan_op in line_item_scan_ops:
            bloom_create_op.connect(scan_op)

    for probe_op in hash_join_probe_ops:
        for build_op in hash_join_build_ops:
            probe_op.connect_build_producer(build_op)

    sum_aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print("parts: {}".format(parts))
    print('')

    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(parts))

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
    # assert tuples[1] == [15.090116526324298]
    assert round(float(tuples[1][0]), 10) == 15.0901165263


if __name__ == "__main__":
    main()
