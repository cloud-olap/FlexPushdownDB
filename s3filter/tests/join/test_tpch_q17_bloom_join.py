# -*- coding: utf-8 -*-
"""TPCH Q17 Bloom Join

"""

import os

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q17
from s3filter.util.test_util import gen_test_id


def test_unbuffered():
    run(False, 0)


def test_buffered():
    run(False, 1024)


def test_parallel_buffered():
    run(True, 1024)


def run(parallel, buffer_size):
    """
    :return: None
    """

    print('')
    print("TPCH Q17 Bloom Join")
    print("-------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Define the operators
    part_scan = query_plan.add_operator(
        tpch_q17.sql_scan_select_partkey_where_brand_container_op('part_scan', query_plan))
    part_project = query_plan.add_operator(tpch_q17.project_partkey_op('part_project', query_plan))
    lineitem_project = query_plan.add_operator(
        tpch_q17.project_orderkey_partkey_quantity_extendedprice_op('lineitem_project', query_plan))
    part_bloom_create = query_plan.add_operator(tpch_q17.bloom_create_partkey_op('part_bloom_create', query_plan))
    lineitem_bloom_use = query_plan.add_operator(
        tpch_q17.bloom_scan_lineitem_select_orderkey_partkey_quantity_extendedprice_where_partkey_bloom_partkey_op(
            'lineitem_bloom_use', query_plan))
    part_lineitem_join = query_plan.add_operator(tpch_q17.join_p_partkey_l_partkey_op('part_lineitem_join', query_plan))
    lineitem_part_avg_group = query_plan.add_operator(
        tpch_q17.group_partkey_avg_quantity_op('lineitem_part_avg_group', query_plan))
    lineitem_part_avg_group_project = query_plan.add_operator(
        tpch_q17.project_partkey_avg_quantity_op('lineitem_part_avg_group_project', query_plan))
    part_lineitem_join_avg_group_join = query_plan.add_operator(
        tpch_q17.join_l_partkey_p_partkey_op('part_lineitem_join_avg_group_join', query_plan))
    lineitem_filter = query_plan.add_operator(tpch_q17.filter_lineitem_quantity_op('lineitem_filter', query_plan))
    extendedprice_sum_aggregate = query_plan.add_operator(
        tpch_q17.aggregate_sum_extendedprice_op('extendedprice_sum_aggregate', query_plan))
    extendedprice_sum_aggregate_project = query_plan.add_operator(
        tpch_q17.project_avg_yearly_op('extendedprice_sum_aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q17.collate_op('collate', query_plan))

    # Connect the operators
    part_scan.connect(part_project)
    part_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_bloom_use)
    lineitem_bloom_use.connect(lineitem_project)
    part_lineitem_join.connect_left_producer(part_project)
    part_lineitem_join.connect_right_producer(lineitem_project)
    part_lineitem_join.connect(lineitem_part_avg_group)
    lineitem_part_avg_group.connect(lineitem_part_avg_group_project)
    part_lineitem_join_avg_group_join.connect_left_producer(lineitem_part_avg_group_project)
    part_lineitem_join_avg_group_join.connect_right_producer(part_lineitem_join)
    part_lineitem_join_avg_group_join.connect(lineitem_filter)
    lineitem_filter.connect(extendedprice_sum_aggregate)
    extendedprice_sum_aggregate.connect(extendedprice_sum_aggregate_project)
    extendedprice_sum_aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

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
    assert tuples[1] == [1274.9142857142856]
