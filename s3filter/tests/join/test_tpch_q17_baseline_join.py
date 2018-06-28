# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 17

These queries tst the performance of several approaches to running the TCP-H Q17 query.

TPCH Query 17

    select
        sum(l_extendedprice) / 7.0 as avg_yearly
    from
        lineitem,
        part
    where
        p_partkey = l_partkey
        and p_brand = cast('Brand#41' as char(10))
        and p_container = cast('SM PACK' as char(10))
        and l_quantity < (
            select
                0.2 * avg(l_quantity)
            from
                lineitem
            where
                l_partkey = p_partkey
        );



"""
import os

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q17
from s3filter.util.test_util import gen_test_id


def test_streamed():
    run(True)


def test_batched():
    run(False)


def run(is_streamed):
    """The baseline tst uses nested loop joins with no projection and no filtering pushed down to s3.

    This works by:

    1. Scanning part and filtering on brand and container
    2. It then scans lineitem
    3. It then joins the two tables (essentially filtering out lineitems that dont include parts that we filtered out in
       step 1)
    4. It then computes the average of l_quantity from the joined table and groups the results by partkey
    5. It then joins these computed averages with the joined table in step 3
    6. It then filters out any rows where l_quantity is less than the computed average

    TODO: There are few ways this can be done, the above is just one.

    :return: None
    """

    print('')
    print("TPCH Q17 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan(None, is_streamed)

    # Define the operators
    part_scan = query_plan.add_operator(tpch_q17.sql_scan_part_select_all_where_brand_and_container_op('part_scan'))
    lineitem_scan = query_plan.add_operator(tpch_q17.sql_scan_lineitem_select_all_where_partkey_op('lineitem_scan'))
    part_project = query_plan.add_operator(tpch_q17.project_partkey_brand_container_op('part_project'))
    part_filter = query_plan.add_operator(tpch_q17.filter_brand_container_op('part_filter'))
    lineitem_project = query_plan.add_operator(
        tpch_q17.project_lineitem_orderkey_partkey_quantity_extendedprice_op('lineitem_project'))
    part_lineitem_join = query_plan.add_operator(tpch_q17.join_p_partkey_l_partkey_op('part_lineitem_join'))
    lineitem_part_avg_group = query_plan.add_operator(tpch_q17.group_partkey_avg_quantity_op('lineitem_part_avg_group'))
    lineitem_part_avg_group_project = query_plan.add_operator(
        tpch_q17.project_partkey_avg_quantity_op('lineitem_part_avg_group_project'))
    part_lineitem_join_avg_group_join = query_plan.add_operator(
        tpch_q17.join_l_partkey_p_partkey_op('part_lineitem_join_avg_group_join'))
    lineitem_filter = query_plan.add_operator(tpch_q17.filter_lineitem_quantity_op('lineitem_filter'))
    extendedprice_sum_aggregate = query_plan.add_operator(
        tpch_q17.aggregate_sum_extendedprice_op('extendedprice_sum_aggregate'))
    extendedprice_sum_aggregate_project = query_plan.add_operator(
        tpch_q17.project_avg_yearly_op('extendedprice_sum_aggregate_project'))
    collate = query_plan.add_operator(tpch_q17.collate_op('collate'))

    # Connect the operators
    part_scan.connect(part_project)
    lineitem_scan.connect(lineitem_project)
    part_project.connect(part_filter)
    part_lineitem_join.connect_left_producer(part_filter)
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

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    collate.print_tuples()

    # Write the metrics and plan graph
    query_plan.print_metrics()

    field_names = ['avg_yearly']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [1274.9142857142856]
