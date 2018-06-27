# -*- coding: utf-8 -*-
"""TPCH Q17 Filtered Join Benchmark

"""

import os

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q17
from s3filter.util.test_util import gen_test_id


def main():
    """
    :return: None
    """

    print('')
    print("TPCH Q17 Filtered Join")
    print("----------------------")

    query_plan = QueryPlan()

    # Define the operators
    part_scan = query_plan.add_operator(tpch_q17.sql_scan_select_partkey_where_brand_container_op('part_scan'))
    lineitem_scan = query_plan.add_operator(
        tpch_q17.sql_scan_lineitem_select_orderkey_partkey_quantity_extendedprice('lineitem_scan'))
    part_project = query_plan.add_operator(tpch_q17.project_partkey_op('part_project'))
    lineitem_project = query_plan.add_operator(
        tpch_q17.project_lineitem_filtered_orderkey_partkey_quantity_extendedprice_op('lineitem_project'))
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
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

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
    assert collate.tuples()[1] == [372414.28999999946]


if __name__ == "__main__":
    main()
