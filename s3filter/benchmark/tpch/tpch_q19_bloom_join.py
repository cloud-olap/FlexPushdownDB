# -*- coding: utf-8 -*-
"""TPCH Q19 Bloom Join Benchmark

"""

import os

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.util.test_util import gen_test_id


def main():
    run(0)
    run(1024)


def run(buffer_size):
    """

    :return: None
    """

    print('')
    print("TPCH Q19 Bloom Join")
    print("-------------------")

    query_plan = QueryPlan(is_async=False, buffer_size=buffer_size)

    # Define the operators
    part_scan = query_plan.add_operator(
        tpch_q19.sql_scan_part_partkey_brand_size_container_where_filtered_op(query_plan))
    lineitem_project = query_plan.add_operator(
        tpch_q19.project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_filtered_op(query_plan))
    part_project = query_plan.add_operator(tpch_q19.project_partkey_brand_size_container_filtered_op(query_plan))
    part_bloom_create = query_plan.add_operator(tpch_q19.bloom_create_partkey_op(query_plan))
    lineitem_bloom_use = query_plan.add_operator(
        tpch_q19.bloom_scan_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_filtered_op(query_plan))
    lineitem_part_join = query_plan.add_operator(tpch_q19.join_op(query_plan))
    filter_op = query_plan.add_operator(tpch_q19.filter_def(query_plan))
    aggregate = query_plan.add_operator(tpch_q19.aggregate_def(query_plan))
    aggregate_project = query_plan.add_operator(tpch_q19.aggregate_project_def(query_plan))
    collate = query_plan.add_operator(tpch_q19.collate_op(query_plan))

    # Connect the operators
    part_scan.connect(part_project)
    part_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_bloom_use)
    lineitem_bloom_use.connect(lineitem_project)
    lineitem_part_join.connect_left_producer(lineitem_project)
    lineitem_part_join.connect_right_producer(part_project)
    lineitem_part_join.connect(filter_op)
    filter_op.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

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

    # Write the metrics
    query_plan.print_metrics()

    field_names = ['revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [3468861.097000001]


if __name__ == "__main__":
    main()
