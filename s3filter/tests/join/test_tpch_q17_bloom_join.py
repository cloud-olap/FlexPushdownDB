import os

from s3filter import ROOT_DIR
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.plan.query_plan import QueryPlan
from s3filter.tests.join.test_tpch_q17_baseline_join import part_scan_op, part_project_op, lineitem_project_op, \
    part_line_item_join_op, lineitem_avg_group_op, lineitem_part_avg_group_project_op, \
    part_lineitem_join_avg_group_op, lineitem_filter_op, extendedprice_sum_aggregate_op, \
    extendedprice_sum_aggregate_project_op, collate_op
from s3filter.util.test_util import gen_test_id


def test():
    """
    :return: None
    """

    query_plan = QueryPlan()

    # Define the operators
    part_scan = query_plan.add_operator(part_scan_op())
    part_project = query_plan.add_operator(part_project_op())
    lineitem_project = query_plan.add_operator(lineitem_project_op())
    part_bloom_create = query_plan.add_operator(
        BloomCreate('p_partkey', 'part_bloom_create', False))
    lineitem_bloom_use = query_plan.add_operator(
        SQLTableScanBloomUse('lineitem.csv',
                             "select "
                             "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                             "from "
                             "  S3Object "
                             "where "
                             "  l_partkey = '182405' ",
                             'l_partkey',
                             'lineitem_bloom_use',
                             False))
    part_lineitem_join = query_plan.add_operator(part_line_item_join_op())
    lineitem_part_avg_group = query_plan.add_operator(lineitem_avg_group_op())
    lineitem_part_avg_group_project = query_plan.add_operator(lineitem_part_avg_group_project_op())
    part_lineitem_join_avg_group_join = query_plan.add_operator(part_lineitem_join_avg_group_op())
    lineitem_filter = query_plan.add_operator(lineitem_filter_op())
    extendedprice_sum_aggregate = query_plan.add_operator(extendedprice_sum_aggregate_op())
    extendedprice_sum_aggregate_project = query_plan.add_operator(extendedprice_sum_aggregate_project_op())
    collate = query_plan.add_operator(collate_op())

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
