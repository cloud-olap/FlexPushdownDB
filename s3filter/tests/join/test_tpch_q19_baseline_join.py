# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 17

These queries tst the performance of several approaches to running the TCP-H Q17 query.

TPCH Query 19

    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#11' as char(10))
        and p_container in (
            cast('SM CASE' as char(10)),
            cast('SM BOX' as char(10)),
            cast('SM PACK' as char(10)),
            cast('SM PKG' as char(10))
        )
        and l_quantity >= 3 and l_quantity <= 3 + 10
        and p_size between 1 and 5
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#44' as char(10))
        and p_container in (
            cast('MED BAG' as char(10)),
            cast('MED BOX' as char(10)),
            cast('MED PKG' as char(10)),
            cast('MED PACK' as char(10))
        )
        and l_quantity >= 16 and l_quantity <= 16 + 10
        and p_size between 1 and 10
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = cast('Brand#53' as char(10))
        and p_container in (
            cast('LG CASE' as char(10)),
            cast('LG BOX' as char(10)),
            cast('LG PACK' as char(10)),
            cast('LG PKG' as char(10))
        )
        and l_quantity >= 24 and l_quantity <= 24 + 10
        and p_size between 1 and 15
        and l_shipmode in (cast('AIR' as char(10)), cast('AIR REG' as char(10)))
        and l_shipinstruct = cast('DELIVER IN PERSON' as char(25))
    );





"""
import os

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.util.test_util import gen_test_id


def test_streamed():
    run(True)


def test_batched():
    run(False)


def run(is_streamed):
    """

    :return: None
    """

    print('')
    print("TPCH Q19 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan(None, is_streamed)

    # Define the operators
    lineitem_scan = query_plan.add_operator(tpch_q19.sql_scan_lineitem_select_all_where_partkey_op())
    part_scan = query_plan.add_operator(tpch_q19.sql_scan_part_select_all_where_partkey_op())
    lineitem_project = query_plan.add_operator(
        tpch_q19.project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_op())
    part_project = query_plan.add_operator(tpch_q19.project_partkey_brand_size_container_op())
    lineitem_part_join = query_plan.add_operator(tpch_q19.join_op())
    filter_op = query_plan.add_operator(tpch_q19.filter_def())
    aggregate = query_plan.add_operator(tpch_q19.aggregate_def())
    aggregate_project = query_plan.add_operator(tpch_q19.aggregate_project_def())
    collate = query_plan.add_operator(tpch_q19.collate_op())

    # Connect the operators
    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)
    lineitem_part_join.connect_left_producer(lineitem_project)
    lineitem_part_join.connect_right_producer(part_project)
    lineitem_part_join.connect(filter_op)
    filter_op.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

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

    # Write the metrics
    query_plan.print_metrics()

    field_names = ['revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [92403.0667]
