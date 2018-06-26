# -*- coding: utf-8 -*-
"""TPCH Q14 Baseline Benchmark

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def main():
    """The baseline tst uses hash joins with no projection and no filtering pushed down to s3.

    :return: None
    """

    print('')
    print("TPCH Q14 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan()

    # Query plan
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = query_plan.add_operator(tpch_q14.sql_scan_lineitem_operator_def('lineitem_scan'))
    lineitem_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_shipdate_operator_def('lineitem_project'))
    part_scan = query_plan.add_operator(tpch_q14.sql_scan_part_operator_def('part_scan'))
    part_project = query_plan.add_operator(tpch_q14.project_partkey_brand_type_operator_def('part_project'))
    lineitem_filter = query_plan.add_operator(
        tpch_q14.filter_shipdate_operator_def(min_shipped_date, max_shipped_date, 'lineitem_filter'))
    part_filter = query_plan.add_operator(tpch_q14.filter_brand12_operator_def('part_filter'))
    join = query_plan.add_operator(tpch_q14.join_lineitem_part_operator_def('join'))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate'))
    aggregate_project = query_plan.add_operator(tpch_q14.project_promo_revenue_operator_def('aggregate_project'))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate'))

    lineitem_scan.connect(lineitem_project)
    lineitem_project.connect(lineitem_filter)
    join.connect_left_producer(lineitem_filter)
    part_scan.connect(part_project)
    part_project.connect(part_filter)
    join.connect_right_producer(part_filter)
    join.connect(aggregate)
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

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [15.090116526324298]


if __name__ == "__main__":
    main()
