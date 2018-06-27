# -*- coding: utf-8 -*-
"""TPCH Q14 Filtered Benchmark

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def main():
    """The filtered tst uses hash joins but first projections and filtering is pushed down to s3.

    :return: None
    """

    print('')
    print("TPCH Q14 Filtered Join")
    print("----------------------")

    query_plan = QueryPlan()

    # Query plan
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = query_plan.add_operator(
        tpch_q14.sql_scan_lineitem_partkey_extendedprice_discount_where_shipdate_operator_def(min_shipped_date,
                                                                                              max_shipped_date,
                                                                                              'lineitem_scan'))
    lineitem_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_project'))
    part_scan = query_plan.add_operator(
        tpch_q14.sql_scan_part_partkey_type_part_where_brand12_operator_def('part_scan'))
    part_project = query_plan.add_operator(tpch_q14.project_partkey_type_operator_def('part_project'))
    join = query_plan.add_operator(tpch_q14.join_lineitem_part_operator_def('join'))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate'))
    aggregate_project = query_plan.add_operator(tpch_q14.project_promo_revenue_operator_def('aggregate_project'))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate'))

    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)
    join.connect_left_producer(lineitem_project)
    join.connect_right_producer(part_project)
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
