import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def test_unbuffered():
    run(parallel=False, use_pandas=False, buffer_size=0, lineitem_parts=1, part_parts=1)


def test_buffered():
    run(parallel=False, use_pandas=False, buffer_size=1024, lineitem_parts=1, part_parts=1)


def test_parallel_buffered():
    run(parallel=True, use_pandas=False, buffer_size=1024, lineitem_parts=1, part_parts=1)


def run(parallel, use_pandas, buffer_size, lineitem_parts, part_parts):
    """The filtered tst uses hash joins but first projections and filtering is pushed down to s3.

    :return: None
    """

    print('')
    print("TPCH Q14 Filtered Join")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = query_plan.add_operator(
        tpch_q14.sql_scan_lineitem_partkey_extendedprice_discount_where_shipdate_sharded_operator_def(min_shipped_date,
                                                                                              max_shipped_date,
                                                                                                      False, 0,
                                                                                              use_pandas,
                                                                                              'lineitem_scan',
                                                                                              query_plan))
    lineitem_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_project', query_plan))
    part_scan = query_plan.add_operator(
        tpch_q14.sql_scan_part_partkey_type_part_where_brand12_partitioned_operator_def(0, 1, use_pandas, 'part_scan', query_plan))
    part_project = query_plan.add_operator(tpch_q14.project_partkey_type_operator_def('part_project', query_plan))
    join = query_plan.add_operator(tpch_q14.join_lineitem_part_operator_def('join', query_plan))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate', query_plan))
    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def('aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))

    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)
    join.connect_left_producer(lineitem_project)
    join.connect_right_producer(part_project)
    join.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

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

    field_names = ['promo_revenue']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert tuples[1] == [15.090116526324298]
