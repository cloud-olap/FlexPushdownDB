# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 14

These queries tst the performance of several approaches to running the TCP-H Q14 query. In particular we tst pushing
predicates down to s3 and applying a bloom filter when joining between lineitem and part. To create a circumstance
where a bloom filter is useful we modify Q14 slightly, so that the join does not require all records from the lineitem
table (i.e. we add the predicate "p_brand = ‘Brand#12'".

TPCH Query 14

    select
       100.00 * sum(case
                when p_type like 'PROMO%'
                then l_extendedprice*(1-l_discount)
                else 0
       end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
       lineitem,
       part
    where
       l_partkey = p_partkey
       and l_shipdate >= date '[DATE]'
       and l_shipdate < date '[DATE]' + interval '1' month


Modified TPCH Query 14

    select
       100.00 * sum(case
                when p_type like 'PROMO%'
                then l_extendedprice*(1-l_discount)
                else 0
       end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
       lineitem,
       part
    where
       l_partkey = p_partkey
       and p_brand = ‘Brand#12'
       and l_shipdate >= date '[DATE]'
       and l_shipdate < date '[DATE]' + interval '1' month


For the purposes of testing we also limit the data extracted from s3 by only retrieving specific lineitems. This both
reduces network traffic and allows results to be verified in Postgres (Only be selecting specific rows can we be sure
that abitrary ordering of records don't affect the accuracy of the result).

We select 12 lineitems, 4 for Brand 11, 4 for Brand 12 and 4 for Brand 13. Each brand subset contains 2 promo items
and 2 non promo items.

The query for this is:

select
    *
from
    lineitem
where
    (l_orderkey = '18436' and l_partkey = '164584') or
    (l_orderkey = '18720' and l_partkey = '92764') or
    (l_orderkey = '12482' and l_partkey = '117405') or
    (l_orderkey = '27623' and l_partkey = '137010') or

    (l_orderkey = '10407' and l_partkey = '43275') or
    (l_orderkey = '17027' and l_partkey = '172729') or
    (l_orderkey = '23302' and l_partkey = '18523') or
    (l_orderkey = '27334' and l_partkey = '94308') or

    (l_orderkey = '15427' and l_partkey = '125586') or
    (l_orderkey = '11590' and l_partkey = '162359') or
    (l_orderkey = '2945'  and l_partkey = '126197') or
    (l_orderkey = '15648' and l_partkey = '143904')


"""

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
    """

    :return: None
    """

    print('')
    print("TPCH Q14 Bloom Join")
    print("-------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    # DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1993-01-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_scan = query_plan.add_operator(
        tpch_q14.sql_scan_part_partkey_type_part_where_brand12_partitioned_operator_def(
            0, 1, use_pandas, 'part_scan', query_plan))
    part_scan_project = query_plan.add_operator(
        tpch_q14.project_partkey_type_operator_def('part_scan_project', query_plan))
    part_bloom_create = query_plan.add_operator(
        tpch_q14.bloom_create_p_partkey_operator_def('part_bloom_create', query_plan))
    lineitem_scan = query_plan.add_operator(
        tpch_q14.bloom_scan_lineitem_where_shipdate_operator_def(min_shipped_date, max_shipped_date,
                                                                 False, 0, use_pandas, 'lineitem_scan',
                                                                 query_plan))
    lineitem_scan_project = query_plan.add_operator(
        tpch_q14.project_partkey_extendedprice_discount_operator_def('lineitem_scan_project', query_plan))
    join = query_plan.add_operator(tpch_q14.join_part_lineitem_operator_def('join', query_plan))
    aggregate = query_plan.add_operator(tpch_q14.aggregate_promo_revenue_operator_def('aggregate', query_plan))
    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def('aggregate_project', query_plan))
    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))

    part_scan.connect(part_scan_project)
    part_scan_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_scan)
    join.connect_left_producer(part_scan_project)
    lineitem_scan.connect(lineitem_scan_project)
    join.connect_right_producer(lineitem_scan_project)
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
