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
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.merge import Merge
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q14
from s3filter.util.test_util import gen_test_id


def test_baseline():
    run(False, False, 0, 1, 1)


def test_buffered():
    run(False, False, 8192, 1, 1)


def test_pandas_unbuffered():
    run(False, True, 0, 1, 1)


def test_parallel_buffered():
    run(True, False, 8192, 1, 1)


def test_parallel_pandas_unbuffered():
    run(True, True, 0, 1, 1)


def test_parallel_pandas_buffered():
    run(True, True, 16, 1, 1)


def test_parallel_pandas_buffered_sharded():
    run(True, True, 16, 32, 4)


def run(parallel, use_pandas, buffer_size, lineitem_parts, part_parts):
    """

    :return: None
    """

    print('')
    print("TPCH Q14 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    # This date is chosen because it triggers the filter to filter out 1 of the rows in the root data set.
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            tpch_q14.sql_scan_lineitem_extra_filtered_operator_def(
                                lineitem_parts != 1,
                                p,
                                use_pandas,
                                'lineitem_scan' + '_' + str(p),
                                query_plan)),
                        range(0, lineitem_parts))

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               tpch_q14.project_partkey_extendedprice_discount_shipdate_operator_def(
                                   'lineitem_project' + '_' + str(p),
                                   query_plan)),
                           range(0, lineitem_parts))

    part_scan = map(lambda p:
                    query_plan.add_operator(
                        tpch_q14.sql_scan_part_operator_def(
                            part_parts != 1,
                            p,
                            part_parts,
                            use_pandas,
                            'part_scan' + '_' + str(p),
                            query_plan)),
                    range(0, part_parts))

    part_project = map(lambda p:
                       query_plan.add_operator(
                           tpch_q14.project_partkey_brand_type_operator_def(
                               'part_project' + '_' + str(p),
                               query_plan)),
                       range(0, part_parts))

    lineitem_filter = map(lambda p:
                          query_plan.add_operator(
                              tpch_q14.filter_shipdate_operator_def(
                                  min_shipped_date,
                                  max_shipped_date,
                                  'lineitem_filter' + '_' + str(p),
                                  query_plan)),
                          range(0, lineitem_parts))

    part_filter = map(lambda p:
                      query_plan.add_operator(
                          tpch_q14.filter_brand12_operator_def('part_filter' + '_' + str(p), query_plan)),
                      range(0, part_parts))

    merge = map(lambda p:
                query_plan.add_operator(Merge(
                    'merge' + '_' + str(p),
                    query_plan,
                    True)),
                range(0, part_parts))

    join_build = map(lambda p:
                     query_plan.add_operator(
                         HashJoinBuild('p_partkey', 'join_build' + '_' + str(p), query_plan, False)),
                     range(0, part_parts))

    join_probe = map(lambda p:
                     query_plan.add_operator(
                         HashJoinProbe(JoinExpression('p_partkey', 'l_partkey'), 'join_probe' + '_' + str(p),
                                       query_plan, False)),
                     range(0, part_parts))

    part_aggregate = map(lambda p:
                         query_plan.add_operator(
                             tpch_q14.aggregate_promo_revenue_operator_def('part_aggregate' + '_' + str(p),
                                                                           query_plan)),
                         range(0, part_parts))

    # aggregate = query_plan.add_operator(
    #     tpch_q14.aggregate_promo_revenue_operator_def('aggregate', query_plan))

    aggregate = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0'])),
                AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_1']))
            ],
            'aggregate',
            query_plan,
            False))

    aggregate_project = query_plan.add_operator(
        tpch_q14.project_promo_revenue_operator_def(
            'aggregate_project',
            query_plan))

    collate = query_plan.add_operator(tpch_q14.collate_operator_def('collate', query_plan))

    # Connect the operators
    map(lambda (p, o): o.connect(lineitem_project[p]), enumerate(lineitem_scan))
    map(lambda (p, o): o.connect(lineitem_filter[p]), enumerate(lineitem_project))
    map(lambda (p, o): o.connect(merge[p % part_parts]), enumerate(lineitem_filter))
    map(lambda (p, o): o.connect(part_project[p]), enumerate(part_scan))
    map(lambda (p, o): o.connect(part_filter[p]), enumerate(part_project))
    map(lambda (p, o): o.connect(join_build[p]), enumerate(part_filter))
    map(lambda (p, o): map(lambda (bp, bo): o.connect_build_producer(bo), enumerate(join_build)), enumerate(join_probe))
    map(lambda (p, o): o.connect_tuple_producer(merge[p]), enumerate(join_probe))
    map(lambda (p, o): o.connect(part_aggregate[p]), enumerate(join_probe))
    map(lambda (p, o): o.connect(aggregate), enumerate(part_aggregate))
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("lineitem parts: {}".format(lineitem_parts))
    print("part_parts: {}".format(part_parts))
    print('')

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
    assert round(float(tuples[1][0]), 10) == 33.4262326420
