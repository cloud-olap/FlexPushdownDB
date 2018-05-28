# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 14

These queries test the performance of several approaches to running the TCP-H Q14 query. In particular we test pushing
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

import re
from datetime import datetime, timedelta

from metric.op_metrics import OpMetrics
from op.aggregate import Aggregate, AggregateExpression
from op.bloom_create import BloomCreate
from op.table_scan_bloom_use import TableScanBloomUse
from op.collate import Collate
from op.compute import Compute, ComputeExpression
from op.filter import Filter, PredicateExpression
from op.join import Join, JoinExpression
from op.table_scan import TableScan
from sql.function import cast, timestamp, sum_fn


def test_join_baseline():
    """The baseline test uses nested loop joins with no projection and no filtering pushed down to s3.

    :return: None
    """

    print("Baseline Join")

    # Query plan
    # This date is chosen because it triggers the filter to filter out 1 of the rows in the root data set.
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    table_scan_1 = TableScan('lineitem.csv',
                             "select * from S3Object "
                             "where "
                             "(l_orderkey = '18436' and l_partkey = '164584') or "
                             "(l_orderkey = '18720' and l_partkey = '92764') or "
                             "(l_orderkey = '12482' and l_partkey = '117405') or "
                             "(l_orderkey = '27623' and l_partkey = '137010') or "

                             "(l_orderkey = '10407' and l_partkey = '43275') or "
                             "(l_orderkey = '17027' and l_partkey = '172729') or "
                             "(l_orderkey = '23302' and l_partkey = '18523') or "
                             "(l_orderkey = '27334' and l_partkey = '94308') or "

                             "(l_orderkey = '15427' and l_partkey = '125586') or "
                             "(l_orderkey = '11590' and l_partkey = '162359') or "
                             "(l_orderkey = '2945' and l_partkey = '126197') or "
                             "(l_orderkey = '15648' and l_partkey = '143904');", 'table_scan_1', False)
    table_scan_2 = TableScan('part.csv',
                             "select * from S3Object;", 'table_scan_2', False)
    lineitem_filter = Filter(PredicateExpression(lambda t:
                                           (cast(t['_10'], timestamp) >= cast(min_shipped_date, timestamp)) and
                                           (cast(t['_10'], timestamp) < cast(max_shipped_date, timestamp))
                                           ), 'lineitem_filter', False)
    part_filter = Filter(PredicateExpression(lambda t: t['_3'] == 'Brand#12'), 'part_filter', False)  # p_brand
    join = Join(JoinExpression('lineitem.csv', '_1', 'part.csv', '_0'), 'join', False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['lineitem.csv._5']) * (1.0 - float(t['lineitem.csv._6']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['part.csv._4']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, 0, ctx)

    def ex2(t, ctx):

        v1 = float(t['lineitem.csv._5']) * (1.0 - float(t['lineitem.csv._6']))  # l_extendedprice and l_discount

        sum_fn(v1, 1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']), 'compute', False)
    collate = Collate('collate', False)

    table_scan_1.connect(lineitem_filter)
    lineitem_filter.connect(join)
    table_scan_2.connect(part_filter)
    part_filter.connect(join)
    join.connect(aggregate)
    aggregate.connect(compute)
    compute.connect(collate)

    # Start the query
    table_scan_1.start()
    table_scan_2.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics([
        table_scan_1,
        table_scan_2,
        lineitem_filter,
        part_filter,
        join,
        aggregate,
        compute,
        collate
    ])


def test_join_filtered():
    """The filtered test uses nested loop joins but first projections and filtering is pushed down to s3.

    :return: None
    """

    print("Filtered Join")

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    table_scan_1 = TableScan('lineitem.csv',
                             "select l_partkey, l_extendedprice, l_discount from S3Object "
                             "where "
                             "cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                             "cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                             "( "
                             "(l_orderkey = '18436' and l_partkey = '164584') or "
                             "(l_orderkey = '18720' and l_partkey = '92764') or "
                             "(l_orderkey = '12482' and l_partkey = '117405') or "
                             "(l_orderkey = '27623' and l_partkey = '137010') or "

                             "(l_orderkey = '10407' and l_partkey = '43275') or "
                             "(l_orderkey = '17027' and l_partkey = '172729') or "
                             "(l_orderkey = '23302' and l_partkey = '18523') or "
                             "(l_orderkey = '27334' and l_partkey = '94308') or "

                             "(l_orderkey = '15427' and l_partkey = '125586') or "
                             "(l_orderkey = '11590' and l_partkey = '162359') or "
                             "(l_orderkey = '2945' and l_partkey = '126197') or "
                             "(l_orderkey = '15648' and l_partkey = '143904')"
                             ") "
                             ";".format(min_shipped_date.strftime('%Y-%m-%d'), max_shipped_date.strftime('%Y-%m-%d')),
                             'table_scan_1', False)
    table_scan_2 = TableScan('part.csv',
                             "select "
                             "  p_partkey, p_type from S3Object "
                             "where "
                             "  p_brand = 'Brand#12' "
                             " ", 'table_scan_2', False)
    join = Join(JoinExpression('lineitem.csv', '_0', 'part.csv', '_0'), 'join', False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['part.csv._1']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, 0, ctx)

    def ex2(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        sum_fn(v1, 1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']), 'compute', False)
    collate = Collate('collate', False)

    table_scan_1.connect(join)
    table_scan_2.connect(join)
    join.connect(aggregate)
    aggregate.connect(compute)
    compute.connect(collate)

    # Start the query
    table_scan_1.start()
    table_scan_2.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics(
        [table_scan_1, table_scan_2, join, aggregate, compute, collate])


def test_join_bloom():
    """

    :return: None
    """

    print("Bloom Join")

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    table_scan_1 = TableScan('part.csv',
                             "select "
                             "  p_partkey, p_type from S3Object "
                             "where "
                             "  p_brand = 'Brand#12' "
                             " ", 'table_scan_1', False)
    part_bloom_create = BloomCreate('_0', 'part_bloom_create', False)  # p_partkey
    table_scan_2 = TableScanBloomUse('lineitem.csv',
                                     "select "
                                     "  l_partkey, l_extendedprice, l_discount from S3Object "
                                     "where "
                                     "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                     "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                                     "  ( "
                                     "      (l_orderkey = '18436' and l_partkey = '164584') or "
                                     "      (l_orderkey = '18720' and l_partkey = '92764') or "
                                     "      (l_orderkey = '12482' and l_partkey = '117405') or "
                                     "      (l_orderkey = '27623' and l_partkey = '137010') or "

                                     "      (l_orderkey = '10407' and l_partkey = '43275') or "
                                     "      (l_orderkey = '17027' and l_partkey = '172729') or "
                                     "      (l_orderkey = '23302' and l_partkey = '18523') or "
                                     "      (l_orderkey = '27334' and l_partkey = '94308') or "

                                     "      (l_orderkey = '15427' and l_partkey = '125586') or "
                                     "      (l_orderkey = '11590' and l_partkey = '162359') or "
                                     "      (l_orderkey = '2945' and l_partkey = '126197') or "
                                     "      (l_orderkey = '15648' and l_partkey = '143904') "
                                     "  ) "
                                     " ".format(
                                         min_shipped_date.strftime('%Y-%m-%d'),
                                         max_shipped_date.strftime('%Y-%m-%d'))
                                     ,
                                     'l_partkey',
                                     'table_scan_2',
                                     False)
    join = Join(JoinExpression('lineitem.csv', '_0', 'part.csv', '_0'), 'join', False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['part.csv._1']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, 0, ctx)

    def ex2(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        sum_fn(v1, 1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']), 'compute', False)
    collate = Collate('collate', False)

    table_scan_1.connect(part_bloom_create)
    part_bloom_create.connect_bloom_consumer(table_scan_2)
    table_scan_1.connect(join)
    table_scan_2.connect(join)
    join.connect(aggregate)
    aggregate.connect(compute)
    compute.connect(collate)

    # Start the query
    table_scan_1.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics(
        [table_scan_1, part_bloom_create, table_scan_2, join, aggregate, compute, collate])


def test_join_semi():
    """

    :return: None
    """

    print("Semi Join")

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_table_scan_1 = TableScan('part.csv',
                                  "select "
                                  "  p_partkey from S3Object "
                                  "where "
                                  "  p_brand = 'Brand#12' "
                                  " ", 'part_table_scan_1', False)
    part_bloom_create = BloomCreate('_0', 'part_bloom_create', False)  # p_partkey
    lineitem_table_scan_1 = TableScanBloomUse('lineitem.csv',
                                              "select "
                                              "  l_partkey from S3Object "
                                              "where "
                                              "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                              "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                                              "  ( "
                                              "      (l_orderkey = '18436' and l_partkey = '164584') or "
                                              "      (l_orderkey = '18720' and l_partkey = '92764') or "
                                              "      (l_orderkey = '12482' and l_partkey = '117405') or "
                                              "      (l_orderkey = '27623' and l_partkey = '137010') or "

                                              "      (l_orderkey = '10407' and l_partkey = '43275') or "
                                              "      (l_orderkey = '17027' and l_partkey = '172729') or "
                                              "      (l_orderkey = '23302' and l_partkey = '18523') or "
                                              "      (l_orderkey = '27334' and l_partkey = '94308') or "

                                              "      (l_orderkey = '15427' and l_partkey = '125586') or "
                                              "      (l_orderkey = '11590' and l_partkey = '162359') or "
                                              "      (l_orderkey = '2945' and l_partkey = '126197') or "
                                              "      (l_orderkey = '15648' and l_partkey = '143904') "
                                              "  ) "
                                              " ".format(min_shipped_date.strftime('%Y-%m-%d'),
                                                         max_shipped_date.strftime('%Y-%m-%d'))
                                              ,
                                              'l_partkey', 'lineitem_table_scan_1', False)
    part_lineitem_join_1 = Join(JoinExpression('lineitem.csv', '_0', 'part.csv', '_0'), 'part_lineitem_join_1',
                                False)  # l_partkey and p_partkey
    join_bloom_create = BloomCreate('lineitem.csv._0', 'join_bloom_create',
                                    False)  # l_partkey (= p_partkey truth be told :) )
    part_table_scan_2 = TableScanBloomUse('part.csv',
                                          "select "
                                          "  p_partkey, p_type from S3Object "
                                          "where "
                                          "  p_brand = 'Brand#12' "
                                          " ",
                                          'p_partkey',
                                          'part_table_scan_2',
                                          False)
    lineitem_table_scan_2 = TableScanBloomUse('lineitem.csv',
                                              "select "
                                              "  l_partkey, l_extendedprice, l_discount from S3Object "
                                              "where "
                                              "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                              "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                                              "  ( "
                                              "      (l_orderkey = '18436' and l_partkey = '164584') or "
                                              "      (l_orderkey = '18720' and l_partkey = '92764') or "
                                              "      (l_orderkey = '12482' and l_partkey = '117405') or "
                                              "      (l_orderkey = '27623' and l_partkey = '137010') or "

                                              "      (l_orderkey = '10407' and l_partkey = '43275') or "
                                              "      (l_orderkey = '17027' and l_partkey = '172729') or "
                                              "      (l_orderkey = '23302' and l_partkey = '18523') or "
                                              "      (l_orderkey = '27334' and l_partkey = '94308') or "

                                              "      (l_orderkey = '15427' and l_partkey = '125586') or "
                                              "      (l_orderkey = '11590' and l_partkey = '162359') or "
                                              "      (l_orderkey = '2945' and l_partkey = '126197') or "
                                              "      (l_orderkey = '15648' and l_partkey = '143904') "
                                              "  ) "
                                              " ".format(min_shipped_date.strftime('%Y-%m-%d'),
                                                         max_shipped_date.strftime('%Y-%m-%d'))
                                              , 'l_partkey', 'lineitem_table_scan_2', False)
    part_lineitem_join_2 = Join(JoinExpression('lineitem.csv', '_0', 'part.csv', '_0'), 'part_lineitem_join_2',
                                False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['part.csv._1']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, 0, ctx)

    def ex2(t, ctx):

        v1 = float(t['lineitem.csv._1']) * (1.0 - float(t['lineitem.csv._2']))  # l_extendedprice and l_discount

        sum_fn(v1, 1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']), 'compute', False)
    collate = Collate('collate', False)

    part_table_scan_1.connect(part_bloom_create)
    part_bloom_create.connect_bloom_consumer(lineitem_table_scan_1)
    part_table_scan_1.connect(part_lineitem_join_1)
    lineitem_table_scan_1.connect(part_lineitem_join_1)
    part_lineitem_join_1.connect(join_bloom_create)
    join_bloom_create.connect_bloom_consumer(part_table_scan_2)
    join_bloom_create.connect_bloom_consumer(lineitem_table_scan_2)
    part_table_scan_2.connect(part_lineitem_join_2)
    lineitem_table_scan_2.connect(part_lineitem_join_2)
    part_lineitem_join_2.connect(aggregate)
    aggregate.connect(compute)
    compute.connect(collate)

    # Start the query
    part_table_scan_1.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics([
        part_table_scan_1,
        part_bloom_create,
        lineitem_table_scan_1,
        part_lineitem_join_1,
        join_bloom_create,
        part_table_scan_2,
        lineitem_table_scan_2,
        part_lineitem_join_2,
        aggregate,
        compute,
        collate
    ])
