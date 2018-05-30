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
from op.aggregate import Aggregate
from op.aggregate_expression import AggregateExpression
from op.bloom_create import BloomCreate
from op.project import Project, ProjectExpr
from op.table_scan_bloom_use import TableScanBloomUse
from op.collate import Collate
from op.filter import Filter
from op.predicate_expression import PredicateExpression
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

    lineitem_scan = TableScan('lineitem.csv',
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
                              "(l_orderkey = '15648' and l_partkey = '143904');",
                              'lineitem_scan',
                              False)

    lineitem_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_5'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_6'], 'l_discount'),
            ProjectExpr(lambda t_: t_['_10'], 'l_shipdate')
        ],
        'lineitem_scan_project',
        False)

    part_scan = TableScan('part.csv',
                          "select * from S3Object;",
                          'part_scan',
                          False)

    part_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpr(lambda t_: t_['_4'], 'p_type')
        ],
        'part_scan_project',
        False)

    lineitem_filter = Filter(
        PredicateExpression(lambda t_:
                            (cast(t_['l_shipdate'], timestamp) >= cast(min_shipped_date, timestamp)) and
                            (cast(t_['l_shipdate'], timestamp) < cast(max_shipped_date, timestamp))),
        'lineitem_filter',
        False)

    part_filter = Filter(
        PredicateExpression(lambda t: t['p_brand'] == 'Brand#12'),
        'part_filter',
        False)  # p_brand

    join = Join(
        JoinExpression('l_partkey', 'p_partkey'),
        'join',
        False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['p_type']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, ctx)

    def ex2(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        sum_fn(v1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)

    project = Project(
        [
            ProjectExpr(lambda t: 100 * t['_0'] / t['_1'], 'promo_revenue')
        ],
        'project',
        False)

    collate = Collate('collate', False)

    lineitem_scan.connect(lineitem_scan_project)
    lineitem_scan_project.connect(lineitem_filter)
    join.connect_left_producer(lineitem_filter)
    part_scan.connect(part_scan_project)
    part_scan_project.connect(part_filter)
    join.connect_right_producer(part_filter)
    join.connect(aggregate)
    aggregate.connect(project)
    project.connect(collate)

    # Start the query
    lineitem_scan.start()
    part_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics([
        lineitem_scan,
        part_scan,
        lineitem_filter,
        part_filter,
        join,
        aggregate,
        project,
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

    lineitem_scan = TableScan('lineitem.csv',
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
                              'lineitem_scan',
                              False)

    lineitem_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_2'], 'l_discount')
        ],
        'lineitem_scan_project',
        False)

    part_scan = TableScan('part.csv',
                          "select "
                          "  p_partkey, p_type from S3Object "
                          "where "
                          "  p_brand = 'Brand#12' "
                          " ",
                          'part_scan',
                          False)

    part_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'p_type')
        ],
        'part_scan_project',
        False)

    join = Join(JoinExpression('l_partkey', 'p_partkey'), 'join', False)  # l_partkey and p_partkey

    def ex1(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['p_type']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, ctx)

    def ex2(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        sum_fn(v1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    project = Project([ProjectExpr(lambda t: 100 * t['_0'] / t['_1'], 'promo_revenue')], 'project', False)
    collate = Collate('collate', False)

    lineitem_scan.connect(lineitem_scan_project)
    part_scan.connect(part_scan_project)
    join.connect_left_producer(lineitem_scan_project)
    join.connect_right_producer(part_scan_project)
    join.connect(aggregate)
    aggregate.connect(project)
    project.connect(collate)

    # Start the query
    lineitem_scan.start()
    part_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics(
        [lineitem_scan, part_scan, join, aggregate, project, collate])


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

    part_scan = TableScan('part.csv',
                          "select "
                          "  p_partkey, p_type from S3Object "
                          "where "
                          "  p_brand = 'Brand#12' "
                          " ",
                          'part_scan',
                          False)

    part_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'p_type')
        ],
        'part_scan_project',
        False)

    part_bloom_create = BloomCreate('p_partkey', 'part_bloom_create', False)  # p_partkey

    lineitem_scan = TableScanBloomUse('lineitem.csv',
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
                                      'lineitem_scan',
                                      False)

    lineitem_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_2'], 'l_discount')
        ],
        'lineitem_scan_project',
        False)

    join = Join(JoinExpression('p_partkey', 'l_partkey'), 'join', False)  # p_partkey and l_partkey

    def ex1(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['p_type']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, ctx)

    def ex2(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        sum_fn(v1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    project = Project([ProjectExpr(lambda t: 100 * t['_0'] / t['_1'], 'promo_revenue')], 'project', False)
    collate = Collate('collate', False)

    part_scan.connect(part_scan_project)
    part_scan_project.connect(part_bloom_create)
    part_bloom_create.connect_bloom_consumer(lineitem_scan)
    # part_scan.connect(join)
    join.connect_left_producer(part_scan_project)
    lineitem_scan.connect(lineitem_scan_project)
    # lineitem_scan.connect(join)
    join.connect_right_producer(lineitem_scan_project)
    join.connect(aggregate)
    aggregate.connect(project)
    project.connect(collate)

    # Start the query
    part_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics(
        [part_scan, part_bloom_create, lineitem_scan, join, aggregate, project, collate])


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

    part_scan_1 = TableScan('part.csv',
                            "select "
                            "  p_partkey from S3Object "
                            "where "
                            "  p_brand = 'Brand#12' "
                            " ",
                            'part_table_scan_1',
                            False)

    part_scan_1_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey')
        ],
        'part_scan_1_project',
        False)

    part_bloom_create = BloomCreate('p_partkey', 'part_bloom_create', False)  # p_partkey
    lineitem_scan_1 = TableScanBloomUse('lineitem.csv',
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
                                        'l_partkey',
                                        'lineitem_table_scan_1',
                                        False)
    lineitem_scan_1_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey')
        ],
        'lineitem_scan_1_project',
        False)

    part_lineitem_join_1 = Join(JoinExpression('p_partkey', 'l_partkey'), 'part_lineitem_join_1',
                                False)  # p_partkey and l_partkey
    join_bloom_create = BloomCreate('l_partkey', 'join_bloom_create',
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

    part_scan_2_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'p_type')
        ],
        'part_scan_2_project',
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
                                                         max_shipped_date.strftime('%Y-%m-%d')),
                                              'l_partkey',
                                              'lineitem_table_scan_2',
                                              False)
    lineitem_scan_2_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpr(lambda t_: t_['_2'], 'l_discount')
        ],
        'lineitem_scan_2_project',
        False)

    part_lineitem_join_2 = Join(JoinExpression('p_partkey', 'l_partkey'),
                                'part_lineitem_join_2',
                                False)  # p_partkey and l_partkey

    def ex1(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        rx = re.compile('^PROMO.*$')

        if rx.search(t['p_type']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        sum_fn(v2, ctx)

    def ex2(t, ctx):

        v1 = float(t['l_extendedprice']) * (1.0 - float(t['l_discount']))  # l_extendedprice and l_discount

        sum_fn(v1, ctx)

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)], 'aggregate', False)
    project = Project([ProjectExpr(lambda t: 100 * t['_0'] / t['_1'], 'promo_revenue')], 'project', False)
    collate = Collate('collate', False)

    part_scan_1.connect(part_scan_1_project)
    part_scan_1_project.connect(part_bloom_create)
    part_bloom_create.connect_bloom_consumer(lineitem_scan_1)
    part_lineitem_join_1.connect_left_producer(part_scan_1_project)
    lineitem_scan_1.connect(lineitem_scan_1_project)
    part_lineitem_join_1.connect_right_producer(lineitem_scan_1_project)
    part_lineitem_join_1.connect(join_bloom_create)
    join_bloom_create.connect_bloom_consumer(part_table_scan_2)
    join_bloom_create.connect_bloom_consumer(lineitem_table_scan_2)
    part_table_scan_2.connect(part_scan_2_project)
    part_lineitem_join_2.connect_left_producer(part_scan_2_project)
    lineitem_table_scan_2.connect(lineitem_scan_2_project)
    part_lineitem_join_2.connect_right_producer(lineitem_scan_2_project)
    part_lineitem_join_2.connect(aggregate)
    aggregate.connect(project)
    project.connect(collate)

    # Start the query
    part_scan_1.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    OpMetrics.print_metrics([
        part_scan_1,
        part_bloom_create,
        lineitem_scan_1,
        part_lineitem_join_1,
        join_bloom_create,
        part_table_scan_2,
        lineitem_table_scan_2,
        part_lineitem_join_2,
        aggregate,
        project,
        collate
    ])
