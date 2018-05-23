# -*- coding: utf-8 -*-
"""Baseline join on TPC-H query 14

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

"""

import re
from datetime import datetime, timedelta
from op.aggregate import Aggregate, AggregateExpression
from op.collate import Collate
from op.compute import Compute, ComputeExpression
from op.filter import Filter, PredicateExpression
from op.join import Join, JoinExpression
from op.log import Log
from op.table_scan import TableScan
from sql.function import cast, timestamp, sum_fn


def test_join_baseline():
    """The baseline test uses nested loop joins with no projection and no filtering.

    Note that it uses a limit clause to make testing speeds sane. For performance measurements, the limit is intended to
    be removed.

    :return: None
    """

    # Query plan
    # This date is chosen because it triggers the filter to filter out 1 of the rows in the root data set.
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=31)

    table_scan_1 = TableScan('lineitem.csv',
                             "select * from S3Object limit 3;")
    table_scan_2 = TableScan('part.csv',
                             "select * from S3Object;")
    table_scan_1_log = Log('TableScan1', False)
    table_scan_2_log = Log('TableScan2', False)
    op_filter = Filter(PredicateExpression(lambda t:
                                   (cast(t['_10'], timestamp) >= cast(min_shipped_date, timestamp)) and
                                   (cast(t['_10'], timestamp) < cast(max_shipped_date, timestamp))
                                   ))
    filter_log = Log('Filter', False)
    join = Join(JoinExpression('lineitem.csv', '_1', 'part.csv', '_0'))  # l_partkey and p_partkey
    join_log = Log('Join', False)

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

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)])
    aggregate_log = Log('Aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']))
    compute_log = Log('Compute', False)
    collate = Collate()

    table_scan_1.connect(table_scan_1_log)
    table_scan_1_log.connect(op_filter)
    op_filter.connect(filter_log)
    filter_log.connect(join)
    table_scan_2.connect(table_scan_2_log)
    table_scan_2_log.connect(join)
    join.connect(join_log)
    join_log.connect(aggregate)
    aggregate.connect(aggregate_log)
    aggregate_log.connect(compute)
    compute.connect(compute_log)
    compute_log.connect(collate)

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
    assert collate.tuples()[1] == [32.68899902938216]


def test_join_filtered():
    """The filtered test uses nested loop joins but first projects and filters the root data using s3 select.

    Note that it uses a limit clause to make testing speeds sane. For performance measurements, the limit is intended to
    be removed.

    :return: None
    """

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=31)

    table_scan_1 = TableScan('lineitem.csv',
                             "select l_partkey, l_extendedprice, l_discount from S3Object "
                             "where "
                             "cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                             "cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                             "limit 3;".format(min_shipped_date.strftime('%Y-%m-%d'), max_shipped_date.strftime('%Y-%m-%d')))
    table_scan_2 = TableScan('part.csv',
                             "select p_partkey, p_type from S3Object;")
    table_scan_1_log = Log('TableScan1', False)
    table_scan_2_log = Log('TableScan2', False)
    join = Join(JoinExpression('lineitem.csv', '_0', 'part.csv', '_0'))  # l_partkey and p_partkey
    join_log = Log('Join', False)

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

    aggregate = Aggregate([AggregateExpression(ex1), AggregateExpression(ex2)])
    aggregate_log = Log('Aggregate', False)
    compute = Compute(ComputeExpression(lambda t: 100 * t['_0'] / t['_1']))
    compute_log = Log('Compute', False)
    collate = Collate()

    table_scan_1.connect(table_scan_1_log)
    table_scan_1_log.connect(join)
    table_scan_2.connect(table_scan_2_log)
    table_scan_2_log.connect(join)
    join.connect(join_log)
    join_log.connect(aggregate)
    aggregate.connect(aggregate_log)
    aggregate_log.connect(compute)
    compute.connect(compute_log)
    compute_log.connect(collate)

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
    assert collate.tuples()[1] == [24.570113647873427]
