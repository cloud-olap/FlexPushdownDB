# -*- coding: utf-8 -*-
"""TPC-H query tests

"""

from datetime import datetime, timedelta

from op.collate import Collate
from op.group import Group
from op.log import Log
from op.sort import Sort, SortExpression
from op.table_scan import TableScan


def test_tpch_q1():
    """TPC-H Q1

    :return: None
    """

    num_rows = 0

    # Query plan

    # select
    #   l_returnflag,
    #   l_linestatus,
    #   sum(l_quantity) as sum_qty,
    #   sum(l_extendedprice) as sum_base_price,
    #   sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    #   sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    #   avg(l_quantity) as avg_qty,
    #   avg(l_extendedprice) as avg_price,
    #   avg(l_discount) as avg_disc,
    #   count(*) as count_order
    # from
    #   lineitem
    # where
    #   l_shipdate <= date '1998-12-01' - interval '[DELTA]' day(3)
    # group by
    #   l_returnflag,
    #   l_linestatus
    # order by
    #   l_returnflag,
    #   l_linestatus;

    delta_days = 60  # TODO: This is supposed to be randomized I think
    shipped_date = datetime.strptime('1998-10-01', '%Y-%m-%d') - timedelta(days=delta_days)
    print(shipped_date.strftime('%Y-%m-%d'))

    ts = TableScan('lineitem.csv',
                   "select * from S3Object "
                   "where cast(l_shipdate as timestamp) <= cast(\'{}\' as timestamp) "
                   "limit 10 ".format(shipped_date.strftime('%Y-%m-%d')))
    log = Log()
    g = Group(
        group_col_indexes=[
            8,  # l_returnflag
            9  # l_linestatus
        ],
        aggregate_expr_strs=[
            'sum(_4)',  # sum(l_quantity)
            'sum(_5)',  # sum(l_extendedprice) as sum_base_price
            'sum(_5 * (1 - _6))',  # sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
            'sum(_5 * (1 - _6) * (1 + _7))',  # sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
            'avg(_4)',  # avg(l_quantity)
            'avg(_5)',  # avg(l_extendedprice)
            'avg(_6)',  # avg(l_discount)
            'count(_0)'  # count(*) as count_order
        ])
    s = Sort([
        SortExpression(0, str, 'ASC'),
        SortExpression(1, str, 'ASC')
    ])
    c = Collate()

    ts.connect(log)
    log.connect(g)
    g.connect(s)
    s.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    t1 = c.tuples()[0]
    t2 = c.tuples()[1]
    t3 = c.tuples()[2]

    # These are correct (ish) though the rounding is arbitrary
    # TODO: Standardise on a rounding for aggregates
    assert t1 == ['A', 'F', 27, 39890.88, 37497.4272, 40122.247104, 27.0, 39890.88, 0.06, 1]
    assert t2 == ['N', 'O', 183, 226555.73, 211877.68959999998, 220594.690152, 26.142857142857142, 32365.104285714286, 0.07000000000000002, 7]
    assert t3 == ['R', 'F', 94.0, 100854.52, 92931.39000000001, 92931.39000000001, 47, 50427.26, 0.08, 2]
