# -*- coding: utf-8 -*-
"""TPC-H query tests

"""
from datetime import datetime, timedelta

from op.collate import Collate
from op.group import Group
from op.log import Log
from op.table_scan import TableScan
from util import aggregateexpression


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

    shipped_date = datetime.strptime('1998-10-01', '%Y-%m-%d') - timedelta(days=60)
    print(shipped_date.strftime('%Y-%m-%d'))

    ts = TableScan('lineitem.csv',
                   "select * from S3Object "
                   "where cast(l_shipdate as timestamp) <= cast(\'{}\' as timestamp) "
                   "limit 10 ".format(shipped_date.strftime('%Y-%m-%d')))
    log = Log()
    # g = Group(group_col_indexes=[8, 9],
    #           aggregate_exprs=[
    #               'sum(_5 * (1 - _6) * (1 + _7))' # sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
    #           ])
    g = Group(group_col_indexes=[8, 9],
              aggregate_expr_strs=[
                  'sum(_5)' # sum(l_extendedprice)
              ])
    c = Collate()

    ts.connect(log)
    log.connect(g)
    g.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples:
        num_rows += 1
        print("{}:{}".format(num_rows, t))
