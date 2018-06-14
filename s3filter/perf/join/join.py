# -*- coding: utf-8 -*-
"""Baseline join on TPC-H query 14

"""

from datetime import datetime, timedelta
from s3filter.op.collate import Collate
from s3filter.op.nested_loop_join import NestedLoopJoin, JoinExpression
from s3filter.op.sql_table_scan import SQLTableScan


def main():
    """TPC-H Q14 join

       :return: None
       """

    # Query plan

    # select
    #   100.00 * sum(case
    #            when p_type like 'PROMO%'
    #            then l_extendedprice*(1-l_discount)
    #            else 0
    #   end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    # from
    #   lineitem,
    #   part
    # where
    #   l_partkey = p_partkey
    #   and l_shipdate >= date '[DATE]'
    #   and l_shipdate < date '[DATE]' + interval '1' month

    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1995-06-01'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    ts1 = SQLTableScan('lineitem.csv',
                       "select * from S3Object "
                       "where "
                       "cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                       "cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                       "limit 3;".format(
                           min_shipped_date.strftime('%Y-%m-%d'),
                           max_shipped_date.strftime('%Y-%m-%d')),
                       'ts1',
                       False)
    ts2 = SQLTableScan('part.csv',
                       "select * from S3Object "
                       ";",
                       'ts2',
                       False)
    j = NestedLoopJoin(JoinExpression('_1', '_0'), 'j', False)
    c = Collate('c', False)

    ts1.connect(j)
    ts2.connect(j)
    j.connect(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    # field_names = ['lineitem.csv._0', 'lineitem.csv._1', 'lineitem.csv._2', 'lineitem.csv._3', 'lineitem.csv._4',
    #                'lineitem.csv._5', 'lineitem.csv._6', 'lineitem.csv._7', 'lineitem.csv._8', 'lineitem.csv._9',
    #                'lineitem.csv._10', 'lineitem.csv._11', 'lineitem.csv._12', 'lineitem.csv._13', 'lineitem.csv._14',
    #                'lineitem.csv._15',
    #                'part.csv._0', 'part.csv._1', 'part.csv._2', 'part.csv._3', 'part.csv._4',
    #                'part.csv._5', 'part.csv._6', 'part.csv._7', 'part.csv._8']


if __name__ == "__main__":
    main()
