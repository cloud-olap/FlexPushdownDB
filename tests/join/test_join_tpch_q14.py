# -*- coding: utf-8 -*-
"""Baseline join on TPC-H query 14

"""

from datetime import datetime, timedelta
from op.collate import Collate
from op.join import Join, JoinExpression
from op.table_scan import TableScan


def test_join():
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

    ts1 = TableScan('lineitem.csv',
                    "select * from S3Object "
                    "where "
                    "cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                    "cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                    "limit 3;".format(min_shipped_date.strftime('%Y-%m-%d'), max_shipped_date.strftime('%Y-%m-%d')))
    ts2 = TableScan('part.csv',
                    "select * from S3Object "
                    ";")
    j = Join(JoinExpression('lineitem.csv', '_1', 'part.csv', '_0'))
    c = Collate()

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

    field_names = ['lineitem.csv._0', 'lineitem.csv._1', 'lineitem.csv._2', 'lineitem.csv._3', 'lineitem.csv._4',
                   'lineitem.csv._5', 'lineitem.csv._6', 'lineitem.csv._7', 'lineitem.csv._8', 'lineitem.csv._9',
                   'lineitem.csv._10', 'lineitem.csv._11', 'lineitem.csv._12', 'lineitem.csv._13', 'lineitem.csv._14',
                   'lineitem.csv._15',
                   'part.csv._0', 'part.csv._1', 'part.csv._2', 'part.csv._3', 'part.csv._4',
                   'part.csv._5', 'part.csv._6', 'part.csv._7', 'part.csv._8']

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == field_names

    assert c.tuples()[1] == ['197', '17936', '2939', '4', '25', '46348.25', '0.04', '0.01', 'N', 'F', '1995-06-13', '1995-05-23',
                  '1995-06-24', 'TAKE BACK RETURN', 'FOB', 's-- quickly final accounts', '17936',
                  'forest salmon misty lemon honeydew', 'Manufacturer#3', 'Brand#35',
                  'STANDARD POLISHED COPPER', '41', 'SM CAN', '1853.93', 'efully. final']
    assert c.tuples()[2] == ['225', '7589', '5090', '5', '31', '46393.98', '0.04', '0.06', 'N', 'O', '1995-06-21', '1995-07-24',
                  '1995-07-04', 'TAKE BACK RETURN', 'FOB', 'special platelets. quickly r', '7589',
                  'blush ghost navy light rosy', 'Manufacturer#2', 'Brand#21', 'ECONOMY ANODIZED NICKEL', '14', 'SM DRUM', '1496.58', 's: carefully express d']
    assert c.tuples()[3] == ['225', '131835', '9375', '6', '12', '22401.96', '0.00', '0.00', 'A', 'F', '1995-06-04', '1995-07-15',
                  '1995-06-08', 'COLLECT COD', 'MAIL', ' unusual requests. bus', '131835',
                  'aquamarine plum dodger honeydew dark', 'Manufacturer#4', 'Brand#42', 'SMALL ANODIZED COPPER',
                  '36', 'WRAP CAN', '1866.83', 's. ironic sent']
