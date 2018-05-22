# -*- coding: utf-8 -*-
"""Baseline join on TPC-H query 14

"""
from datetime import datetime, timedelta
from op.collate import Collate
from op.join import Join
from op.table_scan import TableScan


def main():
    """TPC-H Q14 join

       :return: None
       """

    num_rows = 0

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
    j = Join('lineitem.csv', 1, 'part.csv', 0)
    c = Collate()

    ts1.connect(j)
    ts2.connect(j)
    j.connect(c)

    # Start the query
    ts1.start()
    ts2.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    t1 = c.tuples()[0]
    t2 = c.tuples()[1]
    t3 = c.tuples()[2]

    assert t1 == ['197', '17936', '2939', '4', '25', '46348.25', '0.04', '0.01', 'N', 'F',
                  '1995-06-13', '1995-05-23', '1995-06-24', 'TAKE BACK RETURN', 'FOB',
                  's-- quickly final accounts', '17936', 'forest salmon misty lemon honeydew',
                  'Manufacturer#3', 'Brand#35', 'STANDARD POLISHED COPPER', '41', 'SM CAN',
                  '1853.93', 'efully. final']
    assert t2 == ['225', '7589', '5090', '5', '31', '46393.98', '0.04', '0.06', 'N', 'O',
                  '1995-06-21', '1995-07-24', '1995-07-04', 'TAKE BACK RETURN', 'FOB',
                  'special platelets. quickly r', '7589', 'blush ghost navy light rosy',
                  'Manufacturer#2', 'Brand#21', 'ECONOMY ANODIZED NICKEL', '14', 'SM DRUM',
                  '1496.58', 's: carefully express d']
    assert t3 == ['225', '131835', '9375', '6', '12', '22401.96', '0.00', '0.00', 'A', 'F',
                  '1995-06-04', '1995-07-15', '1995-06-08', 'COLLECT COD', 'MAIL',
                  ' unusual requests. bus', '131835', 'aquamarine plum dodger honeydew dark',
                  'Manufacturer#4', 'Brand#42', 'SMALL ANODIZED COPPER', '36', 'WRAP CAN',
                  '1866.83', 's. ironic sent']


if __name__ == "__main__":
    main()
