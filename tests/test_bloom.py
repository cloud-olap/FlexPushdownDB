# -*- coding: utf-8 -*-
"""Bloom filter tests

"""

from sql.cursor import Cursor
from util.bloom_filter_util import Bloom


def test_bloom():

    # Values to add to our our bloom filter
    v1 = '12'
    v2 = '64'

    bf = Bloom()
    bf.add(v1)
    bf.add(v2)

    cur = Cursor().select('part.csv',
                          "select "
                          "   p_partkey "
                          "from "
                          "   S3Object "
                          "where "
                          "   {}"
                          .format(bf.sql_predicate('p_partkey')))

    try:

        rows = cur.execute()

        v1_present = False
        v2_present = False

        num_rows = 0
        for r in rows:
            num_rows += 1
            if r[0] == v1:
                v1_present = True
            if r[0] == v2:
                v2_present = True

            # print("{}:{}".format(num_rows, r))

        assert v1_present
        assert v2_present
        assert num_rows == 400

    finally:
        cur.close()
