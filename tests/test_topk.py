# -*- coding: utf-8 -*-
"""Top k query tests

"""

from sql.cursor import Cursor
from sql.cursor import LimitStrategy


def test_limit_topk():
    """Executes a top k query by using S3 select'a limit clause
    :return: None
    """

    limit = 500
    num_rows = 0

    cur = Cursor()\
        .select('customer.csv')\
        .limit(limit, LimitStrategy.OP)

    try:
        rows = cur.execute()

        for _ in rows:
            num_rows += 1
            # print("{}:{}".format(num_rows, r))

    finally:
        cur.close()

    assert(num_rows == limit)


def test_abort_topk():
    """Executes a top k query by simply aborting the fetch once the desired number of records has been reached
    :return: None
    """

    limit = 500
    num_rows = 0

    cur = Cursor() \
        .select('customer.csv')

    try:
        rows = cur.execute()

        for _ in rows:
            num_rows += 1
            # print("{}:{}".format(num_rows, r))
            if num_rows >= limit:
                break

    finally:
        cur.close()

    assert (num_rows == limit)
