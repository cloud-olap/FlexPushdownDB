# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import pytest
from sql.cursor import Cursor, LimitStrategy


def test_non_existent_key():
    """Executes a select against a non existent key
    :return: None
    """

    cur = Cursor()\
        .select('does-not-exist.csv')

    try:
        with pytest.raises(Exception):
            cur.execute()
    finally:
        cur.close()


def test_empty_results():
    """Executes a select where no results are returned
    :return: None
    """

    num_rows = 0

    cur = Cursor()\
        .select('customer.csv')\
        .limit(0, LimitStrategy.OP)

    try:
        rows = cur.execute()
        for _ in rows:
            num_rows += 1
            #print("{}:{}".format(num_rows, r))
    finally:
        cur.close()

    assert (num_rows == 0)