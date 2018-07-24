# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import boto3

from s3filter.sql.cursor import Cursor


def test_substring():
    """Executes a select where no results are returned.

    :return: None
    """

    num_rows = 0
    str_ = "0123456789"

    cur = Cursor(boto3.client('s3')).select('region.csv',
                                            "select "
                                            "   '{}', "
                                            "   substring('{}',5,1) "
                                            "from "
                                            "   S3Object s"
                                            .format(str_, str_))

    try:
        rows = cur.execute()
        for r in rows:
            num_rows += 1
            print("{}:{}".format(num_rows, r))

            assert r[1] == '4'
    finally:
        cur.close()
