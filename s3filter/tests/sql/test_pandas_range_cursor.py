# -*- coding: utf-8 -*-
"""Tests for some select edge cases

"""
import StringIO
import cProfile
import pstats
import timeit

import boto3
import pytest
from boto3 import Session
from botocore.config import Config

from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.pandas_range_cursor import PandasRangeCursor

def test_range():
    """Executes a select where results are returned.

    :return: None
    """

    num_rows = 0

    cur = PandasRangeCursor(boto3.client('s3'), 'region.csv')
    cur.add_range(30, 153)
    cur.add_range(156, 196)
    cur.add_range(199, 236)
    cur.add_range(239, 292)
    cur.add_range(295, 416)
    
    try:
        dfs = cur.execute()
        for df in dfs:
            print df
    finally:
        cur.close()

if __name__ == '__main__':
    test_range()
