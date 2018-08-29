# -*- coding: utf-8 -*-
"""Table scan benchmark

"""

import os
import pstats
import scan

import boto3
from boto3 import Session
from botocore.config import Config

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.null import Null
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.cursor import Cursor
from s3filter.sql.native_cursor import NativeCursor
from s3filter.sql.pandas_cursor import PandasCursor
from s3filter.util.test_util import gen_test_id
import pandas as pd

from s3filter.util.timer import Timer


def main():
    # run(use_pandas=False, secure=True, use_native=False)
    # run(use_pandas=False, secure=False, use_native=False)
    # run(use_pandas=True, secure=True, use_native=False)
    # run(use_pandas=True, secure=False, use_native=False)
    run(use_pandas=True, secure=False, use_native=True)  # Note: Native does not support secure=True


def run(use_pandas, secure, use_native):
    print("Cursor | Settings {}".format({'use_pandas': use_pandas, 'secure': secure, 'use_native': use_native}))

    sql = 'select * from S3Object'

    if secure:
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
        session = Session()
        s3 = session.client('s3', config=cfg)
    else:
        cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                     s3={'payload_signing_enabled': False})
        session = Session()
        s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

    if use_pandas and not use_native:

        cur = PandasCursor(s3).select('lineitem.csv', sql)
        dfs = cur.execute()

        rows_returned = 0
        for df in dfs:
            rows_returned += len(df)

        print("{}".format({
            'elapsed_time': round(cur.timer.elapsed(), 5),
            'rows_returned': rows_returned,
            'query_bytes': cur.query_bytes,
            'bytes_scanned': cur.bytes_scanned,
            'bytes_processed': cur.bytes_processed,
            'bytes_returned': "{} ({} MB / {} GB)".format(
                cur.bytes_returned,
                round(float(cur.bytes_returned) / 1000000.0, 5),
                round(float(cur.bytes_returned) / 1000000000.0, 5)),
            'bytes_returned_per_sec': "{} ({} MB / {} GB)".format(
                round(float(cur.bytes_returned) / cur.timer.elapsed(), 5),
                round(float(cur.bytes_returned) / cur.timer.elapsed() / 1000000, 5),
                round(float(cur.bytes_returned) / cur.timer.elapsed() / 1000000000, 5)),
            'time_to_first_record_response':
                None if cur.time_to_first_record_response is None
                else round(cur.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if cur.time_to_last_record_response is None
                else round(cur.time_to_last_record_response, 5)
        }))

    elif use_pandas and use_native:

        closure = {'df': None, 'rows_returned': 0}

        def on_data(data):

            # print("|||")
            # print(type(data))
            # print(data)
            # print("|||")

            # if closure['df'] is None:
            #     closure['df'] = pd.DataFrame(data)
            # else:
            #     closure['df'] = pd.concat([closure['df'], pd.DataFrame(data)], ignore_index=True, copy=False)

            closure['rows_returned'] += len(pd.DataFrame(data))

        cur = NativeCursor(scan).select('lineitem.csv', sql)
        cur.execute(on_data)

        rows_returned = closure['rows_returned']

        print("{}".format({
            'elapsed_time': round(cur.timer.elapsed(), 5),
            'rows_returned': rows_returned,
            'query_bytes': cur.query_bytes,
            'bytes_scanned': cur.bytes_scanned,
            'bytes_processed': cur.bytes_processed,
            'bytes_returned': "{} ({} MB / {} GB)".format(
                cur.bytes_returned,
                round(float(cur.bytes_returned) / 1000000.0, 5),
                round(float(cur.bytes_returned) / 1000000000.0, 5)),
            'bytes_returned_per_sec': "{} ({} MB / {} GB)".format(
                round(float(cur.bytes_returned) / cur.timer.elapsed(), 5),
                round(float(cur.bytes_returned) / cur.timer.elapsed() / 1000000, 5),
                round(float(cur.bytes_returned) / cur.timer.elapsed() / 1000000000, 5)),
            'time_to_first_record_response':
                None if cur.time_to_first_record_response is None
                else round(cur.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if cur.time_to_last_record_response is None
                else round(cur.time_to_last_record_response, 5)
        }))

    else:

        timer = Timer()
        timer.start()

        cur = Cursor(s3).select('lineitem.csv', sql)
        rows = cur.execute()
        for _ in rows:
            pass

        print(timer.elapsed())

    print("Cursor | Done")


if __name__ == "__main__":
    main()
