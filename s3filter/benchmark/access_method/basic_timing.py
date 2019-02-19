# -*- coding: utf-8 -*-
"""Measure some basic timing parameters for indexing

"""
import timeit

import boto3
import pandas as pd

from s3filter.sql.pandas_cursor import PandasCursor
from s3filter.sql.pandas_range_cursor import PandasRangeCursor


def measure_t_req():
    cur = PandasRangeCursor(boto3.client('s3'), 'access_method_benchmark/1-shard-10GB/data_0.csv')

    start_time = timeit.default_timer()
    starts = [x * 1000 ** 3 for x in range(9)]

    end_times = []
    for start in starts:
        cur.add_range(pd.DataFrame({'first_byte': [start], 'last_byte': [start + 1]}))

        dfs = cur.execute()
        for df in dfs:
            print df
        cur.clear_range()
        end_times.append(timeit.default_timer())

    end_time = timeit.default_timer()
    cur.close()

    for n in range(len(starts)):
        print("range {} takes {}".format(n, end_times[n] - start_time if n == 0 else end_times[n] - end_times[n - 1]))
    print("10 range accesses take {}".format(end_time - start_time))
    print("t_req = {}".format(1.0 * (end_time - start_time) / len(starts)))


def measure_R_scan():
    cur = PandasCursor(boto3.client('s3')) \
        .select('access_method_benchmark/10-shards-10GB/data_0.csv',
                'select * from S3Object where cast(F0 AS float) < 0.01; ')

    end_times = []
    start = timeit.default_timer()
    for i in range(3):
        dfs = cur.execute()
        for df in dfs:
            pass
        end_times.append(timeit.default_timer())

    end = timeit.default_timer()

    for n in range(3):
        print("trial {} takes {}".format(n, end_times[n] - start if n == 0 else end_times[n] - end_times[n - 1]))
    print("{} bytes scanned".format(cur.bytes_scanned))
    print("time = {}".format(end - start))
    print("R_scan = {}".format(1.0 * cur.bytes_scanned / (end - start)))

    cur.close()


def main():
    measure_t_req()

    measure_R_scan()


if __name__ == '__main__':
    main()
