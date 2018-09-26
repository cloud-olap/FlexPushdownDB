# -*- coding: utf-8 -*-
"""

"""
import os
import timeit

import paratext

from s3filter.util.csv_util import CSVParser, BaseParser

READ_CHUNK_SIZE = 1024 * 32


def test_serial_csv():
    run(parallel=False, base_parser=BaseParser.pandas)


def test_serial_py_csv():
    run(parallel=False, base_parser=BaseParser.python)


def test_serial_agate_csv():
    run(parallel=False, base_parser=BaseParser.agate)


def test_serial_scan_csv():
    run(parallel=False, base_parser=BaseParser.scan)


def test_parallel_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.pandas)


def test_parallel_py_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.python)


def test_parallel_agate_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.agate)


def test_parallel_scan_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.scan)


def test_paratext_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.paratext)


def callback(df):
    # print(len(df))
    pass


def run(parallel=False, pool_size=-1, base_parser=BaseParser.pandas):
    file_name = '../../../ATTIC/data/lineitem.csv'
    expected_row_count = 6001216
    # file_name = '../../../ATTIC/data/customer.csv'
    # expected_row_count = 150001

    f = open(file_name)
    file_size = os.path.getsize(file_name)

    if base_parser is not BaseParser.paratext:
        parser = CSVParser(callback, parallel, pool_size, base_parser)

        start_time = timeit.default_timer()

        while True:
            chunk = f.read(READ_CHUNK_SIZE)
            if chunk:
                parser.pump(chunk)
            else:
                break
        parser.close()

        row_count = parser.line_count

        stop_time = timeit.default_timer()
    else:
        start_time = timeit.default_timer()
        df = paratext.load_csv_to_pandas(file_name, num_threads=pool_size)
        stop_time = timeit.default_timer()
        row_count = len(df) + 1

    time = stop_time - start_time
    print("Time: {}, Rows: {}, Size: {}, MB/Sec {}".format(
        time, row_count, file_size, (file_size / time) / 1000 / 1000))

    assert expected_row_count == row_count
