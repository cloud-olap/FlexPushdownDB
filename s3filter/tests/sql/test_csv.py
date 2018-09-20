# -*- coding: utf-8 -*-
"""

"""
import timeit

from s3filter.util.csv_util import CSVParser, BaseParser

READ_CHUNK_SIZE = 1024 * 32


def test_serial_csv():
    run(parallel=False, base_parser=BaseParser.pandas)


def test_serial_py_csv():
    run(parallel=False, base_parser=BaseParser.python)


def test_serial_agate_csv():
    run(parallel=False, base_parser=BaseParser.agate)


def test_parallel_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.pandas)


def test_parallel_py_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.python)


def test_parallel_agate_csv():
    run(parallel=True, pool_size=8, base_parser=BaseParser.agate)


def callback(df):
    # print(len(df))
    pass


def run(parallel=False, pool_size=-1, base_parser=BaseParser.pandas):
    f = open('./lineitem.csv')
    # f = open('../../../ATTIC/data/customer.csv')

    parser = CSVParser(callback, parallel, pool_size, base_parser)

    start_time = timeit.default_timer()

    while True:
        chunk = f.read(READ_CHUNK_SIZE)
        if chunk:
            parser.pump(chunk)
            # pass
        else:
            break
    parser.close()

    stop_time = timeit.default_timer()
    print("Time: {}, Rows: {}".format(stop_time - start_time, parser.line_count))

    assert 6001216 == parser.line_count  # lineitem
    # assert 150001 == parser.line_count  # customer
