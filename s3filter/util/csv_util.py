# -*- coding: utf-8 -*-
"""


"""
import cStringIO
import csv
import multiprocessing

import agate
from enum import Enum

import pandas as pd

DEFAULT_PARSE_CHUNK_SIZE = 1024 * 1024 * 0.5


class BaseParser(Enum):
    pandas = 1
    python = 2
    agate = 3


def parse_chunk_str(chunk, base_parser, callback):
    # chunk = cPickle.loads(pickled_chunk)
    stream = cStringIO.StringIO(chunk)
    df = read_csv(stream, base_parser)
    # print("{} | DF: {}".format(timeit.default_timer(), df))

    callback(df)

    return len(df)


def read_csv(stream, base_parser):
    # print("{} | Parsing".format(timeit.default_timer()))

    if base_parser is BaseParser.pandas:
        df = pd.read_csv(stream, header=None, prefix='_', dtype=str, engine='c',
                         quotechar='"', na_filter=False, compression=None, low_memory=False)
    elif base_parser is BaseParser.python:
        csv_reader = csv.reader(stream)
        df = pd.DataFrame(list(csv_reader), dtype=str)
        df = df.add_prefix('_')
    elif base_parser is BaseParser.agate:
        csv_reader = agate.csv_py2.reader(stream)
        df = pd.DataFrame(list(csv_reader), dtype=str)
        df = df.add_prefix('_')
    else:
        raise Exception("Unrecognised base parser '{}'".format(base_parser))

    return df


class CSVParser(object):

    def __init__(self, callback=None, parallel=True, pool_size=8, base_parser=BaseParser.pandas,
                 parse_chunk_size=DEFAULT_PARSE_CHUNK_SIZE):
        self.parallel = parallel
        self.base_parser = base_parser
        self.parse_chunk_size = parse_chunk_size

        self.stream = cStringIO.StringIO()
        self.prev_incomplete_line = None

        self.callback = callback

        if parallel:
            self.pool = multiprocessing.Pool(pool_size)
            self.results = []
        else:
            self.pool = None
            self.results = None

        self.line_count = 0

    def pump(self, chunk):
        if self.parallel:
            self.pump_parallel(chunk)
        else:
            self.pump_serial(chunk)

    def pump_serial(self, chunk):

        self.extract_complete_lines(chunk)

        if self.stream.tell() > self.parse_chunk_size:
            self.parse_chunk_serial()

    def parse_chunk_serial(self):
        self.parse_chunk_stream()
        self.stream.close()
        self.stream = cStringIO.StringIO()

    def pump_parallel(self, chunk):

        self.extract_complete_lines(chunk)

        if self.stream.tell() > self.parse_chunk_size:
            self.parse_chunk_parallel()

    def parse_chunk_parallel(self):
        # chunk = cPickle.dumps(self.stream.getvalue(), cPickle.HIGHEST_PROTOCOL)
        chunk = self.stream.getvalue()
        self.stream.close()
        self.stream = cStringIO.StringIO()
        self.results.append(self.pool.apply_async(parse_chunk_str, (chunk, self.base_parser, self.callback)))

    def flush(self):
        if self.stream.tell() > 0:
            if self.parallel:
                self.parse_chunk_parallel()
            else:
                self.parse_chunk_serial()

    def close(self):

        self.flush()

        if self.pool is not None:
            self.pool.close()
            self.pool.join()

        if self.results is not None:
            for result in self.results:
                self.line_count += result.get()

    def extract_complete_lines(self, chunk):
        if self.prev_incomplete_line is not None:
            self.stream.write(self.prev_incomplete_line)
            self.prev_incomplete_line = None
        if chunk.endswith('\n') and not chunk.endswith('\\n'):
            self.stream.write(chunk)
        else:
            last_newline_pos = chunk.rfind('\n', 0, len(chunk))
            if last_newline_pos == -1:
                self.prev_incomplete_line = chunk
            else:
                self.prev_incomplete_line = chunk[last_newline_pos + 1:]
                self.stream.write(chunk[:last_newline_pos + 1])

    def parse_chunk_stream(self):
        self.stream.seek(0)
        df = read_csv(self.stream, self.base_parser)
        # print("{} | DF: {}".format(timeit.default_timer(), df))
        self.line_count += len(df)

        self.callback(df)
