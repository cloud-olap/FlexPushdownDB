# -*- coding: utf-8 -*-
"""Cursor support

"""
import cStringIO
import csv
import timeit

import agate
import numpy
import pandas as pd
from boto3 import Session
from botocore.config import Config

import s3filter.util.constants
from s3filter.util.timer import Timer
from multiprocessing.dummy import Pool as ThreadPool 


class PandasRangeCursor(object):
    """Represents a database cursor for managing the context of a fetch operation.

    Intended to be modelled after the Python DB API. All it supports at present is select, taking a s3 key and sql
    string to execute. The rows returned from s3 are returned as an iterator.

    Importantly supports streaming of records which gives more control over simply passing Python data structures
    around.

    """

    def __init__(self, s3, s3key):

        self.s3 = s3

        self.s3key = s3key 
        self.ranges = pd.DataFrame() 
        self.event_stream = None

        self.timer = Timer()

        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.query_bytes = len(self.s3key.encode('utf-8'))
        
        #self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0
        
        self.records_str = ''
        self.nthreads = 16

    def add_range(self, range_df):
        
        self.ranges = pd.concat([self.ranges, range_df])
   
    def clear_range(self):
        
        self.ranges = pd.DataFrame()

    def set_nthreads(self, nthreads):
        
        self.nthreads = nthreads

    def get_object_thread(self, byte_range):
        #cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
        #                     s3={'payload_signing_enabled': False})
        #session = Session()
        #s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

        response = self.s3.get_object(
                Bucket=s3filter.util.constants.S3_BUCKET_NAME,
                Key=self.s3key,
                Range='bytes={}-{}'.format(byte_range[0], byte_range[1]))

        return response['Body'].read() + '\n'
        # record = response['Body'].read()
        # self.records_str += record + '\n' 

    def execute(self):
        """Executes the fetch operation. This is different to the DB API as it returns an iterable. Of course we could
        model that API more precisely in future.

        :return: An iterable of the records fetched
        """

        # print("Executing range_cursor")

        #self.timer.start()

        # Note:
        #
        # CSV files use | as a delimiter and have a trailing delimiter so record delimiter is |\n
        #
        # NOTE: As responses are chunked the file headers are only returned in the first chunk. We ignore them for now
        # just because its simpler. It does mean the records are returned as a list instead of a dict though (can change
        # in future).
        #
        # TODO if the ranges is too long, should chop it up. 
        pool = ThreadPool(self.nthreads)
        if len(self.ranges) > 0:
            results = pool.map(self.get_object_thread, self.ranges.values.tolist())
            pool.close()
            pool.join()
            record_str = ''.join(results) 
            self.bytes_returned += len(record_str)
            df = pd.read_csv(cStringIO.StringIO(record_str), sep='|', header=None, prefix='_', dtype=numpy.str, engine='c',
                                     quotechar='"', na_filter=False, compression=None, low_memory=False)
            yield df
        else:
            pd.DataFrame()

    def close(self):
        pass
