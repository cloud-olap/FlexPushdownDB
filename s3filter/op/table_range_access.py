# -*- coding: utf-8 -*-
"""Accessing Table Using Byte Ranges

"""

import sys
import time

from boto3 import Session
from botocore.config import Config

from s3filter.op.message import TupleMessage, StringMessage
from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.plan.cost_estimator import CostEstimator
# from s3filter.sql.native_cursor import NativeCursor
from s3filter.util.constants import *
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

from s3filter.sql.pandas_range_cursor import PandasRangeCursor
import pandas as pd
from ctypes import *
import imp


class TableRangeAccessMetrics(OpMetrics):
    """Extra metrics for a sql table scan

    """

    def __init__(self):
        super(TableRangeAccessMetrics, self).__init__()

        self.rows_returned = 0

        self.time_to_first_response = 0
        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0
        self.num_http_get_requests = 0

        self.cost_estimator = CostEstimator(self)

    def cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """
        return self.cost_estimator.estimate_cost()

    def computation_cost(self, running_time=None, ec2_instance_type=None, os_type=None):
        """
        Estimates the computation cost of the scan operation based on EC2 pricing in the following page:
        <https://aws.amazon.com/ec2/pricing/on-demand/>
        :param running_time: the query running time
        :param ec2_instance_type: the type of EC2 instance as defined by AWS
        :param os_type: the name of the os running on the host machine (Linux, Windows ... etc)
        :return: The estimated computation cost of the table scan operation given the query running time
        """
        return self.cost_estimator.estimate_computation_cost(running_time, ec2_instance_type, os_type)

    def data_cost(self, ec2_region=None):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated data transfer cost of the table scan operation
        """
        return self.cost_estimator.estimate_data_cost(ec2_region) + self.cost_estimator.estimate_request_cost()

    def data_scan_cost(self):
        """
        Estimate the cost of S3 data scanning
        :return: the estimated data scanning costin USD
        """
        return self.cost_estimator.estimate_data_scan_cost()

    def data_transfer_cost(self, ec2_region=None, s3_region=None):
        """
        Estimate the cost of transferring data either by s3 select or normal data transfer fees
        :param ec2_region: the region where the computing node resides
        :param s3_region: the region where the s3 data is stored in
        :return: the estimated data transfer cost in USD
        """
        return self.cost_estimator.estimate_data_transfer_cost(ec2_region, s3_region)

    def requests_cost(self):
        """
        Estimate the cost of the http GET requests
        :return: the estimated http GET request cost for this particular operation
        """
        return self.cost_estimator.estimate_request_cost()

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned,
            'query_bytes': self.query_bytes,
            'bytes_scanned': self.bytes_scanned,
            'bytes_processed': self.bytes_processed,
            'bytes_returned': "{} ({} MB / {} GB)".format(
                self.bytes_returned,
                round(float(self.bytes_returned) / 1000000.0, 5),
                round(float(self.bytes_returned) / 1000000000.0, 5)),
            'bytes_returned_per_sec': "{} ({} MB / {} GB)".format(
                round(float(self.bytes_returned) / self.elapsed_time(), 5),
                round(float(self.bytes_returned) / self.elapsed_time() / 1000000, 5),
                round(float(self.bytes_returned) / self.elapsed_time() / 1000000000, 5)),
            'time_to_first_response': round(self.time_to_first_response, 5),
            'time_to_first_record_response':
                None if self.time_to_first_record_response is None
                else round(self.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if self.time_to_last_record_response is None
                else round(self.time_to_last_record_response, 5),
            'cost': self.cost()

        }.__repr__()


class TableRangeAccess(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def __init__(self, s3key, use_pandas, secure, use_native, name, query_plan, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param s3key: The object key to select against
        """

        super(TableRangeAccess, self).__init__(name, TableRangeAccessMetrics(), query_plan, log_enabled)

        # Boto is not thread safe so need one of these per scan op
        if not use_native:
            if secure:
                cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
                session = Session()
                self.s3 = session.client('s3', config=cfg)
            else:
                cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                             s3={'payload_signing_enabled': False})
                session = Session()
                self.s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)
        # else:
        #     self.fast_s3 = scan

        self.s3key = s3key
        self.s3sql = ''

        self.use_pandas = use_pandas

        self.use_native = use_native
        
        self.cur = PandasRangeCursor(self.s3, self.s3key)

    def set_nthreads(self, nthreads):
        
        self.cur.set_nthreads(nthreads)

    def run(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """
        self.do_run()

    def do_run(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        if self.use_pandas:
            cur = self.execute_pandas_query(self)
        else:
            raise NotImplementedError

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def on_receive(self, ms, producer_name):
        for m in ms:
            if type(m) is TupleMessage:
                pass
            elif isinstance(m, DataFrameMessage):
                self.cur.add_range(m.dataframe)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer_name):
        """ start accessing S3 only after the producer (index scan) finsihes
        """
        self.execute_pandas_query(self)

        Operator.on_producer_completed(self, producer_name)

    @staticmethod
    def execute_pandas_query(op):

        # if op.use_native:
        #     cur = NativeCursor(op.fast_s3).select(op.s3key, op.s3sql)
        #     df = cur.execute()
        #
        #     op.op_metrics.query_bytes = cur.query_bytes
        #     op.op_metrics.rows_returned += len(df)
        #     op.op_metrics.bytes_returned += cur.bytes_returned
        #
        #     op.send(TupleMessage(Tuple(df.columns.values)), op.consumers)
        #     op.send(df, op.consumers)
        #
        #     return cur
        # else:

            dfs = op.cur.execute()
            op.op_metrics.query_bytes = op.cur.query_bytes
            op.op_metrics.time_to_first_response = op.op_metrics.elapsed_time()
            first_tuple = True

            counter = 0

            buffer_ = pd.DataFrame()
            for df in dfs:

                if first_tuple:
                    assert (len(df.columns.values) > 0)
                    op.send(TupleMessage(Tuple(df.columns.values)), op.consumers)
                    first_tuple = False

                    # if op.log_enabled:
                    #     print("{}('{}') | Sending field names: {}"
                    #           .format(op.__class__.__name__, op.name, df.columns.values))

                op.op_metrics.rows_returned += len(df)

                # if op.log_enabled:
                #     print("{}('{}') | Sending field values: {}".format(op.__class__.__name__, op.name, df))

                counter += 1
                if op.log_enabled:
                    sys.stdout.write('.')
                    if counter % 100 == 0:
                        print("Rows {}".format(op.op_metrics.rows_returned))

                op.send(DataFrameMessage(df), op.consumers)
                # buffer_ = pd.concat([buffer_, df], axis=0, sort=False, ignore_index=True, copy=False)
                # if len(buffer_) >= 8192:
                #    op.send(buffer_, op.consumers)
                #    buffer_ = pd.DataFrame()

            #if len(buffer_) > 0:
            #    op.send(buffer_, op.consumers)
            #    del buffer_

            op.op_metrics.bytes_scanned = op.cur.bytes_scanned
            op.op_metrics.bytes_processed = op.cur.bytes_processed
            op.op_metrics.bytes_returned = op.cur.bytes_returned
            op.op_metrics.time_to_first_record_response = op.cur.time_to_first_record_response
            op.op_metrics.time_to_last_record_response = op.cur.time_to_last_record_response
            op.op_metrics.num_http_get_requests = op.cur.num_http_get_requests

            if not op.is_completed():
                op.complete()
            return op.cur
