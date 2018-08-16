# -*- coding: utf-8 -*-
"""
"""
import cProfile
import sys
import time

from boto3 import Session
from botocore.config import Config

from s3filter.op.message import TupleMessage, StringMessage
from s3filter.op.operator_base import Operator, StartMessage
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.sql.cursor import Cursor
from s3filter.plan.cost_estimator import CostEstimator
from s3filter.util.constants import *
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

from s3filter.sql.pandas_cursor import PandasCursor
import pandas as pd


class SQLTableScanMetrics(OpMetrics):
    """Extra metrics for a sql table scan
    """

    def __init__(self):
        super(SQLTableScanMetrics, self).__init__()

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
            'cost': "${0:.8f}".format(self.cost()),
            'cost_for_instance': self.cost_estimator.ec2_instance
        }.__repr__()


class SQLTableScan(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.
    """

    def on_receive(self, ms, producer_name):
        for m in ms:
            if type(m) is StringMessage:
                self.s3sql = m.string_
                if self.async_:
                    self.query_plan.send(StartMessage(), self.name)
                else:
                    self.run()
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __init__(self, s3key, s3sql, use_pandas, secure, name, query_plan, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql
        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        # Boto is not thread safe so need one of these per scan op
        if secure:
            cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
            session = Session()
            self.s3 = session.client('s3', config=cfg)
        else:
            cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                         s3={'payload_signing_enabled': False})
            session = Session()
            self.s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)

        self.s3key = s3key
        self.s3sql = s3sql

        self.use_pandas = use_pandas

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
            cur = self.execute_py_query(self)

        self.op_metrics.bytes_scanned = cur.bytes_scanned
        self.op_metrics.bytes_processed = cur.bytes_processed
        self.op_metrics.bytes_returned = cur.bytes_returned
        self.op_metrics.time_to_first_record_response = cur.time_to_first_record_response
        self.op_metrics.time_to_last_record_response = cur.time_to_last_record_response
        self.op_metrics.num_http_get_requests = cur.num_http_get_requests

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    @staticmethod
    def execute_py_query(op):
        cur = Cursor(op.s3).select(op.s3key, op.s3sql)
        op.op_metrics.query_bytes = cur.query_bytes
        tuples = cur.execute()

        counter = 0
        first_tuple = True
        for t in tuples:

            if op.is_completed():
                break

            op.op_metrics.rows_returned += 1

            if first_tuple:
                # Create and send the record field names
                it = IndexedTuple.build_default(t)
                first_tuple = False

                # if op.log_enabled:
                #     print("{}('{}') | Sending field names: {}"
                #           .format(op.__class__.__name__, op.name, it.field_names()))

                op.send(TupleMessage(Tuple(it.field_names())), op.consumers)

            # if op.log_enabled:
            #     print("{}('{}') | Sending field values: {}".format(op.__class__.__name__, op.name, t))

            counter += 1
            if op.log_enabled:
                if counter % 1000 == 0:
                    sys.stdout.write('.')
                if counter % 100000 == 0:
                    sys.stdout.write('.')
                    print(" Rows {}".format(op.op_metrics.rows_returned))

            op.send(TupleMessage(Tuple(t)), op.consumers)
        return cur

    @staticmethod
    def execute_pandas_query(op):
        cur = PandasCursor(op.s3).select(op.s3key, op.s3sql)
        dfs = cur.execute()
        op.op_metrics.query_bytes = cur.query_bytes
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

            op.send(df, op.consumers)
            # buffer_ = pd.concat([buffer_, df], axis=0, sort=False, ignore_index=True, copy=False)
            # if len(buffer_) >= 8192:
            #    op.send(buffer_, op.consumers)
            #    buffer_ = pd.DataFrame()

        if len(buffer_) > 0:
            op.send(buffer_, op.consumers)
            del buffer_

        return cur


def is_header(tuple_):
    return all([type(field) == str and field.startswith('_') for field in tuple_])
