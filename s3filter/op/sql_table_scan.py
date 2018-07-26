# -*- coding: utf-8 -*-
"""

"""
import cProfile
import time

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.sql.cursor import Cursor
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle


class SQLTableScanMetrics(OpMetrics):
    """Extra metrics for a sql table scan

    """

    # These amounts vary by region, but let's assume it's a flat rate for simplicity
    COST_S3_DATA_RETURNED_PER_GB = 0.0007
    COST_S3_DATA_SCANNED_PER_GB = 0.002

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

        self.cost = 0.0

    def cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """
        return self.bytes_returned * SQLTableScanMetrics.COST_S3_DATA_RETURNED_PER_GB + \
                self.bytes_scanned * SQLTableScanMetrics.COST_S3_DATA_SCANNED_PER_GB

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned,
            'query_bytes': self.query_bytes,
            'bytes_scanned': self.bytes_scanned,
            'bytes_processed': self.bytes_processed,
            'bytes_returned': self.bytes_returned,
            'time_to_first_response': round(self.time_to_first_response, 5),
            'time_to_first_record_response':
                None if self.time_to_first_record_response is None
                else round(self.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if self.time_to_last_record_response is None
                else round(self.time_to_last_record_response, 5),
            'cost': self.cost()

        }.__repr__()


class SQLTableScan(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def __init__(self, s3key, s3sql, name, query_plan, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3 = query_plan.s3

        self.s3key = s3key
        self.s3sql = s3sql

    def run(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """

        if not self.is_profiled:
            self.do_run()
        else:
            cProfile.runctx('self.do_run()', globals(), locals(), self.profile_file_name)

    def do_run(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        cur = Cursor(self.s3).select(self.s3key, self.s3sql)

        tuples = cur.execute()

        self.op_metrics.query_bytes = cur.query_bytes
        self.op_metrics.time_to_first_response = self.op_metrics.elapsed_time()

        first_tuple = True
        for t in tuples:

            if self.is_completed():
                break

            self.op_metrics.rows_returned += 1

            if first_tuple:
                # Create and send the record field names
                it = IndexedTuple.build_default(t)
                first_tuple = False

                if self.log_enabled:
                    print("{}('{}') | Sending field names: {}"
                          .format(self.__class__.__name__, self.name, it.field_names()))

                self.send(TupleMessage(Tuple(it.field_names())), self.consumers)

            if self.log_enabled:
                print("{}('{}') | Sending field values: {}".format(self.__class__.__name__, self.name, t))

            self.send(TupleMessage(Tuple(t)), self.consumers)

        del tuples

        self.op_metrics.bytes_scanned = cur.bytes_scanned
        self.op_metrics.bytes_processed = cur.bytes_processed
        self.op_metrics.bytes_returned = cur.bytes_returned
        self.op_metrics.time_to_first_record_response = cur.time_to_first_record_response
        self.op_metrics.time_to_last_record_response = cur.time_to_last_record_response

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def on_producer_completed(self, producer_name):
        """This event is overridden really just to indicate that it never fires.

        :param producer_name: The completed producer
        :return: None
        """

        raise NotImplementedError
