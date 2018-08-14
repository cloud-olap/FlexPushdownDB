# -*- coding: utf-8 -*-
"""

"""
import cProfile
import time

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.plan.cost_estimator import CostEstimator
from s3filter.sql.cursor import Cursor
from s3filter.util.constants import *
# noinspection PyCompatibility,PyPep8Naming
import os


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
        # TODO:only simple SQL queries are considered. Nested and complex queries will need a lot of work to handle
        self.need_s3select = not (s3sql.lower().replace(';', '').strip() == 'select * from s3object')

    def run(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """

        if not self.is_profiled:
            if self.need_s3select:
                self.do_run_with_s3select()
            else:
                self.do_run_without_s3select()
        else:
            if self.need_s3select:
                cProfile.runctx('self.do_run_with_s3select()', globals(), locals(), self.profile_file_name)
            else:
                cProfile.runctx('self.do_run_without_s3select()', globals(), locals(), self.profile_file_name)

    def do_run_without_s3select(self):

        if not os.path.exists(TABLE_STORAGE_LOC):
            os.makedirs(TABLE_STORAGE_LOC)

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        table_local_file_path = "{}{}".format(TABLE_STORAGE_LOC, self.s3key)

        if not os.path.exists(table_local_file_path):
            self.s3.download_file(
                Bucket=S3_BUCKET_NAME,
                Key=self.s3key,
                Filename=table_local_file_path
            )

        self.op_metrics.time_to_first_response = self.op_metrics.elapsed_time()

        first_tuple = True

        with open(table_local_file_path, 'r') as table_file:
            for t in table_file:
                if self.is_completed():
                    break

                self.op_metrics.rows_returned += 1

                tup = t.split('|')[:-1]

                if first_tuple:
                    # Create and send the record field names
                    it = IndexedTuple.build_default(tup)
                    first_tuple = False

                    if self.log_enabled:
                        print("{}('{}') | Sending field names: {}"
                              .format(self.__class__.__name__, self.name, it.field_names()))

                    self.send(TupleMessage(Tuple(it.field_names())), self.consumers)
                else:
                    self.send(TupleMessage(Tuple(tup)), self.consumers)

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def do_run_with_s3select(self):

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

            # if self.log_enabled:
            #     print("{}('{}') | Sending field values: {}".format(self.__class__.__name__, self.name, t))

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


class SQLShardedTableScan(Operator):
    """Represents a table scan operator which reads from a shard of an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def __init__(self, s3key, s3sql, name, shards, shard_prefix, query_plan, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLShardedTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3 = query_plan.s3

        self.s3key = s3key
        self.s3sql = s3sql
        self.shards = shards
        self.shard_prefix = shard_prefix
        # TODO:only simple SQL queries are considered. Nested and complex queries will need a lot of work to handle
        self.need_s3select = not (s3sql.lower().replace(';', '').strip() == 'select * from s3object')

    def run(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """

        if not self.is_profiled:
            if self.need_s3select:
                self.do_run_with_s3select()
            else:
                self.do_run_without_s3select()
        else:
            if self.need_s3select:
                cProfile.runctx('self.do_run_with_s3select()', globals(), locals(), self.profile_file_name)
            else:
                cProfile.runctx('self.do_run_without_s3select()', globals(), locals(), self.profile_file_name)

    def do_run_without_s3select(self):

        self.op_metrics.timer_start()

        if not os.path.exists(TABLE_STORAGE_LOC):
            os.makedirs(TABLE_STORAGE_LOC)

        first_tuple = True

        for part in self.shards:
            part_key = self.get_part_key(part)

            if self.log_enabled:
                print("{} | {}('{}') | Started"
                      .format(time.time(), self.__class__.__name__, self.name))

            table_local_file_path = "{}{}".format(TABLE_STORAGE_LOC, part_key)

            if not os.path.exists(os.path.dirname(table_local_file_path)):
                os.makedirs(os.path.dirname(table_local_file_path))

            if not os.path.exists(table_local_file_path):
                self.s3.download_file(
                    Bucket=S3_BUCKET_NAME,
                    Key=part_key,
                    Filename=table_local_file_path
                )

            self.op_metrics.time_to_first_response = self.op_metrics.elapsed_time()

            is_header = True

            with open(table_local_file_path, 'r') as table_file:
                for t in table_file:
                    if self.is_completed():
                        break

                    if is_header:
                        is_header = False
                        continue

                    self.op_metrics.rows_returned += 1

                    tup = t.split('|')[:-1]

                    if first_tuple:
                        # Create and send the record field names
                        it = IndexedTuple.build_default(tup)
                        first_tuple = False

                        if self.log_enabled:
                            print("{}('{}') | Sending field names: {}"
                                  .format(self.__class__.__name__, self.name, it.field_names()))

                        self.send(TupleMessage(Tuple(it.field_names())), self.consumers)
                    else:
                        self.send(TupleMessage(Tuple(tup)), self.consumers)

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def do_run_with_s3select(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        first_tuple = True

        for part in self.shards:
            part_key = self.get_part_key(part)
            cur = Cursor(self.s3).select(part_key, self.s3sql)

            tuples = cur.execute()

            self.op_metrics.query_bytes += cur.query_bytes
            if self.op_metrics.time_to_first_response == 0:
                self.op_metrics.time_to_first_response = self.op_metrics.elapsed_time()

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

                # if self.log_enabled:
                #     print("{}('{}') | Sending field values: {}".format(self.__class__.__name__, self.name, t))

                self.send(TupleMessage(Tuple(t)), self.consumers)

            del tuples

            self.op_metrics.bytes_scanned += cur.bytes_scanned
            self.op_metrics.bytes_processed += cur.bytes_processed
            self.op_metrics.bytes_returned += cur.bytes_returned
            if self.op_metrics.time_to_first_record_response is None:
                self.op_metrics.time_to_first_record_response = cur.time_to_first_record_response
            if self.op_metrics.time_to_last_record_response is None:
                self.op_metrics.time_to_last_record_response = cur.time_to_last_record_response

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def get_part_key(self, part):
        fname = os.path.basename(self.s3key)
        filename, ext = os.path.splitext(fname)
        return '{}/{}_{}{}'.format(self.shard_prefix, filename, part, ext)

    def on_producer_completed(self, producer_name):
        """This event is overridden really just to indicate that it never fires.

        :param producer_name: The completed producer
        :return: None
        """

        raise NotImplementedError
