# -*- coding: utf-8 -*-
"""

"""

from plan.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple, LabelledTuple
from sql.cursor import Cursor
from util.timer import Timer


class SQLTableScanMetrics(OpMetrics):
    """Extra metrics for a sql table scan

    """

    def __init__(self):
        super(SQLTableScanMetrics, self).__init__()

        self.rows_returned = 0

        self.time_to_first_response_timer = Timer()

        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned,
            'bytes_scanned': self.bytes_scanned,
            'bytes_processed': self.bytes_processed,
            'bytes_returned': self.bytes_returned,
            'time_to_first_response': round(self.time_to_first_response_timer.elapsed(), 5),

        }.__repr__()


class SQLTableScan(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def __init__(self, s3key, s3sql, name, log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLTableScan, self).__init__(name, SQLTableScanMetrics(), log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql

    def start(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """
        # print("Table Scan | Start {} {}".format(self.key, self.sql))

        self.op_metrics.timer_start()
        self.op_metrics.time_to_first_response_timer.start()

        cur = Cursor().select(self.s3key, self.s3sql)

        tuples = cur.execute()

        first_tuple = True
        for t in tuples:

            if self.is_completed():
                break

            if self.log_enabled:
                print("{}('{}') | {}".format(self.__class__.__name__, self.name, t))

            self.op_metrics.time_to_first_response_timer.stop()

            self.op_metrics.rows_returned += 1

            if first_tuple:
                # Create and send the record field names
                lt = LabelledTuple(t)
                first_tuple = False
                self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

            self.send(TupleMessage(Tuple(t)), self.consumers)

        if not self.is_completed():
            self.complete()

        self.op_metrics.bytes_scanned = cur.bytes_scanned
        self.op_metrics.bytes_processed = cur.bytes_processed
        self.op_metrics.bytes_returned = cur.bytes_returned

        self.op_metrics.timer_stop()

    def on_producer_completed(self, _producer):
        """This event is overridden really just to indicate that it never fires.

        :param _producer: The completed producer
        :return: None
        """

        pass
