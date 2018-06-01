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

    def __init__(self):
        super(SQLTableScanMetrics, self).__init__()
        self.rows_scanned = 0
        self.time_to_first_response_timer = Timer()

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'time_to_first_response': round(self.time_to_first_response_timer.elapsed(), 5),
            'rows_scanned': self.rows_scanned
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

            if self.log_enabled:
                print("Table Scan('{}') | {}".format(self.__class__.__name__, t))

            if self.is_completed():
                break

            self.op_metrics.time_to_first_response_timer.stop()

            self.op_metrics.rows_scanned += 1

            if first_tuple:
                # Create and send the record field names
                lt = LabelledTuple(t)
                first_tuple = False
                self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

            self.send(TupleMessage(Tuple(t)), self.consumers)

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()
