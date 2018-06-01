# -*- coding: utf-8 -*-
"""

"""

from op.operator_base import Operator
from op.message import TupleMessage, BloomMessage
from op.sql_table_scan import SQLTableScanMetrics
from op.tuple import Tuple, LabelledTuple
from sql.cursor import Cursor


class SQLTableScanBloomUse(Operator):

    def __init__(self, s3key, s3sql, bloom_filter_field_name, name, log_enabled):

        super(SQLTableScanBloomUse, self).__init__(name, SQLTableScanMetrics(), log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql

        self.__field_names = None
        self.__tuples = []

        self.__bloom_filter = None

        if type(bloom_filter_field_name) is str:
            self.__bloom_filter_field_name = bloom_filter_field_name
        else:
            raise Exception("Bloom filter field name is of type {}. Field name must be of type str to be "
                            "used in SQL predicate".format(type(bloom_filter_field_name)))

    def on_receive(self, m, _producer):
        """Handles the event of receiving a new tuple from a producer. Will simply append the tuple to the internal
        list.

        :param m: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("BloomScan | {}".format(t))

        if type(m) is BloomMessage:
            # TODO: Can probably start the query here as all this operator waits for is the bloom filter
            self.__bloom_filter = m.bloom_filter
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer):

        self.op_metrics.time_to_first_response_timer.start()

        bloom_filter_sql_predicate = self.__bloom_filter.sql_predicate(self.__bloom_filter_field_name)

        # Append the bloom filter predicate either using where... or and...
        sql_suffix = self.build_sql_suffix(bloom_filter_sql_predicate)

        # print(bloom_filter_sql_predicate)

        sql = self.s3sql + sql_suffix
        cur = Cursor().select(self.s3key, sql)

        tuples = cur.execute()

        first_tuple = True
        for t in tuples:

            # print("Table Scan | {}".format(t))

            if self.is_completed():
                break

            self.op_metrics.time_to_first_response_timer.stop()

            self.op_metrics.rows_scanned += 1

            if first_tuple:
                self.send_field_names(t)
                first_tuple = False

            self.send_data(t)

        Operator.on_producer_completed(self, producer)

    def build_sql_suffix(self, bloom_filter_sql_predicate):

        stripped_sql = self.s3sql.strip()
        if stripped_sql.endswith(';'):
            stripped_sql = stripped_sql[:-1].strip()

        split_sql = stripped_sql.split()
        is_predicate_present = split_sql[-1].lower() is not "s3object"
        if is_predicate_present:
            return " and {} ".format(bloom_filter_sql_predicate)
        else:
            return " where {} ".format(bloom_filter_sql_predicate)

    def send_data(self, t):

        if self.log_enabled:
            print("{}('{}') | Sending data [{}]".format(self.__class__.__name__, self.name, {'data': t}))

        self.send(TupleMessage(Tuple(t)), self.consumers)

    def send_field_names(self, t):

        # Create and send the record field names
        lt = LabelledTuple(t)
        labels = Tuple(lt.labels)

        if self.log_enabled:
            print("{}('{}') | Sending field names [{}]".format(
                self.__class__.__name__,
                self.name,
                {'field_names': labels}))

        self.send(TupleMessage(labels), self.consumers)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {'name': self.name, 's3key': self.s3key})
