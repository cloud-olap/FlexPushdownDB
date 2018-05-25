# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator
from op.tuple import Tuple, LabelledTuple
from sql.cursor import Cursor


class TableScanBloomUse(Operator):

    def __init__(self, key, sql, bloom_filter_field_name, name, log_enabled):

        Operator.__init__(self, name, log_enabled)

        self.key = key
        self.sql = sql

        self.__field_names = None
        self.__tuples = []

        self.__bloom_filter = None

        if type(bloom_filter_field_name) is str:
            self.__bloom_filter_field_name = bloom_filter_field_name
        else:
            raise Exception("Bloom filter field name is of type {}. Field name must be of type string to be "
                            "used in SQL predicate".format(type(bloom_filter_field_name)))

    def on_receive(self, t, _producer):
        """Handles the event of receiving a new tuple from a producer. Will simply append the tuple to the internal
        list.

        :param t: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("BloomScan | {}".format(t))
        if not self.__field_names:
            self.__field_names = t
        else:
            self.__tuples.append(t)

    def on_receive_bloom_filter(self, bloom_filter, _producer):

        # print("BloomScan | {}".format(t))
        self.__bloom_filter = bloom_filter

    def on_producer_completed(self, _producer):

        bloom_filter_sql_predicate = self.__bloom_filter.sql_predicate(self.__bloom_filter_field_name)

        # print(bloom_filter_sql_predicate)

        sql = self.sql + " and " + bloom_filter_sql_predicate
        cur = Cursor().select(self.key, sql)

        tuples = cur.execute()

        first_tuple = True
        for t in tuples:

            # print("Table Scan | {}".format(t))

            if self.is_completed():
                break

            if first_tuple:
                self.send_field_names(t)
                first_tuple = False

            self.send_data(t)

        if not self.is_completed():
            self.complete()

    def send_data(self, t):

        if self.log_enabled:
            print("{}('{}') | Sending data [{}]".format(self.__class__.__name__, self.name, {'data': t}))

        self.send(Tuple(t))

    def send_field_names(self, t):

        # Create and send the record field names
        lt = LabelledTuple(t)
        labels = Tuple(lt.labels)

        if self.log_enabled:
            print("{}('{}') | Sending field names [{}]".format(
                self.__class__.__name__,
                self.name,
                {'field_names': labels}))

        self.send(labels)

    def __repr__(self):
        return {'name': self.name.__repr__(), 'key': self.key.__repr__()}.__repr__()
