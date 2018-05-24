# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator
from op.tuple import Tuple, LabelledTuple
from sql.cursor import Cursor


class TableScanBloomUse(Operator):

    def __init__(self, key, sql, join_field_name):

        Operator.__init__(self)

        self.key = key
        self.sql = sql

        self.__field_names = None
        self.__tuples = []

        self.__bloom_filter = None
        self.__join_field_name = join_field_name

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

        bloom_filter_sql_predicate = self.__bloom_filter.sql_predicate(self.__join_field_name)

        # print(bloom_filter_sql_predicate)

        cur = Cursor().select(self.key, self.sql + " and " + bloom_filter_sql_predicate)

        tuples = cur.execute()

        first_tuple = True
        for t in tuples:

            # print("Table Scan | {}".format(t))

            if first_tuple:
                # Create and send the record field names
                lt = LabelledTuple(t)
                first_tuple = False
                self.send(Tuple(lt.labels))

            if self.is_completed():
                break

            self.send(Tuple(t))

        if not self.is_completed():
            self.complete()
