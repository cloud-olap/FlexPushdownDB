# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator
from op.tuple import Tuple, LabelledTuple
from sql.cursor import Cursor
from util.bloom_filter_util import Bloom


class TableScanBloomCreate(Operator):
    """

    """

    def __init__(self, key, sql, bloom_field):

        Operator.__init__(self)

        self.key = key
        self.sql = sql

        self.__bloom_field = bloom_field

        self.__tuple_consumers = []
        self.__bloom_consumers = []

        self.__bloom_filter = Bloom()

    def connect_tuple_consumer(self, consumer):
        self.__tuple_consumers.append(consumer)
        Operator.add_consumer(self, consumer)

    def connect_bloom_consumer(self, consumer):
        self.__bloom_consumers.append(consumer)
        Operator.connect(self, consumer)

    def send_bloom(self):
        for c in self.__bloom_consumers:
            c.on_receive_bloom_filter(self.__bloom_filter, self)

    def send_tuple(self, t):
        for c in self.__tuple_consumers:
            c.on_receive(t, self)

    def start(self):

        # print("{} | Start {} {}".format(self.__name__, self.key, self.sql))

        cur = Cursor().select(self.key, self.sql)

        tuples = cur.execute()

        # Create the bloom filter and send the tuples
        first_tuple = True
        for t in tuples:

            # print("Table Scan | {}".format(t))

            if first_tuple:
                # Create and send the record field names
                lt = LabelledTuple(t)
                first_tuple = False
                self.send_tuple(Tuple(lt.labels))

            if self.is_completed():
                break

            self.__bloom_filter.add(t[self.__bloom_field])
            self.send_tuple(Tuple(t))

        # Send the bloom filter
        self.send_bloom()

        if not self.is_completed():
            self.complete()
