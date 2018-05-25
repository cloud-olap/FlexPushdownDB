# -*- coding: utf-8 -*-
"""

"""

from op.operator_base import Operator
from op.tuple import LabelledTuple
from util.bloom_filter_util import Bloom


class BloomCreate(Operator):
    """

    """

    def __init__(self, bloom_field, name, log_enabled):

        Operator.__init__(self, name, log_enabled)

        self.__bloom_field = bloom_field

        self.__field_names = None
        self.__tuples = []

        self.__bloom_consumers = []

        self.__bloom_filter = Bloom()

    def connect_bloom_consumer(self, consumer):
        self.__bloom_consumers.append(consumer)
        Operator.connect(self, consumer)

    def send_bloom(self):
        for c in self.__bloom_consumers:
            c.on_receive_bloom_filter(self.__bloom_filter, self)

    def on_receive(self, t, _producer):
        if not self.__field_names:
            # Don't send the field names, just collect them
            self.__field_names = t
        else:
            self.__tuples.append(t)

    def on_producer_completed(self, producer):

        # print("{} | Completed {} {}".format(self.__name__, self.key, self.sql))

        for t in self.__tuples:

            # print("{} | {} {}".format(self.__name__, t))

            if self.is_completed():
                break

            lt = LabelledTuple(t, self.__field_names)
            self.__bloom_filter.add(lt[self.__bloom_field])

        # Send the bloom filter
        if self.log_enabled:
            print("{}('{}') | Sending bloom filter [{}]".format(
                self.__class__.__name__,
                self.name,
                {'bloom_filter': self.__bloom_filter}))

        self.send_bloom()

        if not self.is_completed():
            self.complete()
