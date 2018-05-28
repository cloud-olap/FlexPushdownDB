# -*- coding: utf-8 -*-
"""Bloom filter creation support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage, BloomMessage
from op.tuple import LabelledTuple
from util.bloom_filter_util import Bloom


class BloomCreate(Operator):
    """

    """

    def __init__(self, bloom_field, name, log_enabled):

        super(BloomCreate, self).__init__(name, OpMetrics(), log_enabled)

        self.__bloom_field = bloom_field

        self.__field_names = None
        self.__tuples = []

        self.__bloom_consumers = []

        self.__bloom_filter = Bloom()

    def connect_bloom_consumer(self, consumer):
        self.__bloom_consumers.append(consumer)
        Operator.connect(self, consumer)

    def send_bloom_filter(self):

        if self.log_enabled:
            print("{}('{}') | Sending bloom filter [{}]".format(
                self.__class__.__name__,
                self.name,
                {'bloom_filter': self.__bloom_filter}))

        self.send(BloomMessage(self.__bloom_filter), self.__bloom_consumers)

    def on_receive(self, m, _producer):

        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(t))

    def on_receive_tuple(self, tuple_):
        if not self.__field_names:
            # Don't send the field names, just collect them
            self.__field_names = tuple_
        else:
            self.__tuples.append(tuple_)

    def on_producer_completed(self, producer):

        for t in self.__tuples:

            if self.is_completed():
                break

            lt = LabelledTuple(t, self.__field_names)
            self.__bloom_filter.add(lt[self.__bloom_field])

        # Send the bloom filter
        self.send_bloom_filter()

        if not self.is_completed():
            self.complete()
