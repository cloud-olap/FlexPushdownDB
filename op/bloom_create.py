# -*- coding: utf-8 -*-
"""Bloom filter creation support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage, BloomMessage
from op.tuple import LabelledTuple
from util.bloom_filter_util import Bloom
from util.timer import Timer


class BloomCreateMetrics(OpMetrics):

    def __init__(self):
        super(BloomCreateMetrics, self).__init__()
        self.tuple_count = 0
        self.bloom_filter_bit_array_len = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'tuple_count': self.tuple_count,
            'bloom_filter_bit_array_len': self.bloom_filter_bit_array_len
        }.__repr__()


class BloomCreate(Operator):
    """

    """

    def __init__(self, bloom_field_name, name, log_enabled):
        """

        :param bloom_field_name: The tuple field name to extract values from to create the bloom filter
        :param name:
        :param log_enabled:
        """

        super(BloomCreate, self).__init__(name, BloomCreateMetrics(), log_enabled)

        self.__bloom_field_name = bloom_field_name

        self.__field_names = None

        self.__bloom_consumers = []

        self.__bloom_filter = Bloom()

    def connect_bloom_consumer(self, consumer):
        """

        :param consumer:
        :return:
        """

        self.__bloom_consumers.append(consumer)
        Operator.connect(self, consumer)

    def on_receive(self, m, _producer):
        """

        :param m:
        :param _producer:
        :return:
        """

        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer):
        """

        :param producer:
        :return:
        """

        # Send the bloom filter
        self.__send_bloom_filter()

        Operator.on_producer_completed(self, producer)

    def __send_bloom_filter(self):
        """

        :return:
        """

        if self.log_enabled:
            print("{}('{}') | Sending bloom filter [{}]".format(
                self.__class__.__name__,
                self.name,
                {'bloom_filter': self.__bloom_filter}))

        self.op_metrics.bloom_filter_bit_array_len = self.__bloom_filter.bit_array_len()

        self.send(BloomMessage(self.__bloom_filter), self.__bloom_consumers)

    def __on_receive_tuple(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        if not self.__field_names:
            # Don't send the field names, just collect them
            self.__field_names = tuple_
        else:
            lt = LabelledTuple(tuple_, self.__field_names)

            if self.__bloom_field_name not in lt:
                raise Exception(
                    "Received invalid tuple {}. "
                    "Tuple field names '{}' does not contain field with bloom field name '{}'".format(tuple_, lt.labels, self.__bloom_field_name))
            else:

                self.op_metrics.tuple_count += 1

                self.__bloom_filter.add(lt[self.__bloom_field_name])
