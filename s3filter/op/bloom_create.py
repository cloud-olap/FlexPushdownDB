# -*- coding: utf-8 -*-
"""Bloom filter creation support

"""
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, BloomMessage
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.util.bloom_filter import BloomFilter
from s3filter.util.scalable_bloom_filter import ScalableBloomFilter
from s3filter.util.simple_bloom_filter import SimpleBloomFilter


class BloomCreateMetrics(OpMetrics):
    """Extra metrics

    """

    def __init__(self):
        super(BloomCreateMetrics, self).__init__()
        self.tuple_count = 0
        self.bloom_filter_capacity = 0
        self.bloom_filter_num_bits_set = 0
        self.bloom_filter_num_slices = 0
        self.bloom_filter_num_bits_per_slice = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'tuple_count': self.tuple_count,
            'bloom_filter_capacity': self.bloom_filter_capacity,
            'bloom_filter_num_bits_set': self.bloom_filter_num_bits_set,
            'bloom_filter_num_slices': self.bloom_filter_num_slices,
            'bloom_filter_num_bits_per_slice': self.bloom_filter_num_bits_per_slice
        }.__repr__()


class BloomCreate(Operator):
    """This operator creates a bloom filter from the tuples it receives. Given a field name to create the filter from
    it will add the tuple value corresponding the field name to the internal bloom filter. Once the connected
    producer is complete the bloom filter is sent to any connected consumers.

    """

    def __init__(self, bloom_field_name, name, log_enabled):
        """

        :param bloom_field_name: The tuple field name to extract values from to create the bloom filter
        :param name: The operator name
        :param log_enabled: Logging enabled
        """

        super(BloomCreate, self).__init__(name, BloomCreateMetrics(), log_enabled)

        self.__bloom_field_name = bloom_field_name

        self.__field_names = None

        # These settings are similar to the simple version
        # self.__bloom_filter = ScalableBloomFilter(64, 0.75, ScalableBloomFilter.LARGE_SET_GROWTH)

        # These settings are similar to the simple version
        self.__bloom_filter = BloomFilter(8192, 0.75)

        self.op_metrics.bloom_filter_capacity = self.__bloom_filter.capacity
        self.op_metrics.bloom_filter_num_slices = self.__bloom_filter.num_slices
        self.op_metrics.bloom_filter_num_bits_per_slice = self.__bloom_filter.num_bits_per_slice
        self.op_metrics.bloom_filter_capacity = self.__bloom_filter.capacity

        # The simple filter
        # self.__bloom_filter = SimpleBloomFilter()

    def connect(self, consumer):
        """Overrides the generic connect method to make sure that the connecting operator is an operator that consumes
        bloom filters.

        :param consumer: The consumer to connect
        :return: None
        """

        if type(consumer) is not SQLTableScanBloomUse:
            raise Exception("Illegal consumer. {} operator may only be connected to {} operators"
                            .format(self.__class__.__name__, SQLTableScanBloomUse.__class__.__name__))

        Operator.connect(self, consumer)

    def on_receive(self, m, _producer):
        """Event handler for receiving a message

        :param m: The message
        :param _producer: The producer that sent the message
        :return: None
        """

        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer):
        """Event handler for a completed producer. When producers complete the bloom filter can be sent.

        :param producer: The producer that completed.
        :return: None
        """

        # Send the bloom filter
        self.__send_bloom_filter()

        Operator.on_producer_completed(self, producer)

    def __send_bloom_filter(self):
        """Sends the bloom filter to connected consumers.

        :return: None
        """

        if self.log_enabled:
            print("{}('{}') | Sending bloom filter [{}]".format(
                self.__class__.__name__,
                self.name,
                {'bloom_filter': self.__bloom_filter}))

        self.op_metrics.bloom_filter_num_bits_set = len(self.__bloom_filter)

        self.send(BloomMessage(self.__bloom_filter), self.consumers)

    def __on_receive_tuple(self, tuple_):
        """Event handler for receiving a tuple

        :param tuple_: The received tuple
        :return: None
        """

        if not self.__field_names:

            if self.__bloom_field_name not in tuple_:
                raise Exception(
                    "Received invalid tuple {}. "
                    "Tuple field names '{}' do not contain field with bloom field name '{}'"
                    .format(tuple_, tuple_, self.__bloom_field_name))

            # Don't send the field names, just collect them
            self.__field_names = tuple_

        else:
            lt = IndexedTuple.build(tuple_, self.__field_names)

            self.op_metrics.tuple_count += 1

            # NOTE: Bloom filter only supports ints. Not clear how to make it support strings as yet
            self.__bloom_filter.add(int(lt[self.__bloom_field_name]))
