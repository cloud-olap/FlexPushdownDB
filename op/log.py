# -*- coding: utf-8 -*-
"""Logging support

"""
from metric.op_metrics import OpMetrics
from op.operator_base import Operator


class Log(Operator):
    """This operator simply prints out each tuple, useful for debugging.

    """

    def __init__(self, name, log_enabled):
        """Constructs a new Log operator

        """

        super(Log, self).__init__(name, OpMetrics(), log_enabled)

        self.__name = name
        self.__enabled = log_enabled

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.__bloom_consumers = []

    def connect_bloom_consumer(self, consumer):
        self.__bloom_consumers.append(consumer)
        Operator.connect(self, consumer)

    # noinspection PyUnusedLocal
    def on_receive(self, t, _producer):
        """Handles the event of receiving a new tuple from a producer. Will simply print the tuple.

        :param t: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        if self.__enabled:
            print("Log [{}] | {}: {}".format(self.__name, _producer.key, t))

        self.key = _producer.key

        self.send(t)
