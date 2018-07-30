# -*- coding: utf-8 -*-
"""Merge support

"""
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

import objgraph as objgraph

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.plan.op_metrics import OpMetrics
import pandas as pd

class Merge(Operator):
    """

    """

    def __init__(self, name, query_plan, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Merge, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.field_names = None
        self.producers_received = {}

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a message from a producer. Will simply append the tuple to the internal
        list.

        :param ms: The received messages
        :param producer_name: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                tuples = m.values.tolist()
                for t in tuples:
                    self.__on_receive_tuple(t, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        assert (len(tuple_) > 0)

        if self.field_names is None:
            self.field_names = tuple_
            self.producers_received[producer_name] = True
            self.send(TupleMessage(tuple_), self.consumers)
        else:
            if producer_name not in self.producers_received.keys():
                # This will be the field names tuple, skip it
                self.producers_received[producer_name] = True
            else:
                self.send(TupleMessage(tuple_), self.consumers)

    def on_producer_completed(self, producer_name):
        Operator.on_producer_completed(self, producer_name)
