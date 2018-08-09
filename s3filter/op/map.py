# -*- coding: utf-8 -*-
"""Map support

"""
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import sys

from pandas import DataFrame

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
import pandas as pd

class Map(Operator):
    """Maps tuples using a mapping fn to particular consumers.

    """

    def __init__(self, map_field_name, name, query_plan, log_enabled):
        """Constructs a new Map operator.

        """

        super(Map, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.field_names = None

        self.map_field_name = map_field_name

    def on_receive(self, ms, _producer):
        """Handles the event of receiving a message from a producer.

        :param ms: The received messages
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_)
            elif type(m) is DataFrame:
                self.__on_receive_dataframe(m)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_dataframe(self, df):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        consumer_indexes = pd.to_numeric(df[self.map_field_name]) % len(self.consumers)
        grouped = df.groupby(consumer_indexes)
        for idx, df in grouped:
            operator = self.consumers[idx]
            self.send(df, [operator])

            # if self.log_enabled:
            #     print("{}('{}') | Mapped dataframe {} to operator {}"
            #           .format(self.__class__.__name__, self.name, df, operator))

    def __on_receive_tuple(self, tuple_):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        if self.field_names is None:
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        else:
            it = IndexedTuple.build(tuple_, self.field_names)

            idx = int(it[self.map_field_name]) % len(self.consumers)
            self.send(TupleMessage(tuple_), [self.consumers[idx]])
