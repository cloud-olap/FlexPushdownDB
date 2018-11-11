# -*- coding: utf-8 -*-
"""TopK filter build

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import DataFrameMessage, StringMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd
import time


class TopKFilterBuild(Operator):
    """Builds a SQL filter for the second phase of sampled topK

    """

    def __init__(self, sort_order, data_type, s3sql, pred, name, query_plan, log_enabled):
        """
        Creates a new join operator.

        """
        self.sort_order = sort_order
        self.data_type = data_type
        self.s3sql = s3sql
        self.pred = pred
        self.threshold = None

        super(TopKFilterBuild, self).__init__(name, OpMetrics(), query_plan, log_enabled)

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received messages
        :param producer_name: The producer of the tuple
        :return: None
        """
        for m in ms:
            #if type(m) is TupleMessage:
            #    self.__on_receive_tuple(m.tuple_, producer_name)
            if type(m) is DataFrameMessage:
                self.__on_receive_dataframe(m.dataframe)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, _producer_name):
        # ignore the field names
        pass

    def __on_receive_dataframe(self, df):
        df = df.astype(self.data_type)
        if self.sort_order == 'ASC':
            self.threshold = df.max().values[0]
            where_clause = ' where {} <= {}'.format(self.pred, self.threshold)
        elif self.sort_order == 'DESC':
            self.threshold = df.min().values[0]
            where_clause = ' where {} >= {}'.format(self.pred, self.threshold)
        sql = '{} {};'.format(self.s3sql, where_clause)
        print sql
        self.send(  StringMessage(sql), self.consumers)

    def on_producer_completed(self, producer_name):

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            if self.log_enabled:
                print("{}{}".format(self, self.hashtable))

            Operator.on_producer_completed(self, producer_name)
