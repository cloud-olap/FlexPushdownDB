# -*- coding: utf-8 -*-
"""Join support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, StringMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd


class GroupbyDecoder(Operator):
    """Builds a Decoder for 2-phase groupby
    
    """

    def __init__(self, agg_fields, name, query_plan, log_enabled):
        """
        Creates a new join operator.

        :param key: The key to hash on
        """
        self.group_df = None
        self.agg_fields = agg_fields
        super(GroupbyDecoder, self).__init__(name, OpMetrics(), query_plan, log_enabled)

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received messages
        :param producer_name: The producer of the tuple
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m)
            else:
                raise Exception("Unrecognized message (type={}) {}".format(type(m), m))

    def __on_receive_tuple(self, tuple_, _producer_name):
        
        # ignore the field names
        pass
    
    def __on_receive_dataframe(self, df):
        
        if not type(self.group_df) is pd.DataFrame:
            self.group_df = df.sort_values(by = list(df) ).reset_index(drop=True)
            return

        value_df = pd.DataFrame()
        chunk_size =len(self.group_df) 
        num_chunks = len(df.columns) / chunk_size 
        assert len(df.columns) % chunk_size == 0, "len(df)={}, chunk_size={}".format(len(df.columns), chunk_size)
        
        for i in range(num_chunks):
            sliced_df = df[ df.columns[i*chunk_size : (i+1)*chunk_size] ]
            col_df = sliced_df.transpose().reset_index(drop=True)
            col_df.columns = [ self.agg_fields[i] ]
            value_df = pd.concat([value_df, col_df], axis = 1)
        result_df = pd.concat([self.group_df, value_df], axis=1)
        self.send(result_df, self.consumers)

    def on_producer_completed(self, producer_name):

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            if self.log_enabled:
                print("{}{}".format(self, self.hashtable))

            Operator.on_producer_completed(self, producer_name)
