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


class GroupbyFilterBuildMetrics(OpMetrics):
    """Extra metrics for GroupbyFilterBuild 

    """
    def __init__(self):
        super(GroupbyFilterBuildMetrics, self).__init__()

        self.rows_processed = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_processed': self.rows_processed
        }.__repr__()


class GroupbyFilterBuild(Operator):
    """Builds a SQL filter for groupby

    """

    def __init__(self, exprs, name, query_plan, log_enabled):
        """
        Creates a new join operator.

        """
        self.groups = [] 
        self.aggregate_exprs = exprs
        self.sql = '' 
        super(GroupbyFilterBuild, self).__init__(name, GroupbyFilterBuildMetrics(), query_plan, log_enabled)

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
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, _producer_name):
        # ignore the field names
        pass

    def __on_receive_dataframe(self, df):
        group_fields = list(df)
        self.groups = df.sort_values(by = group_fields).values.tolist()
        self.sql = 'select '        
        for expr_num, expr in enumerate(self.aggregate_exprs):
            for group in self.groups:
                self.sql += ' {}(CASE'.format(expr[0])
                self.sql += ' WHEN '
                self.sql += 'AND'.join([ ' {} = \'{}\' '.format(gname, group[n]) for n, gname in enumerate(group_fields) ])
                self.sql += ' THEN '
                self.sql += expr[1] 
                self.sql += ' ELSE 0 END '
                self.sql += ') AS G{}_AGG{}'.format('_'.join(group), expr_num) 
                if not (expr == self.aggregate_exprs[-1] and group == self.groups[-1]):
                    self.sql += ','
        self.sql += ' from s3Object;'
        self.send(  StringMessage(self.sql), self.consumers)

    def on_producer_completed(self, producer_name):

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            if self.log_enabled:
                print("{}{}".format(self, self.hashtable))

            Operator.on_producer_completed(self, producer_name)
