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

    def __init__(self, group_fields, agg_fields, exprs, name, query_plan, log_enabled):
        """
        Creates a new join operator.

        """
        self.groups = []
        self.group_fields = group_fields
        self.agg_fields = agg_fields
        self.aggregate_exprs = exprs
        self.sql_agg = '' 
        self.sql_scan = ''
        self.hybrid = False
        self.nlargest = None 
        super(GroupbyFilterBuild, self).__init__(name, GroupbyFilterBuildMetrics(), query_plan, log_enabled)

    def set_nlargest(self, nlargest):
        self.hybrid = True
        self.nlargest = nlargest

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
        if self.hybrid:
            total_records = df['_count'].sum()
            remote_aggregate_df = df.nlargest( self.nlargest, ['_count'] ) 
            remote_aggregate_df = remote_aggregate_df[self.group_fields].sort_values(by = self.group_fields) 
            remote_scan_df = df[self.group_fields] 
            remote_scan_df = remote_scan_df[ ~remote_scan_df.index.isin(remote_aggregate_df.index) ]
        
            remote_aggregate_df.reset_index(drop=True, inplace=True) 
            remote_scan_df.reset_index(drop=True, inplace=True) 
        else:
            remote_aggregate_df = df
            remote_aggregate_df.sort_values(by = self.group_fields)

        self.groups = remote_aggregate_df.values.tolist()
        self.sql_agg = 'select '
        for expr_num, expr in enumerate(self.aggregate_exprs):
            for group in self.groups:
                self.sql_agg += ' {}(CASE'.format(expr[0])
                self.sql_agg += ' WHEN '
                self.sql_agg += 'AND'.join([ ' {} = \'{}\' '.format(gname, group[n]) for n, gname in enumerate(self.group_fields) ])
                self.sql_agg += ' THEN '
                self.sql_agg += expr[1] 
                self.sql_agg += ' ELSE 0 END '
                self.sql_agg += ')' #.format('_'.join(group), expr_num) 
                if not (expr == self.aggregate_exprs[-1] and group == self.groups[-1]):
                    self.sql_agg += ','
        self.sql_agg += ' from s3Object;'
        self.send(  StringMessage(self.sql_agg), self.tagged_consumers[0])
        self.send(  remote_aggregate_df, self.tagged_consumers[1])
        
        if self.hybrid:
            assert len(remote_scan_df) > 0
            self.sql_scan = 'select ' + ','.join(self.group_fields + self.agg_fields)
            self.sql_scan += ' from s3Object where '
            for group in self.groups:
                self.sql_scan += 'NOT ('
                self.sql_scan += ' AND'.join([ ' {} = \'{}\' '.format(gname, group[n]) for n, gname in enumerate(self.group_fields) ])
                self.sql_scan += ')'
                if not group == self.groups[-1]:
                    self.sql_scan += 'AND '
            self.send( StringMessage(self.sql_scan), self.tagged_consumers[2] )

    def on_producer_completed(self, producer_name):

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            if self.log_enabled:
                print("{}{}".format(self, self.hashtable))

            Operator.on_producer_completed(self, producer_name)
