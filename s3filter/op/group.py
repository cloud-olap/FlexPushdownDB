# -*- coding: utf-8 -*-
"""Group by with aggregate support

"""

from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.aggregate_expression import AggregateExpressionContext
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

import pandas as pd
import numpy as np

class Group(Operator):
    """A crude group by operator. Allows multiple grouping columns to be specified along with multiple aggregate
    expressions.

    """

    def __init__(self, group_field_names, aggregate_expressions, name, query_plan, log_enabled, pd_expr = None):
        """Creates a new group by operator.

        :param group_field_names: The names of the fields to group by
        :param aggregate_expressions: The list of aggregate expressions
        """

        super(Group, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.group_field_names = group_field_names
        self.aggregate_expressions = aggregate_expressions

        self.field_names = None

        # Dict of group contexts. Each item is indexed by a tuple of the group columns and contains a dict of the
        # evaluated aggregate results
        self.group_contexts = {}

        self.group_fields_id = []
        self.aggregate_df = None
        self.use_pandas = True
        self.pd_expr = pd_expr
        self.producers_received = {}
    
    def on_receive(self, ms, _producer):
        """ Handles the event of receiving a new tuple from a producer. Applies each aggregate function to the tuple and
        then inserts those aggregates into the tuples dict indexed by the grouping columns values.

        Once downstream producers are completed the tuples will be send to downstream consumers.

        :param ms: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, _producer)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
        if not self.field_names:
            # Save the field names
            self.field_names = tuple_
            for e in self.aggregate_expressions:
                e.set_field_names(tuple_)
            for f in self.group_field_names:
                self.group_fields_id.append( tuple_.index(f) )
            self.producers_received[producer_name] = True
        else:
            if producer_name not in self.producers_received.keys():
                self.producers_received[producer_name] = True
                return
            self.use_pandas = False
            # Create a tuple of column values to group by,
            # we use this as the key for a dict of groups and their associated aggregate values
            group_field_values_tuple = self.__build_group_field_values_tuple(tuple_)

            # Get or create the group context
            group_aggregate_expression_contexts = self.group_contexts.get(group_field_values_tuple, {})
            # Evaluate all the expressions for the group
            i = 0
            for e in self.aggregate_expressions:
                group_aggregate_expression_context = group_aggregate_expression_contexts.get(
                    i,
                    AggregateExpressionContext(0.0, {}))
                e.eval_lite(tuple_, group_aggregate_expression_context)
                group_aggregate_expression_contexts[i] = group_aggregate_expression_context
                i += 1

            # Store the group context indexed by the group
            self.group_contexts[group_field_values_tuple] = group_aggregate_expression_contexts
   
    def __df_aggregate_fun(self, df):        
        names = [x.get_aggregate_name() for x in self.aggregate_expressions] + ['__count']
        d = [x.eval_df(df) for x in self.aggregate_expressions] + [df['__count'].sum()]
        return pd.Series(d, index=names)
    
    def __on_receive_dataframe(self, df):

        df['__count'] = np.ones( len(df) ) 
        if not self.pd_expr:
            agg_df = df.groupby(self.group_field_names).apply(self.__df_aggregate_fun).reset_index()
        else:
            agg_df = df.groupby(self.group_field_names).apply(self.pd_expr).reset_index()
        
        if not type(self.aggregate_df) is pd.DataFrame:
            self.aggregate_df = agg_df
        else:
            self.aggregate_df = pd.concat([self.aggregate_df, agg_df], ignore_index=True).groupby(self.group_field_names).sum().reset_index()
    
    def __build_group_field_values_tuple(self, tuple_):
        group_fields = [0] * len(self.group_fields_id)
        n = 0
        for i in self.group_fields_id:
            group_fields[n] = tuple_[i]
            n += 1
        return tuple(group_fields)

    def on_producer_completed(self, producer_name):
        """Handles the event where the producer has completed producing all the tuples it will produce. Once this
        occurs the tuples can be sent to consumers downstream.

        :param producer_name: The producer that has completed
        :return: None
        """
        if producer_name in self.producer_completions.keys():
            self.producer_completions[producer_name] = True
        else:
            raise Exception("Unrecognized producer {} has completed".format(producer_name))
        
        is_all_producers_done = all(self.producer_completions.values())
        if not is_all_producers_done:
            return

        print("Producer Complete ({})".format(self.name))
        if not self.use_pandas:
            # Send the field names
            lt = IndexedTuple.build_default(self.group_field_names + self.aggregate_expressions)
            self.send(TupleMessage(Tuple(lt.field_names())), self.consumers)

            for group_tuple, group_aggregate_contexts in self.group_contexts.items():
    
                if self.is_completed():
                    break

                # Convert the aggregate contexts to their results
                group_fields = list(group_tuple)
                
                group_aggregate_values = list(v.result for v in group_aggregate_contexts.values())
        
                t_ = group_fields + group_aggregate_values
                self.send(TupleMessage(Tuple(t_)), self.consumers)
        else:
            # TODO 
            # for average, devide by __count.
            self.send(TupleMessage(Tuple(list(self.aggregate_df))), self.consumers)
            self.send(self.aggregate_df, self.consumers)
        Operator.on_producer_completed(self, producer_name)
