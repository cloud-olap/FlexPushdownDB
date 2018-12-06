# -*- coding: utf-8 -*-
"""Aggregate support

"""
from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.group import AggregateExpressionContext
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

import pandas as pd
import numpy as np

from s3filter.plan.query_plan import QueryPlan


class AggregateMetrics(OpMetrics):
    """Extra metrics for a project

    """

    def __init__(self):
        super(AggregateMetrics, self).__init__()

        self.rows_aggregated = 0
        self.expressions_evaluated = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_aggregated': self.rows_aggregated,
            'expressions_evaluated': self.expressions_evaluated
        }.__repr__()


class Aggregate(Operator):
    """An operator that will generate aggregates (sums, avgs, etc) from the tuples it receives.

    """

    def __init__(self, expressions, use_pandas, name, query_plan, log_enabled, agg_fun):
        # type: ([], bool, str, QueryPlan, bool, function) -> None
        """Creates a new aggregate operator from the given list of expressions.

        :param use_pandas:
        :param query_plan:
        :param agg_fun:
        :param expressions: List of aggregate expressions.
        :param name: Operator name
        :param log_enabled: Logging enabled.
        """

        super(Aggregate, self).__init__(name, AggregateMetrics(), query_plan, log_enabled)

        for e in expressions:
            if type(e) is not AggregateExpression:
                raise Exception("Illegal expression type {}. All expressions must be of type {}"
                                .format(type(e), AggregateExpression.__class__.__name__))

        self.__expressions = expressions
        self.use_pandas = use_pandas
        
        self.agg_fun = agg_fun
        self.agg_df = pd.DataFrame()

        self.producer_field_names = {}

        # List of expression contexts, each storing the accumulated aggregate result and local vars.
        self.__expression_contexts = None

    def on_receive(self, ms, producer_name):
        """Event handler for receiving a message.

        :param ms: The messages
        :param producer_name: The producer that sent the message
        :return: None
        """

        if self.use_shared_mem:
            self.on_receive_message(ms, producer_name)
        else:
            for m in ms:
                self.on_receive_message(m, producer_name)

    def on_receive_message(self, m, producer_name):
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_, producer_name)
        elif isinstance(m, DataFrameMessage):
            self.__on_receive_dataframe(m.dataframe)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer_name):
        """Event handler for a producer completion event.

        :param producer_name: The producer that completed.
        :return: None
        """

        if producer_name in self.producer_completions.keys():
            self.producer_completions[producer_name] = True
        if self.use_pandas:
            if not self.is_completed() and all(self.producer_completions.values()):
                if len(self.agg_df > 0):
                    self.send(DataFrameMessage(self.agg_df.agg(['sum'])), self.consumers) 
                else: 
                    self.send(DataFrameMessage(pd.DataFrame()), self.consumers)
        else:
            if not self.is_completed() and all(self.producer_completions.values()):
                # Build and send the field names
                field_names = self.__build_field_names()
                self.send(TupleMessage(Tuple(field_names)), self.consumers)

                # Send the field values, if there are any
                if self.__expression_contexts is not None:
                    field_values = self.__build_field_values()
                    self.send(TupleMessage(Tuple(field_values)), self.consumers)

        Operator.on_producer_completed(self, producer_name)

    # def del_(self):
    #     """Cleans up internal data structures, allowing them to be GC'd
    #
    #     :return: None
    #     """
    #
    #     del self.__field_names
    #     del self.__expression_contexts

    def __on_receive_tuple(self, tuple_, producer_name):
        """Event handler for receiving a tuple.

        :param tuple_: The tuple
        :return: None
        """

        if self.log_enabled:
            print("{}('{}') | Received tuple: {}"
                  .format(self.__class__.__name__, self.name, tuple_))

        if producer_name not in self.producer_field_names.keys():
            self.producer_field_names[producer_name] = tuple_
            # TODO: Should check field names from all producers match
        else:
            self.__evaluate_expressions(tuple_, producer_name)

    def __evaluate_expressions(self, tuple_, producer_name):
        """Performs evaluation of all the aggregate expressions for the given tuple.

        :param tuple_: The tuple to pass to the expressions.
        :return: None
        """

        # We have a tuple, initialise the expression contexts
        if self.__expression_contexts is None:
            self.__expression_contexts = list(AggregateExpressionContext(0.0, {}) for _ in self.__expressions)

        # Evaluate the expressions
        i = 0
        for e in self.__expressions:
            ctx = self.__expression_contexts[i]
            e.eval(tuple_, self.producer_field_names[producer_name], ctx)
            i += 1

            self.op_metrics.expressions_evaluated += 1

        self.op_metrics.rows_aggregated += 1

    def __build_field_names(self):
        """Creates the list of field names from the evaluated aggregates. Field names will just be _0, _1, etc.

        :return: The list of field names.
        """

        return IndexedTuple.build_default(self.__expressions).field_names()

    def __build_field_values(self):
        """Creates the list of field values from the evaluated aggregates.

        :return: The field values
        """

        field_values = []
        for c in self.__expression_contexts:
            field_values.append(c.result)

        return field_values

    def __on_receive_dataframe(self, df):

        # if self.log_enabled:
        #     print("{}('{}') | Received dataframe: {}"
        #           .format(self.__class__.__name__, self.name, df))
        if len(df) > 0:
            self.op_metrics.expressions_evaluated += 1
            self.op_metrics.rows_aggregated += len(df)

            df2 = self.agg_fun(df)
            self.agg_df = self.agg_df.append( df2 )
