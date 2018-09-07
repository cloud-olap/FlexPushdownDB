# -*- coding: utf-8 -*-
"""Filter support

"""
import time

import pandas as pd

from s3filter.op.tuple import IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.predicate_expression import PredicateExpression
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle

from s3filter.sql.function import cast, timestamp


class FilterMetrics(OpMetrics):
    """Extra metrics for a Filter

    """

    def __init__(self):
        super(FilterMetrics, self).__init__()

        self.rows_processed = 0
        self.rows_filtered = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_processed': self.rows_processed,
            'rows_filtered': self.rows_filtered
        }.__repr__()


class Filter(Operator):
    """An operator to filter tuples based on the supplied predicate expression.

    """

    def __init__(self, expression, name, query_plan, log_enabled):
        """

        :param expression:
        :param name:
        :param log_enabled:
        """

        super(Filter, self).__init__(name, FilterMetrics(), query_plan, log_enabled)

        if type(expression) is not PredicateExpression:
            raise Exception("Illegal expression type {}. Expression must be of type PredicateExpression"
                            .format(type(expression), PredicateExpression.__class__.__name__))
        else:
            self.expression = expression

        self.field_names_index = None

        self.producers_received = {}

    def on_receive(self, ms, producer_name):
        """Event handler for handling receipt of messages.

        :param ms: The messages
        :param producer_name: The producer of the message
        :return: None
        """

        # print("Filter | {}".format(t))
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_dataframe(self, df):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        if self.log_enabled:
            print("{} | {}('{}') | Received data frame:"
                  .format(time.time(), self.__class__.__name__, self.name))
            print(df)

        self.op_metrics.rows_processed += len(df)

        criterion = self.expression.pd_expr(df)

        filtered_df = df[criterion]

        self.op_metrics.rows_filtered += len(filtered_df)

        self.send(filtered_df, self.consumers)

    def __on_receive_tuple(self, tuple_, producer_name):
        """Event handler to handle receipt of a tuple

        :param tuple_: The tuple
        :return: None
        """

        assert (len(tuple_) > 0)

        if not self.field_names_index:
            self.field_names_index = IndexedTuple.build_field_names_index(tuple_)
            self.producers_received[producer_name] = True
            self.__send_field_names(tuple_)
        else:
            if producer_name not in self.producers_received.keys():
                # This will be the field names tuple, skip it
                self.producers_received[producer_name] = True
            else:
                if self.__evaluate_filter(tuple_):
                    self.op_metrics.rows_filtered += 1
                    self.__send_field_values(tuple_)

    def __send_field_values(self, tuple_):
        """Sends the field values tuple to consumers

        :param tuple_: The field values tuple
        :return: None
        """

        self.send(TupleMessage(tuple_), self.consumers)

    def __evaluate_filter(self, tuple_):
        """Evaluates the predicate expression agains the given tuple

        :param tuple_: The tuple to evaluate against
        :return: Whether the predicate evaluated true or false
        """

        self.op_metrics.rows_processed += 1

        return self.expression.eval(tuple_, self.field_names_index)

    def __send_field_names(self, tuple_):
        """Sends the field names tuple to consumers

        :param tuple_: The field names tuple
        :return: None
        """

        self.send(TupleMessage(tuple_), self.consumers)

    def on_producer_completed(self, producer_name):
        Operator.on_producer_completed(self, producer_name)
