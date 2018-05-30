# -*- coding: utf-8 -*-
"""Aggregate support

"""

from metric.op_metrics import OpMetrics
from op.aggregate_expression import AggregateExpression
from op.group import AggregateExpressionContext
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import LabelledTuple, Tuple


class Aggregate(Operator):
    """

    """

    def __init__(self, expressions, name, log_enabled):
        """

        :param expressions:
        :param name:
        :param log_enabled:
        """

        super(Aggregate, self).__init__(name, OpMetrics(), log_enabled)

        for e in expressions:
            if type(e) is not AggregateExpression:
                raise Exception("Illegal expression type {}. All expressions must be of type {}".format(type(e), AggregateExpression.__class__.__name__))

        self.__expressions = expressions

        self.__field_names = None

        # Dict of aggregate contexts. Each item is indexed by the aggregate order and contains a dict of the
        # evaluated aggregate results
        self.__aggregate_contexts = {}

    def on_receive(self, m, _producer):
        """

        :param m:
        :param _producer:
        :return:
        """

        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer):
        """

        :param producer:
        :return:
        """

        # Send the field names
        aggregate_field_names = self.__build_aggregate_field_names()
        self.send(TupleMessage(Tuple(aggregate_field_names)), self.consumers)

        # Send the field values
        aggregate_field_values = self.__build_aggregate_field_values()
        self.send(TupleMessage(Tuple(aggregate_field_values)), self.consumers)

        del self.__field_names
        del self.__aggregate_contexts

        Operator.on_producer_completed(self, producer)

    def __on_receive_tuple(self, tuple_):
        """

        :param tuple_:
        :param _producer:
        :return:
        """

        if not self.__field_names:
            self.__field_names = tuple_
        else:
            self.__evaluate_aggregates(tuple_)

    def __evaluate_aggregates(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        i = 0
        for e in self.__expressions:
            ctx = self.__get_aggregate_context(i)
            e.eval(tuple_, self.__field_names, ctx)
            i += 1

    def __get_aggregate_context(self, i):
        """

        :param i:
        :return:
        """

        ctx = self.__aggregate_contexts.get(i, AggregateExpressionContext(0.0, {}))
        self.__aggregate_contexts[i] = ctx
        return ctx

    def __build_aggregate_field_values(self):
        """

        :return:
        """

        aggregate_field_values = []
        for i in range(0, len(self.__expressions)):

            if self.is_completed():
                break

            aggregate_context = self.__aggregate_contexts[i]
            aggregate_field_values.append(aggregate_context.result)

        return aggregate_field_values

    def __build_aggregate_field_names(self):
        """

        :return:
        """

        return LabelledTuple(self.__expressions).labels
