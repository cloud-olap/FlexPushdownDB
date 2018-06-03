# -*- coding: utf-8 -*-
"""Aggregate support

"""

from plan.op_metrics import OpMetrics
from op.aggregate_expression import AggregateExpression
from op.group import AggregateExpressionContext
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import LabelledTuple, Tuple


class Aggregate(Operator):
    """An operator that will generate aggregates (sums, avgs, etc) from the tuples it receives.

    """

    def __init__(self, expressions, name, log_enabled):
        """Creates a new aggregate operator from the given list of expressions.

        :param expressions: List of aggregate expressions.
        :param name: Operator name
        :param log_enabled: Logging enabled.
        """

        super(Aggregate, self).__init__(name, OpMetrics(), log_enabled)

        for e in expressions:
            if type(e) is not AggregateExpression:
                raise Exception("Illegal expression type {}. All expressions must be of type {}"
                                .format(type(e), AggregateExpression.__class__.__name__))

        self.__expressions = expressions

        self.__field_names = None

        # Dict of aggregate contexts. Each item is indexed by the aggregate order and contains a dict of the
        # evaluated aggregate results
        self.__aggregate_contexts = {}

    def on_receive(self, m, _producer):
        """Event handler for receiving a message.

        :param m: The message
        :param _producer: The producer that sent the message
        :return: None
        """

        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer):
        """Event handler for a producer completion event.

        :param producer: The producer that completed.
        :return: None
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
        """Event handler for receiving a tuple.

        :param tuple_: The tuple
        :return: None
        """

        if not self.__field_names:
            self.__field_names = tuple_
        else:
            self.__evaluate_aggregates(tuple_)

    def __evaluate_aggregates(self, tuple_):
        """Performs evaluation of all the aggregate expressions for the given tuple.

        :param tuple_: The tuple to pass to the expressions.
        :return: None
        """

        i = 0
        for e in self.__expressions:
            ctx = self.__get_aggregate_context(i)
            e.eval(tuple_, self.__field_names, ctx)
            i += 1

    def __get_aggregate_context(self, i):
        """Returns the context for the given aggregate.

        :param i: The index of the expression
        :return: The expressions aggregate context.
        """

        ctx = self.__aggregate_contexts.get(i, AggregateExpressionContext(0.0, {}))
        self.__aggregate_contexts[i] = ctx
        return ctx

    def __build_aggregate_field_values(self):
        """Creates the list of field values from the evaluated aggregates.

        :return: The field values
        """

        aggregate_field_values = []
        # for ctx in self.__aggregate_contexts.values():
        for i in range(0, len(self.__expressions)):

            if self.is_completed():
                break

            # Check if there is an aggregate result, there may not be if no tuples were received
            # if i in self.__aggregate_contexts:
            aggregate_context = self.__aggregate_contexts[i]
            aggregate_field_values.append(aggregate_context.result)
            # aggregate_field_values.append(ctx.result)

        return aggregate_field_values

    def __build_aggregate_field_names(self):
        """Creates the list of field names from the evaluated aggregates. Field names will just be _0, _1, etc.

        :return: The list of field names.
        """

        return LabelledTuple(self.__aggregate_contexts).labels
