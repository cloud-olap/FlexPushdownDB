# -*- coding: utf-8 -*-
"""Aggregate support

"""

from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.group import AggregateExpressionContext
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import LabelledTuple, Tuple


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

        # List of expression contexts, each storing the accumulated aggregate result and local vars.
        self.__expression_contexts = None

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

        # Build and send the field names
        field_names = self.__build_field_names()
        self.send(TupleMessage(Tuple(field_names)), self.consumers)

        # Send the field values, if there are any
        if self.__expression_contexts is not None:
            field_values = self.__build_field_values()
            self.send(TupleMessage(Tuple(field_values)), self.consumers)

        # Clean up
        self.del_()

        Operator.on_producer_completed(self, producer)

    def del_(self):
        """Cleans up internal data structures, allowing them to be GC'd

        :return: None
        """

        del self.__field_names
        del self.__expression_contexts

    def __on_receive_tuple(self, tuple_):
        """Event handler for receiving a tuple.

        :param tuple_: The tuple
        :return: None
        """

        if not self.__field_names:
            self.__field_names = tuple_
        else:
            self.__evaluate_expressions(tuple_)

    def __evaluate_expressions(self, tuple_):
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
            e.eval(tuple_, self.__field_names, ctx)
            i += 1

    def __build_field_names(self):
        """Creates the list of field names from the evaluated aggregates. Field names will just be _0, _1, etc.

        :return: The list of field names.
        """

        return LabelledTuple(self.__expressions).labels

    def __build_field_values(self):
        """Creates the list of field values from the evaluated aggregates.

        :return: The field values
        """

        field_values = []
        for c in self.__expression_contexts:
            field_values.append(c.result)

        return field_values


