# -*- coding: utf-8 -*-
"""Filter support

"""

from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.predicate_expression import PredicateExpression


class Filter(Operator):
    """An operator to filter tuples based on the supplied predicate expression.

    """

    def __init__(self, expression, name, log_enabled):
        """

        :param expression:
        :param name:
        :param log_enabled:
        """

        super(Filter, self).__init__(name, OpMetrics(), log_enabled)

        if type(expression) is not PredicateExpression:
            raise Exception("Illegal expression type {}. Expression must be of type PredicateExpression"
                            .format(type(expression), PredicateExpression.__class__.__name__))
        else:
            self.expression = expression

        self.field_names = None

    def on_receive(self, m, _producer):
        """Event handler for handling receipt of messages.

        :param m: The message
        :param _producer: The producer of the message
        :return: None
        """

        # print("Filter | {}".format(t))
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_):
        """Event handler to handle receipt of a tuple

        :param tuple_: The tuple
        :return: None
        """

        if not self.field_names:
            self.field_names = tuple_
            self.__send_field_names(tuple_)
        else:
            if self.__evaluate_filter(tuple_):
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

        return self.expression.eval(tuple_, self.field_names)

    def __send_field_names(self, tuple_):
        """Sends the field names tuple to consumers

        :param tuple_: The field names tuple
        :return: None
        """

        self.send(TupleMessage(tuple_), self.consumers)
