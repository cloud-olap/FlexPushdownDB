# -*- coding: utf-8 -*-
"""Filter support

"""

from plan.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.predicate_expression import PredicateExpression


class Filter(Operator):
    """

    """

    def __init__(self, expression, name, log_enabled):
        """

        :param expression:
        :param name:
        :param log_enabled:
        """

        super(Filter, self).__init__(name, OpMetrics(), log_enabled)

        if type(expression) is not PredicateExpression:
            raise Exception("Illegal expression type {}. Expression must be of type PredicateExpression".format(type(expression), PredicateExpression.__class__.__name__))
        else:
            self.expression = expression

        self.field_names = None

    def on_receive(self, m, _producer):
        """

        :param m:
        :param _producer:
        :return:
        """

        # print("Filter | {}".format(t))
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        if not self.field_names:
            self.field_names = tuple_
            self.__send_field_names(tuple_)
        else:
            if self.__evaluate_filter(tuple_):
                self.__send_field_values(tuple_)

    def __send_field_values(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        self.send(TupleMessage(tuple_), self.consumers)

    def __evaluate_filter(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        return self.expression.eval(tuple_, self.field_names)

    def __send_field_names(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        self.send(TupleMessage(tuple_), self.consumers)
