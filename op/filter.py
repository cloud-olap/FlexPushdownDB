# -*- coding: utf-8 -*-
"""Filter support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import LabelledTuple


class PredicateExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names):
        v = self.expr(LabelledTuple(t, field_names))
        return v


class Filter(Operator):

    def __init__(self, expr, name, log_enabled):

        super(Filter, self).__init__(name, OpMetrics(), log_enabled)

        self.expr = expr

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.field_names = None

    def on_receive(self, m, _producer):

        # print("Filter | {}".format(t))
        if type(m) is TupleMessage:
            self.on_receive_tuple(_producer, m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, _producer, tuple_):

        self.key = _producer.key

        if not self.field_names:
            self.field_names = tuple_
            self.send_field_names(tuple_)
        else:
            if self.evaluate_filter(tuple_):
                self.send_fields(tuple_)

    def send_fields(self, tuple_):
        self.send(TupleMessage(tuple_), self.consumers)

    def evaluate_filter(self, tuple_):
        return self.expr.eval(tuple_, self.field_names)

    def send_field_names(self, tuple_):
        self.send(TupleMessage(tuple_), self.consumers)
