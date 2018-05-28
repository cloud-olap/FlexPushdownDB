# -*- coding: utf-8 -*-
"""Aggregate support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import LabelledTuple, Tuple


class ComputeExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names):
        return self.expr(LabelledTuple(t, field_names))

    # def val(self, i, ctx):
    #     return ctx[i]


class Compute(Operator):

    def __init__(self, expr, name, log_enabled):

        super(Compute, self).__init__(name, OpMetrics(), log_enabled)

        self.expr = expr

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.field_names = None

    def on_receive(self, m, producer):
        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_, producer)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, producer):

        self.key = producer.key

        if not self.field_names:
            self.field_names = tuple_
        else:
            ct = self.evaluate_computation(tuple_)
            self.send(TupleMessage(Tuple(['_0'])), self.consumers)
            self.send(TupleMessage(Tuple([ct])), self.consumers)

    def evaluate_computation(self, tuple_):
        ct = self.expr.eval(tuple_, self.field_names)
        return ct
