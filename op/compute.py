# -*- coding: utf-8 -*-
"""Aggregate support

"""

from op.operator_base import Operator
from op.tuple import LabelledTuple, Tuple


class ComputeExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names):
        return self.expr(LabelledTuple(t, field_names))

    # def val(self, i, ctx):
    #     return ctx[i]


class Compute(Operator):

    def __init__(self, expr):

        Operator.__init__(self)

        self.expr = expr

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.field_names = None

    def on_receive(self, t, _producer):

        # print("Aggregate | {}".format(t))

        self.key = _producer.key

        if not self.field_names:
            self.field_names = t
        else:
            ct = self.expr.eval(t, self.field_names)
            self.send(Tuple(['_0']))
            self.send(Tuple([ct]))
