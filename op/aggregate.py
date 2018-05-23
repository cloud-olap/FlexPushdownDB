# -*- coding: utf-8 -*-
"""Aggregate support

"""

from op.operator_base import Operator
from op.tuple import LabelledTuple, Tuple


class AggregateExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names, ctx):
        self.expr(LabelledTuple(t, field_names), ctx)

    # def val(self, i, ctx):
    #     return ctx[i]


class Aggregate(Operator):

    def __init__(self, exprs):

        Operator.__init__(self)

        self.exprs = exprs

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.field_names = None

        self.ctx = {}

    def on_receive(self, t, _producer):

        # print("Aggregate | {}".format(t))

        self.key = _producer.key

        if not self.field_names:
            self.field_names = t
        else:
            for expr in self.exprs:
                expr.eval(t, self.field_names, self.ctx)

    def on_producer_completed(self, producer):

        # Send the headers
        lt = LabelledTuple(self.exprs)
        self.send(lt.labels)

        fields = Tuple()
        for k, v in self.ctx.items():
            fields.append(v)

        self.send(fields)
