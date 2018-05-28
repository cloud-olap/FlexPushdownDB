# -*- coding: utf-8 -*-
"""Aggregate support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import LabelledTuple, Tuple


class AggregateExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names, ctx):
        self.expr(LabelledTuple(t, field_names), ctx)


class Aggregate(Operator):

    def __init__(self, exprs, name, log_enabled):

        super(Aggregate, self).__init__(name, OpMetrics(), log_enabled)

        self.exprs = exprs

        # TODO: This should perhaps be set when a producer is connected to this operator.
        # E.g. When a table scan from a particular table is connected then this op should acquire it's key.
        self.key = None

        self.field_names = None

        self.ctx = {}

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
            self.evaluate_aggregates(tuple_)

    def evaluate_aggregates(self, tuple_):
        for expr in self.exprs:
            expr.eval(tuple_, self.field_names, self.ctx)

    def on_producer_completed(self, producer):

        # Send the field names
        lt = LabelledTuple(self.exprs)
        self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

        fields = Tuple()
        for k, v in self.ctx.items():
            fields.append(v)

        self.send(TupleMessage(fields), self.consumers)
