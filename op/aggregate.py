# -*- coding: utf-8 -*-
"""Aggregate support

"""

from metric.op_metrics import OpMetrics
from op.group import AggregateExprContext2
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

        # Dict of aggregate contexts. Each item is indexed by the aggregate order and contains a dict of the
        # evaluated aggregate results
        self.ctx = {}

    def on_receive(self, m, producer):
        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_, producer)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, _producer):

        if not self.field_names:
            self.field_names = tuple_
        else:
            self.evaluate_aggregates(tuple_)

    def evaluate_aggregates(self, tuple_):
        i = 0
        for expr in self.exprs:
            aggregate_ctx = self.ctx.get(i, AggregateExprContext2(0.0, {}))
            expr.eval(tuple_, self.field_names, aggregate_ctx)
            self.ctx[i] = aggregate_ctx
            i += 1

    def on_producer_completed(self, producer):

        # Send the field names
        lt = LabelledTuple(self.exprs)
        self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

        field_values = []
        for aggregate_idx, aggregate_context in self.ctx.items():

            if self.is_completed():
                break

            field_values.append(aggregate_context.result)

        self.send(TupleMessage(Tuple(field_values)), self.consumers)

        Operator.on_producer_completed(self, producer)
