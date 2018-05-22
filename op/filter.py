# -*- coding: utf-8 -*-
"""Filter support

"""

from op.operator_base import Operator
from op.tuple import LabelledTuple


class PredicateExpression(object):

    def __init__(self, expr):
        self.expr = expr

    def eval(self, t, field_names):
        return self.expr(LabelledTuple(t, field_names))


class Filter(Operator):

    def __init__(self, expr):

        Operator.__init__(self)

        self.expr = expr

        self.field_names = None

    def on_receive(self, t, _producer):

        # print("Filter | {}".format(t))

        if not self.field_names:
            self.field_names = t
            self.send(t)
        else:
            if self.expr.eval(t, self.field_names):
                self.send(t)
