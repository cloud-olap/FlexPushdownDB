# -*- coding: utf-8 -*-
"""Aggregate expression support

"""
import numbers

from op.tuple import LabelledTuple


class AggregateExpression(object):
    """

    """

    def __init__(self, expr):
        """

        :param expr:
        """

        self.__expr = expr

    def eval(self, t, field_names, ctx):
        """

        :param t:
        :param field_names:
        :param ctx:
        :return:
        """

        self.__expr(LabelledTuple(t, field_names), ctx)

        if not isinstance(ctx.result, numbers.Number):
            raise Exception("Illegal aggregate val '{}' of type '{}'. Aggregate expression must evaluate to number"
                            .format(ctx.result, type(ctx.result)))


class AggregateExpressionContext(object):
    """

    """

    def __init__(self, result, vars_):
        """

        :param result:
        :param vars_:
        """

        self.result = result
        self.vars_ = vars_

    def __repr__(self):
        """

        :return:
        """

        return {'result': self.result, 'vars': self.vars_}.__repr__()
