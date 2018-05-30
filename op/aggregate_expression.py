# -*- coding: utf-8 -*-
"""Aggregate expression support

"""

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
