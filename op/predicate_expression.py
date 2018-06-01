# -*- coding: utf-8 -*-
"""Predicate support

"""

from op.tuple import LabelledTuple


class PredicateExpression(object):
    """

    """

    def __init__(self, expr):
        """

        :param expr:
        """

        self.expr = expr

    def eval(self, t, field_names):
        """

        :param t:
        :param field_names:
        :return:
        """

        v = self.expr(LabelledTuple(t, field_names))

        if type(v) is not bool:
            raise Exception("Illegal return type '{}'. Predicate expression must evaluate to bool".format(type(v)))

        return v
