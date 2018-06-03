# -*- coding: utf-8 -*-
"""Predicate support

"""

from op.tuple import LabelledTuple


class PredicateExpression(object):
    """Represents a predicate that can evaluate to true or false

    """

    def __init__(self, expr):
        """Creates a new predicate expression

        :param expr: The predicate expression function
        """

        self.expr = expr

    def eval(self, tuple_, field_names):
        """Evaluates the predicate using the given tuple

        :param tuple_: The tuple to evaluate the expression against
        :param field_names: The names of the fields in the tuple
        :return: True or false
        """

        v = self.expr(LabelledTuple(tuple_, field_names))

        if type(v) is not bool:
            raise Exception("Illegal return type '{}'. Predicate expression must evaluate to bool".format(type(v)))

        return v
