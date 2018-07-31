# -*- coding: utf-8 -*-
"""Predicate support

"""
import numpy

from s3filter.op.tuple import IndexedTuple


class PredicateExpression(object):
    """Represents a predicate that can evaluate to true or false

    """

    def __init__(self, expr=None, pd_expr=None):
        """Creates a new predicate expression

        :param expr: The predicate expression function
        """

        self.expr = expr
        self.pd_expr = pd_expr

    def eval(self, tuple_, field_names_index):
        """Evaluates the predicate using the given tuple

        :param tuple_: The tuple to evaluate the expression against
        :param field_names_index: The names of the fields in the tuple
        :return: True or false
        """

        if self.expr:
            it = IndexedTuple(tuple_, field_names_index)

            v = self.expr(it)

            if type(v) is not bool and type(v) is not numpy.bool_:
                raise Exception("Illegal return type '{}'. "
                                "Predicate expression must evaluate to {} or {}".format(type(v), bool, numpy.bool_))

            return v

        else:
            raise NotImplementedError
