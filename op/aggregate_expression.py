# -*- coding: utf-8 -*-
"""Aggregate expression support

"""

import numbers

from op.tuple import LabelledTuple
from sql.function import sum_fn, count_fn, avg_fn


class AggregateExpression(object):
    """Represents an aggregate expression (e.g. sum(2.5 * col_name) ).

    TODO: This can be reworked... at least to remove the need to pass ctx around.
    """

    SUM = "SUM"
    COUNT = "COUNT"
    AVG = "AVG"

    def __init__(self, expression_type, expr):
        """

        :param expr: The expression (as a function)
        """

        if expression_type is not AggregateExpression.SUM and \
                expression_type is not AggregateExpression.COUNT and \
                expression_type is not AggregateExpression.AVG:
            raise Exception("Illegal expression type '{}'. Expression type must be '{}', '{}', or '{}'"
                            .format(expression_type,
                                    AggregateExpression.SUM,
                                    AggregateExpression.COUNT,
                                    AggregateExpression.AVG))

        self.__expression_type = expression_type
        self.__expr = expr

    def eval(self, t, field_names, ctx):
        """Evaluates the expression using the given tuple, the names of the fields in the tuple and the aggregate
        functions context (which holds any variables and the running result).

        :param t: Tuple to evaluate
        :param field_names: Names of the tuple fields
        :param ctx: The aggregate context
        :return: None
        """

        if self.__expression_type is AggregateExpression.SUM:
            sum_fn(self.__expr(LabelledTuple(t, field_names)), ctx)
        elif self.__expression_type is AggregateExpression.COUNT:
            count_fn(self.__expr(LabelledTuple(t, field_names)), ctx)
        elif self.__expression_type is AggregateExpression.AVG:
            avg_fn(self.__expr(LabelledTuple(t, field_names)), ctx)
        else:
            # Should not happen as its already been checked
            raise Exception("Illegal expression type '{}'. Expression type must be '{}', '{}', or '{}'"
                            .format(self.__expression_type,
                                    AggregateExpression.SUM,
                                    AggregateExpression.COUNT,
                                    AggregateExpression.AVG))

        # self.__expr(LabelledTuple(t, field_names), ctx)

        if not isinstance(ctx.result, numbers.Number):
            raise Exception("Illegal aggregate val '{}' of type '{}'. Aggregate expression must evaluate to number"
                            .format(ctx.result, type(ctx.result)))


class AggregateExpressionContext(object):
    """An aggregate expression context is a place for an aggregate functions to store values between evaluations. For
    example a sum function will want to keep the running sum as it receives tuples. This context class is where that is
    kept.

    """

    def __init__(self, result, vars_):
        """

        :param result:
        :param vars_:
        """

        self.result = result
        self.vars_ = vars_

    def __repr__(self):
        return {'result': self.result, 'vars': self.vars_}.__repr__()
