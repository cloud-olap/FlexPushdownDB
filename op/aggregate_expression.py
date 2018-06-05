# -*- coding: utf-8 -*-
"""Aggregate expression support

"""

import numbers

from op.tuple import LabelledTuple


def sum_fn(ex, ctx):
    if not isinstance(ex, numbers.Number):
        raise Exception("Illegal expression type {} for expression {}. Sum expression must be numeric"
                        .format(type(ex), ex))

    current_sum = ctx.result

    current_sum += ex

    ctx.result = current_sum


def avg_fn(ex, ctx):
    if not isinstance(ex, numbers.Number):
        raise Exception(
            "Illegal expression type {} for expression {}. Sum expression must be numeric".format(type(ex), ex))

    current_count = ctx.vars_.get('.count', 0)
    # current_total = ctx.vars_.get('.total', 0)
    current_running_avg = ctx.result

    current_count += 1
    # current_total += ex
    current_running_avg = (current_running_avg * (float(current_count - 1)) + ex) / float(current_count)

    ctx.vars_['.count'] = current_count
    # ctx.vars_['.total'] = current_total

    # current_avg = current_total / float(current_count)
    # if current_running_avg is not current_avg:
    #     raise Exception("Running average {} should equal average {}".format(current_running_avg, current_avg))

    ctx.result = current_running_avg


def count_fn(_ex, ctx):
    current_count = ctx.result

    current_count += 1

    ctx.result = current_count


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
