# -*- coding: utf-8 -*-
"""
SQL aggregate function support

"""
import sympy


class AggregateExpression:
    """Represents an SQL aggregate such as SUM(x + y - 1)

    Intended to be built and retained during operator execution as it accumulates its internal aggregate value. At the
    end of parsing a stream of tuples, the computed aggregate can be accessed via 'val'

    """

    def __init__(self, expr_str):
        """ Constructs a new aggregate using the given expression. Note column references need to be
        specified as ordinals.

        :param expr_str: An SQL aggregate such as SUM(_1 + _2 - 1)
        """

        # The aggregate expression
        self.expr_str = expr_str
        self.expr = sympy.sympify(self.expr_str, evaluate=False)

        # Converting the expression to a lambda is required for speed apparently
        self.lambda_fn = sympy.lambdify(self.expr.free_symbols, self.expr, {'sum': self.sum_fn, 'count': self.count_fn})

        # The computed aggregate value
        self.val = 0

    def sum_fn(self, v):
        """ Accumulates a sum of the given values passed via v

        :param v: Value of evaluated aggregate expression.
        :return: None
        """
        self.val += v

    def count_fn(self, v):
        """ Accumulates a sum of the given values passed via v

        :param v: Value of evaluated aggregate expression.
        :return: None
        """
        self.val += 1

    def eval(self, t):
        """Evaluates the expression for the given tuple.

        :param t: Tuple to evaluate expression for.
        :return: None
        """

        # Extract the symbols in the expression we need to substitute with values from the tuple.
        #
        # For example:
        #   Given an expression - sum(_1 + _2 - 1)
        #
        # The 'free' symbols are _1 and _2
        #
        # We then need to extract the values for element _1 and _2 from the tuple and substiture them in the
        # expression.

        symbol_values = []
        for s in self.expr.free_symbols:
            symbol_as_ordinal = int(s.name[1:])

            # TODO: Casting everything to float is a hack
            tuple_element = float(t[symbol_as_ordinal])

            symbol_values.append(tuple_element)

        self.lambda_fn(*sympy.flatten(symbol_values))
