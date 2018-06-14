# -*- coding: utf-8 -*-
"""Query plan support

"""
from collections import OrderedDict

from s3filter.plan.graph import Graph
from s3filter.plan.op_metrics import OpMetrics


class QueryPlan(object):
    """Container for the operators in a query. Really a convenience class that allows the plan graph to be
    generated and the operator execution metrics.

    """

    def __init__(self, operators=None):
        # type: (list) -> None
        """

        :param operators:
        """

        if operators is None:
            self.operators = OrderedDict()
        else:
            self.operators = OrderedDict()
            for o in operators:
                self.operators[o.name] = o

    def add_operator(self, operator):
        self.operators[operator.name] = operator
        return operator

    def print_metrics(self):

        print('')

        OpMetrics.print_metrics(list(self.operators.values()))

        print('')

    def write_graph(self, dir_, name):

        graph = Graph(name)

        for o in self.operators.values():
            graph.add_operator(o)

        graph.write(dir_)
