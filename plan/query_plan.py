# -*- coding: utf-8 -*-
"""Query plan support

"""
from collections import OrderedDict

from plan.graph import Graph
from plan.op_metrics import OpMetrics


class QueryPlan(object):

    def __init__(self, name, operators=None):
        # type: (str, list) -> None
        """

        :param operators:
        """

        self.name = name

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
        print('')
        print(self.name)
        print('-' * len(self.name))

        OpMetrics.print_metrics(list(self.operators.values()))

        print('')

    def write_graph(self, name):

        graph = Graph(name)

        for o in self.operators.values():
            graph.add_operator(o)

        graph.write()
