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

    def traverse(self, from_operator, visited_operators):
        """Returns a sorted list of the operators according to where they appear in the query plan, starting from
        the supplied operator.

        :param from_operator: First operator to start from
        :param visited_operators: Operators already traversed
        :return: Sorted list of operators
        """

        if from_operator.name not in visited_operators:
            visited_operators[from_operator.name] = from_operator

        for c in from_operator.consumers:
            visited_operators = self.traverse(c, visited_operators)

        return visited_operators

    def traverse_from_root(self):
        """Returns a sorted list of the operators according to where they appear in the query plan, starting from
        the root operators.

        :return: Sorted list of operators
        """

        # Find the operators at the root of the graph, they will have no producers
        root_operators = []
        for o in self.operators.values():
            if not o.producers:
                root_operators.append(o)

        visited_operators = OrderedDict()
        for o in root_operators:
            visited_operators = self.traverse(o, visited_operators)

        return visited_operators.values()

    def print_metrics(self):

        print('')

        operators = list(self.traverse_from_root())

        OpMetrics.print_metrics(operators)

        print('')

    def write_graph(self, dir_, name):

        graph = Graph(name)

        for o in self.operators.values():
            graph.add_operator(o)

        graph.write(dir_)
