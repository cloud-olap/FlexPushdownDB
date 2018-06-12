# -*- coding: utf-8 -*-
"""Query plan graph support

"""

import pygraphviz

from s3filter.op.join import Join


class Graph(object):
    """Given a list of operators can create a graph showing how the operators are connected. Useful for reasoning
    about a query plan.

    """

    def __init__(self, name):
        self.name = name
        self.graph = pygraphviz.AGraph(directed=True, rankdir="BT")

    def add_operator(self, operator):
        """Adds the given operator to the graph

        :param operator:
        :return: None
        """

        self.graph.add_node(operator.name, label="{}({})"
                            .format(operator.__class__.__name__, operator.name), shape="box")

        if type(operator) is Join:
            self.graph.add_edge(operator.producers[0].name, operator.name, "left", label="left")
            self.graph.add_edge(operator.producers[1].name, operator.name, "right", label="right")
        else:
            for p in operator.producers:
                self.graph.add_edge(p.name, operator.name)

    def write(self, test_output_dir):
        """Writes the graph to an SVG file with the name of the graph.

        :return: None
        """

        self.graph.layout(prog='dot')
        self.graph.draw("{}/{}.svg".format(test_output_dir, self.name))
