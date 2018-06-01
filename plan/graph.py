import pygraphviz

from op.join import Join


class Graph(object):

    def __init__(self, name):
        self.name = name
        self.graph = pygraphviz.AGraph(directed=True, rankdir="BT")

    def add_operator(self, operator):

        self.graph.add_node(operator.name, label="{}({})".format(operator.__class__.__name__, operator.name), shape="box")

        if type(operator) is Join:
            self.graph.add_edge(operator.producers[0].name, operator.name, "left", label="left")
            self.graph.add_edge(operator.producers[1].name, operator.name, "right", label="right")
        else:
            # for c in operator.consumers:
            #     self.graph.add_edge(operator.name, c.name)

            for p in operator.producers:
                self.graph.add_edge(p.name, operator.name)

    def write(self):
        self.graph.layout(prog='dot')
        self.graph.draw("{}.svg".format(self.name))