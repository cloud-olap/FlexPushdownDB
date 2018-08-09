# -*- coding: utf-8 -*-
"""Query plan graph support

"""

import pygraphviz

from s3filter.op.hash_join import HashJoin
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.map import Map
from s3filter.op.nested_loop_join import NestedLoopJoin
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse


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


        # Nodes
        if type(operator) is Map:
            self.graph.add_node(operator.name,
                                label="{}\n({}, '{}')".format(operator.__class__.__name__, operator.name,
                                                              operator.map_field_name),
                                shape="box")
        elif type(operator) is SQLTableScan or type(operator) is SQLTableScanBloomUse:
            self.graph.add_node(operator.name,
                                label="{}\n({})".format(operator.__class__.__name__, operator.name),
                                tooltip="key: '{}'&#10;sql: '{}'".format(operator.s3key, operator.s3sql),
                                shape="box")
        else:
            self.graph.add_node(operator.name, label="{}\n({})"
                                .format(operator.__class__.__name__, operator.name), shape="box")

        # Edges
        if type(operator) is NestedLoopJoin or type(operator) is HashJoin:
            self.graph.add_edge(operator.producers[0].name, operator.name, label="left")
            self.graph.add_edge(operator.producers[1].name, operator.name, label="right")
        elif type(operator) is SQLTableScanBloomUse:
            for p in operator.producers:
                self.graph.add_edge(p.name, operator.name, label="bloomfilter")
        elif type(operator) is HashJoinProbe:
            for p in operator.producers:
                if p.name in operator.tuple_producers:
                    self.graph.add_edge(p.name, operator.name, label="tuples")
                else:
                    self.graph.add_edge(p.name, operator.name, label="hashtable")
        else:
            for p in operator.producers:
                self.graph.add_edge(p.name, operator.name)

    def write(self, test_output_dir):
        """Writes the graph to an SVG file with the name of the graph.

        :return: None
        """

        self.graph.layout(prog='dot')
        self.graph.draw("{}/{}.svg".format(test_output_dir, self.name))
