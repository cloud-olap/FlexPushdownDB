# -*- coding: utf-8 -*-
"""Query plan graph support

"""

import pygraphviz

from s3filter.op.bloom_create import BloomCreate
from s3filter.op.hash_join import HashJoin
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.map import Map
from s3filter.op.nested_loop_join import NestedLoopJoin
from s3filter.op.project import Project
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
                                label="{}\n{}\n({})".format(operator.__class__.__name__, operator.name,
                                                              operator.map_field_name),
                                shape="box")
        elif type(operator) is SQLTableScan:
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({})".format(operator.__class__.__name__, operator.name, operator.s3key),
                                tooltip="sql: '{}'&#10;".format(operator.s3sql),
                                shape="box")
        elif isinstance(operator, SQLTableScanBloomUse):
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({}, {})".format(operator.__class__.__name__, operator.name,
                                                                operator.s3key, operator.get_bloom_filter_field_name()),
                                tooltip="sql: '{}'&#10;".format(operator.s3sql),
                                shape="box")
        elif type(operator) is BloomCreate:
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({})".format(operator.__class__.__name__, operator.name,
                                                            operator.bloom_field_name),
                                shape="box")
        elif type(operator) is Project:
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({})".format(operator.__class__.__name__, operator.name,
                                                            ', '.join([pe.new_field_name for pe in operator.project_exprs])),
                                shape="box")
        elif type(operator) is HashJoinBuild:
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({})".format(operator.__class__.__name__, operator.name,
                                                            operator.key),
                                shape="box")
        elif type(operator) is HashJoinProbe:
            self.graph.add_node(operator.name,
                                label="{}\n{}\n({}, {})".format(operator.__class__.__name__, operator.name,
                                                            operator.join_expr.l_field, operator.join_expr.r_field),
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

        # NOTE: This is taking a long time with the large query plans, uncomment it to see the graph

        # self.graph.layout(prog='dot')
        # self.graph.draw("{}/{}.svg".format(test_output_dir, self.name))
