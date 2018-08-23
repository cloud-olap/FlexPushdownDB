# -*- coding: utf-8 -*-
"""Query plan support

"""
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import traceback
import warnings
from collections import OrderedDict, deque
from logging import warning
from multiprocessing import Queue
import boto3
import networkx
from s3filter.util.constants import *
from boto3.session import Session
from botocore.config import Config

from s3filter.op.operator_base import OperatorCompletedMessage, EvaluatedMessage, EvalMessage, StopMessage
from s3filter.op.sql_table_scan import SQLTableScanMetrics
from s3filter.plan.graph import Graph
from s3filter.plan.op_metrics import OpMetrics
from s3filter.plan.cost_estimator import CostEstimator
from s3filter.util.timer import Timer


class QueryPlan(object):
    """Container for the operators in a query. Really a convenience class that allows the plan graph to be
    generated and the operator execution metrics.

    """

    def __init__(self, operators=None, is_async=False, buffer_size=1024):
        # type: (list, bool, int) -> None
        """

        :param operators:
        """

        self.__timer = Timer()
        self.total_elapsed_time = 0.0
        self.completion_counter = 0

        if operators is None:
            self.operators = OrderedDict()
        else:
            self.operators = OrderedDict()
            for o in operators:
                self.operators[o.name] = o

        self.is_async = is_async
        self.queue = Queue()

        self.buffer_size = buffer_size

    def add_operator(self, operator):
        """Adds the operator to the list of operators in the plan. This method ensures the operators are sorted by
        operator name - which is important only for retrieving the root operators.

        :param operator:
        :return:
        """

        if operator.name in self.operators:
            raise Exception("Cannot add multiple operators with same name. "
                            "Operator '{}' already added".format(operator.name))

        operator.set_query_plan(self)
        operator.set_buffer_size(self.buffer_size)

        self.operators[operator.name] = operator
        sorted_operators = sorted(self.operators.values(), key=lambda o: o.name)
        self.operators = OrderedDict((o.name, o) for o in sorted_operators)

        return operator

    def traverse_depth_first(self, from_operator, visited_operators):
        """Returns a sorted list of the operators according to where they appear in the query plan using depth first
        traversal, starting from the supplied operator.

        :param from_operator: First operator to start from
        :param visited_operators: Operators already traversed
        :return: Sorted list of operators
        """

        if from_operator.name not in visited_operators:
            visited_operators[from_operator.name] = from_operator

        for c in from_operator.consumers:
            visited_operators = self.traverse_depth_first(c, visited_operators)

        return visited_operators

    @staticmethod
    def traverse_breadth_first(from_operators):
        """Returns a sorted list of the operators according to where they appear in the query plan using breadth
        first traversal, starting from the supplied operators.

        :param from_operators: The operators to start from
        :return: Sorted list of operators
        """

        visited_operators = OrderedDict()
        operators_list = deque(from_operators)
        while operators_list:
            o = operators_list.popleft()
            if o.name not in visited_operators:
                visited_operators[o.name] = o

                # Add the consumers except any visited
                for c in o.consumers:
                    if c.name not in visited_operators:
                        operators_list.append(c)

        return visited_operators.values()

    def traverse_topological(self):
        """This method sorts the operators topgraphically and by operator name. This is to ensure
        that the operator metrics are written in the order that most closely aligns with the plan. Note that this
        is not the same order that operators execute in, which may be counterintuitive - e.g. A top operator can
        complete before a scan.

        :return: Sorted list of operators
        """

        g = networkx.DiGraph()

        for o in self.operators.values():
            g.add_node(o.name, value=o)

        for o in self.operators.values():
            for c in o.consumers:
                g.add_edge(o.name, c.name)

        sorted_operators = list(map(lambda name: g.nodes[name]['value'], networkx.topological_sort(g)))

        return sorted_operators

    def traverse_depth_first_from_root(self):
        """Returns a sorted list of the operators according to where they appear in the query plan, starting from
        the root operators.

        :return: Sorted list of operators
        """

        # Find the operators at the root of the graph, they will have no producers
        root_operators = self.find_root_operators()

        visited_operators = OrderedDict()
        for o in root_operators:
            visited_operators = self.traverse_depth_first(o, visited_operators)

        return visited_operators.values()

    def traverse_breadth_first_from_root(self):
        """Returns a sorted list of the operators according to where they appear in the query plan, starting from
        the root operators.

        :return: Sorted list of operators
        """

        # Find the operators at the root of the graph, they will have no producers
        root_operators = self.find_root_operators()

        visited_operators = self.traverse_breadth_first(root_operators)

        return visited_operators

    def traverse_topological_from_root(self):
        visited_operators = self.traverse_topological()
        return visited_operators

    def find_root_operators(self):
        root_operators = []
        for o in self.operators.values():
            if not o.producers:
                root_operators.append(o)
        return root_operators

    def send(self, message, operator_name):
        o = self.operators[operator_name]
        o.queue.put(message)

    def print_metrics(self):

        print("")
        print("Metrics")
        print("-------")
        print("")

        print("Plan")
        print("----")

        print("buffer_size: {}".format(self.buffer_size))
        print("is_parallel: {}".format(self.is_async))
        print("total_elapsed_time: {}".format(round(self.total_elapsed_time, 5)))
        self.print_cost_metrics()

        print("")
        print("Operators")
        print("---------")

        OpMetrics.print_metrics(self.traverse_topological_from_root())

        # self.assert_operator_time_equals_plan_time()

        print("")

    def print_cost_metrics(self):
        cost, bytes_scanned, bytes_returned, rows = self.cost()
        computation_cost = self.computation_cost()
        data_cost = self.data_cost()[0]

        print("total_scanned_bytes: {} MB".format(bytes_scanned * BYTE_TO_MB))
        print("total_returned_bytes: {} MB".format(bytes_returned * BYTE_TO_MB))
        print("total_returned_rows: {}".format(rows))
        print("computation_cost: ${0:.10f}".format(computation_cost))
        print("data_cost: ${0:.10f}".format(data_cost))
        print("total_cost: ${0:.10f}".format(cost))

    def assert_operator_time_equals_plan_time(self):
        """Sanity check to make sure cumulative operator exec time approximately equals total plan exec time. We use a
        margin of error of 0.1 seconds to account for any time not captured during a context switch.

        Disabled when -O flag is set

        :return: None
        """

        if __debug__:

            margin = 0.1

            cum_op_time = 0.0
            for o in self.operators.values():
                cum_op_time += o.op_metrics.elapsed_time()

            upper_time_bound = self.total_elapsed_time * (1.0 + margin)
            lower_time_bound = self.total_elapsed_time * (1.0 - margin)

            assert lower_time_bound <= cum_op_time <= upper_time_bound, \
                "Cumulative operator execution time {} is not within {} seconds of total plan execution time {}".format(
                    margin, cum_op_time, self.total_elapsed_time)

    def write_graph(self, dir_, name):

        graph = Graph(name)

        for o in self.operators.values():
            graph.add_operator(o)

        graph.write(dir_)

    def execute(self):

        if self.is_async:
            map(lambda o: o.init_async(self.queue), self.operators.values())
            map(lambda o: o.boot(), self.operators.values())

        # Find the root operators
        root_operators = self.find_root_operators()

        self.__timer.start()

        # Start the root operators
        for ro in root_operators:
            ro.start()

        # Things become event driven from here, because we may be running async, so we essentially go
        # into an event loop with a state machine to keep track of where we are

        if self.is_async:
            operator_completions = {k: False for k, v in self.operators.items()}
            while not all(operator_completions.values()):
                completed_message = self.listen(OperatorCompletedMessage)
                operator_completions[completed_message.name] = True

        self.__timer.stop()

        self.total_elapsed_time = self.__timer.elapsed()

        if self.is_async:
            # retrieve metrics of all operators. This is important in async queries since the operator runs in a
            # different process.
            operators = self.traverse_topological_from_root()
            for o in operators:
                o.queue.put(EvalMessage("self.op_metrics"))
                o.op_metrics = self.listen(EvaluatedMessage).val

            map(lambda op: op.set_completed(True), self.operators.values())

    def listen(self, message_type):
        try:
            while True:
                item = self.queue.get()
                # item = cPickle.loads(pickled_item)
                # print(item)

                if type(item) == message_type:
                    return item
                else:
                    # Not the message being listened for, warn and skip
                    # This isn't exceptional, but the listener should be made aware that there are messages arriving that
                    # are being ignored
                    warning("While listening for message type {} received message type {} with contents {}".format(message_type, type(item), item))

        except BaseException as e:
            tb = traceback.format_exc(e)
            print(tb)

    def stop(self):
        if self.is_async:
            map(lambda o: o.queue.put(StopMessage()), self.operators.values())
            self.join()
            map(lambda o: o.queue.close(), self.operators.values())
        else:
            pass

    def join(self):
        return map(lambda o: o.join(), self.operators.values())

    def cost(self):
        """
        calculates the overall query cost when runs on S3 by combining the cost of all scan operators in the query
        plus the computation cost based on the EC2 instance type and the query running time
        :return: the estimated cost of the whole query
        """
        total_data_cost, total_scanned_bytes, total_returned_bytes, total_rows = self.data_cost()
        total_compute_cost = self.computation_cost()

        return total_compute_cost + total_data_cost, total_scanned_bytes, total_returned_bytes, total_rows

    def data_cost(self, ec2_region=None):
        """
        calculates the estimated query cost when runs on S3 by combining the cost of all scan operators in the query
        :return: the estimated cost of the whole query
        """
        scan_operators = [op for op in self.operators.values() if hasattr(op.op_metrics, "cost")]

        total_data_cost = 0.0
        total_scanned_bytes = 0
        total_returned_bytes = 0
        total_rows = 0

        for op in scan_operators:
            if op.is_completed():
                total_data_cost += op.op_metrics.data_cost(ec2_region)
                total_returned_bytes += op.op_metrics.bytes_returned
                total_scanned_bytes += op.op_metrics.bytes_scanned
                total_rows += op.op_metrics.rows_returned
            else:
                raise Exception("Can't calculate query cost while one or more scan operators {} are still executing"
                                .format(op.name))

        return total_data_cost, total_scanned_bytes, total_returned_bytes, total_rows

    def computation_cost(self, ec2_instance_type=None, os_type=None):
        """
        calculates the estimated computation cost of the query
        :return: computation cost
        """
        scan_op = None

        for op in self.operators.values():
            if hasattr(op.op_metrics, "cost"):
                scan_op = op
                break

        return scan_op.op_metrics.computation_cost(self.total_elapsed_time, ec2_instance_type, os_type)

    def estimated_cost_for_config(self, ec2_region, ec2_instance_type=None, os_type=None):
        """
        estimate the hypothetical cost if the query runs on EC2 instance in a different region
        :param ec2_region: AWSRegion to calculate the cost for
        :param ec2_instance_type: the code of an EC2 instance as defined by AWS (r4.8xlarge)
        :param os_type: the name of the os running on the host machine (Linux, Windows ... etc)
        :return: the hypothetical estimated cost for the provided region, instance type and os
        """
        return self.computation_cost(ec2_instance_type, os_type) + self.data_cost(ec2_region)
