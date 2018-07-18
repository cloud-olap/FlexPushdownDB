# -*- coding: utf-8 -*-
"""Query plan support

"""
from collections import OrderedDict, deque
from multiprocessing import Queue

import dill
import networkx

from s3filter.op.operator_base import OperatorCompletedMessage, EvaluatedMessage, EvalMessage, StopMessage
from s3filter.plan.graph import Graph
from s3filter.plan.op_metrics import OpMetrics
from s3filter.util.timer import Timer


class QueryPlan(object):
    """Container for the operators in a query. Really a convenience class that allows the plan graph to be
    generated and the operator execution metrics.

    """

    def __init__(self, operators=None, is_streamed=True, is_async=False):
        # type: (list, bool, bool) -> None
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

        self.is_streamed = is_streamed

        self.is_async = is_async
        self.queue = Queue()

    def add_operator(self, operator):
        """Adds the operator to the list of operators in the plan. This method ensures the operators are sorted by
        operator name - which is important only for retrieving the root operators.

        :param operator:
        :return:
        """

        if operator.name in self.operators:
            raise Exception("Cannot add multiple operators with same name. "
                            "Operator '{}' already added".format(operator.name))

        operator.set_streamed(self.is_streamed)

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

    def print_metrics(self):

        print("")
        print("Metrics")
        print("-------")
        print("")

        print("Plan")
        print("----")

        print("is_streamed: {}".format(self.is_streamed))
        print("is_async: {}".format(self.is_async))
        print("total_elapsed_time: {}".format(round(self.total_elapsed_time, 5)))

        print("")
        print("Operators")
        print("---------")

        operators = self.traverse_topological_from_root()

        if self.is_async:
            for o in operators:
                o.queue.put(dill.dumps(EvalMessage("self.op_metrics")))
                o.op_metrics = self.listen(EvaluatedMessage).val

        OpMetrics.print_metrics(operators)

        self.assert_operator_time_equals_plan_time()

        print("")

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

        # Set async
        if self.is_async:
            for o in self.operators.values():
                o.init_async(self.queue)

        # Find the root operators
        root_operators = self.find_root_operators()

        self.__timer.start()

        # Start the root operators
        for o in root_operators:
            o.start()

        # Things become event driven from here, because we may be running async, so we essentially go
        # into an event loop with a state machine to keep track of where we are

        if self.is_async:
            operator_completions = {k: False for k, v in self.operators.items()}
            while not all(operator_completions.values()):
                completed_message = self.listen(OperatorCompletedMessage)
                operator_completions[completed_message.name] = True

        self.__timer.stop()

        self.total_elapsed_time = self.__timer.elapsed()

    def listen(self, message_type):

        while True:
            pickled_item = self.queue.get()
            item = dill.loads(pickled_item)
            # print(item)

            if type(item) == message_type:
                return item
            else:
                raise Exception("Unrecognized message {}".format(item))

    def stop(self):
        map(lambda o: o.queue.put(dill.dumps(StopMessage())), self.operators.values())
        self.join()

    def join(self):
        return map(lambda o: o.join(), self.operators.values())
