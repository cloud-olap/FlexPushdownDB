

from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScanMetrics
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_scan import SQLTableScan, SQLShardedTableScan
from s3filter.op.collate import Collate
from s3filter.util.heap import MaxHeap, MinHeap, HeapTuple
import time

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class Top(Operator):
    """
    Implementation of the TopK operator based on user selected sorting criteria and expressions. This operator
    consumes tuples from producer operators and uses a heap to keep track of the top k tuples.
    """

    def __init__(self, max_tuples, sort_expression, name, query_plan, log_enabled):
        """Creates a new Sort operator.

                :param sort_expression: The sort expression to apply to the tuples
                :param name: The name of the operator
                :param log_enabled: Whether logging is enabled
                """

        super(Top, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.sort_expression = sort_expression

        if sort_expression.sort_order == 'ASC':
            self.heap = MaxHeap(max_tuples)
        else:
            self.heap = MinHeap(max_tuples)

        self.max_tuples = max_tuples

        self.field_names = None

    def on_receive(self, ms, _producer):
        """Handles the receipt of a message from a producer.

        :param ms: The received messages
        :param _producer: The producer that emitted the message
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        """Handles the receipt of a tuple. When a tuple is received, it's compared with the top of the heap to decide
        on adding to the heap or skip it. Given this process, it is guaranteed to keep the k topmost tuples given some
        defined comparison criteria

        :param tuple_: The received tuple
        :return: None
        """
        if not self.field_names:
            # Collect and send field names through
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        elif not is_header(tuple_):
            # Store the tuple in the sorted heap
            ht = HeapTuple(tuple_, self.field_names, self.sort_expression)
            self.heap.push(ht)

    def complete(self):
        """
        When all producers complete, the topk tuples are passed to the next operators.
        :return:
        """
        for t in self.heap.get_topk(self.max_tuples, sort=True):

            if self.is_completed():
                break

            self.send(TupleMessage(t.tuple), self.consumers)

        self.heap.clear()

        super(Top, self).complete()
        self.op_metrics.timer_stop()


class TopKTableScan(Operator):
    """
    This operator scans a table and emits the k topmost tuples based on a user-defined ranking criteria
    """

    def __init__(self, s3key, s3sql, max_tuples, k_scale, sort_expression, shards, processes, name, query_plan, log_enabled):
        """
        Creates a table scan operator that emits only the k topmost tuples from the table
        :param s3key: the table's s3 object key
        :param s3sql: the select statement to apply on the table
        :param max_tuples: the maximum number of tuples to return (K)
        :param k_scale: sampling scale factor to retrieve more sampling tuples (s * K)
        :param sort_expression: the expression on which the table tuples are sorted in order to get the top k
        :param name: the operator name
        :param query_plan: the query plan in which this operator is part of
        :param log_enabled: enable logging
        """
        super(TopKTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3 = query_plan.s3

        self.s3key = s3key
        self.s3sql = s3sql

        self.field_names = None

        self.shards = shards
        self.processes = processes

        self.max_tuples = max_tuples
        self.sort_expression = sort_expression

        if sort_expression.sort_order == 'ASC':
            self.heap = MaxHeap(max_tuples)
        else:
            self.heap = MinHeap(max_tuples)

        self.local_operators = []

        # TODO: The current sampling method has a flaw. If the maximum value happened to be among the sample without
        # duplicates, scanning table shards won't return any tuples and the sample will be considered the topk while
        # this is not necessarily the correct topk. A suggestion is to take a random step back on the cutting threshold
        # to make sure the max value is not used as the threshold
        self.sample_tuples, self.sample_op = TopKTableScan.sample_table(self.s3key, k_scale * self.max_tuples)
        self.field_names = self.sample_tuples[0]
        msv, comp_op = self.get_most_significant_value(self.sample_tuples[1:])

        filtered_sql = "{} WHERE CAST({} AS {}) {} {};".format(self.s3sql.rstrip(';'), self.sort_expression.col_name,
                                                               self.sort_expression.col_type.__name__, comp_op, msv)

        if self.processes == 1:
            ts = SQLTableScan(self.s3key, filtered_sql, 'baseline_topk_table_scan', self.query_plan, self.log_enabled)
            ts.connect(self)
            self.local_operators.append(ts)
        for process in range(self.processes):
            proc_parts = [x for x in range(self.shards) if x % self.processes == process]
            pc = self.query_plan.add_operator(SQLShardedTableScan(self.s3key, filtered_sql,
                                                                  "topk_table_scan_parts_{}".format(proc_parts),
                                                                  proc_parts,
                                                                  self.query_plan, self.log_enabled))
            proc_top = self.query_plan.add_operator(Top(self.max_tuples, self.sort_expression,
                                                        "top_parts_{}".format(proc_parts), self.query_plan,
                                                        self.log_enabled))
            pc.connect(proc_top)
            proc_top.connect(self)

            if self.query_plan.is_async:
                pc.init_async(self.query_plan.queue)
                proc_top.init_async(self.query_plan.queue)

            self.local_operators.append(pc)
            self.local_operators.append(proc_top)

    def run(self):
        """
        starts the topk query execution to do the following. First, select randomly the first k tuples from
        the designated table. Then, retrieve all the tuples larger/smaller than the max/min of the retrieved tuples set
        to filter them and get the global k topmost tuples. This reduces the search space by taking a random sample
        from the table to start with
        :return:
        """
        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        for op in self.local_operators:
            if self.query_plan.is_async:
                op.boot()
            op.start()

    def on_receive(self, messages, producer_name):
        """Handles the receipt of a message from a producer.

        :param messages: The received messages
        :param producer_name: The producer that emitted the message
        :return: None
        """
        for m in messages:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        """Handles the receipt of a tuple. When a tuple is received, it's compared with the top of the heap to decide
        on adding to the heap or skip it. Given this process, it is guaranteed to keep the k topmost tuples given some
        defined comparison criteria

        :param tuple_: The received tuple
        :return: None
        """
        if not self.field_names:
            # Collect and send field names through
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        elif not is_header(tuple_):
            # Store the tuple in the sorted heap
            ht = HeapTuple(tuple_, self.field_names, self.sort_expression)
            self.heap.push(ht)

    def complete(self):
        """
        When all producers complete, the topk tuples are passed to the next operators.
        :return:
        """
        # if the number of tuples beyond the cut-off value is less than k, we need to some tuples from
        # the sample set
        if len(self.heap) < self.max_tuples:
            self.on_receive([TupleMessage(t) for t in self.sample_tuples], self.name)

        for t in self.heap.get_topk(self.max_tuples, sort=True):

            if self.is_completed():
                break

            self.send(TupleMessage(t.tuple), self.consumers)

        self.heap.clear()

        super(TopKTableScan, self).complete()

        self.op_metrics.timer_stop()

    @staticmethod
    def sample_table(s3key, k, keys=None):
        """
        Given a table name, return a random sample of records. Currently, the returned records are the first k tuples
        :param s3key: the s3 object name
        :param k: the number of tuples to return (the number added to the SQL Limit clause
        :param keys: the keys on which table sorting will be applied
        :return: the list of selected keys from the first k tuples in the table
        """
        if keys is None:
            keys = "*"

        sql = "SELECT {} FROM S3Object LIMIT {}".format(", ".join(keys), k)
        q_plan = QueryPlan(is_async=False)
        select = q_plan.add_operator(SQLTableScan(s3key, sql, "sample_{}_scan".format(s3key), q_plan, False))
        collate = q_plan.add_operator(Collate("sample_{}_collate".format(s3key), q_plan, False))
        select.connect(collate)

        q_plan.execute()

        q_plan.print_metrics()

        return collate.tuples(), select

    def get_most_significant_value(self, tuples):
        """
        Returns the cut-off value from the passed tuples in order to retrieve only tuples beyond this point
        :param tuples: the tuples representing the table sample of size k
        :return: the cut-off value
        """
        sort_exp = self.sort_expression
        idx = self.field_names.index(sort_exp.col_index)

        if sort_exp.sort_order == "ASC":
            return min([sort_exp.col_type(t[idx]) for t in tuples]), '<='
        elif sort_exp.sort_order == "DESC":
            return max([sort_exp.col_type(t[idx]) for t in tuples]), '>='


def is_header(tuple_):
    return all([type(field) == str and field.startswith('_') for field in tuple_])
