

from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.op.message import TupleMessage
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScanMetrics, is_header
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_sharded_table_scan import SQLShardedTableScan
from s3filter.op.collate import Collate
from s3filter.util.heap import MaxHeap, MinHeap, HeapTuple
import time
import pandas as pd

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class Top(Operator):
    """
    Implementation of the TopK operator based on user selected sorting criteria and expressions. This operator
    consumes tuples from producer operators and uses a heap to keep track of the top k tuples.
    """

    def __init__(self, max_tuples, sort_expression, use_pandas, name, query_plan, log_enabled):
        """Creates a new Sort operator.

                :param sort_expression: The sort expression to apply to the tuples
                :param name: The name of the operator
                :param log_enabled: Whether logging is enabled
                """

        super(Top, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.sort_expression = sort_expression
        self.use_pandas = use_pandas

        if not self.use_pandas:
            if sort_expression.sort_order == 'ASC':
                self.heap = MaxHeap(max_tuples)
            else:
                self.heap = MinHeap(max_tuples)
        else:
            self.global_topk_df = pd.DataFrame()

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
                self.__on_receive_tuple(m.tuple_, _producer)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m, _producer)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
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

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for a received pandas dataframe. With every dataframe received, the topk tuples are mantained
        and merged

        :param df: The received dataframe
        :return: None
        """
        df[[self.sort_expression.col_index]] = df[[self.sort_expression.col_index]]\
                                                    .astype(self.sort_expression.col_type.__name__)
        if self.sort_expression.sort_order == 'ASC':
            topk_df = df.nsmallest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
            self.global_topk_df = self.global_topk_df.append(topk_df).nsmallest(self.max_tuples,
                                                                                self.sort_expression.col_index)\
                                                                                .head(self.max_tuples)
        elif self.sort_expression.sort_order == 'DESC':
            topk_df = df.nlargest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
            self.global_topk_df = self.global_topk_df.append(topk_df).nlargest(self.max_tuples,
                                                                               self.sort_expression.col_index) \
                                                                               .head(self.max_tuples)

    def complete(self):
        """
        When all producers complete, the topk tuples are passed to the next operators.
        :return:
        """

        if not self.use_pandas:

            for t in self.heap.get_topk(self.max_tuples, sort=True):
                if self.is_completed():
                    break
                self.send(TupleMessage(t.tuple), self.consumers)

            self.heap.clear()
        else:
            if self.sort_expression.sort_order == 'ASC':
                self.global_topk_df = self.global_topk_df.nsmallest(self.max_tuples, self.sort_expression.col_index)\
                    .head(self.max_tuples)
            elif self.sort_expression.sort_order == 'DESC':
                self.global_topk_df = self.global_topk_df.nlargest(self.max_tuples, self.sort_expression.col_index)\
                    .head(self.max_tuples)

            self.send(self.global_topk_df, self.consumers)

        super(Top, self).complete()
        self.op_metrics.timer_stop()


class TopKTableScanMetrics(SQLTableScanMetrics):

    def __init__(self):
        super(TopKTableScanMetrics, self).__init__()

        self.sampling_time = 0.0
        self.sampling_data_cost = 0.0
        self.sampling_computation_cost = 0.0

    def cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """
        return self.sampling_computation_cost + self.sampling_data_cost + \
               super(TopKTableScanMetrics, self).cost()

    def computation_cost(self, running_time=None, ec2_instance_type=None, os_type=None):
        """
        Estimates the computation cost of the scan operation based on EC2 pricing in the following page:
        <https://aws.amazon.com/ec2/pricing/on-demand/>
        :param running_time: the query running time
        :param ec2_instance_type: the type of EC2 instance as defined by AWS
        :param os_type: the name of the os running on the host machine (Linux, Windows ... etc)
        :return: The estimated computation cost of the table scan operation given the query running time
        """
        return self.sampling_computation_cost + \
               super(TopKTableScanMetrics, self).computation_cost(running_time, ec2_instance_type, os_type)

    def data_cost(self, ec2_region=None):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated data transfer cost of the table scan operation
        """
        return self.sampling_data_cost + \
               super(TopKTableScanMetrics, self).data_cost(ec2_region)


class TopKTableScan(Operator):
    """
    This operator scans a table and emits the k topmost tuples based on a user-defined ranking criteria
    """

    def __init__(self, s3key, s3sql, use_pandas, use_native, max_tuples, k_scale, sort_expression, shards, parallel_shards,
                 shards_prefix, processes, name, query_plan, log_enabled):
        """
        Creates a table scan operator that emits only the k topmost tuples from the table
        :param s3key: the table's s3 object key
        :param s3sql: the select statement to apply on the table
        :param use_pandas: use pandas DataFrames as the tuples engine
        :param use_native: use native C++ cursor
        :param max_tuples: the maximum number of tuples to return (K)
        :param k_scale: sampling scale factor to retrieve more sampling tuples (s * K)
        :param sort_expression: the expression on which the table tuples are sorted in order to get the top k
        :param name: the operator name
        :param query_plan: the query plan in which this operator is part of
        :param log_enabled: enable logging
        """
        super(TopKTableScan, self).__init__(name, TopKTableScanMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.query_plan = query_plan
        self.use_pandas = use_pandas
        self.use_native = use_native

        self.field_names = None

        self.shards = shards
        self.parallel_shards = parallel_shards
        self.processes = processes

        self.max_tuples = max_tuples
        self.sort_expression = sort_expression

        if not self.use_pandas:
            if sort_expression.sort_order == 'ASC':
                self.heap = MaxHeap(max_tuples)
            else:
                self.heap = MinHeap(max_tuples)
        else:
            self.global_topk_df = pd.DataFrame()

        self.local_operators = []

        # TODO: The current sampling method has a flaw. If the maximum value happened to be among the sample without
        # duplicates, scanning table shards won't return any tuples and the sample will be considered the topk while
        # this is not necessarily the correct topk. A suggestion is to take a random step back on the cutting threshold
        # to make sure the max value is not used as the threshold
        self.sample_tuples, self.sample_op, q_plan = TopKTableScan.sample_table(self.s3key, k_scale * self.max_tuples,
                                                                        self.sort_expression)
        # self.field_names = self.sample_tuples[0]
        self.msv, comp_op = self.get_most_significant_value(self.sample_tuples[1:])

        self.op_metrics.sampling_data_cost = q_plan.data_cost()[0]
        self.op_metrics.sampling_computation_cost = q_plan.computation_cost()
        self.op_metrics.sampling_time = q_plan.total_elapsed_time

        filtered_sql = "{} WHERE CAST({} AS {}) {} {};".format(self.s3sql.rstrip(';'), self.sort_expression.col_name,
                                                               self.sort_expression.col_type.__name__, comp_op, self.msv)

        if self.processes == 1:
            ts = SQLTableScan(self.s3key, filtered_sql, self.use_pandas, True, self.use_native,
                              'baseline_topk_table_scan',
                              self.query_plan, self.log_enabled)
            ts.connect(self)
            self.local_operators.append(ts)
        else:
            for process in range(self.processes):
                proc_parts = [x for x in range(1, self.shards + 1) if x % self.processes == process]
                pc = self.query_plan.add_operator(SQLShardedTableScan(self.s3key, filtered_sql, self.use_pandas, True,
                                                                      self.use_native,
                                                                      "topk_table_scan_parts_{}".format(proc_parts),
                                                                      proc_parts, shards_prefix,
                                                                      self.parallel_shards,
                                                                      self.query_plan, self.log_enabled))
                proc_top = self.query_plan.add_operator(Top(self.max_tuples, self.sort_expression, True,
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
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
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

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for a received pandas dataframe. With every dataframe received, the topk tuples are mantained
        and merged

        :param df: The received dataframe
        :return: None
        """
        df[[self.sort_expression.col_index]] = df[[self.sort_expression.col_index]] \
                                                    .astype(self.sort_expression.col_type.__name__)

        if self.sort_expression.sort_order == 'ASC':
            topk_df = df.nsmallest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
            self.global_topk_df = self.global_topk_df.append(topk_df).nsmallest(self.max_tuples,
                                                                                self.sort_expression.col_index) \
                                                                                .head(self.max_tuples)
        elif self.sort_expression.sort_order == 'DESC':
            topk_df = df.nlargest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
            self.global_topk_df = self.global_topk_df.append(topk_df).nlargest(self.max_tuples,
                                                                               self.sort_expression.col_index) \
                                                                                .head(self.max_tuples)

    def complete(self):
        """
        When all producers complete, the topk tuples are passed to the next operators.
        :return:
        """
        if not self.use_pandas:
            # if the number of tuples beyond the cut-off value is less than k, we need to some tuples from
            # the sample set
            if len(self.heap) < self.max_tuples:
                self.on_receive([TupleMessage(t) for t in self.sample_tuples], self.name)

            for t in self.heap.get_topk(self.max_tuples, sort=True):
                if self.is_completed():
                    break
                self.send(TupleMessage(t.tuple), self.consumers)

            self.heap.clear()
        else:
            if self.sort_expression.sort_order == 'ASC':
                self.global_topk_df = self.global_topk_df.nsmallest(self.max_tuples, self.sort_expression.col_index) \
                    .head(self.max_tuples)
            elif self.sort_expression.sort_order == 'DESC':
                self.global_topk_df = self.global_topk_df.nlargest(self.max_tuples, self.sort_expression.col_index) \
                    .head(self.max_tuples)

            self.send(self.global_topk_df, self.consumers)

        super(TopKTableScan, self).complete()

        self.op_metrics.timer_stop()

    @staticmethod
    def sample_table(s3key, k, sort_exp):
        """
        Given a table name, return a random sample of records. Currently, the returned records are the first k tuples
        :param s3key: the s3 object name
        :param k: the number of tuples to return (the number added to the SQL Limit clause
        :param sort_exp: the sort expression in which the topk is chosen upon
        :return: the list of selected keys from the first k tuples in the table
        """
        projection = "CAST({} as {})".format(sort_exp.col_name, sort_exp.col_type.__name__)

        sql = "SELECT {} FROM S3Object LIMIT {}".format(projection, k)
        q_plan = QueryPlan(is_async=False)
        select_op = q_plan.add_operator(SQLTableScan(s3key, sql, True, True, False, "sample_{}_scan".format(s3key),
                                                  q_plan, False))
        collate = q_plan.add_operator(Collate("sample_{}_collate".format(s3key), q_plan, False))
        select_op.connect(collate)

        q_plan.execute()

        q_plan.print_metrics()

        return collate.tuples(), select_op, q_plan

    def get_most_significant_value(self, tuples):
        """
        Returns the cut-off value from the passed tuples in order to retrieve only tuples beyond this point
        :param tuples: the tuples representing the table sample of size k
        :return: the cut-off value
        """
        sort_exp = self.sort_expression
        # idx = self.field_names.index(sort_exp.col_index)

        if sort_exp.sort_order == "ASC":
            return min([sort_exp.col_type(t[0]) for t in tuples]), '<='
        elif sort_exp.sort_order == "DESC":
            return max([sort_exp.col_type(t[0]) for t in tuples]), '>='
