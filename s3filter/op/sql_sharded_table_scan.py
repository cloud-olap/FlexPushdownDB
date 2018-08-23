from s3filter.op.sql_table_scan import SQLTableScanMetrics, SQLTableScan, is_header
from s3filter.op.operator_base import Operator, StartMessage, EvalMessage, EvaluatedMessage
from s3filter.op.message import StringMessage, TupleMessage
import pandas as pd
import time
import os


__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class SQLShardedTableScan(Operator):
    """Represents a table scan operator for one or more table shards which reads from an s3 table and emits tuples to
        consuming operators as they are received. Generally starting this operator is what begins a query.
        """

    def __init__(self, s3key, s3sql, use_pandas, secure, name, shards, shard_prefix, parallel_shards, query_plan,
                 log_enabled):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql
        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        """

        super(SQLShardedTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.secure = secure
        self.field_names = None

        self.use_pandas = use_pandas
        self.shard_prefix = shard_prefix
        self.shards = shards
        self.parallel_shards = parallel_shards
        self.query_plan = query_plan

        self.shard_scanner_ops = []

        for shard in shards:
            shard_key_name = self.get_part_key(shard)
            shard_table_scanner = self.query_plan.add_operator(
                SQLTableScan(shard_key_name, self.s3sql, self.use_pandas, self.secure,
                             "shard_table_scan_{}".format(shard), self.query_plan, self.log_enabled)
            )
            shard_table_scanner.connect(self)
            self.shard_scanner_ops.append(shard_table_scanner)

            if self.query_plan.is_async and self.parallel_shards:
                shard_table_scanner.init_async(self.query_plan.queue)

    def run(self):
        """Executes the query and begins emitting tuples.
        :return: None
        """
        self.do_run()

    def do_run(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        for op in self.shard_scanner_ops:
            if self.query_plan.is_async and self.parallel_shards:
                op.boot()
            op.start()

    def on_receive(self, messages, producer_name):
        """Handles the receipt of a message from a producer.

        :param messages: The received messages
        :param producer_name: The producer that emitted the message
        :return: None
        """
        for m in messages:
            if type(m) is StringMessage:
                self.s3sql = m.string_
                if self.async_:
                    self.query_plan.send(StartMessage(), self.name)
                else:
                    self.run()
            elif type(m) is TupleMessage:
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
            self.send(TupleMessage(tuple_), self.consumers)

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for a received pandas dataframe.

        :param df: The received dataframe
        :return: None
        """
        self.send(df, self.consumers)

    def complete(self):
        """
        When all shard scanners complete, the buffered tuples are passed to the next operators.
        :return:
        """

        super(SQLShardedTableScan, self).complete()

        self.op_metrics.timer_stop()

    def get_part_key(self, part):
        fname = os.path.basename(self.s3key)
        filename, ext = os.path.splitext(fname)
        return '{}/{}{}.{}'.format(self.shard_prefix, filename, ext, part)
