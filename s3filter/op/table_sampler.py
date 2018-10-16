"""
Creates and reads from local table index
"""
from s3filter.sql.index_builder import *
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator, StartMessage
from s3filter.op.message import StringMessage, TupleMessage
from random import randint
import pandas as pd
import time
import math


__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class TableRandomSampleGenerator(Operator):
    """Generates byte ranges for an s3 table random sample to be fetched by range table scan.
    """

    index_map = None
    index_size = None

    def __init__(self, s3key, sample_size, batch_size, use_pandas, name, query_plan, log_enabled):
        """Creates a random sample of a s3 table
        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        :param sample_size: the sample size
        :param batch_size: the number of consecutive records to retrieve per byte range in order to reduce the number
        of sampling http requests
        """

        super(TableRandomSampleGenerator, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.sample_size = sample_size
        self.batch_size = batch_size

        self.use_pandas = use_pandas
        self.query_plan = query_plan

        self.populate_index()

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

        self.send(self.get_random_ranges(), self.consumers)

        self.complete()

    def on_receive(self, messages, producer_name):
        pass

    def __on_receive_tuple(self, tuple_, producer_name):
        pass

    def __on_receive_dataframe(self, df, producer_name):
        pass

    def complete(self):
        super(TableRandomSampleGenerator, self).complete()
        self.op_metrics.timer_stop()

    def populate_index(self):
        if TableRandomSampleGenerator.index_map is None:
            TableRandomSampleGenerator.index_map = load_index(self.s3key, ['index'])
            TableRandomSampleGenerator.index_size = max(TableRandomSampleGenerator.index_map.keys())

    def get_random_ranges(self):
        rand_pos = [randint(0, TableRandomSampleGenerator.index_size)
                    for _ in range(math.ceil(self.sample_size / self.batch_size))]
        return get_sequential_records_byte_ranges(TableRandomSampleGenerator.index_map, rand_pos, self.batch_size)
