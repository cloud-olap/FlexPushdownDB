"""
Creates and reads from local table index
"""
import math
import time
from random import randint

from s3filter.op.message import StringMessage, DataFrameMessage
from s3filter.op.operator_base import Operator
from s3filter.plan.op_metrics import OpMetrics
from s3filter.sql.sample_index.s3_index_builder import *
from s3filter.sql.sample_index.memory_index_builder import MemoryIndexHandler

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class TableRandomSampleGenerator(Operator):
    """Generates byte ranges for an s3 table random sample to be fetched by range table scan.
    """

    def __init__(self, s3key, sample_size, batch_size, name, query_plan, log_enabled):
        """Creates a random sample of a s3 table
        :param s3key: The object key to select against
        :param sample_size: the sample size
        :param batch_size: the number of consecutive records to retrieve per byte range in order to reduce the number
        of sampling http requests
        """

        super(TableRandomSampleGenerator, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.sample_size = sample_size

        self.batch_size = batch_size if batch_size <= (BLOCK_SIZE / 2) else BLOCK_SIZE / 2

        # self.index_mng = IndexHandler(self.s3key)
        self.index_mng = MemoryIndexHandler(self.s3key)

    def run(self):
        """start generating random ranges and pass to the next operator.
        :return: None
        """
        self.do_run()

    def do_run(self):

        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        tbl_size = self.index_mng.get_table_size()
        record_ranges = self.build_block_random_ranges(tbl_size)
        byte_ranges = self.index_mng.build_byte_ranges(record_ranges)

        if len(self.consumers) > 0:
            consumer_ranges = []
            for i in range(len(byte_ranges)):
                idx = i % len(self.consumers)

                if idx == len(consumer_ranges):
                    consumer_ranges.append([])

                consumer_ranges[idx].append(byte_ranges[i])

            for con_idx in range(min([len(consumer_ranges), len(self.consumers)])):
                self.send(DataFrameMessage(pd.DataFrame(consumer_ranges[con_idx])), [self.consumers[con_idx]])
                # con_rng_sql = TableRandomSampleGenerator.build_s3_query(consumer_ranges[con_idx])
                # self.send(StringMessage(con_rng_sql), [self.consumers[con_idx]])

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

    def build_random_ranges(self, tbl_size):
        """
        Randomize sample locations within the table
        :return: list of random position within the table size
        """
        rng_start = [randint(0, tbl_size) for _ in range(int(math.ceil(self.sample_size / self.batch_size)))]
        return [(x, x + self.batch_size) for x in rng_start]

    def build_block_random_ranges(self, tbl_size):
        """
        Randomize sample locations within the table each batch from a different block
        :return: list of random position within the table size
        """
        num_batches = int(math.ceil((self.sample_size * 1.0) / self.batch_size))
        block_size = tbl_size / num_batches
        num_blocks = int(math.ceil((1.0 * tbl_size) / block_size))

        ranges = []

        for batch_index in range(num_batches):
            block_range_start = (batch_index % num_blocks) * block_size
            block_range_end = block_range_start + block_size
            if block_range_end > tbl_size:
                block_range_end = tbl_size
            ranges.append(randint(block_range_start, block_range_end - self.batch_size))

        return [(x, x + self.batch_size) for x in ranges]

    @staticmethod
    def build_s3_query(ranges):
        sql_filter = 'CAST(serial AS INT) IN ({})'.format(','.join([str(x) for x in list(sum(ranges, ()))]))
        return 'select * from S3Object where {};'.format(sql_filter)
