"""
Creates and reads from local table index
"""
import numpy as np
import pandas as pd

from s3filter.op.message import DataFrameMessage, TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.plan.op_metrics import OpMetrics
from s3filter.sql.sample_index.s3_index_builder import *

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class ByteRangeBuilder(Operator):
    """Generates byte ranges for an s3 table random sample to be fetched by range table scan.
    """

    def __init__(self, sample_size, batch_size, name, query_plan, log_enabled):
        """Creates a random sample of a s3 table
        :param s3key: The object key to select against
        :param sample_size: the sample size
        :param batch_size: the number of consecutive records to retrieve per byte range in order to reduce the number
        of sampling http requests
        """

        super(ByteRangeBuilder, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.sample_size = sample_size

        self.batch_size = batch_size if batch_size <= (BLOCK_SIZE / 2) else BLOCK_SIZE / 2

        self.byte_ranges_dict = {}

    def on_receive(self, messages, producer_name):
        for msg in messages:
            if type(msg) == DataFrameMessage:
                self.__on_receive_dataframe(msg.dataframe, producer_name)
            elif type(msg) == TupleMessage:
                self.__on_receive_tuple(msg.tuple_)
            else:
                raise Exception("Unrecognized message {}".format(msg))

    def __on_receive_tuple(self, tuple_, producer_name):
        pass

    def __on_receive_dataframe(self, df, producer_name):
        if len(df) > 0:
            df = df.astype(np.int)
            for row in df.values:
                self.byte_ranges_dict[row[0]] = row[1:]

    def complete(self):
        byte_ranges = self.reduce_byte_ranges()
        consumers_count = len(self.consumers)
        consumer_ranges = [list() for _ in range(consumers_count)]

        for i in range(len(byte_ranges)):
            ci = i % consumers_count
            consumer_ranges[ci].append(byte_ranges[i])

        for i in range(consumers_count):
            df = pd.DataFrame(consumer_ranges[i])
            self.send(DataFrameMessage(df), [self.consumers[i]])

        super(ByteRangeBuilder, self).complete()
        self.op_metrics.timer_stop()

    def reduce_byte_ranges(self):
        byte_ranges = []
        record_indices = self.byte_ranges_dict.keys()
        while len(self.byte_ranges_dict) > 0:
            first_record_index = int(record_indices[0])

            if (first_record_index + self.batch_size) in record_indices:
                second_record_index = first_record_index + self.batch_size
            elif (first_record_index - self.batch_size) in record_indices:
                second_record_index = first_record_index
                first_record_index = second_record_index - self.batch_size
            else:
                self.byte_ranges_dict.pop(first_record_index, None)
                continue

            first_record_range = self.byte_ranges_dict.pop(first_record_index)
            record_indices.remove(first_record_index)
            second_record_range = self.byte_ranges_dict.pop(second_record_index)
            record_indices.remove(second_record_index)

            byte_ranges.append([first_record_range[0], second_record_range[1] - 1])

        return byte_ranges
