# -*- coding: utf-8 -*-
"""Join support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, HashTableMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd


class HashJoinBuildMetrics(OpMetrics):
    """Extra metrics for a HashBuild

    """

    def __init__(self):
        super(HashJoinBuildMetrics, self).__init__()

        self.rows_processed = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_processed': self.rows_processed
        }.__repr__()


class HashJoinBuild(Operator):
    """Builds a hash table

    """

    def __init__(self, key, name,  query_plan, log_enabled):
        """
        Creates a new join operator.

        :param key: The key to hash on
        """

        super(HashJoinBuild, self).__init__(name, HashJoinBuildMetrics(), query_plan, log_enabled)

        self.key = key

        self.field_names_index = None

        self.producers_received = {}

        self.hashtable = {}

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received messages
        :param producer_name: The producer of the tuple
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                for t in m.values.tolist():
                    self.on_receive_tuple(t, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, _producer_name):

        if not self.field_names_index:
            self.field_names_index = IndexedTuple.build_field_names_index(tuple_)
            self.send(TupleMessage(tuple_), self.consumers)
            self.producers_received[_producer_name] = True
        else:

            if _producer_name not in self.producers_received.keys():
                # Will be field names, skip
                self.producers_received[_producer_name] = True
            else:
                self.op_metrics.rows_processed += 1
                it = IndexedTuple(tuple_, self.field_names_index)
                itd = self.hashtable.setdefault(it[self.key], [])
                itd.append(tuple_)

            # self.hashtable[it[self.key]] = tuple_

    def on_producer_completed(self, producer_name):

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            if self.log_enabled:
             print("{}{}".format(self, self.hashtable))

            self.send(HashTableMessage(self.hashtable), self.consumers)
            Operator.on_producer_completed(self, producer_name)
