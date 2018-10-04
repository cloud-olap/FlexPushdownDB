# -*- coding: utf-8 -*-
"""Join support

"""
import time

from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, HashTableMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd


class HashJoinProbeMetrics(OpMetrics):
    """Extra metrics for a HashJoinProbe

    """

    def __init__(self):
        super(HashJoinProbeMetrics, self).__init__()

        self.build_rows_processed = 0
        self.tuple_rows_processed = 0
        self.rows_joined = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'build_rows_processed': self.build_rows_processed,
            'tuple_rows_processed': self.tuple_rows_processed,
            'rows_joined': self.rows_joined
        }.__repr__()


class HashJoinProbe(Operator):
    """Performs the join

    """

    def __init__(self, join_expr, name, query_plan, log_enabled):
        """
        Creates a new join operator.
        """

        super(HashJoinProbe, self).__init__(name, HashJoinProbeMetrics(), query_plan, log_enabled)

        self.join_expr = join_expr

        self.field_names_index = None

        self.build_producers = {}
        self.tuple_producers = {}

        self.build_field_names = None
        self.tuple_field_names = None

        self.build_producer_completions = {}
        self.tuple_producer_completions = {}
        # self.tuple_producer_completed = False

        self.hashtable = {}
        self.tuples = []

        self.hashtable_df = None
        self.tuples_df = None

        self.do_join = False
        self.field_names_joined = False

    def connect_build_producer(self, producer):
        """Connects a producer as the producer of left tuples in the join expression

        :param producer: The left producer
        :return: None
        """

        # if self.l_producer_name is not None:
        #     raise Exception("Only 1 left producer can be added. Left producer '{}' already added"
        #                     .format(self.l_producer_name))

        if producer.name is self.tuple_producers:
            raise Exception("Producer cannot be added as both left and right producer. "
                            "Producer '{}' already added as right producer"
                            .format(producer.name))

        self.build_producers[producer.name] = producer
        self.build_producer_completions[producer.name] = False
        producer.connect(self)

    def connect_tuple_producer(self, producer):
        """Connects a producer as the producer of right tuples in the join expression

        :param producer: The right producer
        :return: None
        """

        # if self.tuple_producer_name is not None:
        #     raise Exception("Only 1 right Producer can be added. Right producer '{}' already added"
        #                     .format(self.tuple_producer_name))

        if producer.name in self.build_producers:
            raise Exception("Producer cannot be added as both right and left producer. "
                            "Producer '{}' already added as left producer"
                            .format(producer.name))

        self.tuple_producers[producer.name] = producer.name
        self.tuple_producer_completions[producer.name] = False
        producer.connect(self)

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received messages
        :param producer_name: The producer of the tuple
        :return: None
        """

        # for m in ms:
        m = ms
        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_, producer_name)
        elif isinstance(m, HashTableMessage):
            self.on_receive_hashtable(m.dataframe, producer_name)
        elif isinstance(m, DataFrameMessage):
            self.on_receive_dataframe(m.dataframe, producer_name)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_dataframe(self, df, _producer_name):

        if self.tuples_df is None:
            self.tuples_df = pd.DataFrame()

        self.op_metrics.tuple_rows_processed += len(df)

        self.tuples_df = self.tuples_df.append(df, ignore_index=True, sort=False)

        if self.do_join:
            # if self.log_enabled:
            #     print("{}('{}') | All build producers complete, performing join".format(
            #         self.__class__.__name__,
            #         self.name))

            if self.hashtable_df is not None:
                if not self.field_names_joined:
                    self.join_field_names()
                    self.field_names_joined = True
                self.join_field_values_pd()
            elif self.hashtable is not None:
                if not self.field_names_joined:
                    self.join_field_names()
                    self.field_names_joined = True
                self.join_field_values()
            else:
                raise Exception("All build producers done but have not received a hashtable")

        else:
            # if self.log_enabled:
            #     print("{}('{}') | Not all build producers complete, delaying join".format(
            #         self.__class__.__name__,
            #         self.name))
            pass

    def on_receive_tuple(self, tuple_, producer_name):

        # Check the producer is connected
        if self.build_producers is []:
            raise Exception("Left producers are not connected")

        if self.tuple_producers is []:
            raise Exception("Right producer is not connected")

        # Check which producer sent the tuple
        if producer_name in self.build_producers:

            if self.build_field_names is None:
                if self.join_expr.l_field in tuple_:
                    self.build_field_names = tuple_
                    self.field_names_index = IndexedTuple.build_field_names_index(tuple_)
                else:
                    raise Exception("Join Operator '{}' received invalid left field names tuple {}. "
                                    "Tuple must contain join left field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.l_field))

        elif producer_name in self.tuple_producers:

            if self.tuple_field_names is None:
                if self.join_expr.r_field in tuple_:
                    self.tuple_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid right field names tuple {}. "
                                    "Tuple must contain join right field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.r_field))
            else:

                self.op_metrics.tuple_rows_processed += 1

                self.tuples.append(tuple_)

        else:
            raise Exception(
                "Join Operator '{}' received invalid tuple {} from producer '{}'. "
                "Tuple must be sent from connected left producer '{}' or right producer '{}'."
                    .format(self.name, tuple_, producer_name, self.build_producers, self.tuple_producers))

    def on_receive_hashtable(self, hashtable, _producer_name):

        if type(hashtable) is pd.DataFrame:
            if self.hashtable_df is None:
                self.hashtable_df = pd.DataFrame()
            self.hashtable_df = self.hashtable_df.append(hashtable, sort=False)
        else:
            self.hashtable.update(hashtable)

        self.op_metrics.build_rows_processed = len(hashtable)

    def on_producer_completed(self, producer_name):

        if producer_name in self.build_producers.keys():
            self.build_producer_completions[producer_name] = True
        elif producer_name in self.tuple_producers.keys():
            self.tuple_producer_completions[producer_name] = True
        else:
            raise Exception("Unrecognized producer {} has completed".format(producer_name))

        # Check that we have received a completed event from all the producers
        is_all_build_producers_done = all(self.build_producer_completions.values())

        is_all_producers_done = is_all_build_producers_done and \
                                all(self.tuple_producer_completions.values())

        if is_all_build_producers_done:
            # if self.log_enabled:
            #     print("{} | {}('{}') | All build producers complete, enabling join".format(
            #         time.time(),
            #         self.__class__.__name__,
            #         self.name))

            # Need to build index here rather than build job
            self.hashtable_df.set_index(self.join_expr.l_field, inplace=True, drop=False)

            self.do_join = True

        if is_all_producers_done and not self.is_completed():

            if self.hashtable_df is not None:
                if not self.field_names_joined:
                    self.join_field_names()
                    self.field_names_joined = True
                self.join_field_values_pd()
            elif self.hashtable is not None:
                if not self.field_names_joined:
                    self.join_field_names()
                    self.field_names_joined = True
                self.join_field_values()
            else:
                raise Exception("All producers done but have not received a hashtable")

            self.hashtable_df = None
            self.tuples_df = None

        Operator.on_producer_completed(self, producer_name)

    def join_field_values_pd(self):

        # self.tuples_df = self.tuples_df.set_index(self.join_expr.r_field)
        if len(self.tuples_df) > 0:

            # Peform the join - loop over the right table rows, and use the hashtable for the left table rows
            df = self.hashtable_df.merge(self.tuples_df, how='inner', left_index=True, right_index=False, right_on=self.join_expr.r_field, copy=False)
            df = df.reset_index(drop=True) # Discard the index

            self.op_metrics.rows_joined += len(df)

            # if self.log_enabled:
            #     print("{} | {}('{}') | Sending field values {}".format(
            #         time.time(),
            #         self.__class__.__name__,
            #         self.name,
            #         {'data': df}))

            self.send(df, self.consumers, self)

            self.tuples_df = pd.DataFrame()

    def join_field_values(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """

        # Check that we actually got tuple field names to join on, we may not have as producers may not have produced
        # any
        if self.tuple_field_names is not None:

            outer_tuple_field_index = self.tuple_field_names.index(self.join_expr.r_field)

            for outer_tuple in self.tuples:

                if self.is_completed():
                    break

                outer_tuple_field_value = outer_tuple[outer_tuple_field_index]
                inner_tuples = self.hashtable.get(outer_tuple_field_value, None)

                # if self.log_enabled:
                #     print("{}('{}') | Joining Outer: {} Inner: {}".format(
                #         self.__class__.__name__,
                #         self.name,
                #         outer_tuple,
                #         inner_tuples))

                if inner_tuples is not None:

                    for inner_tuple in inner_tuples:

                        # if l_to_r:
                        #     t = outer_tuple + inner_tuple
                        # else:
                        t = inner_tuple + outer_tuple

                        if self.log_enabled:
                            print("{} | {}('{}') | Sending field values [{}]".format(
                                time.time(),
                                self.__class__.__name__,
                                self.name,
                                {'data': t}))

                        self.op_metrics.rows_joined += 1

                        self.send(TupleMessage(Tuple(t)), self.consumers)

    def join_field_names(self):
        """Examines the collected field names and joins them into a single list, left field names followed by right
        field names. The joined field names tuple is then sent.

        :return: None
        """

        joined_field_names = []

        # We can only emit field name tuples if we
        # received tuples for both sides of the join,
        #  we may not always get them
        # as some reads may return an empty record set
        if self.build_field_names is not None and self.tuple_field_names is not None:

            for field_name in self.build_field_names:
                joined_field_names.append(field_name)

            for field_name in self.tuple_field_names:
                joined_field_names.append(field_name)

            if self.log_enabled:
                print("{} | {}('{}') | Sending field names [{}]".format(
                    time.time(),
                    self.__class__.__name__,
                    self.name,
                    {'field_names': joined_field_names}))

            self.send(TupleMessage(Tuple(joined_field_names)), self.consumers, self)
