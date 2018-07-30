# -*- coding: utf-8 -*-
"""Join support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd

class HashJoinMetrics(OpMetrics):
    """Extra metrics for a project

    """

    def __init__(self):
        super(HashJoinMetrics, self).__init__()

        self.l_rows_processed = 0
        self.r_rows_processed = 0
        self.rows_joined = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'l_rows_processed': self.l_rows_processed,
            'r_rows_processed': self.r_rows_processed,
            'rows_joined': self.rows_joined
        }.__repr__()


class HashJoin(Operator):
    """Implements a inner join using hash tables.

    """

    def __init__(self, join_expr, name,  query_plan, log_enabled):
        """
        Creates a new join operator.

        :param join_expr: The join expression indicating which fields of which key to join on
        """

        super(HashJoin, self).__init__(name, HashJoinMetrics(),  query_plan, log_enabled)

        self.join_expr = join_expr

        self.l_tuples = []
        self.r_tuples = []

        self.l_field_names = None
        self.r_field_names = None

        self.l_producer_name = None
        self.r_producer_name = None

        self.l_producer_completed = False
        self.r_producer_completed = False

    def connect_left_producer(self, producer):
        """Connects a producer as the producer of left tuples in the join expression

        :param producer: The left producer
        :return: None
        """

        if self.l_producer_name is not None:
            raise Exception("Only 1 left producer can be added. Left producer '{}' already added"
                            .format(self.l_producer_name))

        if producer.name is self.r_producer_name:
            raise Exception("Producer cannot be added as both left and right producer. "
                            "Producer '{}' already added as right producer"
                            .format(self.l_producer_name))

        self.l_producer_name = producer.name
        producer.connect(self)

    def connect_right_producer(self, producer):
        """Connects a producer as the producer of right tuples in the join expression

        :param producer: The right producer
        :return: None
        """

        if self.r_producer_name is not None:
            raise Exception("Only 1 right Producer can be added. Right producer '{}' already added"
                            .format(self.r_producer_name))

        if producer.name is self.l_producer_name:
            raise Exception("Producer cannot be added as both right and left producer. "
                            "Producer '{}' already added as left producer"
                            .format(self.l_producer_name))

        self.r_producer_name = producer.name
        producer.connect(self)

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
                tuples = m.values.tolist()
                for t in tuples:
                    self.on_receive_tuple(t, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, producer_name):
        """Check that the tuple has been produced by a connected producer, and contains a field in the
        join expression. If it is from a producer we haven't seen before store its data as field names, otherwise add
        it as a values tuple.

        :param tuple_: The received tuple
        :param producer_name: The producer of the tuple
        :return: None
        """

        # if "l_producer_name" not in self.__dict__:
        #     pass
        #
        # print(self.__dict__)
        # print(self.__class__)
        # print(self.l_producer_name)

        # Check the producer is connected
        if self.l_producer_name is None:
            raise Exception("Left producer is not connected")

        if self.r_producer_name is None:
            raise Exception("Right producer is not connected")

        # Check which producer sent the tuple
        if producer_name == self.l_producer_name:

            if self.l_field_names is None:
                if self.join_expr.l_field in tuple_:
                    self.l_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid left field names tuple {}. "
                                    "Tuple must contain join left field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.l_field))
            else:

                self.op_metrics.l_rows_processed += 1

                self.l_tuples.append(tuple_)

        elif producer_name == self.r_producer_name:

            if self.r_field_names is None:
                if self.join_expr.r_field in tuple_:
                    self.r_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid right field names tuple {}. "
                                    "Tuple must contain join right field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.r_field))
            else:

                self.op_metrics.r_rows_processed += 1

                self.r_tuples.append(tuple_)

        else:
            raise Exception(
                "Join Operator '{}' received invalid tuple {} from producer '{}'. "
                "Tuple must be sent from connected left producer '{}' or right producer '{}'."
                .format(self.name, tuple_, producer_name, self.l_producer_name, self.r_producer_name))

    def on_producer_completed(self, producer_name):
        """Handles the event where a producer has completed producing all the tuples it will produce. Note that the
        Join operator may have multiple producers. Once all producers are complete the operator can send the tuples
        it contains to downstream consumers.

        :type producer_name: The producer that has completed
        :return: None
        """

        if producer_name == self.l_producer_name:
            self.l_producer_completed = True
        elif producer_name == self.r_producer_name:
            self.r_producer_completed = True
        else:
            raise Exception("Unrecognized producer {} has completed".format(producer_name))

        # Check that we have received a completed event from all the producers
        is_all_producers_done = self.l_producer_completed & self.r_producer_completed

        if self.log_enabled:
            print("{}('{}') | Producer completed [{}]".format(
                self.__class__.__name__,
                self.name,
                {'completed_producer': producer_name, 'all_producers_completed': is_all_producers_done}))

        if is_all_producers_done and not self.is_completed():

            # Join and send the field names first
            self.join_field_names()

            # Join and send the joined data tuples
            self.join_field_values()

            # del self.__l_tuples
            # del self.__r_tuples

        Operator.on_producer_completed(self, producer_name)

    def join_field_values(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """

        # Determine which direction the hash join should run
        # The larger relation should remain as a list and the smaller relation should be hashed. If either of the
        # relations are empty then just return
        if len(self.l_tuples) == 0 or len(self.r_tuples) == 0:
            return
        elif len(self.l_tuples) > len(self.r_tuples):
            l_to_r = True
            # r_to_l = not l_to_r
        else:
            l_to_r = False
            # r_to_l = not l_to_r

        if l_to_r:
            outer_tuples_list = self.l_tuples
            inner_tuples_list = self.r_tuples
            inner_tuple_field_name = self.join_expr.r_field
            inner_tuple_field_names = self.r_field_names
            outer_tuple_field_index = self.l_field_names.index(self.join_expr.l_field)
        else:
            outer_tuples_list = self.r_tuples
            inner_tuples_list = self.l_tuples
            inner_tuple_field_name = self.join_expr.l_field
            inner_tuple_field_names = self.l_field_names
            outer_tuple_field_index = self.r_field_names.index(self.join_expr.r_field)

        # Hash the tuples from the smaller set of tuples
        inner_tuples_dict = {}
        for t in inner_tuples_list:
            it = IndexedTuple.build(t, inner_tuple_field_names)
            itd = inner_tuples_dict.setdefault(it[inner_tuple_field_name], [])
            itd.append(t)

        for outer_tuple in outer_tuples_list:

            if self.is_completed():
                break

            outer_tuple_field_value = outer_tuple[outer_tuple_field_index]
            inner_tuples = inner_tuples_dict.get(outer_tuple_field_value, None)

            # if self.log_enabled:
            #     print("{}('{}') | Joining Outer: {} Inner: {}".format(
            #         self.__class__.__name__,
            #         self.name,
            #         outer_tuple,
            #         inner_tuples))

            if inner_tuples is not None:

                for inner_tuple in inner_tuples:

                    if l_to_r:
                        t = outer_tuple + inner_tuple
                    else:
                        t = inner_tuple + outer_tuple

                    # if self.log_enabled:
                    #     print("{}('{}') | Sending field values [{}]".format(
                    #         self.__class__.__name__,
                    #         self.name,
                    #         {'data': t}))

                    self.op_metrics.rows_joined += 1

                    self.send(TupleMessage(Tuple(t)), self.consumers)

    def join_field_names(self):
        """Examines the collected field names and joins them into a single list, left field names followed by right
        field names. The joined field names tuple is then sent.

        :return: None
        """

        joined_field_names = []

        # We can only emit field name tuples if we received tuples for both sides of the join
        if self.l_field_names is not None and self.r_field_names is not None:

            for field_name in self.l_field_names:
                joined_field_names.append(field_name)

            for field_name in self.r_field_names:
                joined_field_names.append(field_name)

            if self.log_enabled:
                print("{}('{}') | Sending field names [{}]".format(
                    self.__class__.__name__,
                    self.name,
                    {'field_names': joined_field_names}))

            self.send(TupleMessage(Tuple(joined_field_names)), self.consumers)
