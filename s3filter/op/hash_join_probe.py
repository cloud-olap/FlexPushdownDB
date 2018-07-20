# -*- coding: utf-8 -*-
"""Join support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, HashTableMessage
from s3filter.op.tuple import Tuple, IndexedTuple


class HashJoinProbeMetrics(OpMetrics):
    """Extra metrics for a HashJoinProbe

    """

    def __init__(self):
        super(HashJoinProbeMetrics, self).__init__()

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


class HashJoinProbe(Operator):
    """Performs the join

    """

    def __init__(self, join_expr, name, query_plan, log_enabled):
        """
        Creates a new join operator.

        :param key: The key to hash on
        """

        super(HashJoinProbe, self).__init__(name, HashJoinProbeMetrics(), query_plan, log_enabled)

        self.join_expr = join_expr

        self.field_names_index = None

        self.build_producers = {}
        self.tuple_producer_name = None

        self.build_field_names = None
        self.tuple_field_names = None

        self.build_producer_completions = {}
        self.tuple_producer_completed = False

        self.hashtable = {}
        self.tuples = []

    def connect_build_producer(self, producer):
        """Connects a producer as the producer of left tuples in the join expression

        :param producer: The left producer
        :return: None
        """

        # if self.l_producer_name is not None:
        #     raise Exception("Only 1 left producer can be added. Left producer '{}' already added"
        #                     .format(self.l_producer_name))

        if producer.name is self.tuple_producer_name:
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

        if self.tuple_producer_name is not None:
            raise Exception("Only 1 right Producer can be added. Right producer '{}' already added"
                            .format(self.tuple_producer_name))

        if producer.name in self.build_producers:
            raise Exception("Producer cannot be added as both right and left producer. "
                            "Producer '{}' already added as left producer"
                            .format(producer.name))

        self.tuple_producer_name = producer.name
        producer.connect(self)

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param m: The received message
        :param producer_name: The producer of the tuple
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is HashTableMessage:
                self.on_receive_hashtable(m.hashtable, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, producer_name):

        # Check the producer is connected
        if self.build_producers is []:
            raise Exception("Left producers are not connected")

        if self.tuple_producer_name is None:
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

        elif producer_name == self.tuple_producer_name:

            if self.tuple_field_names is None:
                if self.join_expr.r_field in tuple_:
                    self.tuple_field_names = tuple_
                else:
                    raise Exception("Join Operator '{}' received invalid right field names tuple {}. "
                                    "Tuple must contain join right field name '{}'."
                                    .format(self.name, tuple_, self.join_expr.r_field))
            else:

                self.op_metrics.r_rows_processed += 1

                self.tuples.append(tuple_)

        else:
            raise Exception(
                "Join Operator '{}' received invalid tuple {} from producer '{}'. "
                "Tuple must be sent from connected left producer '{}' or right producer '{}'."
                    .format(self.name, tuple_, producer_name, self.build_producers, self.tuple_producer_name))

    def on_receive_hashtable(self, hashtable, producer_name):

        self.hashtable.update(hashtable)
        self.op_metrics.l_rows_processed = len(hashtable)

    def on_producer_completed(self, producer_name):

        if producer_name in self.build_producers.keys():
            self.build_producer_completions[producer_name] = True
        elif producer_name == self.tuple_producer_name:
            self.tuple_producer_completed = True
        else:
            raise Exception("Unrecognized producer {} has completed".format(producer_name))

        # Check that we have received a completed event from all the producers
        is_all_producers_done = all(self.build_producer_completions.values()) & self.tuple_producer_completed

        if is_all_producers_done and not self.is_completed():
            self.join_field_names()
            self.join_field_values()

        Operator.on_producer_completed(self, producer_name)

    def join_field_values(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """
        # # Determine which direction the hash join should run
        # # The larger relation should remain as a list and the smaller relation should be hashed. If either of the
        # # relations are empty then just return
        # if len(self.l_tuples) == 0 or len(self.r_tuples) == 0:
        #     return
        # elif len(self.l_tuples) > len(self.r_tuples):
        #     l_to_r = True
        #     # r_to_l = not l_to_r
        # else:
        #     l_to_r = False
        #     # r_to_l = not l_to_r

        # if l_to_r:
        #     outer_tuples_list = self.l_tuples
        #     inner_tuples_list = self.r_tuples
        #     inner_tuple_field_name = self.join_expr.r_field
        #     inner_tuple_field_names = self.r_field_names
        #     outer_tuple_field_index = self.l_field_names.index(self.join_expr.l_field)
        # else:
        #     outer_tuples_list = self.r_tuples
        #     inner_tuples_list = self.l_tuples
        #     inner_tuple_field_name = self.join_expr.l_field
        #     inner_tuple_field_names = self.l_field_names
        outer_tuple_field_index = self.tuple_field_names.index(self.join_expr.r_field)
        #
        # # Hash the tuples from the smaller set of tuples
        # inner_tuples_dict = {}
        # for t in inner_tuples_list:
        #     it = IndexedTuple.build(t, inner_tuple_field_names)
        #     itd = inner_tuples_dict.setdefault(it[inner_tuple_field_name], [])
        #     itd.append(t)

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
                        print("{}('{}') | Sending field values [{}]".format(
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

        # We can only emit field name tuples if we received tuples for both sides of the join
        if self.build_field_names is not None and self.tuple_field_names is not None:

            for field_name in self.build_field_names:
                joined_field_names.append(field_name)

            for field_name in self.tuple_field_names:
                joined_field_names.append(field_name)

            if self.log_enabled:
                print("{}('{}') | Sending field names [{}]".format(
                    self.__class__.__name__,
                    self.name,
                    {'field_names': joined_field_names}))

            self.send(TupleMessage(Tuple(joined_field_names)), self.consumers)

